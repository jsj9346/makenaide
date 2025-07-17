"""
통합 DB 관리자 모듈

🔧 주요 기능:
- PostgreSQL 연결 풀 관리 (기존 db_pool_manager.py 통합)
- 자동 연결 상태 헬스체크
- 연결 재사용으로 성능 최적화  
- 메모리 사용량 모니터링
- 안전한 쿼리 실행
- 트레이딩 관련 DB 작업

📈 통합된 기능:
- DatabaseConnectionPool 클래스 통합
- 컨텍스트 매니저 지원
- 성능 통계 수집
- 자동 복구 메커니즘
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor
import threading
import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, List, Union, Tuple, Any
import psutil
from datetime import datetime
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd

# 환경변수 로딩
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

# DB 설정을 lazy loading으로 처리하여 순환 참조 방지
def _load_db_config():
    """DB 설정을 지연 로딩합니다."""
    try:
        # config 모듈이 있을 경우 사용, 없을 경우 환경변수 직접 사용
        try:
            from config import DB_CONFIG, DB_POOL_CONFIG, MEMORY_LIMITS
            return DB_CONFIG, DB_POOL_CONFIG, MEMORY_LIMITS
        except ImportError:
            # config 모듈을 import할 수 없는 경우 환경변수 직접 사용
            logger.warning("config 모듈을 찾을 수 없어 환경변수를 직접 사용합니다.")
            
            db_config = {
                'host': os.getenv('PG_HOST', 'localhost'),
                'port': os.getenv('PG_PORT', '5432'),
                'dbname': os.getenv('PG_DATABASE', 'makenaide'),
                'user': os.getenv('PG_USER', 'postgres'),
                'password': os.getenv('PG_PASSWORD', '')
            }
            
            db_pool_config = {
                'minconn': 2,
                'maxconn': 10,
                'connection_timeout': 30,
                'idle_timeout': 300,
                'max_retries': 3,
                'retry_delay': 1.0
            }
            
            memory_limits = {
                'MAX_TOTAL_MEMORY_MB': 2048,
                'MAX_SINGLE_PROCESS_MB': 512,
                'DETAIL_ISSUES_LIMIT': 1000,
                'INDICATOR_MEMORY_THRESHOLD': 100
            }
            
            return db_config, db_pool_config, memory_limits
    except Exception as e:
        logger.error(f"DB 설정 로딩 실패: {e}")
        raise

class DBManager:
    """
    통합 DB 관리자 클래스
    
    ✅ 주요 기능:
    - 연결 풀 자동 관리 (싱글톤 패턴)
    - 헬스체크 및 자동 복구
    - 메모리 사용량 추적
    - 성능 통계 수집
    - 안전한 쿼리 실행
    - 트레이딩 관련 DB 작업
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config=None):
        """싱글톤 패턴 구현"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config=None):
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        
        # 설정 로딩
        if config:
            self.db_config = config.get('db_config')
            self.db_pool_config = config.get('db_pool_config')
            self.memory_limits = config.get('memory_limits')
        else:
            self.db_config, self.db_pool_config, self.memory_limits = _load_db_config()
        
        # 연결 풀 초기화
        self.connection_pool = None
        self.pool_stats = {
            'total_connections': 0,
            'active_connections': 0,
            'failed_connections': 0,
            'total_queries': 0,
            'pool_hits': 0,
            'pool_misses': 0
        }
        self._health_check_thread = None
        self._shutdown_flag = False
        
        # 연결 풀 초기화
        self._initialize_pool()
    
    def _initialize_pool(self):
        """연결 풀 초기화"""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.db_pool_config['minconn'],
                maxconn=self.db_pool_config['maxconn'],
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                connect_timeout=self.db_pool_config['connection_timeout']
            )
            
            self.pool_stats['total_connections'] = self.db_pool_config['maxconn']
            logger.info(f"✅ DB 연결 풀 초기화 완료: {self.db_pool_config['minconn']}~{self.db_pool_config['maxconn']} 연결")
            
            # 헬스체크 스레드 시작
            self._start_health_check()
            
        except Exception as e:
            logger.error(f"❌ DB 연결 풀 초기화 실패: {e}")
            raise
    
    def _start_health_check(self):
        """헬스체크 스레드 시작"""
        if self._health_check_thread is None or not self._health_check_thread.is_alive():
            self._health_check_thread = threading.Thread(
                target=self._health_check_worker,
                daemon=True
            )
            self._health_check_thread.start()
            logger.info("🏥 DB 연결 풀 헬스체크 스레드 시작")
    
    def _health_check_worker(self):
        """헬스체크 워커 스레드"""
        while not self._shutdown_flag:
            try:
                # 30초마다 헬스체크 수행
                time.sleep(30)
                self._perform_health_check()
                self._monitor_memory_usage()
                
            except Exception as e:
                logger.warning(f"⚠️ 헬스체크 중 오류: {e}")
    
    def _perform_health_check(self):
        """연결 풀 헬스체크 수행"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    
            if result and result[0] == 1:
                logger.debug("✅ DB 연결 풀 헬스체크 통과")
            else:
                raise Exception("헬스체크 쿼리 결과 이상")
                
        except Exception as e:
            logger.error(f"❌ DB 연결 풀 헬스체크 실패: {e}")
            self._attempt_pool_recovery()
    
    def _monitor_memory_usage(self):
        """메모리 사용량 모니터링"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > self.memory_limits['MAX_SINGLE_PROCESS_MB']:
                logger.warning(f"🧠 DB 풀 메모리 사용량 경고: {memory_mb:.1f}MB")
                
            # 통계 업데이트
            self.pool_stats['memory_usage_mb'] = memory_mb
            
        except Exception as e:
            logger.debug(f"메모리 모니터링 오류: {e}")
    
    def _attempt_pool_recovery(self):
        """연결 풀 복구 시도"""
        try:
            logger.info("🔄 DB 연결 풀 복구 시도...")
            
            # 기존 연결 풀 종료
            if self.connection_pool:
                self.connection_pool.closeall()
            
            # 새 연결 풀 생성
            self._initialize_pool()
            logger.info("✅ DB 연결 풀 복구 성공")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 풀 복구 실패: {e}")
            self.pool_stats['failed_connections'] += 1
    
    @contextmanager
    def get_connection_context(self):
        """
        연결 풀에서 연결을 가져오는 컨텍스트 매니저
        
        Usage:
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
        """
        conn = None
        start_time = time.time()
        
        try:
            # 연결 풀에서 연결 획득
            conn = self.connection_pool.getconn()
            
            if conn is None:
                self.pool_stats['pool_misses'] += 1
                raise Exception("연결 풀에서 연결을 획득할 수 없습니다")
            
            self.pool_stats['pool_hits'] += 1
            self.pool_stats['active_connections'] += 1
            
            # 연결 상태 확인
            if conn.closed:
                logger.warning("⚠️ 닫힌 연결 감지, 새 연결 요청")
                self.connection_pool.putconn(conn)
                conn = self.connection_pool.getconn()
            
            yield conn
            
        except psycopg2.Error as e:
            logger.error(f"❌ DB 연결 오류: {e}")
            if conn:
                conn.rollback()
            self.pool_stats['failed_connections'] += 1
            raise
            
        except Exception as e:
            logger.error(f"❌ 예상치 못한 연결 오류: {e}")
            if conn:
                conn.rollback()
            self.pool_stats['failed_connections'] += 1
            raise
            
        finally:
            # 연결을 풀로 반환
            if conn:
                try:
                    # 진행 중인 트랜잭션 확인 및 커밋
                    if not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                        conn.commit()
                        
                    self.connection_pool.putconn(conn)
                    
                except Exception as e:
                    logger.warning(f"⚠️ 연결 반환 중 오류: {e}")
                    # 문제가 있는 연결은 닫기
                    try:
                        conn.close()
                    except:
                        pass
                finally:
                    self.pool_stats['active_connections'] -= 1
            
            # 쿼리 통계 업데이트
            self.pool_stats['total_queries'] += 1
            query_time = time.time() - start_time
            
            if query_time > 5.0:  # 5초 이상 걸린 쿼리 로깅
                logger.warning(f"🐌 느린 쿼리 감지: {query_time:.2f}초")
    
    def get_connection(self):
        """기존 호환성을 위한 연결 획득 메서드"""
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        """기존 호환성을 위한 연결 반환 메서드"""
        if conn:
            self.connection_pool.putconn(conn)
    
    def execute_query_safe(self, query, params=None, fetchone=False):
        """
        안전한 쿼리 실행 메서드 (연결 풀 사용)
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
            fetchone: True면 fetchone(), False면 fetchall() 사용
            
        Returns:
            쿼리 결과 또는 None
        """
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    if query.strip().upper().startswith('SELECT'):
                        return cur.fetchone() if fetchone else cur.fetchall()
                    # INSERT, UPDATE, DELETE의 경우 자동 커밋됨
                    
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {str(e)}")
            raise
    
    def execute_query(self, query, params=None, fetchone=False):
        """기존 호환성을 위한 execute_query 메서드"""
        return self.execute_query_safe(query, params, fetchone)
    
    def health_check(self) -> Dict[str, Any]:
        """
        DB 연결 상태 확인
        
        Returns:
            Dict: 헬스체크 결과
        """
        result = {
            'status': 'unknown',
            'connection_pool': False,
            'database_accessible': False,
            'response_time_ms': None,
            'error': None
        }
        
        start_time = time.time()
        
        try:
            # 연결 풀 상태 확인
            if self.connection_pool:
                result['connection_pool'] = True
                
                # DB 접근 테스트
                with self.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        if cursor.fetchone()[0] == 1:
                            result['database_accessible'] = True
                            result['status'] = 'healthy'
                
            response_time = (time.time() - start_time) * 1000
            result['response_time_ms'] = round(response_time, 2)
            
        except Exception as e:
            result['status'] = 'unhealthy'
            result['error'] = str(e)
            logger.error(f"❌ DB 헬스체크 실패: {e}")
        
        return result
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """연결 풀 통계 반환"""
        stats = self.pool_stats.copy()
        
        if self.connection_pool:
            # 실시간 연결 상태 추가
            stats['available_connections'] = len(self.connection_pool._pool)
            stats['pool_size'] = self.connection_pool.maxconn
            stats['min_connections'] = self.connection_pool.minconn
        
        # 히트율 계산
        total_requests = stats['pool_hits'] + stats['pool_misses']
        if total_requests > 0:
            stats['hit_rate'] = (stats['pool_hits'] / total_requests) * 100
        else:
            stats['hit_rate'] = 0.0
        
        return stats
    
    def close_pool(self):
        """연결 풀 종료"""
        try:
            self._shutdown_flag = True
            
            if self.connection_pool:
                self.connection_pool.closeall()
                logger.info("✅ DB 연결 풀 종료 완료")
                
        except Exception as e:
            logger.error(f"❌ DB 연결 풀 종료 중 오류: {e}")

    def save_trade_record(self, ticker: str, order_type: str, quantity: float, price: float, 
                          order_id: Union[str, None], status: str, error_message: Union[str, None] = None,
                          gpt_confidence: Union[float, None] = None, gpt_summary: Union[str, None] = None):
        """
        거래 기록과 함께 GPT 분석 결과를 DB에 저장합니다.
        'trade_history' 테이블에 gpt_confidence, gpt_summary 컬럼이 추가되어 있어야 합니다.
        """
        try:
            query = """
                INSERT INTO trade_history
                (ticker, trade_datetime, order_type, quantity, price, order_id, status, error_message, gpt_confidence, gpt_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (ticker, datetime.now(), order_type, quantity, price, order_id, status, error_message, gpt_confidence, gpt_summary)
            self.execute_query_safe(query, params)
            logger.info(f"✅ {ticker} 거래 기록 저장 완료 (GPT Conf: {gpt_confidence}): {order_type} {quantity} @ {price} (status: {status})")
            return True
        except Exception as e:
            logger.error(f"❌ {ticker} 거래 기록 저장 실패: {str(e)}")
            return False

    def save_portfolio_history(self, balances):
        """
        업비트 API에서 가져온 계정 잔고 정보를 portfolio_history 테이블에 저장합니다.
        
        Args:
            balances (list): 업비트 API에서 반환받은 계정 잔고 정보 리스트
            
        Returns:
            bool: 저장 성공 여부
        """
        try:
            if not balances:
                logger.warning("보유 중인 자산이 없어 포트폴리오 히스토리 저장을 스킵합니다.")
                return True
                
            # 현재 시간 기록
            now = datetime.now()
            
            # 각 자산에 대해 레코드 추가
            for item in balances:
                currency = item.get('currency')
                balance = float(item.get('balance', 0))
                avg_price = float(item.get('avg_buy_price', 0))
                
                # 티커 형식 변환 (KRW 제외)
                ticker = f"KRW-{currency}" if currency != 'KRW' else 'KRW'
                
                # portfolio_history 테이블에 데이터 저장
                # 액션은 'HOLD'로 기록 (현재 보유 상태)
                self.execute_query_safe("""
                    INSERT INTO portfolio_history 
                    (ticker, action, qty, price, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    ticker,
                    'HOLD',
                    balance,
                    avg_price,
                    now
                ))
                
            logger.info(f"✅ 포트폴리오 히스토리 저장 완료 (총 {len(balances)}개 자산)")
            return True
            
        except Exception as e:
            logger.error(f"❌ 포트폴리오 히스토리 저장 중 오류 발생: {str(e)}")
            return False

    # UNUSED: 호출되지 않는 함수
    # def save_trend_analysis(self, ticker: str, market_phase: str, confidence: float, reason: str, pattern: str = None, time_window: str = '1d'):
    #     """Save GPT trend analysis result into trend_analysis table."""
    #     sql = (
    #         "INSERT INTO trend_analysis (ticker, action, type, reason, pattern, market_phase, confidence, time_window, created_at, updated_at) "
    #         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) "
    #         "ON CONFLICT (ticker) DO UPDATE SET "
    #         "action = EXCLUDED.action, type = EXCLUDED.type, reason = EXCLUDED.reason, pattern = EXCLUDED.pattern, "
    #         "market_phase = EXCLUDED.market_phase, confidence = EXCLUDED.confidence, time_window = EXCLUDED.time_window, updated_at = CURRENT_TIMESTAMP"
    #     )
    #         
    #     action_val = "BUY" if "Stage 2" in market_phase or "Stage1→Stage2" in market_phase else "HOLD"
    #     type_val = 'weitzwein_stage'
    # 
    #     params = (
    #         ticker,
    #         action_val, 
    #         type_val,
    #         reason,
    #         pattern,
    #         market_phase,
    #         confidence,
    #         time_window
    #     )
    #         
    #     try:
    #         self.execute_query_safe(sql, params)
    #         logger.info(f"✅ {ticker} 추세 분석 결과 DB 저장 완료")
    #         return True
    #     except Exception as e:
    #         logger.error(f"❌ {ticker} 추세 분석 결과 DB 저장 실패: {e}")
    #         return False

    def fetch_ohlcv(self, ticker: str, days: int = 450) -> 'pd.DataFrame':
        """
        Fetch OHLCV data for the given ticker from the DB over the last `days` days.
        """
        sql = (
            "SELECT date, open, high, low, close, volume "
            "FROM ohlcv "
            "WHERE ticker = %s "
            "  AND date >= CURRENT_DATE - INTERVAL '%s days' "
            "ORDER BY date"
        )
        try:
            with self.get_connection_context() as conn:
                df = pd.read_sql(sql, conn, params=(ticker, days))
                # 🔧 [중요] 날짜 컬럼을 인덱스로 설정하여 1970-01-01 문제 해결
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])  # 날짜 형식 보장
                    df.set_index('date', inplace=True)  # 날짜를 인덱스로 설정
                    logger.debug(f"✅ {ticker} OHLCV 데이터 조회 성공: {len(df)}개, 날짜 범위: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")
                return df
        except Exception as e:
            logger.error(f"❌ {ticker} OHLCV 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def insert_ohlcv(self, ticker: str, df: pd.DataFrame):
        """
        Insert OHLCV data into the database.
        """
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cur:
                    for date, row in df.iterrows():
                        cur.execute(
                            """
                            INSERT INTO ohlcv (ticker, date, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker, date) DO NOTHING
                            """,
                            (ticker, date, row['open'], row['high'], row['low'], row['close'], row['volume'])
                        )
                # 컨텍스트 매니저가 자동으로 커밋 처리
            logger.info(f"✅ {ticker} OHLCV 데이터 {len(df)}건 DB에 삽입 완료")
        except Exception as e:
            logger.error(f"❌ {ticker} OHLCV DB 삽입 중 오류: {e}")

    def close(self):
        """Close database connection pool."""
        self.close_pool()

    # ===========================================
    # Disclaimer 관련 메서드들
    # ===========================================
    
    # UNUSED: 호출되지 않는 함수
    # def check_disclaimer_agreement(self, version: str) -> bool:
    #     """특정 버전의 Disclaimer 동의 상태 확인"""
    #     try:
    #         with self.get_connection_context() as conn:
    #             with conn.cursor() as cursor:
    #                 cursor.execute("""
    #                     SELECT id FROM disclaimer_agreements 
    #                     WHERE is_active = TRUE AND agreement_version = %s
    #                     ORDER BY agreed_at DESC LIMIT 1
    #                 """, (version,))
    #                     
    #                 result = cursor.fetchone()
    #                 return result is not None
    #                     
    #     except Exception as e:
    #         logger.error(f"❌ Disclaimer 동의 상태 확인 실패: {e}")
    #         return False
    # 
    # def save_disclaimer_agreement(self, version: str, agreed_by: str = 'user', 
    #                             text_hash: str = None) -> bool:
    #     """Disclaimer 동의 저장"""
    #     try:
    #         with self.get_connection_context() as conn:
    #             with conn.cursor() as cursor:
    #                 cursor.execute("""
    #                     INSERT INTO disclaimer_agreements 
    #                     (agreement_version, agreed_by, agreement_text_hash, is_active)
    #                     VALUES (%s, %s, %s, %s)
    #                 """, (version, agreed_by, text_hash, True))
    #                 conn.commit()
    #                     
    #     logger.info(f"✅ Disclaimer 동의 저장 완료 (버전: {version})")
    #         return True
    #         
    #     except Exception as e:
    #         logger.error(f"❌ Disclaimer 동의 저장 실패: {e}")
    #         return False


# ===========================================
# 전역 인스턴스 및 호환성 함수들
# ===========================================

# 전역 DBManager 인스턴스 (싱글톤)
_db_manager_instance = None

def get_db_manager() -> DBManager:
    """전역 DBManager 인스턴스를 반환합니다."""
    global _db_manager_instance
    if _db_manager_instance is None:
        _db_manager_instance = DBManager()
    return _db_manager_instance

# 기존 호환성을 위한 함수들
def get_db_connection_context():
    """
    기존 get_db_connection_context() 함수와 호환되는 래퍼
    
    Usage:
        with get_db_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    return get_db_manager().get_connection_context()

def get_db_connection():
    """
    기존 get_db_connection() 함수와 호환되는 래퍼
    ⚠️ 주의: 이 함수는 연결 풀을 사용하지 않으므로 권장하지 않음
    가능하면 get_db_connection_context() 사용 권장
    """
    logger.warning("⚠️ get_db_connection() 사용 감지 - get_db_connection_context() 사용 권장")
    
    try:
        db_config, _, _ = _load_db_config()
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password']
        )
        return conn
    except Exception as e:
        logger.error(f"❌ 직접 DB 연결 실패: {e}")
        raise

if __name__ == '__main__':
    # DB 매니저 테스트
    print("🔗 DB 연결 풀 테스트 시작")
    
    try:
        db_manager = DBManager()
        
        # 연결 테스트
        with db_manager.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                result = cursor.fetchone()
                print(f"✅ DB 연결 성공: {result[0][:50]}...")
        
        # 헬스체크 테스트
        health = db_manager.health_check()
        print(f"🏥 헬스체크 결과: {health}")
        
        # 상태 출력
        # print_pool_status()  # UNUSED: 호출되지 않는 함수
        
    except Exception as e:
        print(f"❌ 연결 풀 테스트 실패: {e}")
    
    finally:
        if 'db_manager' in locals():
            db_manager.close_pool() 