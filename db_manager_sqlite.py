"""
SQLite 기반 통합 DB 관리자 모듈

🔧 주요 기능:
- SQLite 연결 풀 관리 (PostgreSQL db_manager.py → SQLite 전환)
- 자동 연결 상태 헬스체크
- 연결 재사용으로 성능 최적화
- 메모리 사용량 모니터링
- 안전한 쿼리 실행
- 트레이딩 관련 DB 작업

📈 SQLite 최적화 특징:
- WAL 모드 활성화 (동시 읽기/쓰기 성능)
- Custom Connection Pool (SQLite 특성 반영)
- 트랜잭션 자동 관리
- 파일 기반 로컬 DB
- Amazon Linux 호환성

🔄 PostgreSQL → SQLite 전환:
- psycopg2 → sqlite3
- ThreadedConnectionPool → Custom SQLite Pool
- PostgreSQL 타입 → SQLite 호환 타입
- 동일한 API 인터페이스 유지 (호환성)
"""

import sqlite3
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
from queue import Queue, Empty
import json

# 환경변수 로딩
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

# DB 설정을 lazy loading으로 처리하여 순환 참조 방지
def _load_db_config():
    """SQLite DB 설정을 지연 로딩합니다."""
    try:
        # config 모듈이 있을 경우 사용, 없을 경우 환경변수 직접 사용
        try:
            from config import SQLITE_CONFIG, DB_POOL_CONFIG, MEMORY_LIMITS
            return SQLITE_CONFIG, DB_POOL_CONFIG, MEMORY_LIMITS
        except ImportError:
            # config 모듈을 import할 수 없는 경우 환경변수 직접 사용
            logger.warning("config 모듈을 찾을 수 없어 환경변수를 직접 사용합니다.")

            # SQLite 설정 (로컬 파일 기반)
            sqlite_config = {
                'database': os.getenv('SQLITE_DATABASE', './makenaide_local.db'),
                'timeout': float(os.getenv('SQLITE_TIMEOUT', '30.0')),
                'check_same_thread': False,  # 멀티스레드 지원
                'isolation_level': None,  # 자동 커밋 모드
                'wal_mode': True,  # WAL 모드 활성화
                'journal_size_limit': 100 * 1024 * 1024,  # 100MB
                'cache_size': -64000,  # 64MB 캐시
                'temp_store': 'memory',  # 임시 테이블 메모리 저장
                'synchronous': 'NORMAL',  # 성능과 안정성 균형
                'foreign_keys': True,  # 외래키 제약 활성화
                'auto_vacuum': 'INCREMENTAL'  # 증분 자동 정리
            }

            # 연결 풀 설정 (SQLite 용으로 조정)
            db_pool_config = {
                'minconn': 1,  # SQLite는 단일 writer이므로 최소 연결
                'maxconn': 5,  # 읽기 전용 연결 몇 개 추가
                'connection_timeout': 30,
                'idle_timeout': 300,
                'max_retries': 3,
                'retry_delay': 1.0,
                'pool_size': 10,  # Queue 크기
                'health_check_interval': 60  # 헬스체크 간격
            }

            memory_limits = {
                'MAX_TOTAL_MEMORY_MB': 1024,  # EC2 환경에 맞게 조정
                'MAX_SINGLE_PROCESS_MB': 256,
                'DETAIL_ISSUES_LIMIT': 1000,
                'INDICATOR_MEMORY_THRESHOLD': 50
            }

            return sqlite_config, db_pool_config, memory_limits
    except Exception as e:
        logger.error(f"DB 설정 로딩 실패: {e}")
        raise

class SQLiteConnectionPool:
    """
    SQLite 전용 연결 풀 클래스

    SQLite의 특성을 고려한 커스텀 연결 풀:
    - 단일 writer, 다중 reader 지원
    - WAL 모드로 동시성 향상
    - 연결 재사용으로 성능 최적화
    """

    def __init__(self, database_path: str, pool_size: int = 10, sqlite_config: dict = None):
        self.database_path = database_path
        self.pool_size = pool_size
        self.sqlite_config = sqlite_config or {}
        self._pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created_connections = 0
        self.minconn = self.sqlite_config.get('minconn', 1)
        self.maxconn = pool_size

        # 초기 연결 생성
        self._initialize_pool()

    def _initialize_pool(self):
        """연결 풀 초기화"""
        try:
            # 디렉토리 생성 (없는 경우)
            db_dir = os.path.dirname(self.database_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                logger.info(f"📁 DB 디렉토리 생성: {db_dir}")

            # 최소 연결 수만큼 미리 생성
            for _ in range(self.minconn):
                conn = self._create_connection()
                self._pool.put(conn)
                self._created_connections += 1

            logger.info(f"✅ SQLite 연결 풀 초기화 완료: {self.minconn}~{self.maxconn} 연결 (DB: {self.database_path})")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 풀 초기화 실패: {e}")
            raise

    def _create_connection(self) -> sqlite3.Connection:
        """새로운 SQLite 연결 생성"""
        try:
            # SQLite 연결 생성
            conn = sqlite3.connect(
                self.database_path,
                timeout=self.sqlite_config.get('timeout', 30.0),
                check_same_thread=self.sqlite_config.get('check_same_thread', False),
                isolation_level=self.sqlite_config.get('isolation_level', None)
            )

            # Row factory 설정 (dict-like 접근)
            conn.row_factory = sqlite3.Row

            # SQLite 최적화 PRAGMA 설정
            cursor = conn.cursor()

            # WAL 모드 활성화 (동시 읽기/쓰기 성능 향상)
            if self.sqlite_config.get('wal_mode', True):
                cursor.execute("PRAGMA journal_mode=WAL")

            # 성능 최적화 설정
            cursor.execute(f"PRAGMA cache_size={self.sqlite_config.get('cache_size', -64000)}")
            cursor.execute(f"PRAGMA temp_store={self.sqlite_config.get('temp_store', 'memory')}")
            cursor.execute(f"PRAGMA synchronous={self.sqlite_config.get('synchronous', 'NORMAL')}")
            cursor.execute(f"PRAGMA journal_size_limit={self.sqlite_config.get('journal_size_limit', 100*1024*1024)}")

            # 외래키 제약 활성화
            if self.sqlite_config.get('foreign_keys', True):
                cursor.execute("PRAGMA foreign_keys=ON")

            # 자동 정리 설정
            if self.sqlite_config.get('auto_vacuum'):
                cursor.execute(f"PRAGMA auto_vacuum={self.sqlite_config['auto_vacuum']}")

            cursor.close()

            logger.debug(f"🔗 새 SQLite 연결 생성 완료: {self.database_path}")
            return conn

        except Exception as e:
            logger.error(f"❌ SQLite 연결 생성 실패: {e}")
            raise

    def getconn(self) -> sqlite3.Connection:
        """연결 풀에서 연결 획득"""
        try:
            # 기존 연결이 있으면 재사용
            try:
                conn = self._pool.get_nowait()

                # 연결 상태 확인
                if self._is_connection_valid(conn):
                    return conn
                else:
                    # 유효하지 않은 연결은 닫고 새로 생성
                    conn.close()

            except Empty:
                # 풀이 비어있으면 새 연결 생성
                pass

            # 새 연결 생성 (최대 연결 수 제한)
            with self._lock:
                if self._created_connections < self.maxconn:
                    conn = self._create_connection()
                    self._created_connections += 1
                    return conn
                else:
                    # 최대 연결 수 도달, 기존 연결이 반환될 때까지 대기
                    conn = self._pool.get(timeout=self.sqlite_config.get('timeout', 30.0))
                    if self._is_connection_valid(conn):
                        return conn
                    else:
                        conn.close()
                        raise Exception("유효하지 않은 연결이 반환됨")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 획득 실패: {e}")
            raise

    def putconn(self, conn: sqlite3.Connection):
        """연결을 풀로 반환"""
        try:
            if conn and self._is_connection_valid(conn):
                # 트랜잭션 상태 확인 및 정리
                if conn.in_transaction:
                    conn.commit()

                # 풀에 다시 넣기 (풀이 가득 찬 경우 연결 닫기)
                try:
                    self._pool.put_nowait(conn)
                except:
                    # 풀이 가득 찬 경우 연결 닫기
                    conn.close()
                    with self._lock:
                        self._created_connections -= 1
            else:
                # 유효하지 않은 연결은 닫기
                if conn:
                    conn.close()
                with self._lock:
                    self._created_connections -= 1

        except Exception as e:
            logger.warning(f"⚠️ SQLite 연결 반환 중 오류: {e}")
            if conn:
                conn.close()

    def _is_connection_valid(self, conn: sqlite3.Connection) -> bool:
        """연결 유효성 검사"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False

    def closeall(self):
        """모든 연결 닫기"""
        try:
            # 풀의 모든 연결 닫기
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except Empty:
                    break
                except Exception as e:
                    logger.warning(f"⚠️ 연결 닫기 중 오류: {e}")

            with self._lock:
                self._created_connections = 0

            logger.info("✅ SQLite 연결 풀 모든 연결 닫기 완료")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 풀 닫기 중 오류: {e}")

class DBManager:
    """
    SQLite 기반 통합 DB 관리자 클래스

    ✅ 주요 기능:
    - SQLite 연결 풀 자동 관리 (싱글톤 패턴)
    - 헬스체크 및 자동 복구
    - 메모리 사용량 추적
    - 성능 통계 수집
    - 안전한 쿼리 실행
    - 트레이딩 관련 DB 작업

    🔄 PostgreSQL → SQLite 전환:
    - 동일한 API 인터페이스 유지
    - SQLite 최적화 적용
    - Amazon Linux 호환성
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
            self.sqlite_config = config.get('sqlite_config')
            self.db_pool_config = config.get('db_pool_config')
            self.memory_limits = config.get('memory_limits')
        else:
            self.sqlite_config, self.db_pool_config, self.memory_limits = _load_db_config()

        # 연결 풀 초기화
        self.connection_pool = None
        self.pool_stats = {
            'total_connections': 0,
            'active_connections': 0,
            'failed_connections': 0,
            'total_queries': 0,
            'pool_hits': 0,
            'pool_misses': 0,
            'memory_usage_mb': 0
        }
        self._health_check_thread = None
        self._shutdown_flag = False

        # 연결 풀 초기화
        self._initialize_pool()

    def _initialize_pool(self):
        """SQLite 연결 풀 초기화"""
        max_retries = self.db_pool_config.get('max_retries', 3)
        retry_delay = self.db_pool_config.get('retry_delay', 1.0)

        for attempt in range(max_retries + 1):
            try:
                logger.info(f"🔄 SQLite 연결 풀 초기화 시도 {attempt + 1}/{max_retries + 1}")

                # SQLite 연결 풀 생성
                self.connection_pool = SQLiteConnectionPool(
                    database_path=self.sqlite_config['database'],
                    pool_size=self.db_pool_config['maxconn'],
                    sqlite_config=self.sqlite_config
                )

                # 연결 테스트
                test_conn = self.connection_pool.getconn()
                cursor = test_conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                self.connection_pool.putconn(test_conn)

                if result and result[0] == 1:
                    self.pool_stats['total_connections'] = self.db_pool_config['maxconn']
                    logger.info(f"✅ SQLite 연결 풀 초기화 완료: {self.db_pool_config['minconn']}~{self.db_pool_config['maxconn']} 연결")

                    # 헬스체크 스레드 시작
                    self._start_health_check()
                    return
                else:
                    raise Exception("SQLite 연결 테스트 실패")

            except Exception as e:
                logger.error(f"❌ SQLite 연결 풀 초기화 시도 {attempt + 1} 실패: {e}")

                if attempt < max_retries:
                    logger.info(f"⏳ {retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 지수 백오프
                else:
                    logger.error(f"❌ SQLite 연결 풀 초기화 최종 실패 (모든 재시도 소진)")
                    raise

    def _start_health_check(self):
        """헬스체크 스레드 시작"""
        if self._health_check_thread is None or not self._health_check_thread.is_alive():
            self._health_check_thread = threading.Thread(
                target=self._health_check_worker,
                daemon=True
            )
            self._health_check_thread.start()
            logger.info("🏥 SQLite 연결 풀 헬스체크 스레드 시작")

    def _health_check_worker(self):
        """헬스체크 워커 스레드"""
        health_check_interval = self.db_pool_config.get('health_check_interval', 60)

        while not self._shutdown_flag:
            try:
                time.sleep(health_check_interval)
                self._perform_health_check()
                self._monitor_memory_usage()
                self._cleanup_database()

            except Exception as e:
                logger.warning(f"⚠️ 헬스체크 중 오류: {e}")

    def _perform_health_check(self):
        """SQLite 연결 풀 헬스체크 수행"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()

            if result and result[0] == 1:
                logger.debug("✅ SQLite 연결 풀 헬스체크 통과")
            else:
                raise Exception("헬스체크 쿼리 결과 이상")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 풀 헬스체크 실패: {e}")
            self._attempt_pool_recovery()

    def _monitor_memory_usage(self):
        """메모리 사용량 모니터링"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            if memory_mb > self.memory_limits['MAX_SINGLE_PROCESS_MB']:
                logger.warning(f"🧠 SQLite 메모리 사용량 경고: {memory_mb:.1f}MB")

            # 통계 업데이트
            self.pool_stats['memory_usage_mb'] = memory_mb

        except Exception as e:
            logger.debug(f"메모리 모니터링 오류: {e}")

    def _cleanup_database(self):
        """SQLite 데이터베이스 정리 작업"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()

                # WAL 체크포인트 (주기적으로 WAL 파일을 메인 DB로 병합)
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")

                # 증분 자동 정리 (설정된 경우)
                if self.sqlite_config.get('auto_vacuum') == 'INCREMENTAL':
                    cursor.execute("PRAGMA incremental_vacuum")

                # 통계 정보 업데이트
                cursor.execute("ANALYZE")

                cursor.close()
                logger.debug("🧹 SQLite 데이터베이스 정리 작업 완료")

        except Exception as e:
            logger.debug(f"데이터베이스 정리 작업 오류: {e}")

    def _attempt_pool_recovery(self):
        """연결 풀 복구 시도"""
        try:
            logger.info("🔄 SQLite 연결 풀 복구 시도...")

            # 기존 연결 풀 종료
            if self.connection_pool:
                self.connection_pool.closeall()

            # 새 연결 풀 생성
            self._initialize_pool()
            logger.info("✅ SQLite 연결 풀 복구 성공")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 풀 복구 실패: {e}")
            self.pool_stats['failed_connections'] += 1

    @contextmanager
    def get_connection_context(self):
        """
        SQLite 연결 풀에서 연결을 가져오는 컨텍스트 매니저

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
                raise Exception("SQLite 연결 풀에서 연결을 획득할 수 없습니다")

            self.pool_stats['pool_hits'] += 1
            self.pool_stats['active_connections'] += 1

            yield conn

        except sqlite3.Error as e:
            logger.error(f"❌ SQLite 연결 오류: {e}")
            if conn and not conn.in_transaction:
                conn.rollback()
            self.pool_stats['failed_connections'] += 1
            raise

        except Exception as e:
            logger.error(f"❌ 예상치 못한 연결 오류: {e}")
            if conn and not conn.in_transaction:
                conn.rollback()
            self.pool_stats['failed_connections'] += 1
            raise

        finally:
            # 연결을 풀로 반환
            if conn:
                try:
                    # 진행 중인 트랜잭션 확인 및 커밋
                    if conn.in_transaction:
                        conn.commit()

                    self.connection_pool.putconn(conn)

                except Exception as e:
                    logger.warning(f"⚠️ SQLite 연결 반환 중 오류: {e}")
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
        안전한 쿼리 실행 메서드 (SQLite 연결 풀 사용)

        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
            fetchone: True면 fetchone(), False면 fetchall() 사용

        Returns:
            쿼리 결과 또는 None
        """
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())

                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchone() if fetchone else cursor.fetchall()
                    cursor.close()
                    return result
                else:
                    # INSERT, UPDATE, DELETE의 경우
                    result = cursor.rowcount
                    cursor.close()
                    return result

        except Exception as e:
            logger.error(f"❌ SQLite 쿼리 실행 실패: {str(e)}")
            logger.error(f"  Query: {query}")
            logger.error(f"  Params: {params}")
            raise

    def execute_query(self, query, params=None, fetchone=False):
        """기존 호환성을 위한 execute_query 메서드"""
        return self.execute_query_safe(query, params, fetchone)

    def health_check(self) -> Dict[str, Any]:
        """
        SQLite DB 연결 상태 확인

        Returns:
            Dict: 헬스체크 결과
        """
        result = {
            'status': 'unknown',
            'connection_pool': False,
            'database_accessible': False,
            'database_file_exists': False,
            'database_size_mb': 0,
            'response_time_ms': None,
            'error': None
        }

        start_time = time.time()

        try:
            # 데이터베이스 파일 존재 확인
            if os.path.exists(self.sqlite_config['database']):
                result['database_file_exists'] = True

                # 파일 크기 확인
                file_size = os.path.getsize(self.sqlite_config['database'])
                result['database_size_mb'] = round(file_size / 1024 / 1024, 2)

            # 연결 풀 상태 확인
            if self.connection_pool:
                result['connection_pool'] = True

                # DB 접근 테스트
                with self.get_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    test_result = cursor.fetchone()
                    cursor.close()

                    if test_result and test_result[0] == 1:
                        result['database_accessible'] = True
                        result['status'] = 'healthy'

            response_time = (time.time() - start_time) * 1000
            result['response_time_ms'] = round(response_time, 2)

        except Exception as e:
            result['status'] = 'unhealthy'
            result['error'] = str(e)
            logger.error(f"❌ SQLite 헬스체크 실패: {e}")

        return result

    def get_pool_stats(self) -> Dict[str, Any]:
        """연결 풀 통계 반환"""
        stats = self.pool_stats.copy()

        if self.connection_pool:
            # 실시간 연결 상태 추가
            stats['available_connections'] = self.connection_pool._pool.qsize()
            stats['pool_size'] = self.connection_pool.maxconn
            stats['min_connections'] = self.connection_pool.minconn
            stats['created_connections'] = self.connection_pool._created_connections

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
                logger.info("✅ SQLite 연결 풀 종료 완료")

        except Exception as e:
            logger.error(f"❌ SQLite 연결 풀 종료 중 오류: {e}")

    def save_trade_record(self, ticker: str, order_type: str, quantity: float, price: float,
                          order_id: Union[str, None], status: str, error_message: Union[str, None] = None,
                          gpt_confidence: Union[float, None] = None, gpt_summary: Union[str, None] = None):
        """
        거래 기록과 함께 GPT 분석 결과를 SQLite DB에 저장합니다.
        'trade_history' 테이블에 gpt_confidence, gpt_summary 컬럼이 있어야 합니다.
        """
        try:
            query = """
                INSERT INTO trade_history
                (ticker, trade_datetime, order_type, quantity, price, order_id, status, error_message, gpt_confidence, gpt_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            # SQLite는 ? placeholder 사용
            params = (ticker, datetime.now().isoformat(), order_type, quantity, price, order_id, status, error_message, gpt_confidence, gpt_summary)
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
            now = datetime.now().isoformat()

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
                    VALUES (?, ?, ?, ?, ?)
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

    def fetch_ohlcv(self, ticker: str, days: int = 450) -> 'pd.DataFrame':
        """
        SQLite DB에서 지정된 ticker의 OHLCV 데이터를 최근 `days` 일 동안 조회합니다.
        """
        # SQLite에서는 INTERVAL 대신 datetime 함수 사용
        query = """
            SELECT date, open, high, low, close, volume
            FROM ohlcv
            WHERE ticker = ?
              AND date >= date('now', '-{} days')
            ORDER BY date
        """.format(days)

        try:
            with self.get_connection_context() as conn:
                df = pd.read_sql_query(query, conn, params=(ticker,))

                # 날짜 컬럼을 인덱스로 설정하여 1970-01-01 문제 해결
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
        OHLCV 데이터를 SQLite 데이터베이스에 삽입합니다.
        """
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                for date, row in df.iterrows():
                    # SQLite에서는 INSERT OR IGNORE 사용 (ON CONFLICT 대신)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO ohlcv (ticker, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (ticker, date.strftime('%Y-%m-%d'), row['open'], row['high'], row['low'], row['close'], row['volume'])
                    )
                cursor.close()
                # 컨텍스트 매니저가 자동으로 커밋 처리
            logger.info(f"✅ {ticker} OHLCV 데이터 {len(df)}건 SQLite DB에 삽입 완료")
        except Exception as e:
            logger.error(f"❌ {ticker} OHLCV SQLite DB 삽입 중 오류: {e}")

    def close(self):
        """Close SQLite database connection pool."""
        self.close_pool()

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
        sqlite_config, _, _ = _load_db_config()
        conn = sqlite3.connect(
            sqlite_config['database'],
            timeout=sqlite_config.get('timeout', 30.0),
            check_same_thread=sqlite_config.get('check_same_thread', False)
        )
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"❌ 직접 SQLite 연결 실패: {e}")
        raise

if __name__ == '__main__':
    # SQLite DB 매니저 테스트
    print("🔗 SQLite 연결 풀 테스트 시작")

    try:
        db_manager = DBManager()

        # 연결 테스트
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version()")
            result = cursor.fetchone()
            cursor.close()
            print(f"✅ SQLite 연결 성공: SQLite {result[0]}")

        # 헬스체크 테스트
        health = db_manager.health_check()
        print(f"🏥 헬스체크 결과: {health}")

        # 풀 통계 출력
        stats = db_manager.get_pool_stats()
        print(f"📊 연결 풀 통계: {stats}")

    except Exception as e:
        print(f"❌ SQLite 연결 풀 테스트 실패: {e}")

    finally:
        if 'db_manager' in locals():
            db_manager.close_pool()