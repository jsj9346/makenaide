#!/usr/bin/env python3
"""
Lambda Scanner with pg8000 - Phase 2 아키텍처 개선
Makenaide 봇의 티커 스캐닝 및 관리 기능을 독립적인 Lambda 함수로 분리
pg8000 Pure Python PostgreSQL 드라이버 사용
"""

import json
import logging
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# PostgreSQL 드라이버 로드
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
    logger.info("✅ psycopg2 사용 가능")
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.info("⚠️ psycopg2 사용 불가")

try:
    import pg8000.native as pg8000
    PG8000_AVAILABLE = True
    logger.info("✅ pg8000 사용 가능")
except ImportError:
    PG8000_AVAILABLE = False
    logger.info("⚠️ pg8000 사용 불가")

if not PSYCOPG2_AVAILABLE and not PG8000_AVAILABLE:
    logger.error("❌ PostgreSQL 드라이버 없음")

# Upbit API 라이브러리
try:
    import pyupbit
    PYUPBIT_AVAILABLE = True
    logger.info("✅ pyupbit 사용 가능")
except ImportError:
    PYUPBIT_AVAILABLE = False
    logger.error("❌ pyupbit 사용 불가")

class ScannerConfig:
    """Scanner Lambda 설정 클래스"""
    
    # DB 연결 설정
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
        'port': int(os.environ.get('DB_PORT', '5432')),
        'database': os.environ.get('DB_NAME', 'makenaide'),
        'user': os.environ.get('DB_USER', 'bruce'),
        'password': os.environ.get('DB_PASSWORD', '0asis314.')
    }
    
    # 스캔 설정
    UPDATE_THRESHOLD_HOURS = 24  # 티커 업데이트 주기 (시간)
    BLACKLIST_FILE = "blacklist.json"  # 블랙리스트 파일명

class DatabaseManager:
    """pg8000 기반 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """DB 연결"""
        try:
            if PSYCOPG2_AVAILABLE:
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
                )
                self.cursor = self.connection.cursor()
                self.connection.autocommit = False
                logger.info("✅ psycopg2로 DB 연결 성공")
            elif PG8000_AVAILABLE:
                self.connection = pg8000.Connection(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    ssl_context=True  # SSL 연결 활성화
                )
                logger.info("✅ pg8000으로 DB 연결 성공 (SSL 활성화)")
            else:
                raise Exception("PostgreSQL 드라이버가 없습니다")
                
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            raise
    
    def execute_query(self, query: str, params=None, fetchone=False, fetchall=False):
        """쿼리 실행"""
        try:
            if PSYCOPG2_AVAILABLE:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                
                if fetchone:
                    return self.cursor.fetchone()
                elif fetchall:
                    return self.cursor.fetchall()
                else:
                    return self.cursor.rowcount
                    
            elif PG8000_AVAILABLE:
                if fetchone:
                    result = self.connection.run(query, stream=params)
                    return result[0] if result else None
                elif fetchall:
                    return self.connection.run(query, stream=params)
                else:
                    self.connection.run(query, stream=params)
                    return 1
            else:
                return self._mock_execute(query, params, fetchone, fetchall)
                
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            self.rollback()
            raise
    
    def _mock_execute(self, query: str, params=None, fetchone=False, fetchall=False):
        """Mock 쿼리 실행 (테스트용)"""
        logger.info(f"📝 Mock Query: {query[:100]}...")
        
        if "SELECT COUNT(*)" in query:
            if fetchone:
                return [0]
        
        return 1
    
    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            if PSYCOPG2_AVAILABLE:
                self.connection.commit()
            logger.info("✅ 트랜잭션 커밋")
        else:
            logger.info("✅ Mock 트랜잭션 커밋")
    
    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection:
            if PSYCOPG2_AVAILABLE:
                self.connection.rollback()
            logger.info("🔄 트랜잭션 롤백")
        else:
            logger.info("🔄 Mock 트랜잭션 롤백")
    
    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔐 DB 연결 종료")

class BlacklistManager:
    """블랙리스트 관리"""
    
    def __init__(self, blacklist_data: Optional[List[str]] = None):
        self.blacklist = blacklist_data or []
        
    def load_blacklist(self) -> List[str]:
        """블랙리스트 로드 (기본값 제공)"""
        # 기본 블랙리스트 (테스트용)
        default_blacklist = [
            "KRW-BTC",  # 비트코인은 제외 (너무 큰 변동성)
            "KRW-ETH",  # 이더리움도 제외
        ]
        
        if self.blacklist:
            return self.blacklist
        else:
            logger.info(f"📋 기본 블랙리스트 사용: {len(default_blacklist)}개 항목")
            return default_blacklist
    
    def is_blacklisted(self, ticker: str) -> bool:
        """티커 블랙리스트 여부 확인"""
        blacklist = self.load_blacklist()
        return ticker in blacklist

class UpbitAPIManager:
    """Upbit API 관리 - Direct API 호출 버전"""
    
    def __init__(self):
        self.base_url = "https://api.upbit.com/v1"
        self.api_available = True  # 항상 사용 가능 (직접 API 호출)
        
    def get_available_tickers(self) -> List[str]:
        """사용 가능한 티커 목록 조회 - 직접 API 호출"""
        try:
            import requests
            import time
            
            url = f"{self.base_url}/market/all"
            time.sleep(0.1)  # API 제한 준수
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                markets = response.json()
                krw_tickers = [
                    market['market'] for market in markets 
                    if market['market'].startswith('KRW-')
                ]
                logger.info(f"📊 Upbit 직접 API 호출 성공: {len(krw_tickers)}개 티커")
                return krw_tickers
            else:
                logger.warning(f"⚠️ Upbit API 응답 오류: {response.status_code}")
                return self._get_fallback_tickers()
                
        except ImportError:
            logger.warning("⚠️ requests 모듈 없음 - fallback 티커 사용")
            return self._get_fallback_tickers()
        except Exception as e:
            logger.error(f"❌ Upbit API 호출 실패: {e}")
            return self._get_fallback_tickers()
    
    def _get_fallback_tickers(self) -> List[str]:
        """Fallback 티커 목록 (API 실패 시)"""
        logger.info("📝 Fallback 티커 목록 사용")
        return [
            "KRW-BTC", "KRW-ETH", "KRW-ADA", "KRW-XRP", "KRW-DOGE", 
            "KRW-SOL", "KRW-MATIC", "KRW-DOT", "KRW-AVAX", "KRW-ATOM", 
            "KRW-NEAR", "KRW-ALGO", "KRW-HBAR", "KRW-ICP", "KRW-AAVE",
            "KRW-UNI", "KRW-LINK", "KRW-CRO", "KRW-OKB", "KRW-SAND"
        ]

class TickerValidator:
    """티커 유효성 검증"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        
    def needs_update(self) -> bool:
        """티커 업데이트 필요 여부 확인"""
        try:
            query = """
            SELECT MAX(updated_at) FROM tickers 
            WHERE updated_at > NOW() - INTERVAL '%s hours'
            """
            result = self.db.execute_query(
                query, (ScannerConfig.UPDATE_THRESHOLD_HOURS,), fetchone=True
            )
            
            if result and result[0]:
                logger.info(f"⏰ 최근 업데이트: {result[0]}")
                return False
            else:
                logger.info("🔄 티커 업데이트 필요")
                return True
                
        except Exception as e:
            logger.error(f"❌ 업데이트 필요성 확인 실패: {e}")
            return True  # 에러 시 업데이트 수행

class LambdaScanner:
    """메인 Scanner Lambda 클래스"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db = DatabaseManager(ScannerConfig.DB_CONFIG)
        self.blacklist_manager = BlacklistManager()
        self.upbit_api = UpbitAPIManager()
        self.validator = TickerValidator(self.db)
        
    def update_tickers(self, force_update: bool = False) -> Dict[str, Any]:
        """티커 업데이트 수행"""
        try:
            logger.info("🚀 티커 업데이트 시작")
            
            # 업데이트 필요성 확인
            if not force_update and not self.validator.needs_update():
                return {
                    'action': 'update_tickers',
                    'status': 'skipped',
                    'message': '최근에 업데이트되어 건너뜀',
                    'timestamp': datetime.now().isoformat()
                }
            
            # 1. 블랙리스트 로드
            blacklist = self.blacklist_manager.load_blacklist()
            logger.info(f"📋 블랙리스트 로드: {len(blacklist)}개")
            
            # 2. Upbit 티커 조회
            current_tickers = self.upbit_api.get_available_tickers()
            logger.info(f"📊 Upbit 티커 조회: {len(current_tickers)}개")
            
            # 3. 블랙리스트 필터링
            filtered_tickers = [
                ticker for ticker in current_tickers 
                if not self.blacklist_manager.is_blacklisted(ticker)
            ]
            logger.info(f"✅ 필터링 후 티커: {len(filtered_tickers)}개")
            
            # 4. DB 업데이트
            new_count = self._insert_new_tickers(filtered_tickers)
            updated_count = self._update_existing_tickers(filtered_tickers)
            
            return {
                'action': 'update_tickers',
                'status': 'success',
                'new_tickers': new_count,
                'updated_tickers': updated_count,
                'total_active': len(filtered_tickers),
                'blacklisted': len(blacklist),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 티커 업데이트 실패: {e}")
            return {
                'action': 'update_tickers',
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _insert_new_tickers(self, tickers: List[str]) -> int:
        """신규 티커 삽입"""
        try:
            new_count = 0
            for ticker in tickers:
                query = """
                INSERT INTO tickers (ticker, created_at, updated_at, is_active)
                VALUES (%s, NOW(), NOW(), true)
                ON CONFLICT (ticker) DO NOTHING
                """
                rows_affected = self.db.execute_query(query, (ticker,))
                if rows_affected > 0:
                    new_count += 1
                    
            logger.info(f"✅ 신규 티커 추가: {new_count}개")
            return new_count
            
        except Exception as e:
            logger.error(f"❌ 신규 티커 삽입 실패: {e}")
            return 0
    
    def _update_existing_tickers(self, active_tickers: List[str]) -> int:
        """기존 티커 상태 업데이트"""
        try:
            # 활성 티커 업데이트
            if active_tickers:
                placeholders = ','.join(['%s'] * len(active_tickers))
                query = f"""
                UPDATE tickers 
                SET updated_at = NOW(), is_active = true
                WHERE ticker IN ({placeholders})
                """
                self.db.execute_query(query, tuple(active_tickers))
            
            # 비활성 티커 업데이트
            if active_tickers:
                query = f"""
                UPDATE tickers 
                SET is_active = false
                WHERE ticker NOT IN ({placeholders})
                """
                inactive_count = self.db.execute_query(query, tuple(active_tickers))
            else:
                query = "UPDATE tickers SET is_active = false"
                inactive_count = self.db.execute_query(query)
                
            logger.info(f"✅ 비활성 티커 업데이트: {inactive_count}개")
            return len(active_tickers)
            
        except Exception as e:
            logger.error(f"❌ 기존 티커 업데이트 실패: {e}")
            return 0
    
    def sync_blacklist(self) -> Dict[str, Any]:
        """블랙리스트 동기화"""
        try:
            logger.info("🔄 블랙리스트 동기화 시작")
            
            blacklist = self.blacklist_manager.load_blacklist()
            
            if blacklist:
                placeholders = ','.join(['%s'] * len(blacklist))
                query = f"""
                UPDATE tickers 
                SET is_active = false, updated_at = NOW()
                WHERE ticker IN ({placeholders})
                """
                updated_count = self.db.execute_query(query, tuple(blacklist))
            else:
                updated_count = 0
                
            return {
                'action': 'sync_blacklist',
                'status': 'success',
                'blacklisted_count': len(blacklist),
                'updated_count': updated_count,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 블랙리스트 동기화 실패: {e}")
            return {
                'action': 'sync_blacklist',
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def full_scan(self) -> Dict[str, Any]:
        """전체 스캔 (티커 업데이트 + 블랙리스트 동기화)"""
        try:
            logger.info("🔍 전체 스캔 시작")
            
            self.db.connect()
            
            # 1. 티커 업데이트
            update_result = self.update_tickers(force_update=True)
            
            # 2. 블랙리스트 동기화
            blacklist_result = self.sync_blacklist()
            
            # 3. 트랜잭션 커밋
            self.db.commit()
            
            return {
                'action': 'full_scan',
                'status': 'success',
                'update_result': update_result,
                'blacklist_result': blacklist_result,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 전체 스캔 실패: {e}")
            self.db.rollback()
            return {
                'action': 'full_scan',
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        finally:
            self.db.close()

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 Scanner Lambda 시작")
        
        # 작업 타입 결정
        action = event.get('action', 'full_scan')
        force_update = event.get('force_update', False)
        
        # Scanner 인스턴스 생성
        scanner = LambdaScanner(event)
        
        # 작업 실행
        if action == 'update_tickers':
            result = scanner.update_tickers(force_update)
        elif action == 'sync_blacklist':
            result = scanner.sync_blacklist()
        elif action == 'full_scan':
            result = scanner.full_scan()
        else:
            result = {
                'action': action,
                'status': 'error',
                'error': f'Unknown action: {action}',
                'timestamp': datetime.now().isoformat()
            }
        
        logger.info("✅ Scanner Lambda 완료")
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'result': result,
                'api_method': 'direct_requests',
                'pyupbit_available': PYUPBIT_AVAILABLE,
                'pg8000_available': PG8000_AVAILABLE,
                'psycopg2_available': PSYCOPG2_AVAILABLE,
                'version': 'PG8000_DIRECT_API_v1.1'
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Scanner Lambda 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e),
                'api_method': 'direct_requests',
                'pyupbit_available': PYUPBIT_AVAILABLE,
                'pg8000_available': PG8000_AVAILABLE,
                'psycopg2_available': PSYCOPG2_AVAILABLE,
                'timestamp': datetime.now().isoformat()
            }
        }

if __name__ == "__main__":
    # 로컬 테스트
    test_event = {'action': 'full_scan', 'force_update': True}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))