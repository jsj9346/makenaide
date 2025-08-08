#!/usr/bin/env python3
"""
Lambda Scanner Core - Phase 2 아키텍처 개선
Makenaide 봇의 티커 스캐닝 및 관리 기능을 독립적인 Lambda 함수로 분리

주요 기능:
1. Upbit API 티커 스캐닝
2. 신규 티커 감지 및 DB 업데이트
3. 블랙리스트 필터링 및 동기화
4. 티커 상태 관리 (is_active)

Author: Phase 2 Architecture Migration
Version: 1.0.0
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
    BLACKLIST_FILE = "blacklist.json"  # 블랙리스트 파일명 (S3에서 로드 예정)

class DatabaseManager:
    """Lambda 환경용 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def get_connection(self):
        """DB 연결 획득"""
        if self.connection is None:
            import psycopg2
            try:
                self.connection = psycopg2.connect(**self.config)
                self.connection.autocommit = False  # 트랜잭션 제어
                logger.info("✅ PostgreSQL 연결 성공")
            except Exception as e:
                logger.error(f"❌ DB 연결 실패: {e}")
                raise
        return self.connection
    
    def execute_query(self, query: str, params: tuple = None, fetchone: bool = False, fetchall: bool = False):
        """쿼리 실행 (트랜잭션 지원)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            
            if fetchone:
                return cursor.fetchone()
            elif fetchall:
                return cursor.fetchall()
            else:
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection:
            self.connection.rollback()
    
    def close(self):
        """연결 종료"""
        if self.connection:
            self.connection.close()
            self.connection = None

class BlacklistManager:
    """블랙리스트 관리 클래스"""
    
    def __init__(self):
        self.blacklist = {}
        
    def load_blacklist(self) -> Dict[str, Any]:
        """
        블랙리스트 로드 (우선순위: S3 → 로컬)
        TODO: S3 통합 시 S3에서 로드하도록 수정
        """
        try:
            # 현재는 빈 블랙리스트 반환 (기본값)
            # 추후 S3 bucket에서 blacklist.json 로드하도록 확장
            logger.info("📋 블랙리스트 로드 중...")
            
            # 임시 하드코딩된 블랙리스트 (예시)
            self.blacklist = {
                # "KRW-EXAMPLE": {
                #     "reason": "Low volume",
                #     "added": "2025-08-04T00:00:00Z"
                # }
            }
            
            logger.info(f"✅ 블랙리스트 로드 완료: {len(self.blacklist)}개 티커")
            return self.blacklist
            
        except Exception as e:
            logger.error(f"❌ 블랙리스트 로드 실패: {e}")
            return {}
    
    def is_blacklisted(self, ticker: str) -> bool:
        """티커가 블랙리스트에 있는지 확인"""
        return ticker in self.blacklist

class UpbitAPIManager:
    """Upbit API 관리 클래스"""
    
    def __init__(self):
        self.api_call_count = 0
        
    def get_available_tickers(self) -> List[str]:
        """현재 거래 가능한 KRW 티커 목록 조회"""
        try:
            import pyupbit
            
            logger.info("🔍 Upbit 티커 목록 조회 중...")
            tickers = pyupbit.get_tickers(fiat="KRW")
            self.api_call_count += 1
            
            if not tickers:
                logger.error("❌ Upbit API에서 티커 목록을 가져올 수 없습니다")
                return []
                
            logger.info(f"✅ Upbit 티커 조회 완료: {len(tickers)}개")
            return tickers
            
        except Exception as e:
            logger.error(f"❌ Upbit API 호출 실패: {e}")
            return []

class TickerValidator:
    """티커 유효성 검증 클래스"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def get_existing_tickers(self) -> Dict[str, datetime]:
        """DB에서 기존 티커 및 업데이트 시간 조회"""
        try:
            results = self.db.execute_query(
                "SELECT ticker, updated_at FROM tickers",
                fetchall=True
            )
            
            if results:
                ticker_times = {row[0]: row[1] for row in results}
                logger.info(f"📊 기존 티커 조회: {len(ticker_times)}개")
                return ticker_times
            else:
                logger.info("📊 기존 티커 없음")
                return {}
                
        except Exception as e:
            logger.error(f"❌ 기존 티커 조회 실패: {e}")
            return {}
    
    def needs_update(self, ticker: str, last_update: datetime) -> bool:
        """티커가 업데이트가 필요한지 확인"""
        if last_update is None:
            return True
            
        threshold = datetime.now() - timedelta(hours=ScannerConfig.UPDATE_THRESHOLD_HOURS)
        return last_update < threshold

class LambdaScanner:
    """Lambda Scanner 메인 클래스"""
    
    def __init__(self):
        self.db = DatabaseManager(ScannerConfig.DB_CONFIG)
        self.blacklist_manager = BlacklistManager()
        self.upbit_api = UpbitAPIManager()
        self.validator = TickerValidator(self.db)
        
    def process_scan_request(self, event: dict) -> dict:
        """스캔 요청 처리"""
        try:
            start_time = time.time()
            
            # 요청 파라미터 파싱
            operation_type = event.get('operation_type', 'update_tickers')
            force_update = event.get('force_update', False)
            sync_blacklist = event.get('sync_blacklist', False)
            
            logger.info(f"🚀 스캐너 작업 시작: {operation_type}")
            
            results = {}
            
            if operation_type == 'update_tickers':
                results = self._update_tickers(force_update)
            elif operation_type == 'sync_blacklist':
                results = self._sync_blacklist()
            elif operation_type == 'full_scan':
                # 티커 업데이트 + 블랙리스트 동기화
                ticker_results = self._update_tickers(force_update)
                blacklist_results = self._sync_blacklist()
                results = {
                    'ticker_update': ticker_results,
                    'blacklist_sync': blacklist_results
                }
            else:
                raise ValueError(f"지원하지 않는 작업 타입: {operation_type}")
            
            elapsed = time.time() - start_time
            
            response = {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'operation_type': operation_type,
                    'execution_time': round(elapsed, 3),
                    'results': results,
                    'api_calls': self.upbit_api.api_call_count,
                    'timestamp': datetime.now().isoformat(),
                    'lambda_version': 'SCANNER_v1.0'
                }
            }
            
            logger.info(f"✅ 스캐너 작업 완료: {elapsed:.3f}초")
            return response
            
        except Exception as e:
            logger.error(f"❌ 스캐너 작업 실패: {e}")
            self.db.rollback()
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            }
        finally:
            self.db.close()
    
    def _update_tickers(self, force_update: bool = False) -> dict:
        """티커 업데이트 수행"""
        try:
            logger.info("📊 티커 업데이트 시작")
            
            # 1. 블랙리스트 로드
            blacklist = self.blacklist_manager.load_blacklist()
            
            # 2. Upbit 티커 조회
            current_tickers = self.upbit_api.get_available_tickers()
            if not current_tickers:
                return {'error': 'Upbit API 응답 없음'}
            
            # 3. 블랙리스트 필터링
            filtered_tickers = [ticker for ticker in current_tickers 
                             if not self.blacklist_manager.is_blacklisted(ticker)]
            
            blacklisted_count = len(current_tickers) - len(filtered_tickers)
            if blacklisted_count > 0:
                logger.info(f"⛔️ 블랙리스트 제외: {blacklisted_count}개 티커")
            
            # 4. 기존 티커 조회
            existing_tickers = self.validator.get_existing_tickers()
            
            # 5. 신규 티커 추가
            new_tickers = set(filtered_tickers) - set(existing_tickers.keys())
            added_count = 0
            
            for ticker in new_tickers:
                try:
                    self.db.execute_query(
                        "INSERT INTO tickers (ticker, created_at, is_active) VALUES (%s, CURRENT_TIMESTAMP, true)",
                        (ticker,)
                    )
                    added_count += 1
                except Exception as e:
                    logger.error(f"❌ {ticker} 추가 실패: {e}")
            
            if added_count > 0:
                logger.info(f"🎉 신규 티커 추가: {added_count}개")
            
            # 6. 기존 티커 업데이트 (필요시)
            updated_count = 0
            
            for ticker in filtered_tickers:
                if ticker in existing_tickers:
                    last_update = existing_tickers[ticker]
                    if force_update or self.validator.needs_update(ticker, last_update):
                        try:
                            self.db.execute_query(
                                "UPDATE tickers SET updated_at = CURRENT_TIMESTAMP WHERE ticker = %s",
                                (ticker,)
                            )
                            updated_count += 1
                        except Exception as e:
                            logger.error(f"❌ {ticker} 업데이트 실패: {e}")
            
            if updated_count > 0:
                logger.info(f"🔄 기존 티커 업데이트: {updated_count}개")
            
            # 7. 블랙리스트 티커 비활성화
            deactivated_count = 0
            blacklisted_in_db = set(existing_tickers.keys()) & set(blacklist.keys())
            
            if blacklisted_in_db:
                for ticker in blacklisted_in_db:
                    try:
                        self.db.execute_query(
                            "UPDATE tickers SET is_active = false WHERE ticker = %s",
                            (ticker,)
                        )
                        deactivated_count += 1
                    except Exception as e:
                        logger.error(f"❌ {ticker} 비활성화 실패: {e}")
                
                logger.info(f"🚫 블랙리스트 티커 비활성화: {deactivated_count}개")
            
            # 8. 트랜잭션 커밋
            self.db.commit()
            
            return {
                'total_upbit_tickers': len(current_tickers),
                'filtered_tickers': len(filtered_tickers),
                'blacklisted_count': blacklisted_count,
                'new_tickers_added': added_count,
                'existing_tickers_updated': updated_count,
                'deactivated_tickers': deactivated_count,
                'existing_tickers_count': len(existing_tickers)
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 티커 업데이트 실패: {e}")
            return {'error': str(e)}
    
    def _sync_blacklist(self) -> dict:
        """블랙리스트와 is_active 컬럼 동기화"""
        try:
            logger.info("🔄 블랙리스트 동기화 시작")
            
            # 1. 블랙리스트 로드
            blacklist = self.blacklist_manager.load_blacklist()
            
            # 2. is_active 컬럼 존재 확인
            column_check = self.db.execute_query(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'tickers' AND column_name = 'is_active'
                """,
                fetchall=True
            )
            
            if not column_check:
                return {'error': 'is_active 컬럼이 존재하지 않습니다. DB 스키마를 먼저 초기화하세요.'}
            
            # 3. 모든 티커를 활성화
            activated = self.db.execute_query("UPDATE tickers SET is_active = true")
            
            # 4. 블랙리스트 티커 비활성화
            deactivated = 0
            if blacklist:
                placeholders = ','.join(['%s'] * len(blacklist))
                deactivated = self.db.execute_query(
                    f"UPDATE tickers SET is_active = false WHERE ticker IN ({placeholders})",
                    tuple(blacklist.keys())
                )
            
            # 5. 결과 조회
            active_count = self.db.execute_query(
                "SELECT COUNT(*) FROM tickers WHERE is_active = true",
                fetchone=True
            )[0]
            
            inactive_count = self.db.execute_query(
                "SELECT COUNT(*) FROM tickers WHERE is_active = false", 
                fetchone=True
            )[0]
            
            # 6. 트랜잭션 커밋
            self.db.commit()
            
            logger.info(f"✅ 블랙리스트 동기화 완료 - 활성: {active_count}, 비활성: {inactive_count}")
            
            return {
                'blacklist_size': len(blacklist),
                'active_tickers': active_count,
                'inactive_tickers': inactive_count,
                'activated_count': activated,
                'deactivated_count': deactivated
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 블랙리스트 동기화 실패: {e}")
            return {'error': str(e)}

def lambda_handler(event, context):
    """Lambda 함수 진입점"""
    try:
        logger.info(f"📥 Lambda Scanner 요청 수신: {json.dumps(event, indent=2)}")
        
        # Scanner 초기화
        scanner = LambdaScanner()
        
        # 요청 처리
        result = scanner.process_scan_request(event)
        
        logger.info("📤 Lambda 응답 준비 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ Lambda 함수 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': f'Lambda 함수 실행 실패: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'operation_type': 'update_tickers',
        'force_update': False,
        'sync_blacklist': False
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))