#!/usr/bin/env python3
"""
🔍 Phase 0: Ticker Scanner Lambda
- 서버리스 아키텍처의 첫 번째 단계
- 독립적으로 실행 가능한 최소 의존성 Lambda 함수
- Upbit API에서 KRW 마켓 티커를 조회하고 기본 필터링 적용
"""

import json
import os
import logging
import time
from datetime import datetime
from typing import Dict, List, Any
import boto3

# AWS 클라이언트 초기화 (지연 로딩)
s3_client = None
eventbridge_client = None

def get_s3_client():
    """S3 클라이언트 지연 로딩"""
    global s3_client
    if s3_client is None:
        s3_client = boto3.client('s3')
    return s3_client

def get_eventbridge_client():
    """EventBridge 클라이언트 지연 로딩"""
    global eventbridge_client
    if eventbridge_client is None:
        eventbridge_client = boto3.client('events')
    return eventbridge_client

def setup_logger():
    """Lambda용 경량 로거 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_logger()

class UpbitTickerScanner:
    """
    Upbit API를 이용한 티커 스캐너 클래스
    기존 scanner.py와 동일한 기능을 서버리스 환경에 최적화
    """
    
    def __init__(self):
        self.s3_bucket = os.getenv('S3_BUCKET', 'makenaide-serverless-data')
        
    def get_upbit_tickers(self) -> List[str]:
        """Upbit API에서 KRW 마켓 티커 목록 조회"""
        try:
            import pyupbit
            
            logger.info("🔍 Upbit API에서 KRW 티커 목록 조회 중...")
            tickers = pyupbit.get_tickers(fiat="KRW")
            
            if not tickers:
                logger.error("❌ Upbit API에서 티커 목록을 가져올 수 없습니다")
                return []
                
            logger.info(f"✅ Upbit API에서 {len(tickers)}개 티커 조회 완료")
            return tickers
            
        except Exception as e:
            logger.error(f"❌ Upbit API 호출 실패: {e}")
            return []
    
    def load_blacklist_from_s3(self) -> Dict[str, Any]:
        """S3에서 블랙리스트 조회 (없으면 빈 딕셔너리 반환)"""
        try:
            s3 = get_s3_client()
            response = s3.get_object(
                Bucket=self.s3_bucket,
                Key='config/blacklist.json'
            )
            blacklist = json.loads(response['Body'].read())
            logger.info(f"✅ S3에서 블랙리스트 로드: {len(blacklist)}개 항목")
            return blacklist
            
        except s3.exceptions.NoSuchKey:
            logger.info("ℹ️ S3에 블랙리스트 파일이 없습니다. 빈 블랙리스트 사용")
            return {}
        except Exception as e:
            logger.warning(f"⚠️ S3 블랙리스트 로드 실패: {e}. 빈 블랙리스트 사용")
            return {}
    
    def apply_blacklist_filter(self, tickers: List[str], blacklist: Dict[str, Any]) -> List[str]:
        """블랙리스트 필터링 적용"""
        if not blacklist:
            return tickers
            
        filtered_tickers = [ticker for ticker in tickers if ticker not in blacklist]
        excluded_count = len(tickers) - len(filtered_tickers)
        
        if excluded_count > 0:
            excluded_tickers = [t for t in tickers if t in blacklist]
            logger.info(f"⛔️ 블랙리스트 제외: {excluded_count}개 티커")
            logger.debug(f"제외된 티커: {excluded_tickers}")
        
        return filtered_tickers
    
    def update_db_tickers(self, filtered_tickers: List[str]) -> bool:
        """
        DB tickers 테이블 업데이트
        기존 scanner.py의 update_tickers() 함수와 동일한 로직
        """
        try:
            import psycopg2
            
            # DB 연결 정보
            conn_params = {
                'host': os.getenv("PG_HOST"),
                'port': os.getenv("PG_PORT", "5432"),
                'dbname': os.getenv("PG_DATABASE"),
                'user': os.getenv("PG_USER"),
                'password': os.getenv("PG_PASSWORD")
            }
            
            logger.info("🔗 PostgreSQL DB 연결 중...")
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            
            try:
                # 기존 티커 조회
                cursor.execute("SELECT ticker, updated_at FROM tickers")
                existing_rows = cursor.fetchall()
                existing_tickers = {row[0]: row[1] for row in existing_rows}
                
                # 새로운 티커 추가 및 기존 티커 업데이트
                current_time = datetime.now()
                new_tickers = []
                updated_tickers = []
                
                for ticker in filtered_tickers:
                    if ticker in existing_tickers:
                        # 기존 티커 업데이트
                        cursor.execute("""
                            UPDATE tickers 
                            SET updated_at = %s, is_active = true 
                            WHERE ticker = %s
                        """, (current_time, ticker))
                        updated_tickers.append(ticker)
                    else:
                        # 새로운 티커 추가
                        cursor.execute("""
                            INSERT INTO tickers (ticker, updated_at, is_active) 
                            VALUES (%s, %s, true)
                        """, (ticker, current_time))
                        new_tickers.append(ticker)
                
                # 비활성화할 티커 처리
                inactive_tickers = set(existing_tickers.keys()) - set(filtered_tickers)
                if inactive_tickers:
                    for ticker in inactive_tickers:
                        cursor.execute("""
                            UPDATE tickers 
                            SET is_active = false, updated_at = %s 
                            WHERE ticker = %s
                        """, (current_time, ticker))
                
                # 변경사항 커밋
                conn.commit()
                
                # 결과 로깅
                logger.info(f"✅ DB 티커 테이블 업데이트 완료:")
                logger.info(f"   - 새로 추가: {len(new_tickers)}개")
                logger.info(f"   - 업데이트: {len(updated_tickers)}개") 
                logger.info(f"   - 비활성화: {len(inactive_tickers)}개")
                
                return True
                
            finally:
                cursor.close()
                conn.close()
                logger.info("🔗 DB 연결 종료")
                
        except Exception as e:
            logger.error(f"❌ DB 티커 업데이트 실패: {e}")
            return False
    
    def save_results_to_s3(self, filtered_tickers: List[str]) -> bool:
        """결과를 S3에 저장"""
        try:
            s3 = get_s3_client()
            
            # 결과 데이터 준비
            result_data = {
                'timestamp': datetime.now().isoformat(),
                'phase': 'ticker_scanner',
                'total_tickers': len(filtered_tickers),
                'tickers': filtered_tickers,
                'status': 'success'
            }
            
            # S3에 저장
            s3.put_object(
                Bucket=self.s3_bucket,
                Key='phase0/updated_tickers.json',
                Body=json.dumps(result_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ S3에 결과 저장 완료: {len(filtered_tickers)}개 티커")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 결과 저장 실패: {e}")
            return False
    
    def trigger_next_phase(self) -> bool:
        """EventBridge를 통해 다음 단계 트리거"""
        try:
            eventbridge = get_eventbridge_client()
            
            # 다음 단계 트리거를 위한 이벤트 발송
            response = eventbridge.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.phase0',
                        'DetailType': 'Ticker Scan Completed',
                        'Detail': json.dumps({
                            'phase': 'ticker_scanner',
                            'status': 'completed',
                            'timestamp': datetime.now().isoformat(),
                            'next_phase': 'selective_data_collection'
                        })
                    }
                ]
            )
            
            if response['FailedEntryCount'] == 0:
                logger.info("✅ EventBridge 다음 단계 트리거 완료")
                return True
            else:
                logger.error(f"❌ EventBridge 트리거 실패: {response}")
                return False
                
        except Exception as e:
            logger.error(f"❌ EventBridge 트리거 실패: {e}")
            return False

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수
    
    Args:
        event: Lambda 이벤트 (EventBridge 또는 수동 실행)
        context: Lambda 컨텍스트
        
    Returns:
        dict: 실행 결과
    """
    
    start_time = time.time()
    logger.info("="*50)
    logger.info("🔍 Phase 0: Ticker Scanner Lambda 시작")
    logger.info("="*50)
    
    try:
        # 스캐너 인스턴스 생성
        scanner = UpbitTickerScanner()
        
        # 1. Upbit API에서 티커 목록 조회
        logger.info("📋 1단계: Upbit API 티커 조회")
        all_tickers = scanner.get_upbit_tickers()
        
        if not all_tickers:
            raise Exception("Upbit API에서 티커를 가져올 수 없습니다")
        
        # 2. 블랙리스트 로드 및 필터링
        logger.info("⛔️ 2단계: 블랙리스트 필터링")
        blacklist = scanner.load_blacklist_from_s3()
        filtered_tickers = scanner.apply_blacklist_filter(all_tickers, blacklist)
        
        # 3. DB tickers 테이블 업데이트
        logger.info("🗄️ 3단계: DB 티커 테이블 업데이트")
        db_success = scanner.update_db_tickers(filtered_tickers)
        
        if not db_success:
            logger.warning("⚠️ DB 업데이트 실패했지만 계속 진행")
        
        # 4. 결과를 S3에 저장
        logger.info("💾 4단계: S3 결과 저장")
        s3_success = scanner.save_results_to_s3(filtered_tickers)
        
        if not s3_success:
            raise Exception("S3 결과 저장 실패")
        
        # 5. 다음 단계 트리거
        logger.info("🚀 5단계: 다음 단계 트리거")
        trigger_success = scanner.trigger_next_phase()
        
        # 실행 완료
        execution_time = time.time() - start_time
        
        result = {
            'statusCode': 200,
            'phase': 'ticker_scanner',
            'status': 'success',
            'execution_time': f"{execution_time:.2f}초",
            'total_tickers': len(all_tickers),
            'filtered_tickers': len(filtered_tickers),
            'db_updated': db_success,
            's3_saved': s3_success,
            'next_phase_triggered': trigger_success,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("="*50)
        logger.info(f"✅ Phase 0 완료: {len(filtered_tickers)}개 티커 처리 ({execution_time:.2f}초)")
        logger.info("="*50)
        
        return result
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_result = {
            'statusCode': 500,
            'phase': 'ticker_scanner',
            'status': 'error',
            'error': str(e),
            'execution_time': f"{execution_time:.2f}초",
            'timestamp': datetime.now().isoformat()
        }
        
        logger.error("="*50)
        logger.error(f"❌ Phase 0 실패: {e} (소요시간: {execution_time:.2f}초)")
        logger.error("="*50)
        
        return error_result

# 로컬 테스트용
if __name__ == "__main__":
    # 환경 변수 설정 (테스트용)
    os.environ.setdefault('S3_BUCKET', 'makenaide-serverless-data')
    
    # 테스트 실행
    test_event = {}
    test_context = {}
    
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2, ensure_ascii=False))