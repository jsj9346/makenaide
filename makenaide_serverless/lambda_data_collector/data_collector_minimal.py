#!/usr/bin/env python3
"""
Lambda Data Collector - Minimal Version (pandas 없음)
AWS Lambda 환경용 경량화된 데이터 수집기
"""

import json
import logging
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DataCollectorConfig:
    """데이터 수집 설정 클래스"""
    
    # DB 연결 설정
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
        'port': int(os.environ.get('DB_PORT', '5432')),
        'database': os.environ.get('DB_NAME', 'makenaide'),
        'user': os.environ.get('DB_USER', 'bruce'),
        'password': os.environ.get('DB_PASSWORD', '0asis314.')
    }

class DatabaseManager:
    """Lambda 환경용 경량화된 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def get_connection(self):
        """DB 연결 획득"""
        if self.connection is None:
            import psycopg2
            try:
                self.connection = psycopg2.connect(**self.config)
                logger.info("✅ PostgreSQL 연결 성공")
            except Exception as e:
                logger.error(f"❌ DB 연결 실패: {e}")
                raise
        return self.connection
    
    def execute_query(self, query: str, params: tuple = None, fetchone: bool = False):
        """쿼리 실행"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if fetchone:
                return cursor.fetchone()
            else:
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

class TechnicalIndicatorBatchCollector:
    """기술적 지표 배치 수집기 (경량 버전)"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def get_technical_data_batch(self, tickers: List[str]) -> Dict[str, Any]:
        """여러 티커의 기술적 지표를 배치로 조회"""
        start_time = time.time()
        logger.info(f"📊 배치 기술적 지표 조회 시작: {len(tickers)}개 티커")
        
        if not tickers:
            return {}
        
        results = {}
        
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Phase 1에서 최적화된 단일 JOIN 쿼리 사용
            for ticker in tickers:
                cursor.execute("""
                    SELECT 
                        s.price, s.atr, s.adx, s.volume_change_7_30, s.supertrend_signal,
                        o.close, o.rsi_14, o.ma_50, o.ma_200, o.bb_upper, o.bb_lower
                    FROM static_indicators s
                    LEFT JOIN (
                        SELECT ticker, close, rsi_14, ma_50, ma_200, bb_upper, bb_lower
                        FROM ohlcv 
                        WHERE ticker = %s 
                        ORDER BY date DESC 
                        LIMIT 1
                    ) o ON s.ticker = o.ticker
                    WHERE s.ticker = %s
                """, (ticker, ticker))
                
                result = cursor.fetchone()
                
                if result:
                    results[ticker] = {
                        'price': result[0],
                        'atr': result[1],
                        'adx': result[2],
                        'volume_change_7_30': result[3],
                        'supertrend_signal': result[4],
                        'close': result[5],
                        'rsi_14': result[6],
                        'ma_50': result[7],
                        'ma_200': result[8],
                        'bb_upper': result[9],
                        'bb_lower': result[10]
                    }
                else:
                    results[ticker] = None
            
            cursor.close()
            
            elapsed = time.time() - start_time
            success_count = len([r for r in results.values() if r is not None])
            
            logger.info(f"✅ 배치 조회 완료: {success_count}/{len(tickers)}개 성공 ({elapsed:.3f}초)")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 배치 기술적 지표 조회 실패: {e}")
            return {}

class UpbitAPICollector:
    """Upbit API 데이터 수집기 (requests 기반)"""
    
    def __init__(self):
        self.base_url = "https://api.upbit.com/v1"
    
    def get_ohlcv_data(self, ticker: str, count: int = 200) -> Optional[List[Dict]]:
        """OHLCV 데이터 조회 (requests 사용)"""
        try:
            url = f"{self.base_url}/candles/days"
            params = {
                'market': ticker,
                'count': count
            }
            
            time.sleep(0.1)  # API 제한 준수
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ {ticker} API 데이터 {len(data)}개 조회 성공")
                return data
            else:
                logger.warning(f"⚠️ {ticker} API 호출 실패: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ {ticker} API 호출 실패: {e}")
            return None

class LambdaDataCollector:
    """Lambda 데이터 수집기 메인 클래스 (경량 버전)"""
    
    def __init__(self):
        self.db = DatabaseManager(DataCollectorConfig.DB_CONFIG)
        self.technical_collector = TechnicalIndicatorBatchCollector(self.db)
        self.upbit_collector = UpbitAPICollector()
        
    def process_data_collection_request(self, event: dict) -> dict:
        """데이터 수집 요청 처리"""
        try:
            start_time = time.time()
            
            # 요청 파라미터 파싱
            collection_type = event.get('collection_type', 'technical_batch')
            tickers = event.get('tickers', [])
            force_fetch = event.get('force_fetch', False)
            
            logger.info(f"🚀 데이터 수집 시작: {collection_type}, {len(tickers)}개 티커")
            
            results = {}
            
            if collection_type == 'technical_batch':
                results = self._collect_technical_data(tickers)
            elif collection_type == 'ohlcv_simple':
                results = self._collect_ohlcv_simple(tickers)
            elif collection_type == 'db_test':
                results = self._test_db_connection()
            else:
                raise ValueError(f"지원하지 않는 수집 타입: {collection_type}")
            
            elapsed = time.time() - start_time
            
            response = {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'collection_type': collection_type,
                    'processed_tickers': len(tickers),
                    'execution_time': round(elapsed, 3),
                    'results': results,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            logger.info(f"✅ 데이터 수집 완료: {elapsed:.3f}초")
            return response
            
        except Exception as e:
            logger.error(f"❌ 데이터 수집 처리 실패: {e}")
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            }
    
    def _collect_technical_data(self, tickers: List[str]) -> dict:
        """기술적 지표 데이터 수집"""
        try:
            return self.technical_collector.get_technical_data_batch(tickers)
        except Exception as e:
            logger.error(f"❌ 기술적 지표 배치 수집 실패: {e}")
            return {}
    
    def _collect_ohlcv_simple(self, tickers: List[str]) -> dict:
        """간단한 OHLCV 데이터 수집"""
        results = {}
        
        for ticker in tickers:
            try:
                data = self.upbit_collector.get_ohlcv_data(ticker, count=10)
                results[ticker] = {
                    'success': data is not None,
                    'records': len(data) if data else 0
                }
            except Exception as e:
                logger.error(f"❌ {ticker} OHLCV 데이터 수집 실패: {e}")
                results[ticker] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results
    
    def _test_db_connection(self) -> dict:
        """DB 연결 테스트"""
        try:
            result = self.db.execute_query("SELECT 1 as test_value", fetchone=True)
            return {
                'db_connection': 'success',
                'test_query': 'passed',
                'result': result[0] if result else None
            }
        except Exception as e:
            logger.error(f"❌ DB 연결 테스트 실패: {e}")
            return {
                'db_connection': 'failed',
                'error': str(e)
            }

def lambda_handler(event, context):
    """Lambda 함수 진입점"""
    try:
        logger.info(f"📥 Lambda 데이터 수집 요청 수신: {json.dumps(event, indent=2)}")
        
        # 데이터 수집기 초기화
        collector = LambdaDataCollector()
        
        # 요청 처리
        result = collector.process_data_collection_request(event)
        
        logger.info("📤 Lambda 응답 준비 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ Lambda 함수 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': f"Lambda 함수 실행 실패: {str(e)}",
                'timestamp': datetime.now().isoformat()
            }
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'collection_type': 'db_test',
        'tickers': [],
        'force_fetch': False
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))