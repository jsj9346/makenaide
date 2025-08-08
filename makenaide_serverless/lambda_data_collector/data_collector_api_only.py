#!/usr/bin/env python3
"""
Lambda Data Collector - API Only Version
psycopg2 없이 API 기능만 테스트하는 버전
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class UpbitAPICollector:
    """Upbit API 데이터 수집기"""
    
    def __init__(self):
        self.base_url = "https://api.upbit.com/v1"
    
    def get_ohlcv_data(self, ticker: str, count: int = 200) -> Optional[List[Dict]]:
        """OHLCV 데이터 조회"""
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
    
    def get_market_list(self) -> Optional[List[Dict]]:
        """마켓 목록 조회"""
        try:
            url = f"{self.base_url}/market/all"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                krw_markets = [m for m in data if m['market'].startswith('KRW-')]
                logger.info(f"✅ KRW 마켓 {len(krw_markets)}개 조회 성공")
                return krw_markets[:10]  # 상위 10개만 반환
            else:
                logger.warning(f"⚠️ 마켓 목록 조회 실패: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 마켓 목록 조회 실패: {e}")
            return None

class LambdaDataCollector:
    """Lambda 데이터 수집기 (API Only 버전)"""
    
    def __init__(self):
        self.upbit_collector = UpbitAPICollector()
        
    def process_data_collection_request(self, event: dict) -> dict:
        """데이터 수집 요청 처리"""
        try:
            start_time = time.time()
            
            # 요청 파라미터 파싱
            collection_type = event.get('collection_type', 'api_test')
            tickers = event.get('tickers', [])
            force_fetch = event.get('force_fetch', False)
            
            logger.info(f"🚀 데이터 수집 시작: {collection_type}, {len(tickers)}개 티커")
            
            results = {}
            
            if collection_type == 'api_test':
                results = self._test_api_connection()
            elif collection_type == 'market_list':
                results = self._get_market_list()
            elif collection_type == 'ohlcv_simple':
                results = self._collect_ohlcv_simple(tickers)
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
                    'timestamp': datetime.now().isoformat(),
                    'lambda_version': 'API_ONLY_v1.0'
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
    
    def _test_api_connection(self) -> dict:
        """API 연결 테스트"""
        try:
            # 간단한 마켓 정보 조회로 API 연결 테스트
            markets = self.upbit_collector.get_market_list()
            return {
                'api_connection': 'success',
                'market_count': len(markets) if markets else 0,
                'sample_markets': markets[:3] if markets else []
            }
        except Exception as e:
            logger.error(f"❌ API 연결 테스트 실패: {e}")
            return {
                'api_connection': 'failed',
                'error': str(e)
            }
    
    def _get_market_list(self) -> dict:
        """마켓 목록 조회"""
        try:
            markets = self.upbit_collector.get_market_list()
            return {
                'success': True,
                'market_count': len(markets) if markets else 0,
                'markets': markets
            }
        except Exception as e:
            logger.error(f"❌ 마켓 목록 조회 실패: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _collect_ohlcv_simple(self, tickers: List[str]) -> dict:
        """간단한 OHLCV 데이터 수집"""
        results = {}
        
        for ticker in tickers:
            try:
                data = self.upbit_collector.get_ohlcv_data(ticker, count=5)
                if data:
                    results[ticker] = {
                        'success': True,
                        'records': len(data),
                        'latest_date': data[0]['candle_date_time_kst'] if data else None,
                        'latest_price': data[0]['trade_price'] if data else None
                    }
                else:
                    results[ticker] = {
                        'success': False,
                        'records': 0
                    }
            except Exception as e:
                logger.error(f"❌ {ticker} OHLCV 데이터 수집 실패: {e}")
                results[ticker] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results

def lambda_handler(event, context):
    """Lambda 함수 진입점"""
    try:
        logger.info(f"📥 Lambda API 테스트 요청 수신: {json.dumps(event, indent=2)}")
        
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
        'collection_type': 'api_test',
        'tickers': [],
        'force_fetch': False
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))