#!/usr/bin/env python3
"""
Lambda Data Collector - Optimized Version
콜드 스타트 최적화 및 지연 로딩 적용
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

# AWS Lambda 환경 설정 - 최소한으로 유지
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 글로벌 변수로 모듈 캐싱
_requests = None
_time = None

def get_requests():
    """지연 로딩: requests 모듈"""
    global _requests
    if _requests is None:
        import requests
        _requests = requests
    return _requests

def get_time():
    """지연 로딩: time 모듈"""
    global _time
    if _time is None:
        import time
        _time = time
    return _time

class OptimizedUpbitAPICollector:
    """최적화된 Upbit API 데이터 수집기"""
    
    def __init__(self):
        self.base_url = "https://api.upbit.com/v1"
        self._requests = None
        
    @property
    def requests(self):
        """지연 로딩된 requests 모듈"""
        if self._requests is None:
            self._requests = get_requests()
        return self._requests
    
    def get_ohlcv_data(self, ticker: str, count: int = 200) -> Optional[List[Dict]]:
        """OHLCV 데이터 조회 - 최적화된 버전"""
        try:
            url = f"{self.base_url}/candles/days"
            params = {
                'market': ticker,
                'count': count
            }
            
            # 지연 로딩된 모듈 사용
            time_module = get_time()
            time_module.sleep(0.05)  # API 제한 준수 (단축)
            
            response = self.requests.get(url, params=params, timeout=8)  # 타임아웃 단축
            
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
        """마켓 목록 조회 - 최적화된 버전"""
        try:
            url = f"{self.base_url}/market/all"
            response = self.requests.get(url, timeout=5)  # 타임아웃 단축
            
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

class OptimizedLambdaDataCollector:
    """최적화된 Lambda 데이터 수집기"""
    
    def __init__(self):
        self._upbit_collector = None
        
    @property
    def upbit_collector(self):
        """지연 로딩된 Upbit 수집기"""
        if self._upbit_collector is None:
            self._upbit_collector = OptimizedUpbitAPICollector()
        return self._upbit_collector
        
    def process_data_collection_request(self, event: dict) -> dict:
        """데이터 수집 요청 처리 - 최적화된 버전"""
        try:
            start_time = datetime.now()
            
            # 요청 파라미터 파싱
            collection_type = event.get('collection_type', 'api_test')
            tickers = event.get('tickers', [])
            
            logger.info(f"🚀 최적화된 데이터 수집 시작: {collection_type}")
            
            results = {}
            
            if collection_type == 'api_test':
                results = self._test_api_connection()
            elif collection_type == 'market_list':
                results = self._get_market_list()
            elif collection_type == 'ohlcv_simple':
                results = self._collect_ohlcv_simple(tickers)
            else:
                raise ValueError(f"지원하지 않는 수집 타입: {collection_type}")
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            response = {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'collection_type': collection_type,
                    'processed_tickers': len(tickers),
                    'execution_time': round(elapsed, 3),
                    'results': results,
                    'timestamp': datetime.now().isoformat(),
                    'lambda_version': 'OPTIMIZED_v2.0',
                    'optimization': 'cold_start_optimized'
                }
            }
            
            logger.info(f"✅ 최적화된 데이터 수집 완료: {elapsed:.3f}초")
            return response
            
        except Exception as e:
            logger.error(f"❌ 데이터 수집 처리 실패: {e}")
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat(),
                    'lambda_version': 'OPTIMIZED_v2.0'
                }
            }
    
    def _test_api_connection(self) -> dict:
        """API 연결 테스트 - 최적화된 버전"""
        try:
            markets = self.upbit_collector.get_market_list()
            return {
                'api_connection': 'success',
                'market_count': len(markets) if markets else 0,
                'sample_markets': markets[:3] if markets else [],
                'optimization_applied': True
            }
        except Exception as e:
            logger.error(f"❌ API 연결 테스트 실패: {e}")
            return {
                'api_connection': 'failed',
                'error': str(e),
                'optimization_applied': True
            }
    
    def _get_market_list(self) -> dict:
        """마켓 목록 조회 - 최적화된 버전"""
        try:
            markets = self.upbit_collector.get_market_list()
            return {
                'success': True,
                'market_count': len(markets) if markets else 0,
                'markets': markets,
                'optimization_applied': True
            }
        except Exception as e:
            logger.error(f"❌ 마켓 목록 조회 실패: {e}")
            return {
                'success': False,
                'error': str(e),
                'optimization_applied': True
            }
    
    def _collect_ohlcv_simple(self, tickers: List[str]) -> dict:
        """간단한 OHLCV 데이터 수집 - 최적화된 버전"""
        results = {}
        
        for ticker in tickers:
            try:
                data = self.upbit_collector.get_ohlcv_data(ticker, count=3)  # 데이터 수 단축
                if data:
                    results[ticker] = {
                        'success': True,
                        'records': len(data),
                        'latest_date': data[0]['candle_date_time_kst'] if data else None,
                        'latest_price': data[0]['trade_price'] if data else None,
                        'optimization_applied': True
                    }
                else:
                    results[ticker] = {
                        'success': False,
                        'records': 0,
                        'optimization_applied': True
                    }
            except Exception as e:
                logger.error(f"❌ {ticker} OHLCV 데이터 수집 실패: {e}")
                results[ticker] = {
                    'success': False,
                    'error': str(e),
                    'optimization_applied': True
                }
        
        return results

def lambda_handler(event, context):
    """최적화된 Lambda 함수 진입점"""
    try:
        logger.info(f"📥 최적화된 Lambda 요청 수신")
        
        # 데이터 수집기 초기화 (지연 로딩)
        collector = OptimizedLambdaDataCollector()
        
        # 요청 처리
        result = collector.process_data_collection_request(event)
        
        logger.info("📤 최적화된 Lambda 응답 준비 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ 최적화된 Lambda 함수 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': f"Lambda 함수 실행 실패: {str(e)}",
                'timestamp': datetime.now().isoformat(),
                'lambda_version': 'OPTIMIZED_v2.0'
            }
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'collection_type': 'api_test',
        'tickers': [],
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))