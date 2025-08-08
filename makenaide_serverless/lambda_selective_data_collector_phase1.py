#!/usr/bin/env python3
"""
📊 Phase 1: Selective Data Collection Lambda
- Phase 0에서 받은 티커 목록에 대해 선택적 필터링 적용
- 월봉 데이터 필터링 (14개월 이상)
- 거래대금 필터링 (24시간 거래대금 3억원 이상)
- OHLCV 데이터 수집 및 기술적 지표 계산
- static_indicators 테이블에 저장
"""

import json
import os
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np

# Layer에서 공통 유틸리티 import
try:
    from makenaide_utils import (
        setup_lambda_logger, get_db_connection_params, 
        save_to_s3, load_from_s3, trigger_next_phase,
        create_lambda_response, LambdaTimer
    )
except ImportError:
    # 로컬 테스트용 fallback
    def setup_lambda_logger(phase_name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def get_db_connection_params():
        return {
            'host': os.getenv("PG_HOST"),
            'port': os.getenv("PG_PORT", "5432"),
            'dbname': os.getenv("PG_DATABASE"),
            'user': os.getenv("PG_USER"),
            'password': os.getenv("PG_PASSWORD")
        }

logger = setup_lambda_logger("Phase1-DataCollection")

# 상수 정의
ONE_HMIL_KRW = 100_000_000  # 1억원
DEFAULT_MIN_TRADE_VOLUME = ONE_HMIL_KRW * 3  # 3억원

class SelectiveDataCollector:
    """
    선택적 데이터 수집기
    기존 makenaide.py의 필터링 로직을 서버리스 환경에 최적화
    """
    
    def __init__(self):
        self.s3_bucket = os.getenv('S3_BUCKET', 'makenaide-serverless-data')
        self.db_params = get_db_connection_params()
        
    def get_input_tickers(self) -> List[str]:
        """Phase 0에서 생성된 티커 목록을 S3에서 로드"""
        try:
            logger.info("📥 Phase 0 결과 로드 중...")
            
            phase0_data = load_from_s3(self.s3_bucket, 'phase0/updated_tickers.json')
            
            if not phase0_data or 'tickers' not in phase0_data:
                logger.error("❌ Phase 0 결과를 찾을 수 없습니다")
                return []
            
            tickers = phase0_data['tickers']
            logger.info(f"✅ Phase 0에서 {len(tickers)}개 티커 로드 완료")
            
            return tickers
            
        except Exception as e:
            logger.error(f"❌ Phase 0 결과 로드 실패: {e}")
            return []
    
    def filter_by_monthly_data_length(self, tickers: List[str], min_months: int = 14) -> List[str]:
        """
        월봉 데이터 길이로 필터링
        기존 filter_tickers.py의 filter_by_monthly_data_length()와 동일
        """
        try:
            import pyupbit
            import time
            
            logger.info(f"📅 월봉 데이터 길이 필터링 시작 (최소 {min_months}개월)")
            
            filtered_tickers = []
            failed_tickers = []
            
            for i, ticker in enumerate(tickers):
                try:
                    # API 호출 제한 고려 (초당 10회)
                    if i > 0 and i % 10 == 0:
                        time.sleep(1)
                    
                    # 월봉 데이터 조회
                    monthly_data = pyupbit.get_ohlcv(ticker, interval="month", count=200)
                    
                    if monthly_data is not None and len(monthly_data) >= min_months:
                        filtered_tickers.append(ticker)
                        logger.debug(f"✅ {ticker}: {len(monthly_data)}개월 데이터 (통과)")
                    else:
                        failed_tickers.append(ticker)
                        data_length = len(monthly_data) if monthly_data is not None else 0
                        logger.debug(f"❌ {ticker}: {data_length}개월 데이터 (부족)")
                
                except Exception as e:
                    logger.warning(f"⚠️ {ticker} 월봉 데이터 조회 실패: {e}")
                    failed_tickers.append(ticker)
            
            logger.info(f"📅 월봉 필터링 완료: {len(filtered_tickers)}개 통과, {len(failed_tickers)}개 제외")
            
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"❌ 월봉 데이터 필터링 실패: {e}")
            return tickers  # 실패시 원본 반환
    
    def filter_by_volume(self, tickers: List[str], min_trade_price_krw: int = DEFAULT_MIN_TRADE_VOLUME) -> List[str]:
        """
        24시간 거래대금으로 필터링
        기존 filter_tickers.py의 filter_by_volume()와 동일
        """
        try:
            import pyupbit
            import requests
            
            logger.info(f"💰 24시간 거래대금 필터링 시작 (최소 {min_trade_price_krw:,}원)")
            
            # Upbit REST API를 통한 실시간 거래대금 조회
            try:
                url = "https://api.upbit.com/v1/ticker"
                params = {"markets": ",".join(tickers)}
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    logger.warning(f"⚠️ Upbit API 응답 오류: {response.status_code}")
                    raise Exception("API 호출 실패")
                
                ticker_data = response.json()
                
            except Exception as e:
                logger.warning(f"⚠️ REST API 실패, pyupbit fallback 사용: {e}")
                # pyupbit fallback
                ticker_data = pyupbit.get_current_price(tickers)
                if isinstance(ticker_data, dict):
                    ticker_data = [{'market': k, 'acc_trade_price_24h': v} for k, v in ticker_data.items()]
            
            # 거래대금 필터링
            filtered_tickers = []
            for data in ticker_data:
                if isinstance(data, dict):
                    market = data.get('market')
                    trade_volume = data.get('acc_trade_price_24h', 0)
                    
                    if trade_volume and trade_volume >= min_trade_price_krw:
                        filtered_tickers.append(market)
                        logger.debug(f"✅ {market}: {trade_volume:,.0f}원 (통과)")
                    else:
                        logger.debug(f"❌ {market}: {trade_volume:,.0f}원 (부족)")
            
            logger.info(f"💰 거래대금 필터링 완료: {len(filtered_tickers)}개 통과")
            
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"❌ 거래대금 필터링 실패: {e}")
            return tickers  # 실패시 원본 반환
    
    def collect_ohlcv_data(self, ticker: str, days: int = 200) -> Optional[pd.DataFrame]:
        """
        단일 티커의 OHLCV 데이터 수집
        기존 data_fetcher.py의 get_ohlcv_d()와 유사
        """
        try:
            import pyupbit
            
            # 일봉 데이터 조회
            ohlcv_data = pyupbit.get_ohlcv(ticker, interval="day", count=days)
            
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"⚠️ {ticker}: OHLCV 데이터 없음")
                return None
            
            # 데이터 검증
            if len(ohlcv_data) < 50:  # 최소 50일 데이터 필요
                logger.warning(f"⚠️ {ticker}: 데이터 부족 ({len(ohlcv_data)}일)")
                return None
            
            # 컬럼명 표준화
            ohlcv_data.columns = ['open', 'high', 'low', 'close', 'volume']
            ohlcv_data.index.name = 'date'
            
            logger.debug(f"✅ {ticker}: {len(ohlcv_data)}일 OHLCV 데이터 수집")
            
            return ohlcv_data
            
        except Exception as e:
            logger.error(f"❌ {ticker} OHLCV 수집 실패: {e}")
            return None
    
    def calculate_technical_indicators(self, ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
        """
        기술적 지표 계산
        기존 data_fetcher.py의 calculate_technical_indicators()와 유사
        """
        try:
            import pandas_ta as ta
            
            # 기본 지표 계산
            indicators = {}
            
            # 이동평균
            indicators['ma5'] = ohlcv_data['close'].rolling(5).mean().iloc[-1]
            indicators['ma10'] = ohlcv_data['close'].rolling(10).mean().iloc[-1]
            indicators['ma20'] = ohlcv_data['close'].rolling(20).mean().iloc[-1]
            indicators['ma60'] = ohlcv_data['close'].rolling(60).mean().iloc[-1]
            indicators['ma120'] = ohlcv_data['close'].rolling(120).mean().iloc[-1]
            indicators['ma200'] = ohlcv_data['close'].rolling(200).mean().iloc[-1]
            
            # RSI
            rsi = ta.rsi(ohlcv_data['close'], length=14)
            indicators['rsi'] = rsi.iloc[-1] if rsi is not None else None
            
            # MACD
            macd = ta.macd(ohlcv_data['close'])
            if macd is not None:
                indicators['macd'] = macd['MACD_12_26_9'].iloc[-1]
                indicators['macd_signal'] = macd['MACDs_12_26_9'].iloc[-1]
                indicators['macd_histogram'] = macd['MACDh_12_26_9'].iloc[-1]
            
            # Bollinger Bands
            bb = ta.bbands(ohlcv_data['close'], length=20)
            if bb is not None:
                indicators['bb_upper'] = bb['BBU_20_2.0'].iloc[-1]
                indicators['bb_middle'] = bb['BBM_20_2.0'].iloc[-1]
                indicators['bb_lower'] = bb['BBL_20_2.0'].iloc[-1]
            
            # ADX
            adx = ta.adx(ohlcv_data['high'], ohlcv_data['low'], ohlcv_data['close'])
            if adx is not None:
                indicators['adx'] = adx['ADX_14'].iloc[-1]
            
            # 거래량 관련
            indicators['volume_ma20'] = ohlcv_data['volume'].rolling(20).mean().iloc[-1]
            indicators['volume_ratio'] = ohlcv_data['volume'].iloc[-1] / indicators['volume_ma20'] if indicators['volume_ma20'] > 0 else 1
            
            # 가격 관련
            indicators['current_price'] = ohlcv_data['close'].iloc[-1]
            indicators['high_52w'] = ohlcv_data['high'].tail(252).max()  # 52주 최고가
            indicators['low_52w'] = ohlcv_data['low'].tail(252).min()   # 52주 최저가
            
            # NaN 값 처리
            for key, value in indicators.items():
                if pd.isna(value):
                    indicators[key] = None
            
            return indicators
            
        except Exception as e:
            logger.error(f"❌ 기술적 지표 계산 실패: {e}")
            return {}
    
    def process_single_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        단일 티커 처리 (OHLCV 수집 + 지표 계산)
        """
        try:
            # OHLCV 데이터 수집
            ohlcv_data = self.collect_ohlcv_data(ticker)
            if ohlcv_data is None:
                return None
            
            # 기술적 지표 계산
            indicators = self.calculate_technical_indicators(ohlcv_data)
            if not indicators:
                return None
            
            # 결과 데이터 구성
            result = {
                'ticker': ticker,
                'processed_at': datetime.now().isoformat(),
                'data_points': len(ohlcv_data),
                **indicators
            }
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 처리 실패: {e}")
            return None
    
    def process_tickers_parallel(self, filtered_tickers: List[str], max_workers: int = 5) -> List[Dict[str, Any]]:
        """
        병렬 처리로 여러 티커의 데이터 수집 및 지표 계산
        """
        logger.info(f"⚡️ 병렬 데이터 수집 시작: {len(filtered_tickers)}개 티커 ({max_workers}개 워커)")
        
        successful_results = []
        failed_tickers = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 작업 제출
            future_to_ticker = {
                executor.submit(self.process_single_ticker, ticker): ticker 
                for ticker in filtered_tickers
            }
            
            # 결과 수집
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result(timeout=30)  # 30초 타임아웃
                    if result:
                        successful_results.append(result)
                        logger.debug(f"✅ {ticker} 처리 완료")
                    else:
                        failed_tickers.append(ticker)
                        logger.debug(f"❌ {ticker} 처리 실패")
                        
                except Exception as e:
                    failed_tickers.append(ticker)
                    logger.warning(f"⚠️ {ticker} 처리 중 예외: {e}")
        
        logger.info(f"⚡️ 병렬 처리 완료: 성공 {len(successful_results)}개, 실패 {len(failed_tickers)}개")
        
        return successful_results
    
    def save_to_database(self, processed_data: List[Dict[str, Any]]) -> bool:
        """
        처리된 데이터를 static_indicators 테이블에 저장
        """
        if not processed_data:
            logger.warning("⚠️ 저장할 데이터가 없습니다")
            return False
        
        try:
            logger.info(f"🗄️ DB 저장 시작: {len(processed_data)}개 레코드")
            
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            try:
                # 기존 데이터 삭제 (당일 데이터만)
                today = datetime.now().date()
                cursor.execute("""
                    DELETE FROM static_indicators 
                    WHERE DATE(created_at) = %s
                """, (today,))
                
                deleted_count = cursor.rowcount
                logger.info(f"🗑️ 기존 데이터 삭제: {deleted_count}개 레코드")
                
                # 새로운 데이터 삽입
                insert_query = """
                    INSERT INTO static_indicators (
                        ticker, ma5, ma10, ma20, ma60, ma120, ma200,
                        rsi, macd, macd_signal, macd_histogram,
                        bb_upper, bb_middle, bb_lower, adx,
                        volume_ma20, volume_ratio, current_price,
                        high_52w, low_52w, data_points,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        NOW(), NOW()
                    )
                """
                
                insert_data = []
                for data in processed_data:
                    insert_data.append((
                        data['ticker'],
                        data.get('ma5'), data.get('ma10'), data.get('ma20'),
                        data.get('ma60'), data.get('ma120'), data.get('ma200'),
                        data.get('rsi'), data.get('macd'), data.get('macd_signal'), data.get('macd_histogram'),
                        data.get('bb_upper'), data.get('bb_middle'), data.get('bb_lower'), data.get('adx'),
                        data.get('volume_ma20'), data.get('volume_ratio'), data.get('current_price'),
                        data.get('high_52w'), data.get('low_52w'), data.get('data_points', 0)
                    ))
                
                cursor.executemany(insert_query, insert_data)
                
                # 커밋
                conn.commit()
                
                logger.info(f"✅ DB 저장 완료: {len(insert_data)}개 레코드 삽입")
                
                return True
                
            finally:
                cursor.close()
                conn.close()
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            return False
    
    def save_results_to_s3(self, filtered_tickers: List[str], processed_data: List[Dict[str, Any]]) -> bool:
        """Phase 1 결과를 S3에 저장"""
        try:
            result_data = {
                'timestamp': datetime.now().isoformat(),
                'phase': 'selective_data_collection',
                'input_tickers': len(filtered_tickers),
                'processed_tickers': len(processed_data),
                'filtered_tickers': filtered_tickers,
                'success_rate': len(processed_data) / len(filtered_tickers) if filtered_tickers else 0,
                'status': 'success'
            }
            
            # 간단한 통계 정보 추가
            if processed_data:
                prices = [d.get('current_price', 0) for d in processed_data if d.get('current_price')]
                result_data['stats'] = {
                    'avg_price': sum(prices) / len(prices) if prices else 0,
                    'max_price': max(prices) if prices else 0,
                    'min_price': min(prices) if prices else 0
                }
            
            return save_to_s3(
                self.s3_bucket,
                'phase1/filtered_tickers_with_data.json',
                result_data
            )
            
        except Exception as e:
            logger.error(f"❌ S3 결과 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수
    """
    
    with LambdaTimer("Phase 1: Selective Data Collection"):
        try:
            # 데이터 수집기 초기화
            collector = SelectiveDataCollector()
            
            # 1. Phase 0 결과 로드
            logger.info("📥 1단계: Phase 0 결과 로드")
            input_tickers = collector.get_input_tickers()
            
            if not input_tickers:
                raise Exception("Phase 0 결과를 로드할 수 없습니다")
            
            # 2. 월봉 데이터 길이 필터링
            logger.info("📅 2단계: 월봉 데이터 필터링")
            monthly_filtered = collector.filter_by_monthly_data_length(input_tickers, min_months=14)
            
            if not monthly_filtered:
                raise Exception("월봉 필터링 후 남은 티커가 없습니다")
            
            # 3. 거래대금 필터링
            logger.info("💰 3단계: 거래대금 필터링")
            volume_filtered = collector.filter_by_volume(monthly_filtered)
            
            if not volume_filtered:
                raise Exception("거래대금 필터링 후 남은 티커가 없습니다")
            
            # 4. OHLCV 데이터 수집 및 지표 계산
            logger.info("📊 4단계: 데이터 수집 및 지표 계산")
            processed_data = collector.process_tickers_parallel(volume_filtered)
            
            if not processed_data:
                raise Exception("데이터 처리 결과가 없습니다")
            
            # 5. DB 저장
            logger.info("🗄️ 5단계: DB 저장")
            db_success = collector.save_to_database(processed_data)
            
            # 6. S3 결과 저장
            logger.info("💾 6단계: S3 결과 저장")
            s3_success = collector.save_results_to_s3(volume_filtered, processed_data)
            
            # 7. 다음 단계 트리거
            logger.info("🚀 7단계: 다음 단계 트리거")
            trigger_success = trigger_next_phase('selective_data_collection', 'comprehensive_filtering')
            
            # 성공 응답
            return create_lambda_response(200, 'selective_data_collection', {
                'input_tickers': len(input_tickers),
                'monthly_filtered': len(monthly_filtered),
                'volume_filtered': len(volume_filtered),
                'processed_data': len(processed_data),
                'db_saved': db_success,
                's3_saved': s3_success,
                'next_phase_triggered': trigger_success
            })
            
        except Exception as e:
            logger.error(f"❌ Phase 1 실행 실패: {e}")
            return create_lambda_response(500, 'selective_data_collection', error=str(e))

# 로컬 테스트용
if __name__ == "__main__":
    # 환경 변수 설정
    os.environ.setdefault('S3_BUCKET', 'makenaide-serverless-data')
    
    # 테스트 실행
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2, ensure_ascii=False))