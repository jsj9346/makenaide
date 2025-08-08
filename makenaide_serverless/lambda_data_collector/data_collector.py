#!/usr/bin/env python3
"""
Lambda Data Collector - Phase 2 아키텍처 개선
Makenaide 봇의 데이터 수집 로직을 독립적인 Lambda 함수로 분리

주요 기능:
1. OHLCV 데이터 수집 (일봉/4시간봉)
2. 기술적 지표 배치 조회
3. 시장 데이터 전처리
4. DB 저장 및 캐싱

Author: Phase 2 Architecture Migration
Version: 1.0.0
"""

import json
import logging
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DataCollectorConfig:
    """데이터 수집 설정 클래스"""
    
    # 기본 수집 설정
    DEFAULT_OHLCV_DAYS = 450
    DEFAULT_4H_LIMIT = 200
    
    # 주요 코인 확장 수집
    MAJOR_COINS = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-DOT"]
    MAJOR_COIN_DAYS = 600
    
    # 배치 처리 설정
    BATCH_SIZE = 10  # 한 번에 처리할 티커 수
    
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
    
    def insert_ohlcv_batch(self, ticker: str, df: pd.DataFrame, table: str = 'ohlcv'):
        """OHLCV 데이터 배치 삽입"""
        if df.empty:
            return
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 기존 데이터 삭제
            cursor.execute(f"DELETE FROM {table} WHERE ticker = %s", (ticker,))
            
            # 새 데이터 삽입
            for date, row in df.iterrows():
                cursor.execute(f"""
                    INSERT INTO {table} (ticker, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """, (ticker, date, row['open'], row['high'], row['low'], row['close'], row['volume']))
            
            conn.commit()
            logger.info(f"✅ {ticker} {table} 데이터 {len(df)}개 저장 완료")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ {ticker} {table} 데이터 저장 실패: {e}")
            raise
        finally:
            cursor.close()
    
    def get_existing_data_count(self, ticker: str, table: str = 'ohlcv') -> int:
        """기존 데이터 개수 조회"""
        try:
            result = self.execute_query(
                f"SELECT COUNT(*) FROM {table} WHERE ticker = %s",
                (ticker,),
                fetchone=True
            )
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"❌ {ticker} 데이터 개수 조회 실패: {e}")
            return 0
    
    def get_latest_timestamp(self, ticker: str, table: str = 'ohlcv') -> Optional[datetime]:
        """최신 타임스탬프 조회"""
        try:
            result = self.execute_query(
                f"SELECT MAX(date) FROM {table} WHERE ticker = %s",
                (ticker,),
                fetchone=True
            )
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"❌ {ticker} 최신 타임스탬프 조회 실패: {e}")
            return None

class MarketDataCollector:
    """마켓 데이터 수집기"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = DataCollectorConfig()
        
    def collect_ohlcv_daily(self, ticker: str, force_fetch: bool = False) -> Optional[pd.DataFrame]:
        """일봉 OHLCV 데이터 수집"""
        try:
            logger.info(f"📊 {ticker} 일봉 데이터 수집 시작")
            
            # 티커 형식 정규화
            if not ticker.startswith("KRW-"):
                ticker = f"KRW-{ticker}"
            
            # 기존 데이터 확인
            existing_count = self.db.get_existing_data_count(ticker)
            latest_date = self.db.get_latest_timestamp(ticker)
            
            # 수집량 결정
            target_count = (self.config.MAJOR_COIN_DAYS 
                          if ticker in self.config.MAJOR_COINS 
                          else self.config.DEFAULT_OHLCV_DAYS)
            
            if not force_fetch and existing_count >= target_count:
                # 증분 업데이트만 필요한지 확인
                if latest_date:
                    days_diff = (datetime.now() - latest_date).days
                    if days_diff <= 1:
                        logger.info(f"⏭️ {ticker} 최신 데이터 존재 - 수집 패스")
                        return None
                    else:
                        logger.info(f"🔄 {ticker} 증분 업데이트 ({days_diff}일 차이)")
                        return self._collect_incremental_data(ticker, latest_date)
            
            # 전체 데이터 수집
            logger.info(f"🆕 {ticker} 전체 데이터 수집 ({target_count}개)")
            return self._collect_full_data(ticker, target_count)
            
        except Exception as e:
            logger.error(f"❌ {ticker} 일봉 데이터 수집 실패: {e}")
            return None
    
    def collect_ohlcv_4h(self, ticker: str, limit: int = None) -> Optional[pd.DataFrame]:
        """4시간봉 OHLCV 데이터 수집"""
        try:
            logger.info(f"📊 {ticker} 4시간봉 데이터 수집 시작")
            
            if not ticker.startswith("KRW-"):
                ticker = f"KRW-{ticker}"
            
            limit = limit or self.config.DEFAULT_4H_LIMIT
            
            # pyupbit API 호출
            df = self._safe_pyupbit_call(ticker, interval="minute240", count=limit)
            
            if df is not None and not df.empty:
                # DB 저장
                self.db.insert_ohlcv_batch(ticker, df, table='ohlcv_4h')
                logger.info(f"✅ {ticker} 4시간봉 {len(df)}개 수집 완료")
                return df
            else:
                logger.warning(f"⚠️ {ticker} 4시간봉 데이터 없음")
                return None
                
        except Exception as e:
            logger.error(f"❌ {ticker} 4시간봉 데이터 수집 실패: {e}")
            return None
    
    def _collect_full_data(self, ticker: str, count: int) -> Optional[pd.DataFrame]:
        """전체 데이터 수집"""
        df = self._safe_pyupbit_call(ticker, interval="day", count=count)
        
        if df is not None and not df.empty:
            # 데이터 품질 검증 및 정제
            df = self._validate_and_clean_data(df, ticker)
            
            # DB 저장
            self.db.insert_ohlcv_batch(ticker, df)
            
            logger.info(f"✅ {ticker} 전체 데이터 {len(df)}개 수집 완료")
            return df
        
        return None
    
    def _collect_incremental_data(self, ticker: str, latest_date: datetime) -> Optional[pd.DataFrame]:
        """증분 데이터 수집"""
        days_to_fetch = min((datetime.now() - latest_date).days + 5, 30)  # 최대 30일
        
        df = self._safe_pyupbit_call(ticker, interval="day", count=days_to_fetch)
        
        if df is not None and not df.empty:
            # 기존 데이터보다 새로운 것만 필터링
            df = df[df.index > latest_date]
            
            if not df.empty:
                df = self._validate_and_clean_data(df, ticker)
                
                # 증분 데이터 추가
                conn = self.db.get_connection()
                cursor = conn.cursor()
                
                try:
                    for date, row in df.iterrows():
                        cursor.execute("""
                            INSERT INTO ohlcv (ticker, date, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker, date) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """, (ticker, date, row['open'], row['high'], row['low'], row['close'], row['volume']))
                    
                    conn.commit()
                    logger.info(f"✅ {ticker} 증분 데이터 {len(df)}개 저장 완료")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"❌ {ticker} 증분 데이터 저장 실패: {e}")
                    raise
                finally:
                    cursor.close()
                
                return df
        
        return None
    
    def _safe_pyupbit_call(self, ticker: str, interval: str, count: int) -> Optional[pd.DataFrame]:
        """안전한 pyupbit API 호출"""
        try:
            import pyupbit
            time.sleep(0.1)  # API 제한 준수
            
            df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
            
            if df is None or df.empty:
                logger.warning(f"⚠️ {ticker} API 응답 없음")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"❌ {ticker} pyupbit API 호출 실패: {e}")
            return None
    
    def _validate_and_clean_data(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """데이터 품질 검증 및 정제"""
        if df.empty:
            return df
        
        # 1. Null 값 제거
        df = df.dropna()
        
        # 2. 0값 제거 (OHLCV 모두 0인 행)
        mask = (df['open'] > 0) & (df['high'] > 0) & (df['low'] > 0) & (df['close'] > 0)
        df = df[mask]
        
        # 3. 가격 논리성 검증 (high >= low, high >= open,close, low <= open,close)
        valid_price_mask = (
            (df['high'] >= df['low']) &
            (df['high'] >= df['open']) &
            (df['high'] >= df['close']) &
            (df['low'] <= df['open']) &
            (df['low'] <= df['close'])
        )
        df = df[valid_price_mask]
        
        # 4. 날짜 인덱스 검증
        if hasattr(df.index, 'year'):
            # 1970년 데이터 제거 (타임스탬프 오류)
            df = df[df.index.year > 2000]
        
        # 5. 중복 제거
        df = df[~df.index.duplicated(keep='last')]
        
        # 6. 정렬
        df = df.sort_index()
        
        logger.info(f"🔧 {ticker} 데이터 정제 완료: {len(df)}개 레코드")
        return df

class TechnicalIndicatorBatchCollector:
    """기술적 지표 배치 수집기"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def get_technical_data_batch(self, tickers: List[str]) -> Dict[str, Any]:
        """여러 티커의 기술적 지표를 배치로 조회 (Phase 1 최적화 적용)"""
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
            logger.info(f"💰 DB 쿼리 최적화: {len(tickers)*2-len(tickers)}개 쿼리 절약")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 배치 기술적 지표 조회 실패: {e}")
            return {}

class LambdaDataCollector:
    """Lambda 데이터 수집기 메인 클래스"""
    
    def __init__(self):
        self.db = DatabaseManager(DataCollectorConfig.DB_CONFIG)
        self.market_collector = MarketDataCollector(self.db)
        self.technical_collector = TechnicalIndicatorBatchCollector(self.db)
        
    def process_data_collection_request(self, event: dict) -> dict:
        """데이터 수집 요청 처리"""
        try:
            start_time = time.time()
            
            # 요청 파라미터 파싱
            collection_type = event.get('collection_type', 'ohlcv_daily')
            tickers = event.get('tickers', [])
            force_fetch = event.get('force_fetch', False)
            
            logger.info(f"🚀 데이터 수집 시작: {collection_type}, {len(tickers)}개 티커")
            
            results = {}
            
            if collection_type == 'ohlcv_daily':
                results = self._collect_daily_data(tickers, force_fetch)
            elif collection_type == 'ohlcv_4h':
                results = self._collect_4h_data(tickers)
            elif collection_type == 'technical_batch':
                results = self._collect_technical_data(tickers)
            elif collection_type == 'mixed':
                # 혼합 수집 (일봉 + 기술적 지표)
                daily_results = self._collect_daily_data(tickers, force_fetch)
                technical_results = self._collect_technical_data(tickers)
                results = {
                    'daily_data': daily_results,
                    'technical_data': technical_results
                }
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
    
    def _collect_daily_data(self, tickers: List[str], force_fetch: bool) -> dict:
        """일봉 데이터 수집"""
        results = {}
        
        for ticker in tickers:
            try:
                df = self.market_collector.collect_ohlcv_daily(ticker, force_fetch)
                results[ticker] = {
                    'success': df is not None,
                    'records': len(df) if df is not None else 0
                }
            except Exception as e:
                logger.error(f"❌ {ticker} 일봉 데이터 수집 실패: {e}")
                results[ticker] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results
    
    def _collect_4h_data(self, tickers: List[str]) -> dict:
        """4시간봉 데이터 수집"""
        results = {}
        
        for ticker in tickers:
            try:
                df = self.market_collector.collect_ohlcv_4h(ticker)
                results[ticker] = {
                    'success': df is not None,
                    'records': len(df) if df is not None else 0
                }
            except Exception as e:
                logger.error(f"❌ {ticker} 4시간봉 데이터 수집 실패: {e}")
                results[ticker] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results
    
    def _collect_technical_data(self, tickers: List[str]) -> dict:
        """기술적 지표 데이터 수집"""
        try:
            return self.technical_collector.get_technical_data_batch(tickers)
        except Exception as e:
            logger.error(f"❌ 기술적 지표 배치 수집 실패: {e}")
            return {}

def lambda_handler(event, context):
    """Lambda 함수 진입점"""
    try:
        logger.info(f"📥 Lambda 데이터 수집 요청 수신: {json.dumps(event, indent=2)}")
        
        # 데이터 수집기 초기화
        collector = LambdaDataCollector()
        
        # 요청 처리
        result = collector.process_data_collection_request(event)
        
        logger.info(f"📤 Lambda 응답: {json.dumps(result, indent=2)}")
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
        'collection_type': 'mixed',
        'tickers': ['KRW-BTC', 'KRW-ETH', 'KRW-ADA'],
        'force_fetch': False
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))