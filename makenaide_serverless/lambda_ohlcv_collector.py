#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide OHLCV 수집기
기능: SQS 트리거 → OHLCV 데이터 수집 → 기술적 지표 계산 → DB 저장
"""

import json
import boto3
import psycopg2
import logging
import os
import pandas as pd
import pandas_ta as ta
import pyupbit
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_connection():
    """PostgreSQL DB 연결"""
    try:
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'],
            port=int(os.environ['PG_PORT']),
            database=os.environ['PG_DATABASE'],
            user=os.environ['PG_USER'],
            password=os.environ['PG_PASSWORD']
        )
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise

def get_ohlcv_data(ticker: str, count: int = 200) -> Optional[pd.DataFrame]:
    """Upbit API에서 OHLCV 데이터 수집"""
    try:
        # KRW 마켓 티커로 변환
        if not ticker.startswith('KRW-'):
            ticker = f'KRW-{ticker}'
        
        df = pyupbit.get_ohlc(ticker, interval="day", count=count)
        
        if df is None or df.empty:
            logger.warning(f"{ticker} OHLCV 데이터 없음")
            return None
            
        # 컬럼명 정리
        df.columns = ['open', 'high', 'low', 'close', 'volume', 'value']
        df = df.drop('value', axis=1)  # value 컬럼 제거
        
        # 인덱스를 date 컬럼으로 변환
        df = df.reset_index()
        df['date'] = pd.to_datetime(df['index']).dt.date
        df = df.drop('index', axis=1)
        
        # 🔧 [수정] 소수점 제한 완전 제거 - 스몰캡 코인 지원
        # 원본 가격 데이터 보존
        
        logger.info(f"✅ {ticker} OHLCV 수집 완료: {len(df)}개 레코드")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} OHLCV 수집 실패: {e}")
        return None

def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """기술적 지표 계산 (실제 계산값 사용)"""
    try:
        # 기본 이동평균선
        df['ma_20'] = ta.sma(df['close'], length=20)
        df['ma_50'] = ta.sma(df['close'], length=50)
        df['ma_200'] = ta.sma(df['close'], length=200)
        
        # RSI
        df['rsi_14'] = ta.rsi(df['close'], length=14)
        
        # MFI
        df['mfi_14'] = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=14)
        
        # Bollinger Bands
        bb = ta.bbands(df['close'], length=20)
        df['bb_upper'] = bb['BBU_20_2.0']
        df['bb_middle'] = bb['BBM_20_2.0'] 
        df['bb_lower'] = bb['BBL_20_2.0']
        
        # MACD
        macd = ta.macd(df['close'])
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
        df['macd_histogram'] = macd['MACDh_12_26_9']
        
        # ADX (실제 계산값)
        adx_result = ta.adx(df['high'], df['low'], df['close'], length=14)
        df['adx'] = adx_result['ADX_14']
        df['plus_di'] = adx_result['DMP_14']
        df['minus_di'] = adx_result['DMN_14']
        
        # ATR
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        # Supertrend
        supertrend = ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=10, multiplier=2.0)
        df['supertrend'] = supertrend['SUPERT_10_2.0']
        df['supertrend_direction'] = supertrend['SUPERTd_10_2.0']
        
        # Donchian Channels
        donchian = ta.donchian(high=df['high'], low=df['low'], close=df['close'], length=20)
        if donchian is not None and not donchian.empty:
            df['donchian_high'] = donchian.iloc[:, -1]  # DCU
            df['donchian_low'] = donchian.iloc[:, 0]    # DCL
        
        # Volume indicators (실제 계산값)
        df['volume_20ma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_20ma']
        
        # Volume Change (7일/30일 비율) - 실제 계산
        volume_7d = df['volume'].rolling(window=7).mean()
        volume_30d = df['volume'].rolling(window=30).mean()
        df['volume_change_7_30'] = volume_7d / volume_30d
        
        # NVT Relative - 실제 계산
        market_cap = df['close'] * df['volume']
        volume_90d = df['volume'].rolling(window=90).mean()
        df['nvt_relative'] = market_cap / (df['close'] * volume_90d)
        
        # Pivot Points
        df['pivot'] = (df['high'] + df['low'] + df['close']) / 3
        df['r1'] = 2 * df['pivot'] - df['low']
        df['s1'] = 2 * df['pivot'] - df['high']
        
        # Support and Resistance
        df['support'] = df['low'].rolling(window=20).min()
        df['resistance'] = df['high'].rolling(window=20).max()
        
        # High/Low 60일
        df['high_60'] = df['high'].rolling(window=60).max()
        df['low_60'] = df['low'].rolling(window=60).min()
        
        # Fibonacci Levels
        high_20 = df['high'].rolling(window=20).max()
        low_20 = df['low'].rolling(window=20).min()
        diff = high_20 - low_20
        df['fibo_382'] = high_20 - (diff * 0.382)
        df['fibo_618'] = high_20 - (diff * 0.618)
        
        logger.info("✅ 기술적 지표 계산 완료 (실제 계산값 사용)")
        return df
        
    except Exception as e:
        logger.error(f"❌ 기술적 지표 계산 실패: {e}")
        return df

def save_ohlcv_to_db(ticker: str, df: pd.DataFrame):
    """OHLCV 데이터를 DB에 저장"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 티커에서 KRW- 제거
        clean_ticker = ticker.replace('KRW-', '') if ticker.startswith('KRW-') else ticker
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ohlcv (ticker, date, open, high, low, close, volume, 
                                 ma_20, ma_50, ma_200, rsi_14, mfi_14, 
                                 bb_upper, bb_middle, bb_lower,
                                 macd, macd_signal, macd_histogram,
                                 adx, plus_di, minus_di, atr,
                                 supertrend, supertrend_direction,
                                 donchian_high, donchian_low,
                                 volume_20ma, volume_ratio, volume_change_7_30, nvt_relative,
                                 pivot, r1, s1, support, resistance,
                                 high_60, low_60, fibo_382, fibo_618)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume,
                    ma_20=EXCLUDED.ma_20, ma_50=EXCLUDED.ma_50, ma_200=EXCLUDED.ma_200,
                    rsi_14=EXCLUDED.rsi_14, mfi_14=EXCLUDED.mfi_14,
                    bb_upper=EXCLUDED.bb_upper, bb_middle=EXCLUDED.bb_middle, bb_lower=EXCLUDED.bb_lower,
                    macd=EXCLUDED.macd, macd_signal=EXCLUDED.macd_signal, macd_histogram=EXCLUDED.macd_histogram,
                    adx=EXCLUDED.adx, plus_di=EXCLUDED.plus_di, minus_di=EXCLUDED.minus_di, atr=EXCLUDED.atr,
                    supertrend=EXCLUDED.supertrend, supertrend_direction=EXCLUDED.supertrend_direction,
                    donchian_high=EXCLUDED.donchian_high, donchian_low=EXCLUDED.donchian_low,
                    volume_20ma=EXCLUDED.volume_20ma, volume_ratio=EXCLUDED.volume_ratio,
                    volume_change_7_30=EXCLUDED.volume_change_7_30, nvt_relative=EXCLUDED.nvt_relative,
                    pivot=EXCLUDED.pivot, r1=EXCLUDED.r1, s1=EXCLUDED.s1,
                    support=EXCLUDED.support, resistance=EXCLUDED.resistance,
                    high_60=EXCLUDED.high_60, low_60=EXCLUDED.low_60,
                    fibo_382=EXCLUDED.fibo_382, fibo_618=EXCLUDED.fibo_618,
                    updated_at=CURRENT_TIMESTAMP
            """, (
                clean_ticker, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'],
                row.get('ma_20'), row.get('ma_50'), row.get('ma_200'), 
                row.get('rsi_14'), row.get('mfi_14'),
                row.get('bb_upper'), row.get('bb_middle'), row.get('bb_lower'),
                row.get('macd'), row.get('macd_signal'), row.get('macd_histogram'),
                row.get('adx'), row.get('plus_di'), row.get('minus_di'), row.get('atr'),
                row.get('supertrend'), row.get('supertrend_direction'),
                row.get('donchian_high'), row.get('donchian_low'),
                row.get('volume_20ma'), row.get('volume_ratio'), 
                row.get('volume_change_7_30'), row.get('nvt_relative'),
                row.get('pivot'), row.get('r1'), row.get('s1'),
                row.get('support'), row.get('resistance'),
                row.get('high_60'), row.get('low_60'),
                row.get('fibo_382'), row.get('fibo_618')
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ {clean_ticker} OHLCV 저장 완료: {len(df)}개 레코드")
        
    except Exception as e:
        logger.error(f"❌ {ticker} OHLCV 저장 실패: {e}")
        if conn:
            conn.rollback()
        raise

def save_static_indicators(ticker: str, latest_row: pd.Series):
    """정적 지표를 static_indicators 테이블에 저장 (실제 계산값 사용)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 티커에서 KRW- 제거
        clean_ticker = ticker.replace('KRW-', '') if ticker.startswith('KRW-') else ticker
        
        # Supertrend 신호 변환
        supertrend_signal = 'neutral'
        if latest_row.get('supertrend_direction') == 1:
            supertrend_signal = 'bull'
        elif latest_row.get('supertrend_direction') == -1:
            supertrend_signal = 'bear'
        
        cursor.execute("""
            INSERT INTO static_indicators (
                ticker, volume_change_7_30, nvt_relative, price, high_60, low_60,
                pivot, s1, r1, resistance, support, atr, adx, supertrend_signal,
                rsi_14, ma20, volume_ratio, volume, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(ticker) DO UPDATE SET
                volume_change_7_30=EXCLUDED.volume_change_7_30,
                nvt_relative=EXCLUDED.nvt_relative,
                price=EXCLUDED.price,
                high_60=EXCLUDED.high_60,
                low_60=EXCLUDED.low_60,
                pivot=EXCLUDED.pivot,
                s1=EXCLUDED.s1,
                r1=EXCLUDED.r1,
                resistance=EXCLUDED.resistance,
                support=EXCLUDED.support,
                atr=EXCLUDED.atr,
                adx=EXCLUDED.adx,
                supertrend_signal=EXCLUDED.supertrend_signal,
                rsi_14=EXCLUDED.rsi_14,
                ma20=EXCLUDED.ma20,
                volume_ratio=EXCLUDED.volume_ratio,
                volume=EXCLUDED.volume,
                updated_at=CURRENT_TIMESTAMP
        """, (
            clean_ticker,
            latest_row.get('volume_change_7_30'),
            latest_row.get('nvt_relative'),
            latest_row.get('close'),  # price
            latest_row.get('high_60'),
            latest_row.get('low_60'),
            latest_row.get('pivot'),
            latest_row.get('s1'),
            latest_row.get('r1'),
            latest_row.get('resistance'),
            latest_row.get('support'),
            latest_row.get('atr'),
            latest_row.get('adx'),
            supertrend_signal,
            latest_row.get('rsi_14'),
            latest_row.get('ma_20'),
            latest_row.get('volume_ratio'),
            latest_row.get('volume'),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ {clean_ticker} static_indicators 저장 완료")
        
    except Exception as e:
        logger.error(f"❌ {ticker} static_indicators 저장 실패: {e}")
        if conn:
            conn.rollback()
        raise

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 Makenaide OHLCV 수집기 시작")
        
        processed_count = 0
        failed_count = 0
        
        # SQS Records 처리
        for record in event.get('Records', []):
            try:
                # SQS 메시지 파싱
                message_body = json.loads(record['body'])
                ticker = message_body.get('ticker')
                
                if not ticker:
                    logger.warning("티커 정보가 없는 메시지")
                    continue
                
                logger.info(f"🔄 {ticker} 처리 시작")
                
                # 1. OHLCV 데이터 수집
                df = get_ohlcv_data(ticker, count=200)
                if df is None or df.empty:
                    logger.warning(f"⚠️ {ticker} OHLCV 데이터 없음")
                    failed_count += 1
                    continue
                
                # 2. 기술적 지표 계산
                df_with_indicators = calculate_technical_indicators(df)
                
                # 3. OHLCV 데이터 저장
                save_ohlcv_to_db(ticker, df_with_indicators)
                
                # 4. 정적 지표 저장 (최신값만)
                latest_row = df_with_indicators.iloc[-1]
                save_static_indicators(ticker, latest_row)
                
                processed_count += 1
                logger.info(f"✅ {ticker} 처리 완료")
                
            except Exception as e:
                logger.error(f"❌ 레코드 처리 실패: {e}")
                failed_count += 1
                continue
        
        # 결과 반환
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'OHLCV 수집 완료',
                'processed_count': processed_count,
                'failed_count': failed_count,
                'timestamp': datetime.now().isoformat()
            })
        }
        
        logger.info(f"✅ OHLCV 수집 완료: 성공 {processed_count}개, 실패 {failed_count}개")
        return result
        
    except Exception as e:
        logger.error(f"❌ OHLCV 수집 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        } 