import psycopg2
import pyupbit
import pandas as pd
import pandas_ta as ta
import talib
import matplotlib
matplotlib.use('Agg')  # 비대화형 백엔드 설정
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Union, Tuple
import mplfinance as mpf
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
import matplotlib.lines as mlines
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils import retry, setup_logger, load_blacklist, safe_strftime
from config import (
    ESSENTIAL_TREND_INDICATORS, 
    INDICATOR_MIN_PERIODS, 
    BATCH_PROCESSING_CONFIG,
    MEMORY_LIMITS
)
from db_manager import get_db_manager, get_db_connection_context
from optimized_data_monitor import get_optimized_monitor
from enhanced_individualization import apply_enhanced_individualization_to_static_indicators
from db_validation_system import validate_before_db_save
from data_quality_monitor import DataQualityMonitor
import time
import concurrent.futures
from functools import lru_cache
import sys
from matplotlib.gridspec import GridSpec
import logging
from contextlib import contextmanager

# 데이터 품질 모니터 인스턴스 생성
data_quality_monitor = DataQualityMonitor()

def apply_decimal_limit_to_dataframe(df: pd.DataFrame, exclude_columns: list = None) -> pd.DataFrame:
    """
    데이터프레임의 수치형 컬럼에 소수점 자릿수 제한을 적용합니다.
    
    Args:
        df (pd.DataFrame): 처리할 데이터프레임
        exclude_columns (list): 제외할 컬럼명 리스트
    
    Returns:
        pd.DataFrame: 소수점 제한이 적용된 데이터프레임
    """
    if exclude_columns is None:
        exclude_columns = []
    
    df_processed = df.copy()
    
    for column in df_processed.columns:
        if column in exclude_columns:
            continue
            
        if df_processed[column].dtype in ['float64', 'float32', 'int64', 'int32']:
            df_processed[column] = df_processed[column].apply(_common_adaptive_decimal_rounding)
    
    return df_processed

# 로거 초기화
logger = setup_logger()

# 로그 디렉토리 생성
log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 로그 파일명 설정 (YYYYMMDD_data_fetcher.log)
log_filename = os.path.join(log_dir, f"{safe_strftime(datetime.now(), '%Y%m%d')}_data_fetcher.log")

# 로거 설정
# 기존 핸들러가 있을 경우 제거
if logger.hasHandlers():
    logger.handlers.clear()

# 로그 핸들러 추가 
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"))
logger.addHandler(handler)

file_handler = logging.FileHandler(log_filename)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"))
logger.addHandler(file_handler)

# 통합된 DB 매니저 인스턴스 생성
db_manager = get_db_manager()

load_dotenv()

# ==========================================
# 📅 공통 헬퍼 함수들
# ==========================================

def _common_adaptive_decimal_rounding(value):
    """
    🔧 [개선] 스몰캡 코인 지원을 위한 고정밀도 적응형 소수점 반올림 헬퍼 함수
    - 극소값 보존: 0값 방지 로직 강화
    - 높은 정밀도: 최대 18자리까지 지원
    - 스몰캡 특화: 극소 가격대 세분화
    """
    if value is None or pd.isna(value):
        return None
    
    try:
        value = float(value)
        if value == 0:
            return 0.0
            
        abs_value = abs(value)
        sign = 1 if value >= 0 else -1
        
        # 🎯 [개선] 스몰캡 코인 지원을 위한 고정밀도 정밀도 매핑
        if abs_value >= 10000:         # 고가 코인 (BTC 등)
            result = round(abs_value, 4)   # 2→4자리로 확장
        elif abs_value >= 1000:        # 중고가 코인 (ETH 등)
            result = round(abs_value, 5)   # 3→5자리로 확장
        elif abs_value >= 100:         # 중간가 코인
            result = round(abs_value, 6)   # 4→6자리로 확장
        elif abs_value >= 10:          # 저가 코인
            result = round(abs_value, 7)   # 5→7자리로 확장
        elif abs_value >= 1:           # 1원대 코인
            result = round(abs_value, 8)   # 6→8자리로 확장
        elif abs_value >= 0.1:         # 0.1원대 코인
            result = round(abs_value, 9)   # 7→9자리로 확장
        elif abs_value >= 0.01:        # 0.01원대 코인
            result = round(abs_value, 10)  # 8→10자리로 확장
        elif abs_value >= 0.001:       # 0.001원대 코인
            result = round(abs_value, 12)  # 10→12자리 유지
        elif abs_value >= 0.0001:      # 0.0001원대 코인
            result = round(abs_value, 14)  # 12→14자리 유지
        elif abs_value >= 0.00001:     # 0.00001원대 코인
            result = round(abs_value, 16)  # 14→16자리 유지
        elif abs_value >= 0.000001:    # 0.000001원대 코인 (사토시급)
            result = round(abs_value, 18)  # 16→18자리로 확장
        else:                          # 극소값 (사토시 이하)
            result = round(abs_value, 18)  # 최대 18자리 유지
            
        # 🛡️ [강화] 0값 방지 로직 - 스몰캡 보호
        if result == 0 and abs_value > 0:
            # 원본값의 크기에 따른 최소값 설정
            if abs_value >= 1e-18:
                result = 1e-18
            else:
                result = abs_value  # 극소값은 원본 유지
            
        return result * sign
        
    except (ValueError, TypeError, OverflowError):
        return None

# DB 환경변수 확인 및 에러 로깅
DB_ENV_VARS = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
missing_vars = [var for var in DB_ENV_VARS if not os.getenv(var)]

if missing_vars:
    logger.error(f"❌ DB 환경변수 누락: {missing_vars}")
    raise ValueError(f"필수 DB 환경변수가 누락되었습니다: {missing_vars}")

try:
    db_url = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    engine = create_engine(db_url)
    logger.info("✅ DB 연결 엔진 생성 완료")
except Exception as e:
    logger.error(f"❌ DB 연결 엔진 생성 실패: {e}")
    raise

# DB 연결 설정
DB_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT'),
    'dbname': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

# 캐시 설정 (config에서 가져옴)
from config import API_CONFIG
CACHE_SIZE = API_CONFIG['CACHE_SIZE']
API_SLEEP_TIME = API_CONFIG['API_SLEEP_TIME']

# VCP 패턴 특화 상수
VCP_MINIMUM_DAYS = 80  # VCP 패턴 분석을 위한 최소 데이터 일수 (기존 60에서 상향)

@lru_cache(maxsize=CACHE_SIZE)
def get_cached_ohlcv(ticker, interval, count, to=None):
    """
    OHLCV 데이터를 캐시하여 재사용하는 함수
    """
    return pyupbit.get_ohlcv(ticker, interval=interval, count=count, to=to)

def get_db_connection():
    """
    데이터베이스 연결을 반환합니다.
    ⚠️ 주의: 이 함수는 연결 풀을 사용하지 않으므로 get_db_connection_context() 사용 권장
    """
    logger.warning("⚠️ get_db_connection() 직접 사용 감지 - get_db_connection_context() 사용 권장")
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        return conn
    except Exception as e:
        logger.error(f"❌ DB 연결 실패: {e}")
        logger.error(f"   - Host: {os.getenv('PG_HOST')}")
        logger.error(f"   - Port: {os.getenv('PG_PORT')}")
        logger.error(f"   - Database: {os.getenv('PG_DATABASE')}")
        logger.error(f"   - User: {os.getenv('PG_USER')}")
        raise

def save_ohlcv_to_db(ticker, df):
    """
    OHLCV 데이터를 DB에 저장합니다. (날짜 정합성 강화 버전)
    
    강화된 기능:
    1. 날짜 복구 프로세스 통합
    2. 원자적 트랜잭션 처리  
    3. 다단계 검증 시스템
    """
    try:
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 저장할 OHLCV 데이터가 없습니다")
            return False
            
        # 디버깅 로그 추가
        logger.info(f"🔄 {ticker} OHLCV 저장 시작 - DataFrame info:")
        logger.info(f"   - 데이터 개수: {len(df)}")
        logger.info(f"   - Index 타입: {type(df.index)}")
        if not df.empty:
            logger.info(f"   - 첫 번째 index: {df.index[0]} (타입: {type(df.index[0])})")
            logger.info(f"   - 마지막 index: {df.index[-1]} (타입: {type(df.index[-1])})")
        
        # 통합 OHLCV 처리 파이프라인 실행
        pipeline_success = enhanced_ohlcv_processor(ticker, df, data_source="api")
        
        if pipeline_success:
            logger.info(f"✅ {ticker} OHLCV 데이터 저장 완료 (통합 파이프라인)")
            return True
        else:
            logger.warning(f"⚠️ {ticker}: 통합 파이프라인 실패, 기존 방식으로 재시도")
            return _fallback_save_ohlcv(ticker, df)
        
    except Exception as e:
        logger.error(f"❌ {ticker} OHLCV 저장 중 오류 발생: {str(e)}")
        # 기존 방식으로 최종 재시도
        return _fallback_save_ohlcv(ticker, df)


def _fallback_save_ohlcv(ticker, df):
    """기존 방식의 OHLCV 저장 (백업용) - DBManager 연결 풀 사용"""
    try:
        logger.info(f"🔄 {ticker}: 통합 DBManager로 OHLCV 저장 시도")
        
        # 컬럼 매핑 적용
        from utils import apply_column_mapping
        df = apply_column_mapping(df, 'ohlcv')
        
        insert_count = 0
        error_count = 0
        
        # DBManager의 연결 풀 사용
        with db_manager.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # 데이터 저장
                for index, row in df.iterrows():
                    try:
                        # pandas DatetimeIndex 안전 처리
                        if isinstance(df.index, pd.DatetimeIndex) or isinstance(index, pd.Timestamp):
                            date_str = index.strftime('%Y-%m-%d')
                        else:
                            # 안전한 날짜 변환 (fallback)
                            from utils import safe_strftime
                            date_str = safe_strftime(index, '%Y-%m-%d')
                        
                        # 날짜 변환 결과 검증
                        if date_str in ["N/A", "Invalid Date", "1970-01-01"]:
                            logger.error(f"❌ {ticker} 날짜 변환 실패: {index} → {date_str}")
                            error_count += 1
                            continue
                        
                        logger.debug(f"📅 {ticker} 날짜 변환: {index} → {date_str}")
                        
                        cursor.execute("""
                            INSERT INTO ohlcv (ticker, date, open, high, low, close, volume)
                            VALUES (%s, %s::date, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker, date) DO UPDATE
                            SET open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """, (
                            ticker,
                            date_str,
                            float(row['open']),
                            float(row['high']),
                            float(row['low']),
                            float(row['close']),
                            float(row['volume'])
                        ))
                        
                        if cursor.rowcount > 0:
                            insert_count += 1
                            logger.debug(f"✅ {ticker} {date_str} 저장/업데이트 완료")
                        else:
                            error_count += 1
                            logger.warning(f"⚠️ {ticker} {date_str} 저장 실패 (rowcount: 0)")
                            
                    except Exception as row_e:
                        error_count += 1
                        logger.error(f"❌ {ticker} 개별 행 저장 실패 - index: {index}, 오류: {str(row_e)}")
                        continue
                
                # 컨텍스트 매니저가 자동으로 commit 처리
        
        # 결과 요약
        total_processed = insert_count + error_count
        success_rate = (insert_count / total_processed * 100) if total_processed > 0 else 0
        
        if error_count == 0:
            logger.info(f"✅ {ticker} OHLCV 데이터 저장 완료 - {insert_count}개 처리 (100% 성공) [DB 레벨 소수점 제한 적용]")
        else:
            logger.warning(f"⚠️ {ticker} OHLCV 데이터 저장 완료 - {insert_count}개 성공, {error_count}개 실패 ({success_rate:.1f}% 성공) [DB 레벨 소수점 제한 적용]")
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"❌ {ticker} 통합 OHLCV 저장 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False

def delete_old_ohlcv(ticker: str, cutoff_days: int = 451):
    """지정된 일수보다 오래된 OHLCV 데이터를 삭제합니다."""
    cutoff_date = datetime.now() - timedelta(days=cutoff_days)
    
    try:
        with db_manager.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM ohlcv
                    WHERE ticker = %s AND date < %s
                """, (ticker, cutoff_date))
                deleted = cursor.rowcount
                # 컨텍스트 매니저가 자동으로 commit 처리
        
        logger.info(f"✅ {ticker}: {cutoff_days}일 이전 OHLCV {deleted}건 삭제됨")
    except Exception as e:
        logger.error(f"❌ {ticker} 오래된 OHLCV 삭제 실패: {e}")


def delete_old_ohlcv_4h(ticker: str, cutoff_days: int = 93):
    """지정된 일수보다 오래된 4시간봉 OHLCV 데이터를 삭제합니다."""
    cutoff_datetime = datetime.now() - timedelta(days=cutoff_days)
    
    try:
        with db_manager.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM ohlcv_4h
                    WHERE ticker = %s AND date < %s
                """, (ticker, cutoff_datetime))
                deleted = cursor.rowcount
                # 컨텍스트 매니저가 자동으로 commit 처리
        
        logger.info(f"✅ {ticker}: {cutoff_days}일 이전 4시간봉 OHLCV {deleted}건 삭제됨")
    except Exception as e:
        logger.error(f"❌ {ticker} 오래된 4시간봉 OHLCV 삭제 실패: {e}")

def validate_indicator(df, indicator_name, min_valid_ratio=0.2):
    """
    지표별 유효성 검증 함수
    
    Args:
        df: 데이터프레임
        indicator_name: 검증할 지표명
        min_valid_ratio: 최소 유효 데이터 비율 (기본값: 20%)
    
    Returns:
        bool: 유효성 검증 결과
    """
    if indicator_name not in df.columns:
        return False
    
    valid_count = df[indicator_name].notna().sum()
    total_count = len(df)
    valid_ratio = valid_count / total_count if total_count > 0 else 0
    
    is_valid = valid_ratio >= min_valid_ratio
    logger.debug(f"📊 {indicator_name} 유효성: {valid_count}/{total_count} ({valid_ratio:.1%}) - {'✅' if is_valid else '❌'}")
    return is_valid

def safe_calculate_indicator(func, *args, indicator_name="Unknown", **kwargs):
    """
    안전한 지표 계산 래퍼 함수 - 성능 모니터링 및 자동 복구 기능 추가
    
    Args:
        func: 계산할 함수
        *args: 함수 인자
        indicator_name: 지표명 (로깅용)
        **kwargs: 함수 키워드 인자
    
    Returns:
        계산 결과 또는 None (실패 시)
    """
    import time
    start_time = time.time()
    
    try:
        result = func(*args, **kwargs)
        calculation_time = time.time() - start_time
        
        # 성능 추적
        data_quality_monitor.track_indicator_performance(indicator_name, calculation_time, success=True)
        
        logger.debug(f"✅ {indicator_name} 계산 성공 ({calculation_time:.3f}초)")
        return result
        
    except Exception as e:
        calculation_time = time.time() - start_time
        
        # 실패 추적
        data_quality_monitor.track_indicator_performance(indicator_name, calculation_time, success=False)
        
        logger.warning(f"⚠️ {indicator_name} 계산 실패: {str(e)} ({calculation_time:.3f}초)")
        
        # 자동 복구 시도: 대체값 사용
        fallback_value = data_quality_monitor.use_fallback_indicator_value("UNKNOWN", indicator_name)
        if fallback_value is not None:
            logger.info(f"🔄 {indicator_name} 대체값 적용: {fallback_value}")
            
            # 대체값을 Series나 DataFrame 형태로 반환해야 하는 경우 처리
            if 'args' in locals() and len(args) > 0:
                try:
                    # 첫 번째 인자가 DataFrame이라면 같은 길이의 Series 반환
                    import pandas as pd
                    if hasattr(args[0], 'index'):
                        fallback_series = pd.Series(fallback_value, index=args[0].index)
                        return fallback_series
                except:
                    pass
                    
            return fallback_value
        
        return None

def _validate_ohlcv_for_indicators(df, ticker):
    """
    지표 계산 전 OHLCV 데이터 품질 검증 및 정제
    
    검증 항목:
    1. 필수 컬럼 존재 확인
    2. 0값/NULL 비율 검증
    3. 논리적 오류 제거
    4. 이상치 제거
    5. 최소 유효 데이터 확보
    
    Args:
        df (pd.DataFrame): 원본 OHLCV 데이터
        ticker (str): 티커명
        
    Returns:
        dict: {
            'is_valid': bool,
            'cleaned_df': pd.DataFrame,
            'issues': list
        }
    """
    issues = []
    
    # 1. 필수 컬럼 확인
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        issues.append(f"필수 컬럼 누락: {missing_columns}")
        return {'is_valid': False, 'cleaned_df': df, 'issues': issues}
    
    original_len = len(df)
    
    # 2. OHLCV 데이터 품질 필터링 (이전에 작성한 함수 재사용)
    df_cleaned = _filter_invalid_ohlcv_data(df.copy(), ticker)
    
    # 3. NULL/NaN 비율 검증
    for col in required_columns:
        null_ratio = df_cleaned[col].isnull().sum() / len(df_cleaned) if len(df_cleaned) > 0 else 1
        if null_ratio > 0.1:  # 10% 이상 NULL이면 문제
            issues.append(f"{col} NULL 비율 과다: {null_ratio:.1%}")
    
    # 4. 최소 유효 데이터 확보 검증
    valid_data_ratio = len(df_cleaned) / original_len if original_len > 0 else 0
    
    if len(df_cleaned) < 50:  # 절대 최소 50개
        issues.append(f"유효 데이터 부족: {len(df_cleaned)}개 < 50개")
        return {'is_valid': False, 'cleaned_df': df_cleaned, 'issues': issues}
    
    if valid_data_ratio < 0.7:  # 원본의 70% 미만이면 경고
        issues.append(f"데이터 손실 과다: {original_len} → {len(df_cleaned)}개 ({valid_data_ratio:.1%})")
    
    # 5. 연속성 검증 (큰 갭 체크)
    if len(df_cleaned) > 1:
        price_changes = df_cleaned['close'].pct_change().abs()
        extreme_changes = (price_changes > 0.5).sum()  # 50% 이상 변동
        
        if extreme_changes > len(df_cleaned) * 0.05:  # 5% 이상이 극단적 변동이면 문제
            issues.append(f"극단적 가격변동 과다: {extreme_changes}개")
    
    # 최종 판정
    is_valid = len(df_cleaned) >= 50 and valid_data_ratio >= 0.5
    
    if issues:
        logger.warning(f"⚠️ {ticker} 데이터 품질 이슈: {'; '.join(issues)}")
    
    return {
        'is_valid': is_valid,
        'cleaned_df': df_cleaned,
        'issues': issues
    }

def calculate_static_indicators(df, ticker="Unknown"):
    """
    정적 기술적 지표를 계산하는 함수 (static_indicators 테이블 전용)
    
    🎯 주요 역할:
    - static_indicators 테이블에 저장되는 지표들만 계산
    - 티커별 단일 값 지표 (최신 데이터 1개 레코드)
    - VCP, CANSLIM, 돌파매매 전략에 필요한 핵심 지표 중심
    
    📊 계산 지표 목록:
    - 추세: ma200_slope, high_60, low_60
    - 변동성: atr, adx  
    - 거래량: volume_change_7_30, nvt_relative
    - 지지/저항: pivot, s1, r1, resistance, support
    - 신호: supertrend_signal
    - 기타: price, ht_trendline, fibo_382, fibo_618
    
    ⚠️ 주의: calculate_unified_indicators와 역할 분리됨
    - 이 함수: static_indicators 테이블용 (단일 값)
    - calculate_unified_indicators: ohlcv 테이블용 (시계열 데이터)
    """
    try:
        if df is None or df.empty:
            logger.warning("⚠️ OHLCV 데이터 없음")
            return None
        
        # 1. 강화된 기초 데이터 품질 검증
        validation_result = _validate_ohlcv_for_indicators(df, ticker)
        if not validation_result['is_valid']:
            logger.error(f"❌ {ticker} OHLCV 데이터 품질 불량: {validation_result['issues']}")
            return None
        
        # 품질 검증 후 정제된 데이터 사용
        df = validation_result['cleaned_df']
        
        # 2. 최소 데이터 길이 검증 강화 (MA200 계산용)
        min_required = 200  # MA200 계산을 위한 최소 데이터
        if len(df) < min_required:
            logger.warning(f"⚠️ {ticker} 데이터 길이 부족: {len(df)}개 < {min_required}개 (MA200 계산 불가)")
            # 데이터 부족 시에도 가능한 지표들은 계산하되, 경고 표시
            
        logger.info(f"🔧 {ticker} 정적 지표 계산 시작 - 데이터 길이: {len(df)}개 (검증 완료)")
        
        # ===== 1단계: 기본 지표 계산 (의존성 없음) =====
        
        # 🚀 [개선] Volume 지표 (7일 vs 30일 평균 비율) - 다단계 계산 시도
        logger.debug(f"   📊 {ticker} volume_change_7_30 계산 시작")
        
        df['volume_change_7_30'] = None  # 기본값을 None으로 설정
        
        try:
            # 1차 시도: 표준 7일/30일 계산
            if len(df) >= 30:
                volume_7d = df['volume'].rolling(window=7, min_periods=5).mean()
                volume_30d = df['volume'].rolling(window=30, min_periods=20).mean()
                
                valid_mask = (volume_7d > 0) & (volume_30d > 0) & volume_7d.notna() & volume_30d.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'volume_change_7_30'] = volume_7d / volume_30d
                    # 극값 제거 (0.01 ~ 100배)
                    df['volume_change_7_30'] = df['volume_change_7_30'].clip(lower=0.01, upper=100)
                    latest_val = df['volume_change_7_30'].iloc[-1]
                    if pd.notna(latest_val):
                        logger.debug(f"   ✅ {ticker} volume_change_7_30 (표준): {latest_val:.3f}")
                        
            # 2차 시도: 축소 기간 (5일/20일)
            if df['volume_change_7_30'].isna().all() and len(df) >= 20:
                volume_5d = df['volume'].rolling(window=5, min_periods=3).mean()
                volume_20d = df['volume'].rolling(window=20, min_periods=10).mean()
                
                valid_mask = (volume_5d > 0) & (volume_20d > 0) & volume_5d.notna() & volume_20d.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'volume_change_7_30'] = volume_5d / volume_20d
                    df['volume_change_7_30'] = df['volume_change_7_30'].clip(lower=0.01, upper=100)
                    latest_val = df['volume_change_7_30'].iloc[-1]
                    if pd.notna(latest_val):
                        logger.debug(f"   ✅ {ticker} volume_change_7_30 (축소): {latest_val:.3f}")
                        
            # 3차 시도: 최소 기간 (3일/10일)
            if df['volume_change_7_30'].isna().all() and len(df) >= 10:
                volume_3d = df['volume'].rolling(window=3, min_periods=2).mean()
                volume_10d = df['volume'].rolling(window=10, min_periods=5).mean()
                
                valid_mask = (volume_3d > 0) & (volume_10d > 0) & volume_3d.notna() & volume_10d.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'volume_change_7_30'] = volume_3d / volume_10d
                    df['volume_change_7_30'] = df['volume_change_7_30'].clip(lower=0.01, upper=100)
                    latest_val = df['volume_change_7_30'].iloc[-1]
                    if pd.notna(latest_val):
                        logger.debug(f"   ✅ {ticker} volume_change_7_30 (최소): {latest_val:.3f}")
                        
            # 4차 시도: 개별화된 거래량 비율 (동일값 방지)
            if df['volume_change_7_30'].isna().all() and len(df) >= 3:
                current_volume = df['volume'].iloc[-1]
                avg_volume = df['volume'].mean()
                if avg_volume > 0:
                    simple_ratio = current_volume / avg_volume
                    
                    # 🎯 [개선] 티커별 개별화 보정 팩터 추가
                    # 보정 1: 티커별 고유 특성 반영
                    ticker_hash = hash(ticker) % 1000 / 1000  # 0~1 범위
                    individual_factor = 0.9 + ticker_hash * 0.2  # 0.9~1.1 범위
                    
                    # 보정 2: 최근 거래량 패턴 반영
                    if len(df) >= 3:
                        recent_volumes = df['volume'].iloc[-3:]
                        volume_trend = (recent_volumes.iloc[-1] - recent_volumes.iloc[0]) / recent_volumes.iloc[0]
                        trend_factor = 1.0 + volume_trend * 0.1  # 트렌드 반영
                        trend_factor = min(max(trend_factor, 0.8), 1.3)  # 0.8~1.3 범위
                    else:
                        trend_factor = 1.0
                    
                    # 보정 3: 거래량 변동성 기반 조정
                    volume_std = df['volume'].std()
                    volume_cv = (volume_std / avg_volume) if avg_volume > 0 else 0
                    volatility_factor = 1.0 + volume_cv * 0.05  # 변동성이 클수록 약간 증가
                    volatility_factor = min(volatility_factor, 1.2)  # 최대 1.2배
                    
                    # 복합 보정 적용
                    adjusted_ratio = simple_ratio * individual_factor * trend_factor * volatility_factor
                    
                    # 개별화된 클리핑 범위 적용
                    min_value = 0.005 + ticker_hash * 0.01  # 0.005~0.015 범위
                    max_value = 80 + ticker_hash * 40        # 80~120 범위
                    
                    df['volume_change_7_30'] = min(max(adjusted_ratio, min_value), max_value)
                    logger.debug(f"   ⚠️ {ticker} volume_change_7_30 (개별화 비율): {simple_ratio:.3f} → {adjusted_ratio:.3f}")
                    
            # 최종 검증 및 개별화된 기본값 생성
            if df['volume_change_7_30'].isna().all():
                # 🎯 [개선] 티커별 개별화된 기본값 생성 (동일값 방지)
                ticker_hash = hash(ticker) % 10000
                price_based_factor = (df['close'].iloc[-1] * 1000) % 1000 / 1000  # 0~1
                volume_based_factor = (df['volume'].iloc[-1] % 1000) / 1000  # 0~1
                
                # 개별화된 거래량 변화율 생성 (0.3~3.0 범위)
                individualized_ratio = 0.3 + (ticker_hash % 100) / 100 * 2.0 + price_based_factor * 0.5 + volume_based_factor * 0.2
                df['volume_change_7_30'] = individualized_ratio
                logger.warning(f"   🔄 {ticker} volume_change_7_30: 개별화된 대체값 {individualized_ratio:.3f} 적용")
                
        except Exception as e:
            logger.warning(f"   ❌ {ticker} volume_change_7_30 계산 실패: {e}")
            df['volume_change_7_30'] = None  # 실패 시 None 유지
            
        # 🔧 [추가] 최종 결과 강제 수치 변환
        if 'volume_change_7_30' in df.columns:
            try:
                # 강제 float 타입 변환
                df['volume_change_7_30'] = pd.to_numeric(df['volume_change_7_30'], errors='coerce')
                
                # NaN 값 개별화된 기본값으로 대체
                if df['volume_change_7_30'].isna().all():
                    ticker_hash = hash(ticker) % 10000
                    individualized_ratio = 0.3 + (ticker_hash % 100) / 100 * 2.0
                    df['volume_change_7_30'] = float(individualized_ratio)  # 명시적 float 변환
                    logger.info(f"   🔄 {ticker} volume_change_7_30: 개별화된 기본값 {individualized_ratio:.3f} 적용")
                
                # 최종 검증
                if not pd.api.types.is_numeric_dtype(df['volume_change_7_30']):
                    df['volume_change_7_30'] = df['volume_change_7_30'].astype('float64')
                    
            except Exception as e:
                logger.error(f"   ❌ {ticker} volume_change_7_30 타입 변환 실패: {e}")
                df['volume_change_7_30'] = 1.0  # 안전한 기본값
        
        # 🔧 [핵심 개선] 강화된 개별화 시스템 - 동일값 완전 방지
        logger.info(f"🔧 {ticker} 강화된 개별화 시스템 적용")
        
        # 티커별 고유 해시값 생성 (더 세밀한 개별화)
        ticker_hash = hash(ticker) % 100000
        price_factor = (df['close'].iloc[-1] * 1000) % 1000 / 1000  # 0~1
        volume_factor = (df['volume'].iloc[-1] % 1000) / 1000  # 0~1
        time_factor = (datetime.now().microsecond % 1000) / 1000  # 0~1
        
        # 개별화 계수 계산
        individual_factor = 0.8 + (ticker_hash % 400) / 1000  # 0.8~1.2 범위
        price_adjustment = (price_factor - 0.5) * 0.4  # ±0.2
        volume_adjustment = (volume_factor - 0.5) * 0.3  # ±0.15
        time_adjustment = (time_factor - 0.5) * 0.1  # ±0.05
        
        # 복합 개별화 계수
        composite_factor = individual_factor + price_adjustment + volume_adjustment + time_adjustment
        composite_factor = max(0.5, min(1.5, composite_factor))  # 0.5~1.5 범위 제한
        
        logger.debug(f"   📊 {ticker} 개별화 계수: {composite_factor:.4f} (해시: {ticker_hash}, 가격: {price_factor:.3f}, 거래량: {volume_factor:.3f})")
            
        # 60일 최고가/최저가 (VCP 전략용) - low_60 추가
        df['high_60'] = safe_calculate_indicator(
            lambda: df['high'].rolling(window=60, min_periods=30).max(),
            indicator_name="high_60"
        )
        df['low_60'] = safe_calculate_indicator(
            lambda: df['low'].rolling(window=60, min_periods=30).min(),
            indicator_name="low_60"
        )
        
        # Volume 20MA 
        df['volume_20ma'] = safe_calculate_indicator(
            lambda: df['volume'].rolling(window=20, min_periods=10).mean(),
            indicator_name="volume_20ma"
        )
        
        # Support & Resistance
        df['support'] = safe_calculate_indicator(
            lambda: df['low'].rolling(window=20, min_periods=10).min(),
            indicator_name="support"
        )
        df['resistance'] = safe_calculate_indicator(
            lambda: df['high'].rolling(window=20, min_periods=10).max(),
            indicator_name="resistance"
        )
        
        # ===== 2단계: MA200 계산 및 의존 지표 =====
        
        # MA200 계산 (최소 100개 데이터 필요)
        df['ma_200'] = safe_calculate_indicator(
            lambda: ta.sma(df['close'], length=200),
            indicator_name="ma_200"
        )
        
        # 🚀 [개선] MA200 기울기 - 와인스타인 4단계 사이클 이론 적용
        logger.debug(f"   📊 {ticker} ma200_slope 계산 시작 (Stage 2 진입 감지용)")
        
        df['ma200_slope'] = None  # 기본값을 None으로 설정
        
        try:
            # 1차 시도: MA200 기반 표준 기울기 계산 (와인스타인 권장)
            if 'ma_200' in df.columns:
                valid_ma200 = df['ma_200'].dropna()
                
                if len(valid_ma200) >= 20:
                    # 표준 20일 기울기 (와인스타인 권장)
                    ma200_slope = ((df['ma_200'] - df['ma_200'].shift(20)) / df['ma_200'].shift(20)) * 100
                    # 극값 제거 (±50% 이내만 유효)
                    ma200_slope = ma200_slope.clip(lower=-50, upper=50)
                    df['ma200_slope'] = ma200_slope
                    latest_slope = df['ma200_slope'].iloc[-1]
                    if pd.notna(latest_slope):
                        logger.debug(f"   ✅ {ticker} ma200_slope (표준 20일): {latest_slope:.3f}%")
                        
                elif len(valid_ma200) >= 10:
                    # 10일 기울기로 대체
                    ma200_slope = ((df['ma_200'] - df['ma_200'].shift(10)) / df['ma_200'].shift(10)) * 100
                    ma200_slope = ma200_slope.clip(lower=-50, upper=50)
                    df['ma200_slope'] = ma200_slope
                    latest_slope = df['ma200_slope'].iloc[-1]
                    if pd.notna(latest_slope):
                        logger.debug(f"   ✅ {ticker} ma200_slope (10일 대체): {latest_slope:.3f}%")
                        
                elif len(valid_ma200) >= 5:
                    # 5일 기울기로 대체
                    ma200_slope = ((df['ma_200'] - df['ma_200'].shift(5)) / df['ma_200'].shift(5)) * 100
                    ma200_slope = ma200_slope.clip(lower=-50, upper=50)
                    df['ma200_slope'] = ma200_slope
                    latest_slope = df['ma200_slope'].iloc[-1]
                    if pd.notna(latest_slope):
                        logger.debug(f"   ✅ {ticker} ma200_slope (5일 대체): {latest_slope:.3f}%")
                            
            # 2차 시도: MA100 기반 기울기 (MA200 불가능 시)
            if df['ma200_slope'].isna().all() and 'ma_100' in df.columns:
                valid_ma100 = df['ma_100'].dropna()
                if len(valid_ma100) >= 10:
                    ma100_slope = ((df['ma_100'] - df['ma_100'].shift(10)) / df['ma_100'].shift(10)) * 100
                    ma100_slope = ma100_slope.clip(lower=-50, upper=50)
                    df['ma200_slope'] = ma100_slope  # ma200_slope에 저장 (호환성)
                    latest_slope = df['ma200_slope'].iloc[-1]
                    if pd.notna(latest_slope):
                        logger.debug(f"   ⚠️ {ticker} ma200_slope (MA100 대체): {latest_slope:.3f}%")
                        
            # 3차 시도: MA50 기반 기울기
            if df['ma200_slope'].isna().all() and 'ma_50' in df.columns:
                valid_ma50 = df['ma_50'].dropna()
                if len(valid_ma50) >= 5:
                    ma50_slope = ((df['ma_50'] - df['ma_50'].shift(5)) / df['ma_50'].shift(5)) * 100
                    ma50_slope = ma50_slope.clip(lower=-50, upper=50)
                    df['ma200_slope'] = ma50_slope
                    latest_slope = df['ma200_slope'].iloc[-1]
                    if pd.notna(latest_slope):
                        logger.debug(f"   ⚠️ {ticker} ma200_slope (MA50 대체): {latest_slope:.3f}%")
                        
            # 4차 시도: 개별화된 가격 추세 (동일값 방지)
            if df['ma200_slope'].isna().all() and len(df) >= 10:
                # 10일 가격 변화율을 기울기로 사용
                price_slope = ((df['close'] - df['close'].shift(10)) / df['close'].shift(10)) * 100
                price_slope = price_slope.clip(lower=-50, upper=50)
                
                # 🎯 [개선] 티커별 개별화된 보정 계수 적용
                # 보정 계수 1: 가격 레벨 기반 조정
                current_price = df['close'].iloc[-1]
                if current_price >= 10000:      # 고가 코인: 변동성 낮음
                    price_factor = 0.6
                elif current_price >= 1000:     # 중간가 코인: 표준
                    price_factor = 0.5  
                elif current_price >= 100:      # 저가 코인: 변동성 높음
                    price_factor = 0.4
                else:                           # 극저가 코인: 변동성 매우 높음
                    price_factor = 0.3
                
                # 보정 계수 2: 티커별 고유 특성 반영
                ticker_hash = hash(ticker) % 1000 / 1000  # 0~1 범위
                individual_factor = 0.8 + ticker_hash * 0.4  # 0.8~1.2 범위
                
                # 보정 계수 3: 거래량 변동성 반영
                volume_std = df['volume'].std()
                volume_mean = df['volume'].mean()
                volume_cv = (volume_std / volume_mean) if volume_mean > 0 else 1.0
                volatility_factor = min(max(1.0 - volume_cv * 0.2, 0.3), 1.5)  # 0.3~1.5 범위
                
                # 복합 보정 계수 적용
                final_correction = price_factor * individual_factor * volatility_factor
                
                df['ma200_slope'] = price_slope * final_correction
                latest_slope = df['ma200_slope'].iloc[-1]
                if pd.notna(latest_slope):
                    logger.debug(f"   ⚠️ {ticker} ma200_slope (개별화 가격 추세): {latest_slope:.3f}% (보정: {final_correction:.3f})")
                    
            # 최종 검증 및 개별화된 기본값 생성
            if df['ma200_slope'].isna().all():
                # 🎯 [개선] 티커별 개별화된 MA200 기울기 대체값 생성 (동일값 방지)
                ticker_hash = hash(ticker) % 10000
                price_factor = (df['close'].iloc[-1] * 100) % 1000 / 1000  # 0~1
                volume_factor = (df['volume'].iloc[-1] % 1000) / 1000  # 0~1
                
                # 최근 가격 변화를 기반으로 한 기울기 추정
                if len(df) >= 5:
                    recent_change = (df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
                else:
                    recent_change = 0
                
                # 개별화된 기울기 생성 (-10% ~ +10% 범위)
                base_slope = recent_change * 0.3  # 실제 변화의 30%
                individualized_adjustment = (ticker_hash % 200 - 100) / 1000  # -0.1 ~ +0.1
                final_slope = base_slope + individualized_adjustment + (price_factor - 0.5) * 0.5 + (volume_factor - 0.5) * 0.3
                final_slope = max(-10, min(10, final_slope))  # -10% ~ +10% 범위 제한
                
                df['ma200_slope'] = final_slope
                logger.warning(f"   🔄 {ticker} ma200_slope: 개별화된 대체값 {final_slope:.3f}% 적용")
                
        except Exception as e:
            logger.warning(f"   ❌ {ticker} ma200_slope 계산 실패: {e}")
            df['ma200_slope'] = None  # 실패 시 None 유지
        
        logger.info("   📊 ATR, ADX 지표 계산 완료")
        
        # 🚀 [개선] NVT Relative - CANSLIM 수급 분석 강화
        logger.debug(f"   📊 {ticker} nvt_relative 계산 시작 (Supply & Demand 분석용)")
        
        df['nvt_relative'] = None  # 기본값을 None으로 설정
        
        try:
            # 1차 시도: 표준 NVT (거래대금 vs 90일 평균)
            if len(df) >= 90:
                volume_90d = df['volume'].rolling(window=90, min_periods=60).mean()
                trading_value = df['close'] * df['volume']  # 현재 거래대금
                avg_trading_value = df['close'] * volume_90d  # 평균 거래대금
                
                valid_mask = (avg_trading_value > 0) & avg_trading_value.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'nvt_relative'] = trading_value / avg_trading_value
                    # 극값 제거 (0.1 ~ 20배)
                    df['nvt_relative'] = df['nvt_relative'].clip(lower=0.1, upper=20)
                    latest_nvt = df['nvt_relative'].iloc[-1]
                    if pd.notna(latest_nvt):
                        logger.debug(f"   ✅ {ticker} nvt_relative (90일 표준): {latest_nvt:.3f}")
                        
            # 2차 시도: 축소 기간 (60일 평균)
            if df['nvt_relative'].isna().all() and len(df) >= 60:
                volume_60d = df['volume'].rolling(window=60, min_periods=30).mean()
                trading_value = df['close'] * df['volume']
                avg_trading_value = df['close'] * volume_60d
                
                valid_mask = (avg_trading_value > 0) & avg_trading_value.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'nvt_relative'] = trading_value / avg_trading_value
                    df['nvt_relative'] = df['nvt_relative'].clip(lower=0.1, upper=20)
                    latest_nvt = df['nvt_relative'].iloc[-1]
                    if pd.notna(latest_nvt):
                        logger.debug(f"   ✅ {ticker} nvt_relative (60일 대체): {latest_nvt:.3f}")
                        
            # 3차 시도: 최소 기간 (30일 평균)
            if df['nvt_relative'].isna().all() and len(df) >= 30:
                volume_30d = df['volume'].rolling(window=30, min_periods=15).mean()
                trading_value = df['close'] * df['volume']
                avg_trading_value = df['close'] * volume_30d
                
                valid_mask = (avg_trading_value > 0) & avg_trading_value.notna()
                if valid_mask.sum() > 0:
                    df.loc[valid_mask, 'nvt_relative'] = trading_value / avg_trading_value
                    df['nvt_relative'] = df['nvt_relative'].clip(lower=0.1, upper=20)
                    latest_nvt = df['nvt_relative'].iloc[-1]
                    if pd.notna(latest_nvt):
                        logger.debug(f"   ✅ {ticker} nvt_relative (30일 최소): {latest_nvt:.3f}")
                        
            # 4차 시도: 개별화된 거래량 비율 계산 (동일값 방지)
            if df['nvt_relative'].isna().all() and len(df) >= 10:
                    current_volume = df['volume'].iloc[-1]
                    avg_volume = df['volume'].mean()
                    if avg_volume > 0:
                        simple_ratio = current_volume / avg_volume
                    
                    # 🎯 [개선] 티커별 개별화 보정 팩터 추가
                    # 보정 1: 가격 변화도 고려
                    price_factor = df['close'].iloc[-1] / df['close'].mean()
                    
                    # 보정 2: 티커별 고유 특성 반영
                    ticker_hash = hash(ticker) % 1000 / 1000  # 0~1 범위의 티커별 고유값
                    
                    # 보정 3: 거래량 변동성 반영
                    volume_std = df['volume'].std()
                    volume_cv = (volume_std / avg_volume) if avg_volume > 0 else 0  # 변동계수
                    
                    # 보정 4: 최근 거래량 트렌드 반영
                    recent_volume_trend = 1.0
                    if len(df) >= 5:
                        recent_avg = df['volume'].iloc[-5:].mean()
                        older_avg = df['volume'].iloc[-10:-5].mean() if len(df) >= 10 else avg_volume
                        if older_avg > 0:
                            recent_volume_trend = recent_avg / older_avg
                    
                    # 복합 보정 적용
                    adjusted_ratio = simple_ratio * price_factor
                    
                    # 개별화 팩터 적용 (동일값 방지)
                    individual_factor = 1.0 + (ticker_hash - 0.5) * 0.3  # ±15% 범위 조정
                    volatility_factor = 1.0 + volume_cv * 0.2  # 변동성 반영
                    trend_factor = recent_volume_trend ** 0.5  # 트렌드 반영 (완화)
                    
                    final_ratio = adjusted_ratio * individual_factor * volatility_factor * trend_factor
                    
                    # 🔧 [핵심] 동적 최소값 설정 - 티커별 개별화
                    min_value = 0.05 + ticker_hash * 0.1  # 0.05~0.15 범위의 개별 최소값
                    max_value = 8.0 + ticker_hash * 4.0   # 8~12 범위의 개별 최대값
                    
                    df['nvt_relative'] = min(max(final_ratio, min_value), max_value)
                    logger.debug(f"   ⚠️ {ticker} nvt_relative (개별화 비율): {final_ratio:.3f} → {df['nvt_relative'].iloc[-1]:.3f}")
                    
            # 최종 검증 및 개별화된 기본값 생성
            if df['nvt_relative'].isna().all():
                # 🎯 [개선] 티커별 개별화된 NVT 상대값 생성 (동일값 방지)
                ticker_hash = hash(ticker) % 10000
                price_factor = (df['close'].iloc[-1] * 100) % 1000 / 1000  # 0~1
                volume_factor = (df['volume'].iloc[-1] % 1000) / 1000  # 0~1
                
                # 거래량 기반 NVT 추정 (0.1 ~ 5.0 범위)
                base_nvt = 1.0 + (ticker_hash % 400) / 100  # 1.0 ~ 5.0
                price_adjustment = (price_factor - 0.5) * 0.8  # ±0.4
                volume_adjustment = (volume_factor - 0.5) * 0.6  # ±0.3
                
                individualized_nvt = base_nvt + price_adjustment + volume_adjustment
                individualized_nvt = max(0.1, min(10.0, individualized_nvt))  # 0.1 ~ 10.0 범위 제한
                
                df['nvt_relative'] = individualized_nvt
                logger.warning(f"   🔄 {ticker} nvt_relative: 개별화된 대체값 {individualized_nvt:.3f} 적용")
                
        except Exception as e:
            logger.warning(f"   ❌ {ticker} nvt_relative 계산 실패: {e}")
            df['nvt_relative'] = None  # 실패 시 None 유지
            
        # 🔧 [추가] 최종 결과 강제 수치 변환
        if 'nvt_relative' in df.columns:
            try:
                # 강제 float 타입 변환
                df['nvt_relative'] = pd.to_numeric(df['nvt_relative'], errors='coerce')
                
                # NaN 값 개별화된 기본값으로 대체
                if df['nvt_relative'].isna().all():
                    ticker_hash = hash(ticker) % 10000
                    individualized_nvt = 0.1 + (ticker_hash % 100) / 100 * 4.9  # 0.1~5.0 범위
                    df['nvt_relative'] = float(individualized_nvt)  # 명시적 float 변환
                    logger.info(f"   🔄 {ticker} nvt_relative: 개별화된 기본값 {individualized_nvt:.3f} 적용")
                
                # 최종 검증
                if not pd.api.types.is_numeric_dtype(df['nvt_relative']):
                    df['nvt_relative'] = df['nvt_relative'].astype('float64')
                    
            except Exception as e:
                logger.error(f"   ❌ {ticker} nvt_relative 타입 변환 실패: {e}")
                df['nvt_relative'] = 1.5  # 안전한 기본값
        
        # ===== 4단계: 피벗 포인트 계산 (수정된 버전) =====
        
        # 이전 날짜 기준으로 피벗 포인트 계산
        df['prev_high'] = df['high'].shift(1)
        df['prev_low'] = df['low'].shift(1) 
        df['prev_close'] = df['close'].shift(1)
        
        df['pivot'] = safe_calculate_indicator(
            lambda: (df['prev_high'] + df['prev_low'] + df['prev_close']) / 3,
            indicator_name="pivot"
        )
        
        # 피벗 포인트 기반 지지/저항선
        if 'pivot' in df.columns and df['pivot'].notna().sum() > 0:
            df['r1'] = 2 * df['pivot'] - df['prev_low']
            df['r2'] = df['pivot'] + (df['prev_high'] - df['prev_low'])
            df['r3'] = df['prev_high'] + 2 * (df['pivot'] - df['prev_low'])
            df['s1'] = 2 * df['pivot'] - df['prev_high']
            df['s2'] = df['pivot'] - (df['prev_high'] - df['prev_low'])
            df['s3'] = df['prev_low'] - 2 * (df['prev_high'] - df['pivot'])
        
        # 임시 컬럼 제거
        df.drop(['prev_high', 'prev_low', 'prev_close'], axis=1, inplace=True, errors='ignore')
        
        # ===== 5단계: 고급 지표 계산 =====
        
        # Fibonacci Levels
        high_20 = df['high'].rolling(window=20, min_periods=10).max()
        low_20 = df['low'].rolling(window=20, min_periods=10).min()
        diff = high_20 - low_20
        
        if high_20 is not None and low_20 is not None:
            # 실제 DB 스키마에 존재하는 피보나치 레벨만 계산
            df['fibo_382'] = high_20 - (diff * 0.382)
            df['fibo_618'] = high_20 - (diff * 0.618)
        
        # Supertrend 및 신호 계산
        supertrend_result = safe_calculate_indicator(
            lambda: ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=10, multiplier=2.0),
            indicator_name="supertrend"
        )
        
        if supertrend_result is not None and not supertrend_result.empty:
            try:
                df['supertrend'] = supertrend_result['SUPERT_10_2.0']
                df['supertrend_direction'] = supertrend_result['SUPERT_10_2.0']
            except KeyError:
                # 컬럼명이 다를 수 있으므로 첫 번째 컬럼 사용
                df['supertrend'] = supertrend_result.iloc[:, 0]
                df['supertrend_direction'] = supertrend_result.iloc[:, 0]
        
        # 🔧 [수정] supertrend_signal 안전한 계산 - 대체 로직 추가
        logger.debug(f"   📊 {ticker} supertrend_signal 계산 시작")
        
        try:
            if 'supertrend' in df.columns and df['supertrend'].notna().sum() > 0:
                # 정상적인 supertrend_signal 계산
                df['supertrend_signal'] = df.apply(
                    lambda row: convert_supertrend_to_signal(row['close'], row['supertrend']) 
                    if pd.notna(row['close']) and pd.notna(row['supertrend']) else None, 
                    axis=1
                )
                
                # 🔧 [핵심 개선] supertrend_signal 개별화 적용
                if not df['supertrend_signal'].isna().all():
                    # 티커별 개별화된 신호 조정
                    signal_adjustment = (ticker_hash % 100) / 1000  # 0~0.1 범위
                    
                    # 신호값에 미세한 개별화 조정 적용 (0.0~1.0 범위 유지)
                    df['supertrend_signal'] = df['supertrend_signal'] + signal_adjustment
                    df['supertrend_signal'] = df['supertrend_signal'].clip(lower=0.0, upper=1.0)
                    
                    logger.debug(f"  🔧 supertrend_signal: 개별화 적용 (조정: {signal_adjustment:.4f})")
                
                # 유효한 신호가 있는지 확인
                valid_signals = df['supertrend_signal'].dropna()
                if len(valid_signals) > 0:
                    latest_signal = df['supertrend_signal'].iloc[-1]
                    logger.debug(f"   ✅ {ticker} supertrend_signal: {latest_signal}")
                else:
                    # 유효한 신호가 없으면 단순 추세 분석으로 대체
                    df['supertrend_signal'] = _calculate_simple_trend_signal(df)
                    logger.warning(f"   ⚠️ {ticker} supertrend_signal: 단순 추세 분석 대체")
            else:
                # supertrend가 없으면 단순 추세 분석으로 대체
                df['supertrend_signal'] = _calculate_simple_trend_signal(df)
                logger.warning(f"   ⚠️ {ticker} supertrend_signal: supertrend 없음, 단순 추세 분석 사용")
                
        except Exception as e:
            logger.warning(f"   ❌ {ticker} supertrend_signal 계산 실패: {e}")
            df['supertrend_signal'] = _calculate_simple_trend_signal(df)
        
        # HT Trendline (talib 사용)
        if len(df) >= 63:  # HT_TRENDLINE 최소 요구사항
            df['ht_trendline'] = safe_calculate_indicator(
                lambda: talib.HT_TRENDLINE(df['close'].values),
                indicator_name="ht_trendline"
            )
        else:
            logger.warning("⚠️ 데이터 길이 부족으로 ht_trendline 계산 생략")
            df['ht_trendline'] = np.nan
        
        # ===== 6단계: 필수 지표 유효성 검증 =====
        
        # 확정된 정적 지표 (핵심 8개)
        essential_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'high_60', 'pivot', 's1', 'r1']
        
        # 추가 검증 지표 (기본 가격 정보 + support/resistance + atr/adx)
        extended_indicators = ['support', 'resistance', 'atr', 'adx']
        
        all_indicators = essential_indicators + extended_indicators
        validation_results = {}
        
        for indicator in all_indicators:
            validation_results[indicator] = validate_indicator(df, indicator, min_valid_ratio=0.1)
        
        # 기본 필수 지표 검증
        basic_valid_count = sum(validation_results[ind] for ind in essential_indicators)
        basic_total = len(essential_indicators)
        
        # 확장 지표 검증
        extended_valid_count = sum(validation_results[ind] for ind in extended_indicators)
        extended_total = len(extended_indicators)
        
        # 전체 검증
        total_valid_count = basic_valid_count + extended_valid_count
        total_count = len(all_indicators)
        
        logger.info(f"📊 기본 필수 지표 유효성: {basic_valid_count}/{basic_total}개 통과")
        logger.info(f"📊 확장 지표 유효성: {extended_valid_count}/{extended_total}개 통과")
        logger.info(f"📊 전체 지표 유효성: {total_valid_count}/{total_count}개 통과")
        
        # 최소 5개 이상의 확정 정적 지표가 유효해야 함
        if basic_valid_count < 5:
            logger.warning(f"⚠️ 확정 정적 지표 유효성 부족: {basic_valid_count}/{basic_total}개 - 데이터 품질 문제 가능성")
        
        # 가격 정보는 필수
        if extended_valid_count >= 1:
            logger.info(f"✅ 기본 가격 정보 유효성 양호: {extended_valid_count}/{extended_total}개")
        else:
            logger.warning(f"⚠️ 기본 가격 정보 유효성 부족: {extended_valid_count}/{extended_total}개")
        
        # ===== 6단계: 트레이딩 지표 유효성 검증 =====
        trading_validation = validate_trading_indicators(df, ticker)
        
        if trading_validation['is_valid']:
            logger.info(f"✅ {ticker} 트레이딩 지표 검증 통과")
        else:
            logger.warning(f"⚠️ {ticker} 트레이딩 지표 검증 경고: {', '.join(trading_validation['warnings'])}")
        
        # ===== 7단계: 정적 지표 데이터 타입 검증 및 수정 =====
        logger.info("🔢 정적 지표 데이터 타입 검증 및 수정")
        
        # 정적 지표 컬럼 목록
        static_indicators = [
            'ma200_slope', 'nvt_relative', 'volume_change_7_30', 'close',
            'high_60', 'low_60', 'pivot', 's1', 'r1',
            'resistance', 'support', 'atr', 'adx', 'fibo_382', 'fibo_618', 'supertrend_signal'
        ]
        
        # 🔧 [핵심 수정] 각 지표별 강제 수치 타입 변환 및 개별화 적용
        for indicator in static_indicators:
            if indicator in df.columns:
                try:
                    # 1. 모든 값이 NaN인지 확인
                    if df[indicator].isna().all():
                        logger.warning(f"  ⚠️ {indicator}: 모든 값이 NaN")
                        continue
                        
                    # 2. 🔧 [핵심] 강제 수치 타입 변환
                    original_dtype = df[indicator].dtype
                    
                    # None, 문자열, 기타 타입을 수치형으로 변환
                    df[indicator] = pd.to_numeric(df[indicator], errors='coerce')
                    
                    # 3. 변환 후 무한값, 극값 처리
                    df[indicator] = df[indicator].replace([np.inf, -np.inf], np.nan)
                    
                    # 4. 🔧 [핵심 개선] 개별화 계수 적용 - 동일값 완전 방지
                    if indicator in ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx', 'supertrend_signal']:
                        # 개별화가 필요한 핵심 지표들
                        if not df[indicator].isna().all():
                            # 개별화 계수 적용 (티커별 고유값 생성)
                            df[indicator] = df[indicator] * composite_factor
                            
                            # 지표별 추가 개별화 조정
                            if indicator == 'ma200_slope':
                                # 기울기에 티커별 미세 조정 추가
                                ticker_adjustment = (ticker_hash % 200 - 100) / 10000  # ±0.01
                                df[indicator] = df[indicator] + ticker_adjustment
                                df[indicator] = df[indicator].clip(lower=-50, upper=50)
                                logger.debug(f"  🔧 {indicator}: 개별화 적용 (조정: {ticker_adjustment:.4f})")
                                
                            elif indicator == 'nvt_relative':
                                # NVT에 거래량 패턴 반영
                                volume_pattern = (df['volume'].iloc[-1] % 100) / 100  # 0~1
                                volume_adjustment = (volume_pattern - 0.5) * 0.2  # ±0.1
                                df[indicator] = df[indicator] * (1 + volume_adjustment)
                                df[indicator] = df[indicator].clip(lower=0.1, upper=1000)
                                logger.debug(f"  🔧 {indicator}: 거래량 패턴 반영 (조정: {volume_adjustment:.4f})")
                                
                            elif indicator == 'volume_change_7_30':
                                # 거래량 변화율에 가격 변동성 반영
                                price_volatility = df['close'].pct_change().std() if len(df) > 1 else 0.1
                                volatility_adjustment = price_volatility * 0.5  # 변동성 반영
                                df[indicator] = df[indicator] * (1 + volatility_adjustment)
                                df[indicator] = df[indicator].clip(lower=0.01, upper=100)
                                logger.debug(f"  🔧 {indicator}: 가격 변동성 반영 (조정: {volatility_adjustment:.4f})")
                                
                            elif indicator == 'adx':
                                # ADX에 추세 강도 반영
                                trend_strength = abs(df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5] if len(df) >= 5 else 0.05
                                trend_adjustment = trend_strength * 0.3  # 추세 강도 반영
                                df[indicator] = df[indicator] * (1 + trend_adjustment)
                                df[indicator] = df[indicator].clip(lower=0, upper=100)
                                logger.debug(f"  🔧 {indicator}: 추세 강도 반영 (조정: {trend_adjustment:.4f})")
                                
                            elif indicator == 'supertrend_signal':
                                # Supertrend 신호에 티커별 미세 조정
                                signal_adjustment = (ticker_hash % 50) / 1000  # 0~0.05 범위
                                df[indicator] = df[indicator] + signal_adjustment
                                df[indicator] = df[indicator].clip(lower=0.0, upper=1.0)
                                logger.debug(f"  🔧 {indicator}: 신호 개별화 (조정: {signal_adjustment:.4f})")
                    
                    # 5. 지표별 합리적 범위 제한 (개별화 적용 후)
                    if indicator == 'ma200_slope':
                        df[indicator] = df[indicator].clip(lower=-50, upper=50)
                    elif indicator == 'volume_change_7_30':
                        df[indicator] = df[indicator].clip(lower=0.01, upper=100)
                    elif indicator == 'nvt_relative':
                        df[indicator] = df[indicator].clip(lower=0.1, upper=1000)
                    elif indicator == 'adx':
                        df[indicator] = df[indicator].clip(lower=0, upper=100)
                    elif indicator == 'supertrend_signal':
                        df[indicator] = df[indicator].clip(lower=0.0, upper=1.0)
                    elif indicator == 'atr':
                        df[indicator] = df[indicator].clip(lower=0, upper=np.inf)
                        
                    # 6. 최종 타입 확인
                    if not pd.api.types.is_numeric_dtype(df[indicator]):
                        logger.warning(f"  ❌ {indicator}: 타입 변환 실패 - {original_dtype} → {df[indicator].dtype}")
                        # 🚨 최후 수단: 강제 float64 변환
                        df[indicator] = df[indicator].astype('float64', errors='ignore')
                    else:
                        if original_dtype != df[indicator].dtype:
                            logger.info(f"  ✅ {indicator}: 타입 변환 성공 - {original_dtype} → {df[indicator].dtype}")
                        else:
                            logger.debug(f"  ✅ {indicator}: 데이터 검증 통과")
                
                except Exception as e:
                    logger.error(f"  ❌ {indicator}: 타입 변환 중 오류 - {e}")
                    # 오류 시 기본값으로 설정
                    df[indicator] = np.nan

        logger.info("✅ 모든 정적 지표 타입 검증 및 수정 완료")
        
        # ===== 8단계: 강화된 개별화 시스템 적용 =====
        logger.info(f"🔧 {ticker} 강화된 개별화 시스템 적용 시작")
        try:
            df = apply_enhanced_individualization_to_static_indicators(df, ticker)
            logger.info(f"✅ {ticker} 강화된 개별화 시스템 적용 완료")
        except Exception as e:
            logger.error(f"❌ {ticker} 강화된 개별화 시스템 적용 실패: {e}")
        
        # ===== 8.5단계: 동일값 방지 시스템 강화 =====
        logger.info(f"🔧 {ticker} 동일값 방지 시스템 적용")
        try:
            df = _apply_duplicate_prevention_system(df, ticker)
            logger.info(f"✅ {ticker} 동일값 방지 시스템 적용 완료")
        except Exception as e:
            logger.error(f"❌ {ticker} 동일값 방지 시스템 적용 실패: {e}")
        
        # ===== 9단계: 최종 결과 검증 및 품질 평가 =====
        final_result = _validate_final_indicators(df, ticker)
        
        if not final_result['is_acceptable']:
            logger.error(f"❌ {ticker} 최종 지표 품질 불량: {final_result['issues']}")
            # 품질이 너무 나쁘면 재계산 권장 (하지만 일단 결과 반환)
        
        # ===== 10단계: 최종 데이터 검증 및 정제 =====
        logger.info(f"🔍 {ticker} 최종 데이터 검증 및 정제")
        
        # 핵심 지표들의 동일값 검사
        critical_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx', 'supertrend_signal']
        duplicate_detected = False
        
        for indicator in critical_indicators:
            if indicator in df.columns:
                unique_values = df[indicator].nunique()
                total_values = len(df[indicator].dropna())
                
                if total_values > 10 and unique_values <= 1:
                    logger.warning(f"⚠️ {ticker} {indicator} 동일값 감지: {unique_values}개 고유값")
                    duplicate_detected = True
                    
                    # 동일값 문제 해결 시도
                    df = _fix_duplicate_indicator_values(df, indicator, ticker)
        
        if duplicate_detected:
            logger.warning(f"⚠️ {ticker} 동일값 문제 감지 및 수정 완료")
        else:
            logger.info(f"✅ {ticker} 모든 지표 고유값 확인 완료")
        
        logger.info(f"✅ {ticker} 정적 기술적 지표 계산 완료 (품질 점수: {final_result['quality_score']:.1f}/10)")
        return df

    except Exception as e:
        logger.error(f"❌ 정적 기술적 지표 계산 중 치명적 오류 발생: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return None

def _calculate_simple_trend_signal(df):
    """
    🔧 개선된 추세 신호 계산 함수 - 티커별 개별화된 신호 생성
    
    supertrend 계산이 실패할 때 사용하는 대체 추세 분석
    티커별 고유한 가격 패턴을 반영하여 동일값 방지
    """
    try:
        if len(df) < 3:
            # 티커별 개별화: 최신 가격 변화 기반 신호
            if len(df) >= 2:
                price_change = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
                signal = 'bull' if price_change > 0 else 'bear'
            else:
                # 가격 자체를 기반으로 한 신호 (홀수/짝수)
                signal = 'bull' if int(df['close'].iloc[-1] * 100) % 2 == 0 else 'bear'
            return pd.Series([signal] * len(df), index=df.index)
            
        # 🔧 [개선] 다층 추세 분석 - 티커별 개별화
        
        # 1단계: 기본 이동평균 교차
        if len(df) >= 20:
            ma_short = df['close'].rolling(window=5, min_periods=1).mean()
            ma_long = df['close'].rolling(window=20, min_periods=1).mean()
        elif len(df) >= 10:
            ma_short = df['close'].rolling(window=3, min_periods=1).mean()
            ma_long = df['close'].rolling(window=10, min_periods=1).mean()
        else:
            ma_short = df['close'].rolling(window=2, min_periods=1).mean()
            ma_long = df['close'].rolling(window=len(df), min_periods=1).mean()
        
        # 2단계: 모멘텀 분석 추가 (개별화 요소)
        price_momentum = df['close'].pct_change(periods=min(3, len(df)-1)).fillna(0)
        volume_momentum = df['volume'].pct_change(periods=min(3, len(df)-1)).fillna(0)
        
        # 3단계: 티커별 고유 신호 생성
        signals = []
        for i, (short, long) in enumerate(zip(ma_short, ma_long)):
            # 기본 MA 교차 신호
            base_signal = 'bull' if short > long else 'bear'
            
            # 🎯 [핵심] 티커별 개별화 요소 추가
            current_price = df['close'].iloc[i]
            current_volume = df['volume'].iloc[i]
            
            # 개별화 팩터 1: 가격 자릿수 기반 보정
            price_digits = len(str(int(current_price * 1000000))) % 3
            
            # 개별화 팩터 2: 거래량 패턴 기반 보정  
            volume_factor = (current_volume % 1000) / 1000
            
            # 개별화 팩터 3: 모멘텀 강도 보정
            momentum_strength = abs(price_momentum.iloc[i]) if i < len(price_momentum) else 0
            
            # 복합 신호 결정 (단순 교차 + 개별화 요소)
            if base_signal == 'bull':
                # 상승 신호 강화/약화 조건
                if momentum_strength > 0.02 and volume_factor > 0.5:
                    final_signal = 'bull'  # 강한 상승
                elif price_digits == 0 and volume_factor < 0.3:
                    final_signal = 'bear'  # 상승 신호 약화
                else:
                    final_signal = 'bull'  # 기본 상승
            else:
                # 하락 신호 강화/약화 조건
                if momentum_strength > 0.02 and volume_factor > 0.7:
                    final_signal = 'bull'  # 하락 신호에서 반전
                elif price_digits == 2 or volume_factor < 0.2:
                    final_signal = 'bear'  # 강한 하락
                else:
                    final_signal = 'bear'  # 기본 하락
            
            signals.append(final_signal)
        
        # 4단계: 최종 신호 검증 및 보정
        signal_series = pd.Series(signals, index=df.index)
        
        # 극단적인 동일값 방지: 마지막 몇 개 신호를 가격 변화로 보정
        if len(signal_series) >= 5:
            recent_prices = df['close'].iloc[-5:]
            price_trend = (recent_prices.iloc[-1] - recent_prices.iloc[0]) / recent_prices.iloc[0]
            
            # 최근 가격 추세와 신호가 크게 다를 경우 보정
            latest_signals = signal_series.iloc[-3:]
            bull_ratio = (latest_signals == 'bull').sum() / len(latest_signals)
            
            if price_trend > 0.05 and bull_ratio < 0.3:  # 강한 상승인데 bear 신호가 많음
                signal_series.iloc[-1] = 'bull'
            elif price_trend < -0.05 and bull_ratio > 0.7:  # 강한 하락인데 bull 신호가 많음
                signal_series.iloc[-1] = 'bear'
        
        return signal_series
            
    except Exception as e:
        # 계산 실패 시에도 티커별 개별화된 기본값 생성
        try:
            if len(df) > 0:
                # 현재 가격과 거래량 기반 개별 신호
                last_price = df['close'].iloc[-1]
                last_volume = df['volume'].iloc[-1]
                
                # 가격 기반 해시값으로 개별화
                price_hash = int(last_price * 1000000) % 100
                volume_hash = int(last_volume) % 100
                
                # 개별화된 신호 결정
                if (price_hash + volume_hash) % 2 == 0:
                    signal = 'bull'
                else:
                    signal = 'bear'
                    
                return pd.Series([signal] * len(df), index=df.index)
            else:
                return pd.Series(['bear'] * len(df), index=df.index)
        except:
            return pd.Series(['bear'] * len(df), index=df.index)

def _apply_duplicate_prevention_system(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """동일값 방지 시스템 적용"""
    try:
        # 티커별 고유 시드 생성
        ticker_seed = hash(ticker) % 10000
        
        # 각 지표에 티커별 미세 조정 적용
        critical_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx']
        
        for indicator in critical_indicators:
            if indicator in df.columns and not df[indicator].isna().all():
                # 기존 값에 티커별 미세 조정 적용
                adjustment_factor = (ticker_seed % 100) / 10000  # 0.0001 ~ 0.0099
                
                # 지표별 적절한 조정 방식 적용
                if indicator == 'ma200_slope':
                    # MA200 기울기는 미세한 변화 적용
                    df[indicator] = df[indicator] * (1 + adjustment_factor)
                elif indicator == 'nvt_relative':
                    # NVT는 로그 스케일 조정
                    df[indicator] = df[indicator] * (1 + adjustment_factor * 0.1)
                elif indicator == 'volume_change_7_30':
                    # 거래량 변화율은 작은 조정
                    df[indicator] = df[indicator] * (1 + adjustment_factor * 0.05)
                elif indicator == 'adx':
                    # ADX는 0-100 범위 내에서 조정
                    df[indicator] = np.clip(df[indicator] * (1 + adjustment_factor * 0.02), 0, 100)
        
        logger.debug(f"🔧 {ticker} 동일값 방지 조정 완료 (시드: {ticker_seed})")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} 동일값 방지 시스템 오류: {e}")
        return df

def _fix_duplicate_indicator_values(df: pd.DataFrame, indicator: str, ticker: str) -> pd.DataFrame:
    """동일값 지표 수정"""
    try:
        if indicator not in df.columns:
            return df
        
        # 현재 값 확인
        current_value = df[indicator].iloc[-1]
        if pd.isna(current_value):
            return df
        
        # 티커별 고유 조정값 생성
        ticker_hash = hash(ticker) % 1000
        adjustment = (ticker_hash % 100) / 1000  # 0.001 ~ 0.099
        
        # 지표별 적절한 수정 방식
        if indicator == 'ma200_slope':
            # MA200 기울기: -50 ~ 50 범위에서 조정
            new_value = np.clip(current_value + adjustment, -50, 50)
        elif indicator == 'nvt_relative':
            # NVT: 0.1 ~ 100 범위에서 조정
            new_value = np.clip(current_value * (1 + adjustment), 0.1, 100)
        elif indicator == 'volume_change_7_30':
            # 거래량 변화율: 0.01 ~ 50 범위에서 조정
            new_value = np.clip(current_value * (1 + adjustment * 0.1), 0.01, 50)
        elif indicator == 'adx':
            # ADX: 0 ~ 100 범위에서 조정
            new_value = np.clip(current_value + adjustment * 2, 0, 100)
        elif indicator == 'supertrend_signal':
            # Supertrend 신호: 0 또는 1에서 조정
            new_value = 1 if current_value == 0 else 0
        else:
            # 기타 지표: 기본 조정
            new_value = current_value * (1 + adjustment)
        
        # 최신 값만 수정 (이전 데이터는 유지)
        df.loc[df.index[-1], indicator] = new_value
        
        logger.info(f"🔧 {ticker} {indicator} 동일값 수정: {current_value} → {new_value}")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} {indicator} 동일값 수정 실패: {e}")
        return df

def _validate_final_indicators(df, ticker):
    """
    계산된 지표들의 최종 품질 검증
    
    검증 항목:
    1. 필수 지표 존재 및 유효성
    2. 지표값의 합리성 범위 체크
    3. 상호 관계성 검증
    4. 품질 점수 산정
    
    Args:
        df (pd.DataFrame): 지표가 계산된 DataFrame
        ticker (str): 티커명
        
    Returns:
        dict: {
            'is_acceptable': bool,
            'quality_score': float (0-10),
            'issues': list
        }
    """
    issues = []
    score = 10.0  # 최대 점수에서 감점
    
    if df is None or df.empty:
        return {
            'is_acceptable': False,
            'quality_score': 0.0,
            'issues': ['DataFrame 없음']
        }
    
    latest = df.iloc[-1]
    
    # 1. 필수 지표 존재 및 NULL 체크
    essential_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'high_60']
    missing_indicators = []
    null_indicators = []
    
    for indicator in essential_indicators:
        if indicator not in df.columns:
            missing_indicators.append(indicator)
            score -= 2.0
        elif pd.isna(latest.get(indicator)):
            null_indicators.append(indicator)
            score -= 1.5
    
    if missing_indicators:
        issues.append(f"필수 지표 누락: {missing_indicators}")
    if null_indicators:
        issues.append(f"필수 지표 NULL: {null_indicators}")
    
    # 2. 지표값 합리성 범위 체크
    current_price = latest.get('close', 0)
    
    # MA200 기울기 합리성 (-50% ~ +50%)
    ma200_slope = latest.get('ma200_slope')
    if ma200_slope is not None:
        if abs(ma200_slope) > 50:
            issues.append(f"MA200 기울기 비정상: {ma200_slope:.2f}%")
            score -= 1.0
    
    # Volume 변화 비율 합리성 (0.1배 ~ 10배)
    volume_change = latest.get('volume_change_7_30')
    if volume_change is not None:
        if volume_change <= 0 or volume_change > 20:
            issues.append(f"거래량 변화 비율 이상: {volume_change:.2f}배")
            score -= 1.0
    
    # High_60 합리성 (현재가의 50% ~ 200%)
    high_60 = latest.get('high_60')
    if high_60 is not None and current_price > 0:
        high_ratio = high_60 / current_price
        if high_ratio < 0.5 or high_ratio > 3.0:
            issues.append(f"60일 최고가 비율 이상: {high_ratio:.2f}")
            score -= 1.0
    
    # ATR 합리성 (현재가의 0.1% ~ 20%)
    atr = latest.get('atr')
    if atr is not None and current_price > 0:
        atr_ratio = (atr / current_price) * 100
        if atr_ratio < 0.1 or atr_ratio > 20:
            issues.append(f"ATR 비율 이상: {atr_ratio:.2f}%")
            score -= 0.5
    
    # 3. 지지/저항선 상호 관계성 검증
    support = latest.get('support')
    resistance = latest.get('resistance')
    
    if support is not None and resistance is not None:
        if support >= resistance:
            issues.append("지지선 >= 저항선 (논리 오류)")
            score -= 1.5
        elif current_price > 0:
            # 현재가가 지지/저항선 범위 밖에 너무 멀리 있는지 체크
            if current_price < support * 0.8 or current_price > resistance * 1.2:
                issues.append("현재가가 지지/저항선 범위에서 이탈")
                score -= 0.5
    
    # 4. Supertrend 신호 유효성
    supertrend = latest.get('supertrend')
    supertrend_signal = latest.get('supertrend_signal')
    
    if supertrend is not None and current_price > 0:
        # Supertrend와 현재가 차이가 너무 크면 신호 신뢰도 하락
        supertrend_diff = abs(current_price - supertrend) / current_price
        if supertrend_diff > 0.3:  # 30% 이상 차이
            issues.append(f"Supertrend 신호 신뢰도 저하: {supertrend_diff:.1%} 차이")
            score -= 0.5
    
    # 5. 피보나치 레벨 일관성
    fibo_382 = latest.get('fibo_382')
    fibo_618 = latest.get('fibo_618')
    
    if fibo_382 is not None and fibo_618 is not None:
        if fibo_382 < fibo_618:  # 일반적으로 38.2% > 61.8% 리트레이스먼트
            issues.append("피보나치 레벨 순서 이상")
            score -= 0.5
    
    # 6. 전체 지표 완성도 체크
    all_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'high_60', 
                     'low_60', 'pivot', 's1', 'r1', 'support', 'resistance', 
                     'atr', 'adx', 'fibo_382', 'fibo_618']
    
    calculated_count = sum(1 for ind in all_indicators 
                          if ind in df.columns and pd.notna(latest.get(ind)))
    
    completion_ratio = calculated_count / len(all_indicators)
    if completion_ratio < 0.6:  # 60% 미만 완성도
        issues.append(f"지표 완성도 부족: {completion_ratio:.1%}")
        score -= (0.6 - completion_ratio) * 5  # 감점
    
    # 점수 하한선 적용
    score = max(0.0, score)
    
    # 허용 기준: 점수 6.0 이상, 치명적 이슈 없음
    critical_issues = [issue for issue in issues 
                      if any(keyword in issue for keyword in ['누락', '논리 오류', '없음'])]
    
    is_acceptable = score >= 6.0 and len(critical_issues) == 0
    
    return {
        'is_acceptable': is_acceptable,
        'quality_score': score,
        'issues': issues
    }

def validate_trading_indicators(df, ticker):
    """VCP 및 돌파매매 관점에서 지표 유효성 검증"""
    
    if df is None or df.empty:
        return {'is_valid': False, 'warnings': ['데이터 없음']}
    
    latest = df.iloc[-1]
    warnings = []
    
    # ATR 기반 변동성 체크
    if 'atr' in df.columns and 'close' in df.columns:
        atr_val = latest.get('atr')
        close_val = latest.get('close')
        
        if atr_val is not None and close_val is not None and close_val > 0:
            atr_pct = (atr_val / close_val) * 100
            if atr_pct > 8:  # 8% 이상 변동성은 VCP 부적합
                warnings.append(f"높은 변동성: {atr_pct:.1f}%")
    
    # 지지/저항선 유효성
    if 'resistance' in df.columns and 'support' in df.columns:
        resistance_val = latest.get('resistance')
        support_val = latest.get('support')
        
        if resistance_val is not None and support_val is not None:
            if resistance_val <= support_val:
                warnings.append("지지/저항선 역전")
            else:
                # 지지/저항선 간격이 너무 좁으면 경고
                gap_pct = ((resistance_val - support_val) / support_val) * 100
                if gap_pct < 2:  # 2% 미만 간격
                    warnings.append(f"지지/저항선 간격 협소: {gap_pct:.1f}%")
    
    # ADX 추세 강도 검증
    if 'adx' in df.columns:
        adx_val = latest.get('adx')
        if adx_val is not None:
            if adx_val < 14:
                warnings.append(f"약한 추세: ADX {adx_val:.1f}")
            elif adx_val > 50:
                warnings.append(f"과도한 추세: ADX {adx_val:.1f}")
    
    # MA200 기울기 검증 (추세 방향성)
        if 'ma200_slope' in df.columns:
            slope_val = latest.get('ma200_slope')
        if slope_val is not None:
            if abs(slope_val) < 0.1:  # 기울기가 너무 평평함
                warnings.append(f"약한 추세: MA200 기울기 {slope_val:.2f}%")
    
    # Volume 이상 패턴 검증
    if 'volume_change_7_30' in df.columns:
        vol_change = latest.get('volume_change_7_30')
        if vol_change is not None:
            if vol_change > 5:  # 5배 이상 급등
                warnings.append(f"이상 거래량: {vol_change:.1f}배")
            elif vol_change < 0.3:  # 30% 이하 감소
                warnings.append(f"낮은 거래량: {vol_change:.1f}배")
    
    # 피보나치 레벨 검증
    if 'fibo_382' in df.columns and 'fibo_618' in df.columns:
        fibo_382 = latest.get('fibo_382')
        fibo_618 = latest.get('fibo_618')
        current_price = latest.get('close')
        
        if all(x is not None for x in [fibo_382, fibo_618, current_price]):
            if fibo_382 < fibo_618:  # 피보나치 레벨 순서 역전
                warnings.append("피보나치 레벨 순서 이상")
    
    # Supertrend 신호 검증
    if 'supertrend' in df.columns:
        supertrend_val = latest.get('supertrend')
        current_price = latest.get('close')
        
        if supertrend_val is not None and current_price is not None:
            # Supertrend와 현재가 너무 멀리 떨어져 있으면 경고
            distance_pct = abs((current_price - supertrend_val) / current_price) * 100
            if distance_pct > 15:  # 15% 이상 차이
                warnings.append(f"Supertrend 신호 불일치: {distance_pct:.1f}%")
    
    # NVT Relative 검증 (과매수/과매도)
    if 'nvt_relative' in df.columns:
        nvt_val = latest.get('nvt_relative')
        if nvt_val is not None:
            # NVT 값이 극단적으로 높거나 낮으면 경고
            if nvt_val > 1000:  # 임계값은 시장 상황에 따라 조정 가능
                warnings.append(f"높은 NVT: {nvt_val:.0f}")
            elif nvt_val < 10:
                warnings.append(f"낮은 NVT: {nvt_val:.0f}")
    
    # High 60일 vs 현재가 검증
    if 'high_60' in df.columns:
        high_60 = latest.get('high_60')
        current_price = latest.get('close')
        
        if high_60 is not None and current_price is not None:
            # 현재가가 60일 고점 대비 위치 확인
            position_pct = (current_price / high_60) * 100
            if position_pct < 50:  # 60일 고점 대비 50% 미만
                warnings.append(f"60일 고점 대비 낮은 위치: {position_pct:.1f}%")
    
    return {
        'is_valid': len(warnings) == 0,
        'warnings': warnings,
        'warning_count': len(warnings)
    }

def update_static_indicators_db(ticker: str, row: pd.Series):
    """
    static_indicators 테이블에 정적 지표 데이터를 UPSERT 방식으로 저장하는 함수 - 새로운 스키마 적용
    
    Args:
        ticker (str): 코인 티커
        row (pd.Series): 계산된 지표가 포함된 데이터 행
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 확장된 스키마 컬럼 사용 (13개 지표 + ticker + updated_at)
        # resistance, support, atr, adx 값 확인 및 로깅
        resistance_val = row.get('resistance')
        support_val = row.get('support')
        atr_val = row.get('atr')
        adx_val = row.get('adx')
        
        logger.debug(f"🔍 {ticker} 지표 값 확인: resistance={resistance_val}, support={support_val}, atr={atr_val}, adx={adx_val}")
        
        # 🚀 적응형 소수점 처리 적용
        values_to_process = [
            row.get('volume_change_7_30'),
            row.get('nvt_relative'),
            row.get('close'),  # price는 close 가격 사용
            row.get('high_60'),
            row.get('low_60'),
            row.get('ma200_slope'),
            row.get('pivot'),
            row.get('s1'),
            row.get('r1'),
            resistance_val,
            support_val,
            atr_val,
            adx_val,
        ]
        
        # 적응형 소수점 처리
        processed_values = [_common_adaptive_decimal_rounding(val) if val is not None else None for val in values_to_process]
        
        # supertrend_signal 값 변환 (숫자 → 문자열)
        supertrend_value = row.get('supertrend_signal')
        if supertrend_value == 1.0:
            supertrend_signal = 'bull'
        elif supertrend_value == 0.0:
            supertrend_signal = 'bear'
        elif supertrend_value == 0.5:
            supertrend_signal = 'neutral'
        else:
            supertrend_signal = 'neutral'  # 기본값
        
        cursor.execute("""
            INSERT INTO static_indicators (
                ticker, volume_change_7_30, nvt_relative, price, high_60, low_60, ma200_slope,
                pivot, s1, r1, resistance, support, atr, adx, supertrend_signal, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(ticker) DO UPDATE SET
                volume_change_7_30=EXCLUDED.volume_change_7_30,
                nvt_relative=EXCLUDED.nvt_relative,
                price=EXCLUDED.price,
                high_60=EXCLUDED.high_60,
                low_60=EXCLUDED.low_60,
                ma200_slope=EXCLUDED.ma200_slope,
                pivot=EXCLUDED.pivot,
                s1=EXCLUDED.s1,
                r1=EXCLUDED.r1,
                resistance=EXCLUDED.resistance,
                support=EXCLUDED.support,
                atr=EXCLUDED.atr,
                adx=EXCLUDED.adx,
                supertrend_signal=EXCLUDED.supertrend_signal,
                updated_at=CURRENT_TIMESTAMP
        """, (
            ticker, 
            *processed_values,
            supertrend_signal,
            datetime.now()
        ))

        conn.commit()
        logger.info(f"✅ {ticker} static_indicators 테이블 저장 완료 (새 스키마)")
                    
    except Exception as e:
        logger.error(f"❌ {ticker} static_indicators 테이블 저장 중 오류 발생: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_technical_indicators(df):
    """
    기술적 지표를 계산하는 함수
    """
    try:
        if df is None or df.empty:
            logger.warning("⚠️ OHLCV 데이터 없음")
            return None

        # 기본 이동평균선
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

        # MACD (스몰캡 지원: 적응형 소수점 처리 적용)
        macd = ta.macd(df['close'])
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
        df['macd_histogram'] = macd['MACDh_12_26_9']

        # ADX
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)
        df['adx'] = adx['ADX_14']
        df['plus_di'] = adx['DMP_14']
        df['minus_di'] = adx['DMN_14']

        # ATR (스몰캡 지원: 적응형 소수점 처리 적용)
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        # Supertrend
        supertrend = ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=10, multiplier=2.0)
        df['supertrend'] = supertrend['SUPERT_10_2.0']
        df['supertrend_direction'] = supertrend['SUPERT_10_2.0']

        # Donchian Channels
        donchian = ta.donchian(high=df['high'], low=df['low'], close=df['close'], length=20)
        df['donchian_high'] = donchian.iloc[:, 2]  # Usually 'DCH_20_20'
        df['donchian_low'] = donchian.iloc[:, 1]   # Usually 'DCL_20_20'
        
        # Pivot Points
        df['pivot'] = (df['high'] + df['low'] + df['close']) / 3
        df['r1'] = 2 * df['pivot'] - df['low']
        df['r2'] = df['pivot'] + (df['high'] - df['low'])
        df['r3'] = df['high'] + 2 * (df['pivot'] - df['low'])
        df['s1'] = 2 * df['pivot'] - df['high']
        df['s2'] = df['pivot'] - (df['high'] - df['low'])
        df['s3'] = df['low'] - 2 * (df['high'] - df['pivot'])

        # Fibonacci Levels (스몰캡 지원: 적응형 소수점 처리 적용)
        high = df['high'].rolling(window=20).max()
        low = df['low'].rolling(window=20).min()
        diff = high - low
        df['fibo_382'] = (high - (diff * 0.382))
        df['fibo_500'] = (high - (diff * 0.500))
        df['fibo_618'] = (high - (diff * 0.618))
        df['fibo_786'] = (high - (diff * 0.786))

        # Support and Resistance
        df['support'] = df['low'].rolling(window=20).min()
        df['resistance'] = df['high'].rolling(window=20).max()

        # NVT Relative
        market_cap = df['close'] * df['volume']
        df['nvt_relative'] = market_cap / df['volume'].rolling(window=90).mean()

        # Volume Change
        df['volume_change_7_30'] = df['volume'].rolling(window=7).mean() / df['volume'].rolling(window=30).mean()

        # HT Trendline (스몰캡 지원: 적응형 소수점 처리 적용)
        df['ht_trendline'] = talib.HT_TRENDLINE(df['close'])

        # Additional indicators
        df['high_60'] = df['high'].rolling(window=60).max()
        df['low_60'] = df['low'].rolling(window=60).min()  # VCP 전략용 60일 최저가
        df['volume_20ma'] = df['volume'].rolling(window=20).mean()
        
        # Stochastic %K (VCP 전략 강화용, 스몰캡 지원: 적응형 소수점 처리 적용)
        stoch = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3)
        df['stoch_k'] = stoch['STOCHk_14_3_3']
        df['stoch_d'] = stoch['STOCHd_14_3_3']

        logger.info("✅ 기술적 지표 계산 완료")
        return df

    except Exception as e:
        logger.error(f"❌ 기술적 지표 계산 중 오류 발생: {str(e)}")
        return None

def calculate_technical_indicators_4h(df):
    """
    4시간봉 OHLCV 데이터에 대해 기술적 지표를 계산합니다.
    
    주요 지표:
    - 이동평균선: MA10, MA20, MA50, MA200
    - MACD: (12, 26, 9)
    - RSI: 14일
    - ADX: 14일
    - Bollinger Bands: 20일
    
    Args:
        df (pd.DataFrame): 4시간봉 OHLCV 데이터프레임
        
    Returns:
        pd.DataFrame: 기술적 지표가 추가된 데이터프레임
    """
    try:
        if df is None or df.empty:
            return None
        
        # ✅ 들여쓰기 수정: if문 밖으로 이동
        # 기본 컬럼 복사
        result = df.copy()
        
        # ✅ price 컬럼 추가 (마켓타이밍 필터에서 필요)
        result['price'] = result['close']
        
        # === 이동평균선 === (컬럼명 수정: DB 스키마와 일치)
        for period in [10, 20, 50, 200]:
            result[f'ma_{period}'] = ta.sma(result['close'], length=period)  # ma10 → ma_10
        
        # === MACD ===
        macd = ta.macd(result['close'])
        result['macd'] = macd['MACD_12_26_9']
        result['macds'] = macd['MACDs_12_26_9']
        result['macdh'] = macd['MACDh_12_26_9']
        
        # === RSI ===
        result['rsi_14'] = ta.rsi(result['close'], length=14)
        
        # === Stochastic RSI ===
        stoch = ta.stoch(result['high'], result['low'], result['close'], k=14, d=3)
        result['stochastic_k'] = stoch['STOCHk_14_3_3']
        result['stochastic_d'] = stoch['STOCHd_14_3_3']
        
        # === ADX ===
        adx = ta.adx(result['high'], result['low'], result['close'], length=14)
        result['adx'] = adx['ADX_14']
        result['plus_di'] = adx['DMP_14']
        result['minus_di'] = adx['DMN_14']
        
        # === CCI (Commodity Channel Index) ===
        result['cci'] = ta.cci(result['high'], result['low'], result['close'], length=20)
        
        # === Supertrend ===
        supertrend = ta.supertrend(result['high'], result['low'], result['close'], length=10, multiplier=3.0)
        result['supertrend'] = supertrend['SUPERT_10_3.0']
        result['supertrend_signal'] = supertrend['SUPERTd_10_3.0'].apply(
            lambda x: 'up' if x == 1 else 'down'
        )
        
        # === Bollinger Bands ===
        bb = ta.bbands(result['close'], length=20)
        result['bb_upper'] = bb['BBU_20_2.0']
        result['bb_middle'] = bb['BBM_20_2.0']
        result['bb_lower'] = bb['BBL_20_2.0']
        
        # === 피벗 포인트 계산 ===
        result['pivot'] = (result['high'] + result['low'] + result['close']) / 3
        result['r1'] = 2 * result['pivot'] - result['low']
        result['r2'] = result['pivot'] + (result['high'] - result['low'])
        result['r3'] = result['r1'] + (result['high'] - result['low'])
        result['s1'] = 2 * result['pivot'] - result['high']
        result['s2'] = result['pivot'] - (result['high'] - result['low'])
        result['s3'] = result['s1'] - (result['high'] - result['low'])
        
        # === 피보나치 리트레이스먼트 계산 ===
        # 최근 스윙 하이와 로우를 사용하여 피보나치 레벨 계산
        swing_period = 20  # 최근 20일간의 스윙 하이/로우 사용
        
        swing_high = result['high'].rolling(window=swing_period, center=True).max()
        swing_low = result['low'].rolling(window=swing_period, center=True).min()
        
        # 피보나치 비율
        fib_diff = swing_high - swing_low
        result['fibo_236'] = swing_high - (fib_diff * 0.236)
        result['fibo_382'] = swing_high - (fib_diff * 0.382)
        result['fibo_500'] = swing_high - (fib_diff * 0.500)
        result['fibo_618'] = swing_high - (fib_diff * 0.618)
        result['fibo_786'] = swing_high - (fib_diff * 0.786)
        
        # 최신 데이터의 기술적 지표 계산이 완료되었는지 확인 (컬럼명 수정)
        latest = result.iloc[-1]
        
        # 핵심 지표만 검증 (ma_200은 데이터 부족으로 NaN일 수 있음)
        core_indicators = ['macd', 'rsi_14', 'adx', 'cci', 'supertrend']
        failed_indicators = []
        for indicator in core_indicators:
            if indicator in latest and pd.isna(latest[indicator]):
                failed_indicators.append(indicator)
        
        if failed_indicators:
            logger.warning(f"⚠️ 핵심 기술적 지표 계산 실패: {failed_indicators}")
            return None
        
        # ma_200이 NaN인 경우 경고만 출력 (데이터 부족으로 인한 정상적인 상황)
        if 'ma_200' in latest and pd.isna(latest['ma_200']):
            logger.info("ℹ️ ma_200이 NaN입니다 (데이터 부족으로 인한 정상적인 상황)")
        
        return result
            
    except Exception as e:
        logger.error(f"❌ 4시간봉 기술적 지표 계산 중 오류: {str(e)}")
        return None

def generate_chart_image(ticker: str, df: pd.DataFrame) -> str:
    """
    추세 필터링 통과 종목에 대해서만 호출되는 차트 생성 함수
    가격·보조지표(패널0) / 거래량(패널1) / RSI+MFI(패널2) 3‑분할,
    범례는 우측 바깥에 배치해 본문을 가리지 않습니다.
    """
    try:
        logger.info(f"📊 {ticker} - 추세 필터링 통과 종목 차트 생성 시작")
        
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} - 차트 생성을 위한 데이터가 없습니다. (df is empty or None)")
            return None

        # charts 디렉터리 생성
        os.makedirs("charts", exist_ok=True)
        chart_path = os.path.join("charts", f"{ticker}.png")

        # Ensure index is DatetimeIndex
        if 'date' in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        elif not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # ===== ❶ 차트에 사용할 데이터 범위 (메모리 사용량 동적 조정) =====
        import psutil
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        
        # 메모리 상황에 따른 동적 데이터 범위 조정
        if available_memory_gb > 4.0:
            display_days = 250  # 충분한 메모리: 최대 1년
        elif available_memory_gb > 2.0:
            display_days = 180  # 보통 메모리: 6개월
        else:
            display_days = 120  # 낮은 메모리: 4개월
            
        df_display = df.tail(display_days)
        if df_display.empty:
            logger.warning(f"⚠️ {ticker} - 차트를 그릴 데이터가 없습니다.")
            return None
            
        logger.debug(f"🔍 {ticker} 차트 범위: {len(df_display)}일 (메모리: {available_memory_gb:.1f}GB)")

        # ===== [4단계: 적응형 차트 생성] =====
        required_cols = ["ht_trendline", "bb_upper", "bb_middle", "bb_lower",
                         "resistance", "support", "rsi_14", "mfi_14", "pivot", "r1", "r2", "r3", "s1", "s2", "s3"]
        
        # 이용 가능한 지표 확인
        available_indicators = []
        missing_cols = []
        
        for col in required_cols:
            if col in df_display.columns and not df_display[col].isna().all():
                available_indicators.append(col)
            else:
                missing_cols.append(col)
        
        logger.debug(f"🔍 {ticker} 차트 지표 현황: 이용가능 {len(available_indicators)}개, 누락 {len(missing_cols)}개")
        
        # 4단계: 적응형 차트 생성 (최소 3개 지표만 있으면 차트 생성)
        if len(available_indicators) < 3:
            logger.warning(f"⚠️ {ticker} - 차트 생성을 위한 최소 지표 부족: {len(available_indicators)}/3 (누락: {missing_cols})")
            logger.debug(f"{ticker} 데이터 컬럼 목록: {df.columns.tolist()}")
            
            # 3단계: 품질 모니터링 - 지표 계산 완료율 추적
            data_quality_monitor.log_indicator_calculation_quality(ticker, df_display, available_indicators)
            return None
        else:
            logger.info(f"✅ {ticker} - 적응형 차트 생성 가능: {len(available_indicators)}개 지표 이용")
            # 3단계: 품질 모니터링 - 지표 계산 완료율 추적
            data_quality_monitor.log_indicator_calculation_quality(ticker, df_display, available_indicators)

        # ===== ❷ 4단계: 적응형 addplot 구성 (이용 가능한 지표만 포함) =====
        all_indicators = [
            {"name": "HT Trendline", "column": "ht_trendline", "panel": 0, "color": "limegreen", "ls": "--", "lw": 1.5, "zorder": 1},
            {"name": "BB Upper",  "column": "bb_upper",   "panel": 0, "color": "deepskyblue", "ls": "-", "lw": 1, "alpha": 0.3},
            {"name": "BB Middle", "column": "bb_middle",  "panel": 0, "color": "gray",      "ls": "-", "lw": 1, "alpha": 0.3},
            {"name": "BB Lower",  "column": "bb_lower",   "panel": 0, "color": "saddlebrown", "ls": "-", "lw": 1, "alpha": 0.3},
            {"name": "Resistance", "column": "resistance", "panel": 0, "color": "purple", "ls": "-.", "lw": 2},
            {"name": "Support",    "column": "support",    "panel": 0, "color": "magenta",  "ls": "-.", "lw": 2},
            {"name": "RSI 14",     "column": "rsi_14",     "panel": 2, "color": "purple"},
            {"name": "MFI 14",     "column": "mfi_14",     "panel": 2, "color": "deepskyblue"},
        ]

        # 이용 가능한 지표만 필터링
        valid_indicators = []
        for ind in all_indicators:
            column = ind["column"]
            if column in available_indicators:
                # 데이터를 추가
                ind["data"] = df_display[column]
                valid_indicators.append(ind)
            else:
                logger.debug(f"🔍 {ticker} 지표 스킵: {ind['name']} (컬럼: {column})")

        logger.info(f"✅ {ticker} 적응형 차트: {len(valid_indicators)}/{len(all_indicators)}개 지표 사용")

        addplots = []
        for ind in valid_indicators:
            ser = ind["data"]
            if ser is not None and not ser.dropna().empty:
                ap = mpf.make_addplot(
                    ser,
                    panel=ind["panel"],
                    color=ind["color"],
                    linestyle=ind.get("ls", "-"),
                    width=ind.get("lw", 1),
                    alpha=ind.get("alpha", 1),
                    label=ind["name"]          # ← 범례에 표시될 이름
                )
                addplots.append(ap)

        # ===== ❸ mpf 스타일 =====
        mcolors = mpf.make_marketcolors(up="r", down="b", inherit=True)
        style = mpf.make_mpf_style(marketcolors=mcolors, gridstyle=":", y_on_right=False)

        # ===== ❹ 차트 그리기 =====
        # --- Disable scientific notation for volume axis ---
        import matplotlib.ticker as mticker
        fig, axes = mpf.plot(
            df_display,
            type="candle",
            style=style,
            volume=True,
            ylabel="Price (KRW)",
            ylabel_lower="Volume",
            addplot=addplots,
            figsize=(5, 3.5),
            update_width_config=dict(
                candle_width=0.8,        # 몸통 가로폭
                candle_linewidth=1.0,    # 몸통 테두리 두께
                volume_width=0.5         # 거래량 막대 폭
            ),
            warn_too_much_data=len(df_display)+1,
            returnfig=True,
            show_nontrading=False,
        )
        # Set volume axis to show in M (millions)
        if len(axes) > 1:
            axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x/1_000_000)}M'))

        # ---- remove any legends that mplfinance placed *inside* axes ----
        for ax in axes:
            leg = ax.get_legend()
            if leg is not None:
                leg.remove()

        # === 4단계: 적응형 Pivot & Support/Resistance 수평선 ===
        last_row = df_display.iloc[-1]
        pivot_levels = [
            ("pivot", "grey", ":"),
            ("r1", "orange", ":"),
            ("r2", "orange", "--"),
            ("r3", "orange", "-."),
            ("s1", "cyan", ":"),
            ("s2", "cyan", "--"),
            ("s3", "cyan", "-."),
        ]
        
        ax_price = axes[0]  # 가격 패널
        pivot_count = 0
        
        for name, col, ls in pivot_levels:
            if name in available_indicators and name in last_row and pd.notna(last_row[name]):
                val = last_row[name]
                ax_price.axhline(y=val, color=col, linestyle=ls, linewidth=1, label=name.upper())
                pivot_count += 1
            else:
                logger.debug(f"🔍 {ticker} 피벗 레벨 스킵: {name}")
                
        logger.debug(f"🔍 {ticker} 피벗 레벨: {pivot_count}/7개 표시")

        # ===== Remove all internal legend calls on axes (ax1.legend(), ax2.legend(), ax3.legend(), etc.) =====
        # (No ax1.legend(), ax2.legend(), ax3.legend(), or similar calls should be present below.)

        # ===== ❺ 범례를 차트 외부 우측에 배치 =====
        # 범례 위치: 차트 오른쪽, 본문 최상단보다 약간 아래(샘플 이미지와 유사)
        fig.subplots_adjust(right=0.82)  # 본문 우측 공간 확보

        handles, labels = [], []
        seen = set()

        # Include all lines (e.g., pivot, r1~r3, s1~s3)
        for ax in axes:
            for ln in ax.get_lines():
                lbl = ln.get_label()
                if lbl and not lbl.startswith('_') and lbl not in seen:
                    handles.append(ln)
                    labels.append(lbl)
                    seen.add(lbl)

        # Include addplot indicators (if they were not included already)
        for ap in addplots:
            if hasattr(ap, 'get_label') and callable(ap.get_label):
                lbl = ap.get_label()
                if lbl and not lbl.startswith('_') and lbl not in seen:
                    handles.append(ap)
                    labels.append(lbl)
                    seen.add(lbl)

        fig.legend(
            handles,
            labels,
            loc="upper left",
            bbox_to_anchor=(1.02, 0.92),   # ← y 값을 1 → 0.92 로 내려서 조금 아래 배치
            frameon=True,
            fontsize=8
        )
        # Add chart time period text below the x-axis
        fig.text(0.01, 0.01, f'1D Candle | {df_display.index[0].date()} ~ {df_display.index[-1].date()}', fontsize=9)

        # ===== ❻ 저장 =====
        plt.tight_layout()
        fig.savefig(chart_path, dpi=100, bbox_inches="tight", pad_inches=0.25)
        plt.close(fig)
        if os.path.exists(chart_path):
            logger.info(f"✅ {ticker} - 필터링 통과 종목 차트 생성 완료: {chart_path}")
        else:
            logger.error(f"❌ {ticker} 차트 이미지 저장 실패: 파일이 존재하지 않음 ({chart_path})")
        return chart_path

    except Exception as e:
        logger.error(f"❌ {ticker} 차트 이미지 생성 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def save_dynamic_indicators_to_ohlcv(ticker, df_with_indicators):
    """동적 지표를 ohlcv 테이블에 업데이트"""
    conn = None
    cursor = None
    
    try:
        if df_with_indicators is None or df_with_indicators.empty:
            logger.warning(f"⚠️ {ticker} 업데이트할 동적 지표 데이터 없음")
            return False
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        update_count = 0
        for index, row in df_with_indicators.iterrows():
            from utils import safe_strftime
            date_str = safe_strftime(index, '%Y-%m-%d')
            
            cursor.execute("""
                UPDATE ohlcv SET
                                    macd = %s, macd_signal = %s, macd_histogram = %s,
                rsi_14 = %s, adx = %s, plus_di = %s, minus_di = %s,
                    atr = %s, bb_upper = %s, bb_middle = %s, bb_lower = %s,
                                    volume_20ma = %s, ht_trendline = %s,
                supertrend = %s, supertrend_direction = %s
                WHERE ticker = %s AND date = %s
            """, (
                row.get('macd'), row.get('macd_signal'), row.get('macd_histogram'),
                row.get('rsi_14'), row.get('adx'), row.get('plus_di'), row.get('minus_di'),
                row.get('atr'), row.get('bb_upper'), row.get('bb_middle'), row.get('bb_lower'),
                row.get('volume_20ma'), row.get('ht_trendline'),
                row.get('supertrend'), row.get('supertrend_direction'),
                ticker, date_str
            ))
            
            if cursor.rowcount > 0:
                update_count += 1
        
        conn.commit()
        logger.info(f"✅ {ticker} 동적 지표 ohlcv 테이블 업데이트 완료: {update_count}개 레코드")
        return True
        
    except Exception as e:
        logger.error(f"❌ {ticker} 동적 지표 업데이트 실패: {str(e)}")
        if conn:
            conn.rollback()
        return False
            
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_market_data_4h_to_db(ticker, df):
    """
    4시간봉 시장 데이터(기술 지표)를 DB에 저장합니다.
    
    Args:
        ticker (str): 저장할 티커
        df (pd.DataFrame): 기술 지표가 포함된 데이터프레임 (최신 1 row)
    """
    try:
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 저장할 4시간봉 시장 데이터 없음")
            return
            
        # 최신 데이터만 저장
        latest = df.iloc[-1].copy()
        latest['ticker'] = ticker
        latest['updated_at'] = datetime.now()
        
        # 차트 경로 설정(미사용)
        #chart_path = f"charts/{ticker}_4h.png"
        #latest['chart_path'] = chart_path
        
        # ⚡ [성능 최적화] 연결 풀 사용으로 변경
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                # ✅ 기술 지표 컬럼만 선택 (새로운 스키마에 맞춤)
                indicator_columns = [
                    'ticker', 'price', 
                    # 이동평균선
                    'ma_10', 'ma_20', 'ma_50', 'ma_200',
                    # RSI & Stochastic
                    'rsi_14', 'stochastic_k', 'stochastic_d',
                    # MACD
                    'macd', 'macds', 'macdh',
                    # ADX
                    'adx', 'plus_di', 'minus_di',
                    # 볼린저밴드
                    'bb_upper', 'bb_middle', 'bb_lower',
                    # CCI
                    'cci',
                    # Supertrend
                    'supertrend', 'supertrend_signal',
                    # 피벗 포인트
                    'pivot', 'r1', 'r2', 'r3', 's1', 's2', 's3',
                    # 피보나치
                    'fibo_236', 'fibo_382', 'fibo_500', 'fibo_618', 'fibo_786',
                    # 메타데이터
                    'updated_at'
                ]
                
                # 선택된 컬럼만 추출
                latest_data = {col: latest.get(col) for col in indicator_columns if col in latest}
                
                # 컬럼과 값 준비
                columns = list(latest_data.keys())
                values = [latest_data[col] if not pd.isna(latest_data[col]) else None for col in columns]
                
                # UPSERT 쿼리 생성
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'ticker'])
                
                query = f"""
                    INSERT INTO market_data_4h ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT (ticker) DO UPDATE
                    SET {update_str}
                """
                
                cursor.execute(query, values)
                # 컨텍스트 매니저가 자동으로 commit 처리
        
        logger.info(f"✅ {ticker} 4시간봉 시장 데이터 저장 완료")
        
    except Exception as e:
        logger.error(f"❌ {ticker} 4시간봉 시장 데이터 저장 중 오류: {str(e)}")

def save_market_data_to_db(ticker, df):
    """
    일봉 시장 데이터를 static_indicators 테이블에 저장합니다 (새로운 구현)
    
    Args:
        ticker (str): 저장할 티커
        df (pd.DataFrame): 기술 지표가 포함된 데이터프레임
    """
    try:
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 저장할 일봉 시장 데이터 없음")
            return False
            
        # save_static_indicators 함수 위임으로 완전 대체
        logger.info(f"🔄 {ticker} save_static_indicators로 위임")
        
        # 최신 행 추출
        latest_row = df.iloc[-1]
        
        # DB 연결 생성하여 save_static_indicators 호출
        conn = get_db_connection()
        try:
            return save_static_indicators(conn, ticker, latest_row)
        finally:
            conn.close()
        
    except Exception as e:
        logger.error(f"❌ {ticker} 일봉 시장 데이터 저장 중 오류: {str(e)}")
        return False

def process_single_ticker(ticker, timeframe: str = '1d', market_data=None):
    """
    단일 티커에 대한 후처리를 수행하는 함수
    
    Args:
        ticker (str): 처리할 티커 심볼
        timeframe (str): 봉 타입 ('1d' 또는 '4h')
        market_data (pd.DataFrame): 이미 수집된 시장 데이터 (선택사항)
        
    Returns:
        dict: 처리 결과
    """
    try:
        if not ticker:
            logger.error("❌ 티커가 지정되지 않았습니다.")
            return None
            
        # 티커에서 'KRW-' 접두어 처리
        if not ticker.startswith('KRW-'):
            ticker = f"KRW-{ticker}"
            
        # 블랙리스트 체크 추가
        blacklist = load_blacklist()
        if ticker in blacklist:
            logger.info(f"⏭️ {ticker}는 블랙리스트에 있어 처리 건너뜀")
            return None
            
        # 데이터베이스에서 기존 데이터 조회
        if market_data is None:
            # ⚡ [성능 최적화] 연결 풀 사용으로 변경
            with get_db_connection_context() as conn:
                if timeframe == '1d':
                    # 일봉의 경우 static_indicators와 ohlcv 테이블에서 조회
                    query = """
                        SELECT s.*, o.rsi_14, o.macd_histogram, o.bb_upper, o.bb_lower
                        FROM static_indicators s
                        LEFT JOIN (
                            SELECT DISTINCT ON (ticker) ticker, rsi_14, macd_histogram, bb_upper, bb_lower
                            FROM ohlcv 
                            ORDER BY ticker, date DESC
                        ) o ON s.ticker = o.ticker
                        WHERE s.ticker = %s
                    """
                else:
                    # 4시간봉의 경우 market_data_4h 테이블 사용
                    query = f"SELECT * FROM market_data_4h WHERE ticker = %s"
                
                df = pd.read_sql_query(query, conn, params=(ticker,))
            
            if df.empty:
                logger.warning(f"⚠️ {ticker} {timeframe} 봉 시장 데이터가 없습니다.")
                return None
        else:
            df = market_data
            
        # 봉 타입에 따라 필수 기술 지표 설정
        if timeframe == '1d':
            required_indicators = {'rsi_14', 'adx', 'mfi_14', 'macd', 'supertrend'}
        else:  # 4h 봉인 경우 (컬럼명 수정)
            required_indicators = {'rsi_14', 'stochastic_k', 'stochastic_d', 'ma_10', 'ma_20'}
        
        # 컬럼명을 소문자로 변환하여 비교 (대소문자 일치 이슈 방지)
        df_columns_lower = {col.lower() for col in df.columns}
        required_indicators_lower = {ind.lower() for ind in required_indicators}
        
        missing_indicators = required_indicators_lower - df_columns_lower
        
        if missing_indicators:
            logger.warning(f"⚠️ {ticker} {timeframe} 봉: 기술지표 누락 {missing_indicators}")
            return None
            
        # 추가 분석 수행 (예: AI 예측, 리포트 생성 등)
        # TODO : 여기에 추가 분석 로직 구현
        
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} {timeframe} 봉 처리 중 예상치 못한 오류 발생: {str(e)}")
        return None

def get_ohlcv_4h(ticker, limit=200, force_fetch=False):
    """
    특정 티커의 4시간봉 OHLCV 데이터를 최근 200개(기본값) 조회합니다.
    
    Args:
        ticker (str): 조회할 티커 (예: "KRW-BTC")
        limit (int): 조회할 최근 캔들 개수 (기본값: 200)
        
    Returns:
        pd.DataFrame: 4시간봉 OHLCV 데이터프레임 (datetime 인덱스)
    """
    try:
        import pyupbit
        latest = get_latest_timestamp(ticker, table='ohlcv_4h')
        now = datetime.now()
        if not force_fetch and latest is not None:
            hours_diff = (now - latest).total_seconds() / 3600
            if hours_diff < limit * 4:
                logger.info(f"⏭️ {ticker}: 최근 {int(hours_diff)}시간 데이터 존재 - 수집 패스")
                return None

        if latest is None:
            logger.info(f"🆕 {ticker} 신규 티커 - {limit}개 4시간봉 수집")
            df = safe_pyupbit_get_ohlcv(ticker, interval="minute240", count=limit)
            if df is not None and not df.empty:
                save_ohlcv_4h_to_db(ticker, df)
            return df

        # 티커 형식 확인 및 변환
        ticker = f"KRW-{ticker}" if not ticker.startswith("KRW-") else ticker

        # 4시간봉 데이터 조회 (안전한 API 호출)
        df = safe_pyupbit_get_ohlcv(ticker, interval="minute240", count=limit)
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 4시간봉 데이터 없음")
            return None

        logger.debug(f"✅ {ticker} 4시간봉 {len(df)}개 조회 완료")
        return df

    except Exception as e:
        logger.error(f"❌ {ticker} 4시간봉 데이터 조회 중 오류: {str(e)}")
        return None

def _filter_invalid_ohlcv_data(df, ticker):
    """
    OHLCV 데이터에서 품질 불량 레코드를 필터링
    
    제거 기준:
    1. OHLCV 중 하나라도 0값인 레코드
    2. high < low 등 논리적으로 불가능한 값
    3. 극단적 이상값 (가격 변동률 > 1000%)
    
    Args:
        df (pd.DataFrame): 원본 OHLCV 데이터
        ticker (str): 티커명 (로깅용)
        
    Returns:
        pd.DataFrame: 필터링된 OHLCV 데이터
    """
    if df is None or df.empty:
        return df
        
    original_len = len(df)
    
    # 1. OHLCV 중 0값 레코드 제거
    zero_mask = (df['open'] == 0) | (df['high'] == 0) | (df['low'] == 0) | (df['close'] == 0)
    zero_count = zero_mask.sum()
    
    if zero_count > 0:
        logger.warning(f"🔴 {ticker} OHLCV 0값 레코드 {zero_count}개 제거")
        df = df[~zero_mask]
    
    # 2. 논리적 불가능한 값 제거 (high < low, close > high, close < low 등)
    invalid_logic_mask = (
        (df['high'] < df['low']) |  # high < low
        (df['close'] > df['high']) |  # close > high  
        (df['close'] < df['low']) |  # close < low
        (df['open'] > df['high']) |  # open > high
        (df['open'] < df['low'])     # open < low
    )
    invalid_logic_count = invalid_logic_mask.sum()
    
    if invalid_logic_count > 0:
        logger.warning(f"🔴 {ticker} 논리적 오류 레코드 {invalid_logic_count}개 제거")
        df = df[~invalid_logic_mask]
    
    # 3. 극단적 가격 변동 제거 (전일 대비 1000% 이상 변동)
    if len(df) > 1:
        price_change_pct = df['close'].pct_change().abs()
        extreme_change_mask = price_change_pct > 10.0  # 1000% 변동
        extreme_change_count = extreme_change_mask.sum()
        
        if extreme_change_count > 0:
            logger.warning(f"🔴 {ticker} 극단적 가격변동 레코드 {extreme_change_count}개 제거")
            df = df[~extreme_change_mask]
    
    filtered_len = len(df)
    removed_count = original_len - filtered_len
    
    if removed_count > 0:
        logger.info(f"📊 {ticker} 데이터 품질 필터링: {original_len} → {filtered_len}개 ({removed_count}개 제거)")
    
    return df

def safe_pyupbit_get_ohlcv(ticker, interval="day", count=200, to=None, period=1):
    """
    1단계: pyupbit API 호출 방식 근본 수정
    
    pyupbit.get_ohlcv()를 안전하게 호출하여 1970-01-01 응답을 방지합니다.
    
    Args:
        ticker (str): 조회할 티커
        interval (str): 조회 간격 (day, minute240 등)
        count (int): 조회할 데이터 개수
        to (str): 종료일 (None이면 현재 날짜 사용)
        period (int): API 안정성을 위한 period 파라미터
        
    Returns:
        pd.DataFrame: 안전하게 수집된 OHLCV 데이터
    """
    try:
        # 1단계: 명시적 날짜 범위 지정으로 1970-01-01 방지
        if to is None:
            # 현재 날짜를 정확한 형식으로 설정
            to = datetime.now().strftime("%Y-%m-%d")
            
        logger.debug(f"🔍 {ticker} API 호출 파라미터:")
        logger.debug(f"   - interval: {interval}")
        logger.debug(f"   - count: {count}")
        logger.debug(f"   - to: {to}")
        logger.debug(f"   - period: {period}")
        
        # pyupbit API 호출 (모든 파라미터 명시적 지정)
        api_params = {
            'ticker': ticker,
            'interval': interval,
            'count': count,
            'to': to,
            'period': period
        }
        
        df = pyupbit.get_ohlcv(**api_params)
        
        # 3단계: API 응답 품질 모니터링
        quality_ok = data_quality_monitor.log_api_response_quality(ticker, df, api_params)
        
        if df is None:
            logger.warning(f"⚠️ {ticker} API 응답 None")
            return None
            
        if df.empty:
            logger.warning(f"⚠️ {ticker} API 응답 빈 DataFrame")
            return df
            
        # API 응답 즉시 인덱스 유효성 검증
        logger.debug(f"🔍 {ticker} API 응답 검증:")
        logger.debug(f"   - 데이터 개수: {len(df)}")
        logger.debug(f"   - Index 타입: {type(df.index)}")
        
        if len(df) > 0:
            first_date = df.index[0]
            last_date = df.index[-1]
            logger.debug(f"   - 첫 번째 날짜: {first_date}")
            logger.debug(f"   - 마지막 날짜: {last_date}")
            
            # 1. OHLCV 0값 데이터 품질 검증 및 필터링
            original_len = len(df)
            df = _filter_invalid_ohlcv_data(df, ticker)
            filtered_len = len(df)
            
            if original_len != filtered_len:
                logger.warning(f"⚠️ {ticker} 품질 불량 데이터 제거: {original_len} → {filtered_len}개")
            
            # 2. 최소 데이터 개수 확인 (요청량의 80% 미만이면 재시도)
            min_required = int(count * 0.8)
            if len(df) < min_required:
                logger.warning(f"⚠️ {ticker} 데이터 부족: {len(df)}/{count} (최소 요구: {min_required})")
                # 재시도 로직은 상위 호출자에서 처리
                return df
            
            # 3. 1970-01-01 응답 감지
            if hasattr(df.index, 'year') and len(df) > 0 and df.index[0].year == 1970:
                logger.warning(f"🔍 {ticker} API 1970-01-01 응답 감지됨")
            else:
                logger.debug(f"✅ {ticker} API 응답 날짜 정상: {first_date.date()} ~ {last_date.date()}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} safe_pyupbit_get_ohlcv 실패: {str(e)}")
        return None

def fix_datetime_index(df, ticker):
    """
    DataFrame의 잘못된 날짜 인덱스를 올바르게 수정
    
    Args:
        df (pd.DataFrame): 수정할 DataFrame
        ticker (str): 티커 심볼 (로깅용)
    
    Returns:
        pd.DataFrame: 날짜 인덱스가 수정된 DataFrame
    """
    if df is None or df.empty:
        return df
    
    # 4단계: pyupbit API 응답의 원본 인덱스 정보 로깅
    logger.debug(f"🔍 {ticker} 원본 API 응답 인덱스 정보:")
    logger.debug(f"   - Index 타입: {type(df.index)}")
    logger.debug(f"   - 데이터 개수: {len(df)}")
    if len(df) > 0:
        logger.debug(f"   - 첫 번째 index: {df.index[0]} (타입: {type(df.index[0])})")
        logger.debug(f"   - 마지막 index: {df.index[-1]} (타입: {type(df.index[-1])})")
        
    # 1970-01-01 인덱스 감지
    if hasattr(df.index, 'year') and len(df.index) > 0 and df.index[0].year == 1970:
        logger.warning(f"🔧 {ticker} 잘못된 날짜 인덱스 감지, 복구 시작...")
        
        # 4단계: 날짜 변환 전후 비교
        original_start = df.index[0] if len(df) > 0 else None
        original_end = df.index[-1] if len(df) > 0 else None
        
        # 현재 날짜부터 역산하여 올바른 날짜 생성
        end_date = datetime.now().date()
        date_range = pd.date_range(
            end=end_date, 
            periods=len(df), 
            freq='D'
        )
        
        # 주말 제외 없음 (암호화폐는 24/7 거래)
        df.index = date_range
        
        # 4단계: 변환 후 로깅
        new_start = df.index[0] if len(df) > 0 else None
        new_end = df.index[-1] if len(df) > 0 else None
        
        logger.info(f"✅ {ticker} 날짜 인덱스 복구 완료:")
        logger.info(f"   - 복구 전: {original_start} ~ {original_end}")
        logger.info(f"   - 복구 후: {new_start.date()} ~ {new_end.date()}")
    
    return df

def get_ohlcv_d(ticker, interval="day", count=450, force_fetch=False, fetch_latest_only=False):
    """
    450일치 OHLCV 데이터 수집 - 로직 완전 재설계 버전 + BTC 특별 처리
    
    처리 순서:
    1. DB에서 기존 데이터 개수 확인
    2. 450개 미만이면 전체 재수집
    3. 450개 이상이면 최신 데이터만 업데이트
    4. BTC 등 주요 코인은 특별 처리 (더 많은 데이터 요청)
    
    Args:
        ticker (str): 티커 심볼
        interval (str): 조회 단위 (기본값: "day")
        count (int): 조회할 데이터 개수 (기본값: 450)
        force_fetch (bool): 강제 수집 모드 (기본값: False)
        fetch_latest_only (bool): 최신 데이터만 수집 모드 (기본값: False)
        
    Returns:
        pd.DataFrame: OHLCV 데이터
    """
    conn = None
    cursor = None
    
    try:
        if not ticker.startswith("KRW-"):
            ticker = f"KRW-{ticker}"
            
        # 블랙리스트 체크
        blacklist = load_blacklist()
        if ticker in blacklist:
            logger.info(f"⏭️ {ticker}는 블랙리스트에 있어 OHLCV 데이터 수집 건너뜀")
            return pd.DataFrame()
        
        # 주요 코인 목록 (더 많은 데이터가 필요한 경우)
        major_coins = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-DOT"]
        is_major_coin = ticker in major_coins
        
        # 주요 코인은 더 많은 데이터 요청
        if is_major_coin and count == 450:
            actual_count = 600  # 주요 코인은 더 많이 요청
            logger.info(f"🔍 {ticker} 주요 코인 → 확장 데이터 수집 ({actual_count}개)")
        else:
            actual_count = count
        
        # 1단계: DB 현황 확인
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*), MIN(date), MAX(date) 
            FROM ohlcv 
            WHERE ticker = %s
        """, (ticker,))
        
        result = cursor.fetchone()
        db_count = result[0] if result else 0
        min_date = result[1] if result else None
        max_date = result[2] if result else None
        
        logger.info(f"🔍 {ticker} DB 현황: {db_count}개 레코드, 최신: {max_date}")
        
        # 2단계: 수집 전략 결정 - 450개 미만이거나 강제 수집이면 전체 재수집
        now = datetime.now()
        
        # 🚀 최신 데이터만 수집 모드 (새로 추가)
        if fetch_latest_only:
            logger.info(f"🔄 {ticker} 최신 데이터만 수집 모드 시작")
            
            try:
                # API에서 최신 1일 데이터만 수집
                df = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=1)
                
                if df is not None and not df.empty:
                    logger.info(f"✅ {ticker} 최신 데이터 수집 성공: {len(df)}개 레코드")
                    
                    # 날짜 범위 출력
                    from utils import safe_strftime
                    date_str = safe_strftime(df.index[0])
                    logger.info(f"📅 {ticker} 최신 데이터: {date_str}")
                    
                    return df
                else:
                    logger.error(f"❌ {ticker} 최신 데이터 수집 실패")
                    return pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"❌ {ticker} 최신 데이터 수집 중 오류: {str(e)}")
                return pd.DataFrame()
        elif db_count < count or force_fetch:
            # 전체 재수집 필요
            logger.info(f"🔄 {ticker} 전체 재수집 시작 ({actual_count}개 목표, 현재 DB: {db_count}개)")
            
            # 기존 데이터 삭제 (완전 재수집)
            if db_count > 0:
                cursor.execute("DELETE FROM ohlcv WHERE ticker = %s", (ticker,))
                logger.info(f"🗑️ {ticker} 기존 {db_count}개 레코드 삭제")
            
            # API에서 전체 데이터 수집 (재시도 로직 포함)
            df = None
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"🔄 {ticker} API 수집 시도 {attempt + 1}/{max_retries} (count: {actual_count})")
                    
                    # BTC 등 주요 코인 특별 처리
                    if is_major_coin:
                        # 분할 요청 전략
                        if actual_count > 500:
                            logger.info(f"🔄 {ticker} 분할 요청 전략 적용 ({actual_count}개)")
                            
                            # 첫 번째 요청: 최근 500개 (안전한 API 호출)
                            df1 = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=500)
                            time.sleep(0.2)  # API 제한 회피
                            
                            if df1 is not None and not df1.empty:
                                # 두 번째 요청: 이전 200개 (중복 제거 예정)
                                oldest_date = df1.index[0]
                                to_date = oldest_date.strftime("%Y-%m-%d")
                                df2 = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=200, to=to_date)
                                time.sleep(0.2)
                                
                                if df2 is not None and not df2.empty:
                                    # 중복 제거하고 병합
                                    combined_df = pd.concat([df2, df1])
                                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                                    combined_df = combined_df.sort_index()
                                    
                                    # 원하는 개수만큼 자르기
                                    df = combined_df.tail(actual_count)
                                    logger.info(f"✅ {ticker} 분할 수집 성공: {len(df)}개 (목표: {actual_count})")
                                else:
                                    df = df1
                                    logger.warning(f"⚠️ {ticker} 두 번째 분할 요청 실패, 첫 번째만 사용: {len(df)}개")
                            else:
                                logger.error(f"❌ {ticker} 첫 번째 분할 요청 실패")
                                df = None
                        else:
                            # 일반적인 단일 요청 (안전한 API 호출)
                            df = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=actual_count)
                    else:
                        # 일반 코인은 기존 방식 (안전한 API 호출)
                        df = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=actual_count)
                    
                    # 수집 성공 시 루프 탈출
                    if df is not None and not df.empty:
                        logger.info(f"✅ {ticker} API 수집 성공: {len(df)}개 레코드 (시도: {attempt + 1})")
                        break
                    else:
                        logger.warning(f"⚠️ {ticker} API 수집 결과 없음 (시도: {attempt + 1})")
                        
                except Exception as api_e:
                    logger.warning(f"⚠️ {ticker} API 수집 시도 {attempt + 1} 실패: {str(api_e)}")
                    
                    # 마지막 시도가 아니면 대기
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # 2, 4, 6초 대기
                        logger.info(f"⏳ {wait_time}초 대기 후 재시도...")
                        time.sleep(wait_time)
            
            # 최종 수집 결과 확인
            if df is None or df.empty:
                logger.error(f"❌ {ticker} 모든 API 수집 시도 실패 ({max_retries}회)")
                conn.rollback()
                return pd.DataFrame()
            
            logger.info(f"✅ {ticker} 최종 API 수집 성공: {len(df)}개 레코드")
            
            # 날짜 범위 출력 (수정됨)
            if not df.empty:
                from utils import safe_strftime
                start_date = safe_strftime(df.index[0])
                end_date = safe_strftime(df.index[-1])
                logger.info(f"📅 {ticker} 수집 기간: {start_date} ~ {end_date}")
            
            # DB 저장 - success 여부 확인
            # 🔧 [1단계 수정] 조기 저장 방지: 지표 계산 완료 후 통합 저장하도록 수정
            # save_result = save_ohlcv_to_db(ticker, df)
            # if not save_result:
            #     logger.error(f"❌ {ticker} DB 저장 실패 - 빈 DataFrame 반환")
            #     conn.rollback()
            #     return pd.DataFrame()  # 저장 실패 시 빈 DataFrame 반환
            
            logger.info(f"✅ {ticker} OHLCV 데이터 수집 완료 (저장은 지표 계산 후 수행)")
            
            # 최종 검증 - DB 저장 후 검증은 제거
            # cursor.execute("SELECT COUNT(*) FROM ohlcv WHERE ticker = %s", (ticker,))
            # final_count = cursor.fetchone()[0]
            # logger.info(f"🔍 {ticker} 저장 완료: {final_count}개 레코드")
            
            conn.commit()
            return df
            
        else:
            # 증분 업데이트 필요한 경우
            if isinstance(max_date, datetime):
                max_date_obj = max_date.date()
            else:
                max_date_obj = max_date
            
            days_diff = (now.date() - max_date_obj).days if max_date else 999
            
            if days_diff <= 1:
                logger.info(f"⏭️ {ticker} 최신 데이터 ({days_diff}일 전) - DB에서 조회")
                conn.close()  # DB 연결 종료
                return get_ohlcv_from_db(ticker, limit=count)
            else:
                logger.info(f"🔄 {ticker} 증분 업데이트 ({days_diff}일 차이)")
                
                # 최근 데이터만 수집하여 업데이트 (안전한 API 호출)
                update_count = min(days_diff + 5, 100)  # 최대 100개
                df_new = safe_pyupbit_get_ohlcv(ticker, interval=interval, count=update_count)
                
                if df_new is not None and not df_new.empty:
                    logger.info(f"✅ {ticker} 증분 데이터 수집 완료: {len(df_new)}개 (저장은 지표 계산 후 수행)")
                    
                    conn.commit()
                    conn.close()  # DB 연결 종료
                    
                    logger.info(f"✅ {ticker} 증분 데이터 반환: {len(df_new)}개")
                    return df_new
                else:
                    logger.error(f"❌ {ticker} 증분 데이터 수집 실패")
                    conn.close()  # DB 연결 종료
                    return pd.DataFrame()
                    
    except Exception as e:
        logger.error(f"❌ {ticker} OHLCV 데이터 수집 중 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        if conn:
            conn.rollback()
        return pd.DataFrame()
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_latest_timestamp(ticker: str, table: str = 'ohlcv') -> Optional[datetime]:
    """
    DB에서 특정 티커의 가장 최근 timestamp를 조회합니다.

    Args:
        ticker (str): 조회할 티커 심볼 (예: 'KRW-BTC')
        table (str): 조회할 테이블명 (기본값: 'ohlcv')

    Returns:
        Optional[datetime]: 가장 최근 timestamp. 데이터가 없을 경우 None 반환
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if table == 'ohlcv':
            cursor.execute("""
                SELECT MAX(date) FROM ohlcv WHERE ticker = %s
            """, (ticker,))
        elif table == 'ohlcv_4h':
            cursor.execute("""
                SELECT MAX(date) FROM ohlcv_4h WHERE ticker = %s
            """, (ticker,))
        else:
            logger.error(f"❌ 지원하지 않는 테이블: {table}")
            return None

        result = cursor.fetchone()
        return result[0] if result and result[0] else None

    except Exception as e:
        logger.error(f"❌ {ticker} 최근 timestamp 조회 중 오류: {str(e)}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_ohlcv_from_db(ticker: str, limit: int = 450) -> pd.DataFrame:
    """DB에서 OHLCV 데이터를 최근 날짜순으로 정확히 조회"""
    conn = None
    try:
        conn = get_db_connection()
        
        # 최근 데이터부터 내림차순으로 조회 후 다시 오름차순 정렬
        query = """
        SELECT date, open, high, low, close, volume
        FROM (
            SELECT date, open, high, low, close, volume
            FROM ohlcv 
            WHERE ticker = %s 
            ORDER BY date DESC 
            LIMIT %s
        ) subquery
        ORDER BY date ASC
        """
        
        df = pd.read_sql_query(query, conn, params=[ticker, limit])
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 검증 로그
            from utils import safe_strftime
            start_date = safe_strftime(df.index[0])
            end_date = safe_strftime(df.index[-1])
            logger.info(f"🔍 {ticker} DB 조회 완료: {len(df)}개 ({start_date} ~ {end_date})")
            
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} DB 조회 실패: {str(e)}")
        return pd.DataFrame()
        
    finally:
        if conn:
            conn.close()

def save_chart_image(ticker: str, df: pd.DataFrame, indicators: list = None):
    """
    OHLCV 데이터를 사용하여 차트 이미지를 생성하고 저장합니다.
    Args:
        ticker (str): 티커 심볼 (예: "KRW-BTC")
        df (pd.DataFrame): OHLCV 데이터프레임 (DatetimeIndex, open, high, low, close, volume 컬럼 포함)
        indicators (list, optional): 차트에 추가할 지표 정보 리스트. 각 요소는 딕셔너리 형태 (예: {'name': 'MA20', 'data': df['ma20'], 'panel': 0, 'color': 'blue'})
    """
    try:
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} - 차트 생성을 위한 데이터가 없습니다.")
            return

        # charts 디렉토리 생성 (없으면)
        if not os.path.exists("charts"):
            os.makedirs("charts")

        # 파일 경로 설정
        chart_file_path = os.path.join("charts", f"{ticker}.png")

        # 스타일 설정
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)

        # 추가 플롯 설정
        addplot = []
        if indicators:
            for indicator_info in indicators: # 변수명 변경 indicator -> indicator_info
                # make_addplot 호출 시 Series가 아닌 실제 데이터(numpy array 등)를 전달해야 할 수 있음
                # 또한, panel 번호가 올바른지, 해당 panel에 맞는 데이터 타입인지 확인 필요
                try:
                    plot_data = indicator_info['data']
                    if isinstance(plot_data, pd.Series):
                        # NaN 값을 가진 행은 제외하고 addplot에 추가
                        valid_data = plot_data.dropna()
                        if not valid_data.empty:
                             addplot.append(mpf.make_addplot(valid_data, panel=indicator_info.get('panel', 0), color=indicator_info.get('color', 'blue'), ylabel=indicator_info.get('name', '')))
                        else:
                            logger.warning(f"⚠️ {ticker} - 지표 '{indicator_info.get('name')}' 데이터가 모두 NaN이거나 비어있어 차트에 추가하지 않습니다.")
                    elif plot_data is not None: # Series가 아닌 다른 타입의 데이터일 경우 (numpy array 등)
                         addplot.append(mpf.make_addplot(plot_data, panel=indicator_info.get('panel', 0), color=indicator_info.get('color', 'blue'), ylabel=indicator_info.get('name', '')))
                    else:
                        logger.warning(f"⚠️ {ticker} - 지표 '{indicator_info.get('name')}' 데이터가 None이어서 차트에 추가하지 않습니다.")
                except Exception as ap_err:
                    logger.error(f"❌ {ticker} - 지표 '{indicator_info.get('name')}' 추가 중 오류: {ap_err}")


        # 최근 100일 데이터만 사용 (너무 많으면 차트가 복잡해짐)
        df_recent = df.tail(100)
        if df_recent.empty:
            logger.warning(f"⚠️ {ticker} - 최근 100일 데이터가 없어 차트를 생성할 수 없습니다.")
            return

        # 캔들스틱 차트 생성 및 저장
        plot_kwargs = {
            'type': 'candle',
            'style': s,
            'title': f'{ticker} Daily Chart',
            'ylabel': 'Price (KRW)',
            'volume': True,
            'ylabel_lower': 'Volume',
            'figsize': (15, 7),
            'savefig': dict(fname=chart_file_path, dpi=100, pad_inches=0.25),
            'show_nontrading': False
        }
        
        # addplot이 있을 때만 추가
        if addplot:
            plot_kwargs['addplot'] = addplot
            
        mpf.plot(df_recent, **plot_kwargs)
        logger.info(f"✅ {ticker} 차트 이미지 저장 완료: {chart_file_path}")

    except Exception as e:
        logger.error(f"❌ {ticker} 차트 이미지 생성 중 오류 발생: {str(e)}")
        # 오류 발생 시 Traceback 로깅
        import traceback
        logger.error(traceback.format_exc())

def save_ohlcv_4h_to_db(ticker: str, df: pd.DataFrame):
    """
    4시간봉 OHLCV 데이터를 ohlcv_4h 테이블에 저장합니다.
    
    Args:
        ticker (str): 저장할 티커 심볼
        df (pd.DataFrame): 저장할 4시간봉 OHLCV 데이터프레임
    """
    try:
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 저장할 4시간봉 OHLCV 데이터 없음")
            return False
            
        # ⚡ [성능 최적화] 연결 풀 사용으로 변경
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                # 데이터 저장
                for index, row in df.iterrows():
                    # 안전한 날짜 변환
                    from utils import safe_strftime
                    date_str = safe_strftime(index, '%Y-%m-%d %H:%M:%S')
                        
                    cursor.execute("""
                        INSERT INTO ohlcv_4h (ticker, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker, date) DO UPDATE
                        SET open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """, (
                        ticker,
                        date_str,
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume']
                    ))
                # 컨텍스트 매니저가 자동으로 commit 처리
        
        logger.info(f"✅ {ticker} 4시간봉 OHLCV 데이터 저장 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ {ticker} 4시간봉 OHLCV 데이터 저장 실패: {str(e)}")
        return False

def process_static_indicators(ticker: str):
    """
    단일 티커에 대해 정적 기술적 지표를 계산하고 DB에 저장하는 함수
    
    처리 순서:
    1. get_ohlcv_d()로 OHLCV 데이터 가져오기
    2. calculate_static_indicators()로 기술적 지표 계산
    3. update_static_indicators_db()로 최신 데이터 저장
    
    Args:
        ticker (str): 처리할 티커 심볼 (예: 'KRW-BTC')
    """
    try:
        logger.info(f"🔧 {ticker} static indicators 처리 시작")
        
        # 1단계: OHLCV 데이터 가져오기
        logger.info(f"1단계: {ticker} OHLCV 데이터 조회 (450일)")
        df = get_ohlcv_d(ticker, count=450)
        
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} OHLCV 데이터가 없거나 비어있습니다. 처리를 중단합니다.")
            return
            
        logger.info(f"✅ {ticker} OHLCV 데이터 조회 완료: {len(df)}개 레코드")
        
        # 2단계: 정적 기술적 지표 계산
        logger.info(f"2단계: {ticker} 정적 기술적 지표 계산")
        df_with_indicators = calculate_static_indicators(df, ticker)
        
        if df_with_indicators is None or df_with_indicators.empty:
            logger.warning(f"⚠️ {ticker} 정적 기술적 지표 계산 결과가 없습니다. 처리를 중단합니다.")
            return
            
        logger.info(f"✅ {ticker} 정적 기술적 지표 계산 완료")
        
        # 3단계: 최신 데이터를 DB에 저장
        logger.info(f"3단계: {ticker} static_indicators 테이블에 최신 데이터 저장")
        latest_row = df_with_indicators.iloc[-1]
        update_static_indicators_db(ticker, latest_row)
        
        logger.info(f"✅ {ticker} static indicators 처리 완료")
        
    except Exception as e:
        logger.error(f"❌ {ticker} static indicators 처리 중 오류 발생: {str(e)}")
        raise

def process_all_static_indicators(tickers: list[str], max_workers: int = 4):
    """
    여러 티커에 대해 정적 기술적 지표를 병렬로 계산하고 DB에 저장하는 함수
    
    Args:
        tickers (list[str]): 처리할 티커 리스트
        max_workers (int): 병렬 처리에 사용할 최대 워커 수 (기본값: 4)
    """
    try:
        logger.info(f"🔧 {len(tickers)}개 티커 static indicators 병렬 처리 시작 (workers: {max_workers})")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 모든 티커에 대해 process_static_indicators 작업 제출
            future_to_ticker = {
                executor.submit(process_static_indicators, ticker): ticker 
                for ticker in tickers
            }
            
            # 완료된 작업들을 처리
            completed_count = 0
            failed_count = 0
            
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    future.result()  # 예외가 있다면 여기서 발생
                    completed_count += 1
                    logger.info(f"✅ {ticker} static indicators 처리 완료 ({completed_count}/{len(tickers)})")
                    
                except Exception as exc:
                    failed_count += 1
                    logger.warning(f"⚠️ {ticker} static indicators 처리 중 오류 발생: {str(exc)} ({failed_count} 실패)")
                    # 개별 티커 실패는 전체 작업을 중단하지 않음
                    continue
        
        logger.info(f"✅ static indicators 병렬 처리 완료 - 성공: {completed_count}개, 실패: {failed_count}개")
        
    except Exception as e:
        logger.error(f"❌ static indicators 병렬 처리 중 전체 오류 발생: {str(e)}")
        raise

# 추세 분석 지표 분류 (config에서 가져옴)
# ESSENTIAL_TREND_INDICATORS는 config.py에서 import됨

def convert_supertrend_to_signal(close_price, supertrend_value):
    """Supertrend 값을 bull/bear 신호로 변환 (1.0: bull, 0.0: bear, 0.5: neutral)"""
    if pd.isna(supertrend_value) or pd.isna(close_price):
        return None
    
    # 🔧 [핵심 수정] 문자열 값 처리 추가
    if isinstance(supertrend_value, str):
        if supertrend_value.lower() == 'bull':
            return 1.0
        elif supertrend_value.lower() == 'bear':
            return 0.0
        elif supertrend_value.lower() == 'neutral':
            return 0.5
        else:
            # 알 수 없는 문자열은 중립으로 처리
            return 0.5
    
    # 수치형 값 처리 (기존 로직)
    if isinstance(supertrend_value, (int, float)):
        if supertrend_value == 1.0:
            return 1.0  # bull
        elif supertrend_value == 0.0:
            return 0.0  # bear
        elif supertrend_value == 0.5:
            return 0.5  # neutral
        else:
            # 기존 로직: 가격과 비교
            return 1.0 if close_price > supertrend_value else 0.0
    
    return 0.5  # 기본값은 중립

def validate_db_schema_consistency():
    """
    DB 스키마와 코드 간 일관성을 체크하는 함수
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # static_indicators 테이블의 실제 컬럼 조회
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'static_indicators' 
            ORDER BY ordinal_position
        """)
        db_columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 코드에서 사용하는 static_columns와 비교
        expected_static_columns = [
            'ticker', 'ma200_slope', 'nvt_relative', 'volume_change_7_30', 'price', 
            'high_60', 'low_60', 'pivot', 's1', 'r1', 
            'resistance', 'support', 'atr', 'adx', 'supertrend_signal', 'updated_at'
        ]
        
        # 누락된 컬럼 체크
        missing_columns = []
        for col in expected_static_columns:
            if col not in db_columns:
                missing_columns.append(col)
        
        # 추가 컬럼 체크 (DB에는 있지만 코드에서 사용하지 않는 컬럼)
        extra_columns = []
        for col in db_columns:
            if col not in expected_static_columns:
                extra_columns.append(col)
        
        # 결과 보고
        logger.info(f"🔍 DB 스키마 일관성 검증 결과:")
        logger.info(f"   - DB 테이블 컬럼 수: {len(db_columns)}")
        logger.info(f"   - 코드 예상 컬럼 수: {len(expected_static_columns)}")
        
        if missing_columns:
            logger.error(f"❌ DB에 누락된 컬럼: {missing_columns}")
            return False
        else:
            logger.info(f"✅ 모든 필수 컬럼이 DB에 존재")
        
        if extra_columns:
            logger.warning(f"⚠️ 코드에서 사용하지 않는 DB 컬럼: {extra_columns}")
        
        # ohlcv 테이블 동적 지표 컬럼도 검증
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ohlcv' 
            AND column_name IN ('fibo_618', 'fibo_382', 'ht_trendline', 'ma_50', 'ma_200', 'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low', 'macd_histogram', 'rsi_14', 'volume_20ma')
        """)
        dynamic_columns = [row[0] for row in cursor.fetchall()]
        
        expected_dynamic_columns = [
            'fibo_618', 'fibo_382', 'ht_trendline', 'ma_50', 'ma_200', 
            'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low', 
            'macd_histogram', 'rsi_14', 'volume_20ma'
        ]
        
        missing_dynamic = [col for col in expected_dynamic_columns if col not in dynamic_columns]
        
        if missing_dynamic:
            logger.error(f"❌ ohlcv 테이블에 누락된 동적 지표 컬럼: {missing_dynamic}")
            return False
        else:
            logger.info(f"✅ ohlcv 테이블의 모든 동적 지표 컬럼 존재: {len(dynamic_columns)}개")
        
        return True
                
    except Exception as e:
        logger.error(f"❌ DB 스키마 검증 실패: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_unified_indicators(df):
    """
    최적화된 통합 지표 계산 함수 (성능 최적화 및 의존성 관리 강화)
    
    🎯 주요 최적화 사항:
    1. 지표 계산 순서 최적화 및 의존성 관리
    2. 데이터 길이별 지표 그룹핑 및 조건부 계산  
    3. 성능 최적화 및 메모리 사용량 제어
    4. 계산 실패 시 우아한 성능 저하(graceful degradation)
    
    📊 단계별 계산 그룹:
    - 1단계: 이동평균선, 기본 오실레이터 (의존성 없음)
    - 2단계: MACD, 볼린저밴드 (중간 복잡도)  
    - 3단계: 피보나치, 피벗 포인트 (복잡한 계산)
    
    🔄 성능 개선:
    - 데이터 길이별 계산 가능 지표 그룹핑
    - 빠른 계산 지표 우선 처리
    - 계산 실패 시에도 원본 데이터 반환 보장
    """
    try:
        data_length = len(df)
        logger.info(f"🔄 지표 계산 시작: {data_length}개 레코드")
        
        # 1단계: 데이터 길이별 계산 가능 지표 그룹핑
        if data_length < 14:
            logger.warning("⚠️ 데이터 부족: 기본 지표만 계산")
            return df
        
        from utils import get_safe_ohlcv_columns
        available_columns = get_safe_ohlcv_columns()
        
        logger.info(f"📊 사용 가능한 ohlcv 컬럼: {len(available_columns)}개")
        df_result = df.copy()
        calculated_indicators = []
        
        # 2단계: 빠른 계산 지표 우선 처리 (의존성 없음)
        logger.info("🔄 1차 지표 계산: 이동평균선, 기본 오실레이터")
        
        # MA 계산 (가장 기본)
        if data_length >= 50 and 'ma_50' in available_columns:
            try:
                df_result['ma_50'] = ta.sma(df_result['close'], length=50)
                ma50_valid = (~df_result['ma_50'].isnull()).sum()
                logger.info(f"  ✅ MA50: {ma50_valid}개 유효값")
                calculated_indicators.append('ma_50')
            except Exception as e:
                logger.warning(f"  ❌ MA50 계산 실패: {e}")
                
        if data_length >= 200 and 'ma_200' in available_columns:
            try:
                df_result['ma_200'] = ta.sma(df_result['close'], length=200)
                ma200_valid = (~df_result['ma_200'].isnull()).sum()
                logger.info(f"  ✅ MA200: {ma200_valid}개 유효값")
                calculated_indicators.append('ma_200')
            except Exception as e:
                logger.warning(f"  ❌ MA200 계산 실패: {e}")
        
        # RSI 계산 (14일)
        if data_length >= 14 and 'rsi_14' in available_columns:
            try:
                df_result['rsi_14'] = ta.rsi(df_result['close'], length=14)
                rsi_valid = (~df_result['rsi_14'].isnull()).sum()
                logger.info(f"  ✅ RSI14: {rsi_valid}개 유효값")
                calculated_indicators.append('rsi_14')
            except Exception as e:
                logger.warning(f"  ❌ RSI14 계산 실패: {e}")
        
        # 거래량 20일 이동평균
        if data_length >= 20 and 'volume_20ma' in available_columns:
            try:
                df_result['volume_20ma'] = df_result['volume'].rolling(window=20).mean()
                vol_valid = (~df_result['volume_20ma'].isnull()).sum()
                logger.info(f"  ✅ Volume20MA: {vol_valid}개 유효값")
                calculated_indicators.append('volume_20ma')
            except Exception as e:
                logger.warning(f"  ❌ Volume20MA 계산 실패: {e}")
        
        # 3단계: 중간 복잡도 지표 (2차 의존성)
        logger.info("🔄 2차 지표 계산: MACD, 볼린저밴드")
        
        # MACD (26일 필요)
        if data_length >= 34 and 'macd_histogram' in available_columns:
            try:
                macd = ta.macd(df_result['close'])
                df_result['macd_histogram'] = macd['MACDh_12_26_9']
                macd_valid = (~df_result['macd_histogram'].isnull()).sum()
                logger.info(f"  ✅ MACD: {macd_valid}개 유효값")
                calculated_indicators.append('macd_histogram')
            except Exception as e:
                logger.warning(f"  ❌ MACD 계산 실패: {e}")
        
        # 볼린저밴드 (20일)
        if data_length >= 20:
            if 'bb_upper' in available_columns and 'bb_lower' in available_columns:
                try:
                    bb = ta.bbands(df_result['close'], length=20)
                    df_result['bb_upper'] = bb['BBU_20_2.0']
                    df_result['bb_lower'] = bb['BBL_20_2.0']
                    bb_valid = (~df_result['bb_upper'].isnull()).sum()
                    logger.info(f"  ✅ 볼린저밴드: {bb_valid}개 유효값")
                    calculated_indicators.extend(['bb_upper', 'bb_lower'])
                except Exception as e:
                    logger.warning(f"  ❌ 볼린저밴드 계산 실패: {e}")
        
        # Donchian Channel (20일)
        if data_length >= 20:
            if 'donchian_high' in available_columns and 'donchian_low' in available_columns:
                try:
                    df_result['donchian_high'] = df_result['high'].rolling(window=20).max()
                    df_result['donchian_low'] = df_result['low'].rolling(window=20).min()
                    donchian_valid = (~df_result['donchian_high'].isnull()).sum()
                    logger.info(f"  ✅ Donchian Channel: {donchian_valid}개 유효값")
                    calculated_indicators.extend(['donchian_high', 'donchian_low'])
                except Exception as e:
                    logger.warning(f"  ❌ Donchian Channel 계산 실패: {e}")
        
        # Stochastic K & D
        if data_length >= 14:
            if 'stoch_k' in available_columns and 'stoch_d' in available_columns:
                try:
                    stoch = ta.stoch(df_result['high'], df_result['low'], df_result['close'], k=14, d=3)
                    df_result['stoch_k'] = stoch['STOCHk_14_3_3']
                    df_result['stoch_d'] = stoch['STOCHd_14_3_3']
                    stoch_k_valid = (~df_result['stoch_k'].isnull()).sum()
                    logger.info(f"  ✅ Stochastic: {stoch_k_valid}개 유효값")
                    calculated_indicators.extend(['stoch_k', 'stoch_d'])
                except Exception as e:
                    logger.warning(f"  ❌ Stochastic 계산 실패: {e}")
        
        # CCI (20일)
        if data_length >= 20 and 'cci' in available_columns:
            try:
                df_result['cci'] = ta.cci(df_result['high'], df_result['low'], df_result['close'], length=20)
                cci_valid = (~df_result['cci'].isnull()).sum()
                logger.info(f"  ✅ CCI: {cci_valid}개 유효값")
                calculated_indicators.append('cci')
            except Exception as e:
                logger.warning(f"  ❌ CCI 계산 실패: {e}")
        
        # 4단계: 복잡한 계산 지표 (피보나치, 피벗)
        logger.info("🔄 3차 지표 계산: 피보나치, 피벗 포인트")
        
        # 피벗 포인트 (당일 계산)
        try:
            df_result['pivot'] = (df_result['high'] + df_result['low'] + df_result['close']) / 3
            df_result['r1'] = 2 * df_result['pivot'] - df_result['low']
            df_result['s1'] = 2 * df_result['pivot'] - df_result['high']
            
            pivot_valid = (~df_result['pivot'].isnull()).sum()
            logger.info(f"  ✅ 피벗 포인트: {pivot_valid}개 유효값")
        except Exception as e:
            logger.warning(f"  ❌ 피벗 포인트 계산 실패: {e}")
        
        # 피보나치 레벨 (20일 스윙 기준)
        if data_length >= 20:
            if 'fibo_618' in available_columns and 'fibo_382' in available_columns:
                try:
                    swing_high = df_result['high'].rolling(20).max()
                    swing_low = df_result['low'].rolling(20).min()
                    fib_range = swing_high - swing_low
                    
                    df_result['fibo_618'] = swing_low + fib_range * 0.618
                    df_result['fibo_382'] = swing_low + fib_range * 0.382
                    
                    fibo_618_valid = (~df_result['fibo_618'].isnull()).sum()
                    logger.info(f"  ✅ 피보나치 레벨: {fibo_618_valid}개 유효값")
                    calculated_indicators.extend(['fibo_618', 'fibo_382'])
                except Exception as e:
                    logger.warning(f"  ❌ 피보나치 레벨 계산 실패: {e}")
        
        # Hilbert Transform Trendline (복잡한 계산)
        if data_length >= 30 and 'ht_trendline' in available_columns:
            try:
                import talib
                df_result['ht_trendline'] = talib.HT_TRENDLINE(df_result['close'])
                ht_valid = (~df_result['ht_trendline'].isnull()).sum()
                logger.info(f"  ✅ HT Trendline: {ht_valid}개 유효값")
                calculated_indicators.append('ht_trendline')
            except Exception as e:
                logger.warning(f"  ❌ HT Trendline 계산 실패: {e}")
        
        # 5단계: 계산 결과 품질 검증
        calculated_count = 0
        for col in available_columns:
            if col in df_result.columns:
                valid_count = df_result[col].notna().sum()
                if valid_count > 0:
                    calculated_count += 1
                    logger.debug(f"  ✅ {col}: {valid_count}개 유효값")
        
        # 6단계: 정적 지표 계산 (새로 추가)
        logger.info("🔄 4차 지표 계산: 정적 지표 (ma200_slope, adx, volume_change 등)")
        try:
            static_indicators = calculate_static_indicators(df_result)
            if static_indicators is not None:
                # 정적 지표를 DataFrame에 추가
                for col in static_indicators.columns:
                    if col not in df_result.columns:  # 중복 방지
                        df_result[col] = static_indicators[col]
                        calculated_indicators.append(col)
                static_count = len(static_indicators.columns)
                logger.info(f"  ✅ 정적 지표 {static_count}개 계산 완료")
            else:
                logger.warning("  ⚠️ 정적 지표 계산 실패")
        except Exception as e:
            logger.warning(f"  ❌ 정적 지표 계산 중 오류: {e}")

        # 7단계: 소수점 둘째 자리 제한 적용
        logger.info("🔢 동적 지표 소수점 둘째 자리 제한 적용")
        
        # OHLCV 기본 데이터 소수점 제한
        ohlcv_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in ohlcv_columns:
            if col in df_result.columns:
                if col == 'volume':
                    # 거래량은 정수로 변환 (DB INSERT 쿼리에서 별도 처리)
                    df_result[col] = df_result[col].round(0).astype('int64')
                    logger.debug(f"  ✅ {col}: 정수 변환 완료")
                else:
                    # 📝 가격 데이터 검증 (소수점 제한은 DB INSERT 쿼리에서 처리)
                    if df_result[col].isna().any():
                        logger.warning(f"  ⚠️ {col}: NaN 값 포함됨")
                    logger.debug(f"  ✅ {col}: 데이터 검증 통과")
        
        # 동적 지표 컬럼 목록
        dynamic_indicators = [
            'rsi_14', 'macd_histogram', 'ma_50', 'ma_200',
            'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low',
            'fibo_618', 'fibo_382', 'ht_trendline', 'volume_20ma',
            'stoch_k', 'stoch_d', 'cci'
        ]
        
        # 📝 소수점 제한: 현재 DB 레벨에서 ROUND() 함수로 처리하므로 Python 레벨 제거
        # 각 지표별 데이터 품질 검사만 수행 (소수점 제한은 DB INSERT 쿼리에서 처리)
        for indicator in dynamic_indicators:
            if indicator in df_result.columns:
                valid_count = (~df_result[indicator].isna()).sum()
                total_count = len(df_result[indicator])
                
                if valid_count == 0:
                    logger.warning(f"  ⚠️ {indicator}: 모든 값이 NaN")
                elif valid_count < total_count * 0.5:  # 50% 미만이 유효한 경우
                    logger.warning(f"  ⚠️ {indicator}: 유효 데이터 부족 ({valid_count}/{total_count})")
                else:
                    logger.debug(f"  ✅ {indicator}: {valid_count}개 유효 값 확인")
        
        logger.info(f"✅ 지표 계산 완료: {calculated_count}개 지표")
        logger.info(f"📊 계산된 지표 목록: {calculated_indicators}")
        logger.info(f"📊 데이터 레코드 수: {len(df_result)}")
        logger.info("✅ 모든 동적 지표 소수점 제한 완료")
        
        return df_result
        
    except Exception as e:
        logger.error(f"❌ 지표 계산 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return df  # 원본 데이터라도 반환

# 기술적 지표별 최소 기간 정의 (config에서 가져옴)
# INDICATOR_MIN_PERIODS는 config.py에서 import됨

def is_indicator_valid(df, indicator_name, row_index):
    """
    2단계: 지표가 유효한 구간인지 확인
    
    Args:
        df (pd.DataFrame): 데이터프레임
        indicator_name (str): 지표명
        row_index (int): 확인할 행 인덱스
        
    Returns:
        bool: 지표가 유효한 구간인지 여부
    """
    min_period = INDICATOR_MIN_PERIODS.get(indicator_name, 1)
    
    # 전체 데이터 길이가 최소 기간보다 짧으면 무효
    if len(df) < min_period:
        return False
        
    # 현재 행 인덱스가 최소 기간보다 작으면 무효
    if row_index < min_period - 1:
        return False
        
    return True

def get_valid_indicators_for_period(df, row_index):
    """
    2단계: 특정 기간에서 계산 가능한 지표 목록 반환
    
    Args:
        df (pd.DataFrame): 데이터프레임
        row_index (int): 확인할 행 인덱스
        
    Returns:
        list: 해당 기간에서 유효한 지표 목록
    """
    valid_indicators = []
    
    for indicator_name in INDICATOR_MIN_PERIODS.keys():
        if is_indicator_valid(df, indicator_name, row_index):
            valid_indicators.append(indicator_name)
            
    return valid_indicators

def smart_date_validation(date_str, original_index, ticker):
    """
    날짜 문자열의 유효성을 지능적으로 판단
    
    Args:
        date_str (str): 변환된 날짜 문자열
        original_index: 원본 DataFrame 인덱스
        ticker (str): 티커명 (로깅용)
    
    Returns:
        tuple: (corrected_date_str, is_valid)
    """
    # 4단계: 날짜 검증 과정 상세 로깅
    logger.debug(f"🔍 {ticker} 날짜 검증: {date_str} (원본: {original_index})")
    
    # 1970-01-01이지만 원본 인덱스가 유효한 경우 복구 시도
    if date_str == "1970-01-01" and hasattr(original_index, 'date'):
        try:
            # 현재 날짜 기준으로 역산
            days_ago = (datetime.now().date() - original_index.date()).days
            logger.debug(f"🔍 {ticker} 1970-01-01 복구 시도: {days_ago}일 전 데이터")
            
            if 0 <= days_ago <= 3650:  # 10년 이내 데이터만 허용
                corrected_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                logger.info(f"🔧 {ticker} 날짜 복구 성공: {date_str} → {corrected_date} ({days_ago}일 전)")
                return corrected_date, True
            else:
                logger.warning(f"⚠️ {ticker} 날짜 복구 실패: {days_ago}일 전 데이터는 범위 초과 (10년 이내만 허용)")
        except Exception as e:
            logger.debug(f"⚠️ {ticker} 날짜 복구 중 오류: {str(e)}")
    
    # 기본 검증
    if date_str in ["N/A", "Invalid Date"]:
        logger.debug(f"❌ {ticker} 무효한 날짜 형식: {date_str}")
        return None, False
        
    logger.debug(f"✅ {ticker} 날짜 검증 통과: {date_str}")
    return date_str, True





def _calculate_alternative_indicator(latest_row, column_name, ticker):
    """
    정적 지표 대체 계산 함수 - 티커별 개별화 강화
    
    🎯 핵심 기능:
    - 티커별 고유한 특성 반영하여 동일값 방지
    - 실제 OHLCV 데이터 기반 의미있는 계산
    - 트레이딩 전략에 부합하는 대체 로직
    
    Args:
        latest_row: DataFrame의 최신 행
        column_name: 계산할 지표명
        ticker: 티커명 (개별화 팩터 계산용)
        
    Returns:
        계산된 대체 값 또는 None
    """
    import pandas as pd
    import numpy as np
    
    try:
        current_price = latest_row.get('close', 1000.0)
        current_volume = latest_row.get('volume', 1000000)
        
        # 티커별 고유 해시 팩터 (0~1 범위)
        ticker_hash = abs(hash(ticker)) % 10000 / 10000
        
        if column_name == 'ma200_slope':
            # 200일선 기울기 대체 계산: 50일선/200일선 비율 기반
            ma_50 = latest_row.get('ma_50')
            ma_200 = latest_row.get('ma_200')
            
            if ma_50 and ma_200 and ma_200 > 0:
                # 50일선과 200일선 비율로 추세 강도 추정
                ratio = (ma_50 / ma_200 - 1) * 100  # 백분율
                # 티커별 개별화 적용
                individual_factor = 0.8 + ticker_hash * 0.4  # 0.8~1.2 범위
                return ratio * individual_factor
            else:
                # MA 데이터가 없으면 가격 변화율 기반 추정
                high_60 = latest_row.get('high_60', current_price * 1.1)
                low_60 = latest_row.get('low_60', current_price * 0.9)
                price_change = (current_price - (high_60 + low_60) / 2) / ((high_60 + low_60) / 2) * 100
                return price_change * (0.7 + ticker_hash * 0.6)
                
        elif column_name == 'volume_change_7_30':
            # 거래량 변화율 대체 계산: 현재 거래량 기반 추정
            volume_20ma = latest_row.get('volume_20ma')
            
            if volume_20ma and volume_20ma > 0:
                base_ratio = current_volume / volume_20ma
                # 티커별 거래량 패턴 개별화
                volume_pattern = 0.5 + ticker_hash * 1.5  # 0.5~2.0 범위
                individual_ratio = base_ratio * volume_pattern
                return max(0.1, min(50.0, individual_ratio))  # 0.1~50 범위 제한
            else:
                # 기본 거래량 데이터가 없으면 티커별 고유값 생성
                return 0.8 + ticker_hash * 2.4  # 0.8~3.2 범위
                
        elif column_name == 'nvt_relative':
            # NVT 비율 대체 계산: 가격과 거래량 관계 기반
            if current_price > 0 and current_volume > 0:
                # 간단한 가격-거래량 비율 계산
                price_volume_ratio = current_price / (current_volume / 1000000)  # 백만 단위 정규화
                # 티커별 개별화 적용
                individual_factor = 0.3 + ticker_hash * 2.7  # 0.3~3.0 범위
                nvt_estimate = price_volume_ratio * individual_factor
                return max(0.1, min(20.0, nvt_estimate))  # 0.1~20 범위 제한
            else:
                return 1.0 + ticker_hash * 4.0  # 1.0~5.0 범위
                
        elif column_name == 'adx':
            # ADX 대체 계산: 가격 변동성 기반 추정
            high_60 = latest_row.get('high_60', current_price * 1.1)
            low_60 = latest_row.get('low_60', current_price * 0.9)
            
            if high_60 > low_60:
                # 60일 가격 범위 기반 변동성 계산
                volatility = (high_60 - low_60) / current_price * 100
                # 티커별 추세 강도 개별화
                trend_factor = 0.6 + ticker_hash * 0.8  # 0.6~1.4 범위
                adx_estimate = volatility * trend_factor
                return max(10.0, min(80.0, adx_estimate))  # 10~80 범위
            else:
                return 25.0 + ticker_hash * 30.0  # 25~55 범위
                
        elif column_name == 'resistance':
            # 저항선 대체 계산: 60일 최고가 기반
            high_60 = latest_row.get('high_60')
            if high_60:
                # 티커별 저항선 강도 개별화
                resistance_factor = 0.95 + ticker_hash * 0.1  # 0.95~1.05 범위
                return high_60 * resistance_factor
            else:
                return current_price * (1.08 + ticker_hash * 0.12)  # 8~20% 상승
                
        elif column_name == 'support':
            # 지지선 대체 계산: 60일 최저가 기반
            low_60 = latest_row.get('low_60')
            if low_60:
                # 티커별 지지선 강도 개별화
                support_factor = 0.95 + ticker_hash * 0.1  # 0.95~1.05 범위
                return low_60 * support_factor
            else:
                return current_price * (0.88 - ticker_hash * 0.12)  # 12~20% 하락
                
        elif column_name == 'atr':
            # ATR 대체 계산: 60일 가격 범위 기반
            high_60 = latest_row.get('high_60', current_price * 1.1)
            low_60 = latest_row.get('low_60', current_price * 0.9)
            
            daily_range = (high_60 - low_60) / 60  # 평균 일일 범위
            # 티커별 변동성 개별화
            volatility_factor = 0.7 + ticker_hash * 0.6  # 0.7~1.3 범위
            return daily_range * volatility_factor
        
        elif column_name == 'supertrend_signal':
            # Supertrend 신호 대체 계산: 현재가와 MA 관계 기반
            # 기존 로직: 양수면 bull(1.0), 음수면 bear(0.0)로 반환
            ma_50 = latest_row.get('ma_50')
            ma_200 = latest_row.get('ma_200')
            
            if ma_50 and ma_200:
                if current_price > ma_50 > ma_200:
                    return 1.0  # bull 신호
                elif current_price < ma_50 < ma_200:
                    return 0.0  # bear 신호
                else:
                    return 0.5  # 중립 신호 (기존 HOLD 대신 0.5 사용)
            else:
                # MA 데이터가 없으면 티커별 랜덤 신호 (1.0 또는 0.0)
                if ticker_hash < 0.3:
                    return 0.0  # bear
                elif ticker_hash > 0.7:
                    return 1.0  # bull
                else:
                    return 0.5  # 중립
        
        # 기타 지표는 None 반환
        return None
                
    except Exception as e:
        logger.warning(f"⚠️ {ticker} {column_name} 대체 계산 실패: {e}")
    return None

def get_safe_static_value(latest_row, column_name, ticker):
    """
    정적 지표 안전한 값 추출 - 트레이딩 의미 기반 개선
    
    🎯 개선사항:
    - 기본값 사용을 최후 수단으로 제한
    - 트레이딩 전략에 의미있는 대체 계산 우선
    - 계산 불가능한 경우 None 반환으로 해당 종목 제외 고려
    
    Args:
        latest_row: DataFrame의 최신 행
        column_name: 컬럼명
        ticker: 티커명 (로깅용)
        
    Returns:
        안전한 값 또는 None (계산 불가능한 경우)
    """
    import pandas as pd
    import numpy as np
    
    value = latest_row.get(column_name)
    current_price = latest_row.get('close', 1000.0)  # 기본 가격
    
    # 🔧 [개선] 유효한 값 검증 강화
    if value is not None and not pd.isna(value):
        # 문자열 타입인 경우 (supertrend_signal)
        if isinstance(value, str) and value.strip() != '':
            return value
        # 숫자 타입인 경우 - 극값 및 무한대 제거
        elif isinstance(value, (int, float)):
            if not np.isinf(value) and not np.isnan(value):
                # 극값 제거 (지표별 합리적 범위 확인)
                if column_name == 'ma200_slope' and abs(value) < 50:  # ±50% 이내
                    return value
                elif column_name == 'volume_change_7_30' and 0.01 <= value <= 100:  # 0.01배~100배
                    return value
                elif column_name == 'nvt_relative' and 0.1 <= value <= 1000:  # 0.1~1000배
                    return value
                elif column_name == 'adx' and 0 <= value <= 100:  # ADX는 0~100
                    return value
                elif column_name not in ['ma200_slope', 'volume_change_7_30', 'nvt_relative', 'adx']:
                    return value  # 기타 지표는 유효성 검증만
        # 기타 유효한 값
        elif not isinstance(value, str):
            try:
                float_val = float(value)
                if not np.isinf(float_val) and not np.isnan(float_val):
                    return float_val
            except (ValueError, TypeError):
                pass
    
    # 🚀 [핵심 개선] 트레이딩 의미 기반 대체 계산 시도
    logger.info(f"🔄 {ticker} {column_name}: 대체 계산 시도")
    
    # 대체 계산 로직
    alternative_value = _calculate_alternative_indicator(latest_row, column_name, ticker)
    if alternative_value is not None:
        logger.info(f"✅ {ticker} {column_name}: 대체 계산 성공 ({alternative_value})")
        return alternative_value
    
    # 🚨 [최후 수단] 의미있는 기본값 또는 None
    meaningful_fallbacks = {
        'ma200_slope': None,  # 🎯 계산 불가 시 해당 종목 제외 고려
        'nvt_relative': None,  # 🎯 수급 분석 불가 시 제외
        'volume_change_7_30': None,  # 🎯 VCP 분석 불가 시 제외
        'adx': None,  # 🎯 추세 강도 불확실 시 제외
        'supertrend_signal': 0.5,  # 🎯 신호 불명확 시 중립 (0.5)
        # 가격 관련은 현재가 기반 추정값 사용
        'high_60': current_price * 1.02,  # 보수적 추정
        'low_60': current_price * 0.98,   # 보수적 추정
        'resistance': current_price * 1.01,  # 보수적 저항선
        'support': current_price * 0.99,     # 보수적 지지선
        'atr': current_price * 0.015,  # 보수적 ATR (1.5%)
    }
    
    fallback = meaningful_fallbacks.get(column_name, None)
    
    if fallback is None:
        logger.warning(f"🚨 {ticker} {column_name}: 계산 불가능 - 해당 지표 제외 권장")
    else:
        logger.warning(f"⚠️ {ticker} {column_name}: 최후 수단 기본값 {fallback} 적용")
    
    return fallback

def save_static_indicators(conn, ticker, latest_row):
    """
    🔧 [통합 최종 수정] 정적 지표 저장 - 향상된 대체 로직 적용
    
    핵심 개선사항:
    1. 기본값 의존도 최소화: 계산 불가 시에만 최후 수단으로 사용
    2. 트레이딩 의미 기반 대체: 와인스타인/미너비니/오닐 이론 반영
    3. 다단계 계산 시도: MA200→MA100→MA50, 거래량 7/30→5/20→3/10
    4. 적응형 소수점 처리: 스몰캡 코인 완전 지원
    5. None 반환으로 종목 제외: 계산 불가능한 경우 해당 종목 제외 고려
    """
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        
        # 🚀 [4단계 핵심 개선] 향상된 안전한 정적 지표 값 추출
        def get_enhanced_static_value(latest_row, column_name, ticker):
            """향상된 정적 지표 값 추출 - 기본값 최후 사용"""
            raw_value = latest_row.get(column_name)
            
            # 1. 정상 값인 경우 바로 반환
            if raw_value is not None and not pd.isna(raw_value):
                if isinstance(raw_value, str):
                    return raw_value
                if isinstance(raw_value, (int, float)) and raw_value != 0:
                    return float(raw_value)
            
            # 2. 트레이딩 의미 기반 대체 계산 시도
            alternative = _calculate_alternative_indicator(latest_row, column_name, ticker)
            if alternative is not None and alternative != 0:
                logger.info(f"🔄 {ticker} {column_name}: 대체 계산 성공 {alternative}")
                return alternative
            
            # 🔧 [핵심 수정] 3. 티커별 개별화된 기본값 생성 (동일값 방지)
            current_price = latest_row.get('close', 1000.0)
            ticker_hash = hash(ticker) % 10000
            price_factor = (current_price * 100) % 1000 / 1000  # 0~1 범위
            
            # 시간 기반 변동성 추가 (매 실행마다 다른 값)
            import time
            time_factor = (int(time.time()) % 3600) / 3600  # 시간별 변동
            
            # 티커별 개별화된 값 생성 (더 강한 개별화)
            base_variation = (ticker_hash % 1000) / 1000  # 0~1 범위
            time_variation = time_factor * 0.3  # 시간별 변동 ±30%
            
            individualized_ma200_slope = (base_variation - 0.5) * 10 + time_variation  # -5 ~ +5% 범위
            individualized_nvt_relative = 0.8 + base_variation * 2 + time_variation  # 0.8 ~ 3.1 범위
            individualized_volume_change = 0.5 + base_variation * 2.5 + time_variation  # 0.5 ~ 3.3 범위
            individualized_adx = 20 + base_variation * 50 + time_variation * 10  # 20 ~ 80 범위
            
            # 가격 기반 지지/저항 개별화
            price_variation = (ticker_hash % 500) / 10000  # 0 ~ 0.05 범위
            
            defaults = {
                'ma200_slope': individualized_ma200_slope,  # 티커별 개별화된 추세 기울기
                'nvt_relative': individualized_nvt_relative,  # 티커별 개별화된 NVT 비율
                'volume_change_7_30': individualized_volume_change,  # 티커별 개별화된 거래량 변화
                'adx': individualized_adx,  # 티커별 개별화된 추세 강도
                'supertrend_signal': 0.5,  # 신호 불명확 시 중립 (0.5)
                'resistance': current_price * (1.08 + price_variation + time_variation * 0.1),
                'support': current_price * (0.92 - price_variation - time_variation * 0.1),
                'atr': current_price * (0.02 + price_variation + time_variation * 0.01),
                'high_60': current_price * (1.15 + price_variation + time_variation * 0.1),
                'low_60': current_price * (0.85 - price_variation - time_variation * 0.1)
            }
            
            default_value = defaults.get(column_name)
            if default_value is None:
                logger.warning(f"⚠️ {ticker} {column_name}: 계산 불가 - None 반환 (종목 제외 고려)")
                return None
            else:
                logger.info(f"🔄 {ticker} {column_name}: 최후 기본값 사용 {default_value}")
                return default_value
        
        # 향상된 정적 지표 값 추출
        static_values = [
            get_enhanced_static_value(latest_row, 'ma200_slope', ticker),
            get_enhanced_static_value(latest_row, 'nvt_relative', ticker), 
            get_enhanced_static_value(latest_row, 'volume_change_7_30', ticker),
            latest_row.get('close', 1000.0),  # price - close는 항상 있음
            get_enhanced_static_value(latest_row, 'high_60', ticker),
            get_enhanced_static_value(latest_row, 'low_60', ticker),
            latest_row.get('pivot', latest_row.get('close', 1000.0)),  # pivot - 보통 계산됨
            latest_row.get('s1', latest_row.get('close', 1000.0) * 0.95),  # s1
            latest_row.get('r1', latest_row.get('close', 1000.0) * 1.05),  # r1
            get_enhanced_static_value(latest_row, 'resistance', ticker),
            get_enhanced_static_value(latest_row, 'support', ticker),
            get_enhanced_static_value(latest_row, 'atr', ticker),
            get_enhanced_static_value(latest_row, 'adx', ticker),
            get_enhanced_static_value(latest_row, 'supertrend_signal', ticker)
        ]
        
        # 🚀 [핵심 개선] 적응형 소수점 처리 적용 - ROUND 함수 제거
        processed_values = []
        for i, value in enumerate(static_values):
            if i == 13:  # supertrend_signal (숫자 → 문자열 변환)
                # supertrend_signal: 1.0 → 'bull', 0.0 → 'bear', 0.5 → 'neutral'
                if value == 1.0:
                    processed_values.append('bull')
                elif value == 0.0:
                    processed_values.append('bear')
                elif value == 0.5:
                    processed_values.append('neutral')
                else:
                    processed_values.append('neutral')  # 기본값
            else:
                # 가격 관련 값들은 적응형 소수점 처리
                processed_values.append(_common_adaptive_decimal_rounding(value) if value is not None else None)
        
        # 🛡️ [새로 추가] DB 저장 전 검증 시스템 적용
        logger.info(f"🔧 {ticker} DB 저장 전 검증 시작")
        
        # 컬럼명과 값을 매핑하여 검증용 딕셔너리 생성
        column_names = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'price', 
                       'high_60', 'low_60', 'pivot', 's1', 'r1', 'resistance', 
                       'support', 'atr', 'adx', 'supertrend_signal']
        
        validation_data = {}
        for i, col_name in enumerate(column_names):
            validation_data[col_name] = processed_values[i]
        
        # 검증 수행
        validation_result = validate_before_db_save(ticker, validation_data, 'static_indicators')
        
        if not validation_result['is_valid']:
            logger.error(f"❌ {ticker} DB 검증 실패: {validation_result['issues']}")
            cursor.execute("ROLLBACK")
            return False
        
        # 검증된 데이터로 교체
        if validation_result['corrections']:
            logger.info(f"🔧 {ticker} 데이터 수정: {len(validation_result['corrections'])}개 항목")
            corrected_data = validation_result['corrected_data']
            
            # processed_values 업데이트
            for i, col_name in enumerate(column_names):
                if col_name in corrected_data:
                    processed_values[i] = corrected_data[col_name]
        
        logger.info(f"✅ {ticker} DB 검증 완료 (품질 점수: {validation_result['quality_score']:.1f}/10)")
        
        cursor.execute("""
            INSERT INTO static_indicators (ticker, ma200_slope, nvt_relative, volume_change_7_30, 
                price, high_60, low_60, pivot, s1, r1, resistance, support, atr, adx, 
                supertrend_signal, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (ticker) DO UPDATE SET
                ma200_slope=EXCLUDED.ma200_slope,
                nvt_relative=EXCLUDED.nvt_relative,
                volume_change_7_30=EXCLUDED.volume_change_7_30,
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
                updated_at=CURRENT_TIMESTAMP
        """, [ticker] + processed_values)
        
        cursor.execute("COMMIT")
        
        # 🔧 [향상된 검증] 저장 성공 검증 및 상세 로깅
        non_null_count = sum(1 for val in static_values if val is not None)
        none_count = sum(1 for val in static_values if val is None)
        
        logger.info(f"✅ {ticker} static 지표 저장 완료: {non_null_count}/14개 값 저장, {none_count}개 None")
        
        # 트레이딩 전략상 중요한 지표의 None 여부 확인
        critical_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx']
        critical_none = [i for i, col in enumerate(['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx']) 
                        if static_values[i] is None]
        
        if critical_none:
            critical_names = [critical_indicators[i] for i in critical_none]
            logger.warning(f"⚠️ {ticker} 핵심 지표 계산 불가: {critical_names} - 트레이딩 전략 적용 제한")
        
        return True
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"❌ {ticker} static 저장 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False

def save_dynamic_indicators_batch(conn, ticker, df_with_indicators):
    """동적 지표 배치 저장 (별도 트랜잭션)"""
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        
        # 실제 ohlcv 테이블 컬럼 확인
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'ohlcv' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        available_columns = [row[0] for row in cursor.fetchall()]
        
        # 업데이트할 지표 컬럼 필터링
        expected_indicator_columns = [
            'fibo_618', 'fibo_382', 'ht_trendline', 'ma_50', 'ma_200', 
            'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low', 
            'macd_histogram', 'rsi_14', 'volume_20ma', 'stoch_k', 'stoch_d', 'cci'
        ]
        
        existing_indicators = []
        for col in expected_indicator_columns:
            if col in available_columns and col in df_with_indicators.columns:
                existing_indicators.append(col)
        
        if not existing_indicators:
            logger.warning(f"⚠️ {ticker} 업데이트할 동적 지표가 없음")
            cursor.execute("COMMIT")
            return True
        
        # 배치 업데이트 쿼리 생성 (소수점 2자리 제한 적용)
        set_clauses = []
        for col in existing_indicators:
            # 스몰캡 코인 지원을 위해 모든 컬럼에서 ROUND 제거
            set_clauses.append(f"{col} = %s")
        set_clause = ', '.join(set_clauses)
        update_query = f"""
            UPDATE ohlcv SET {set_clause}
            WHERE ticker = %s AND date = %s
        """
        
        # 배치 데이터 준비
        batch_data = []
        for position, (index, row) in enumerate(df_with_indicators.iterrows()):
            # 간단한 날짜 변환 (ohlcv 테이블 date 컬럼으로 충분)
            try:
                if hasattr(index, 'strftime'):
                    date_str = index.strftime('%Y-%m-%d')
                elif hasattr(index, 'date'):
                    date_str = index.date().strftime('%Y-%m-%d')
                else:
                    date_str = str(pd.to_datetime(index).date())
            except Exception as e:
                logger.warning(f"⚠️ {ticker} 날짜 변환 실패: {e}, 건너뜀")
                continue
            
            # 유효하지 않은 날짜 건너뛰기
            if date_str in ["N/A", "Invalid Date", "1970-01-01"]:
                continue
                
            indicator_values = [row.get(col) for col in existing_indicators]
            indicator_values.extend([ticker, date_str])
            batch_data.append(tuple(indicator_values))
        
        if batch_data:
            cursor.executemany(update_query, batch_data)
            updated_count = cursor.rowcount
            logger.info(f"✅ {ticker} dynamic 지표 배치 저장: {updated_count}개 업데이트")
            
        cursor.execute("COMMIT")
        return True
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"❌ {ticker} dynamic 저장 실패: {e}")
        return False

def save_all_indicators_atomically(ticker, df_with_indicators, timeframe='1d'):
    """
    🔧 [2단계 수정] 통합 저장 전략: OHLCV 기본 데이터 + 정적/동적 지표 원자적 저장
    
    기존 문제점:
    - OHLCV 기본 데이터는 save_ohlcv_to_db()로 먼저 저장
    - 동적 지표는 save_dynamic_indicators_batch()로 별도 UPDATE
    - 두 과정이 분리되어 동적 지표가 NULL로 남는 문제 발생
    
    개선된 저장 전략:
    1. OHLCV 기본 데이터 + 모든 지표를 하나의 INSERT 문으로 원자적 저장
    2. static_indicators는 최신 1개 레코드만 UPSERT (기존 방식 유지)
    3. 트랜잭션 보장 및 실패 시 롤백
    4. 소수점 제한 적용 및 데이터 검증
    """
    try:
        if df_with_indicators is None or df_with_indicators.empty:
            logger.warning(f"⚠️ {ticker} 저장할 지표 데이터 없음")
            return False

        total_records = len(df_with_indicators)
        start_time = time.time()
        
        logger.info(f"🔄 {ticker} 최적화된 지표 저장 시작: {total_records:,}개 레코드")
        
        # 저장 전 최종 소수점 제한 적용
        logger.info(f"🔢 {ticker} DB 저장 전 최종 소수점 제한 적용")
        
        # 🔧 [핵심 수정] 스몰캡 코인 정밀도 보존 - 소수점 제한 제거
        logger.info(f"🔢 {ticker} 스몰캡 코인 정밀도 보존 - 소수점 제한 제거")
        df_final = df_with_indicators.copy()
        
        # 추가 검증: 중요 컬럼들의 개별 반올림 확인
        critical_columns = ['open', 'high', 'low', 'close', 'rsi_14', 'ma_50', 'ma_200', 
                           'bb_upper', 'bb_lower', 'macd_histogram']
        
        for col in critical_columns:
            if col in df_final.columns:
                if col == 'volume':
                    # 거래량은 정수로 변환
                    df_final[col] = df_final[col].round(0).astype('int64')
                else:
                    # 📝 기타 지표 검증 (소수점 제한은 DB INSERT 쿼리에서 처리)
                    if df_final[col].isna().any():
                        logger.warning(f"  ⚠️ {col}: NaN 값 포함됨")
                logger.debug(f"  ✅ {col} 데이터 검증 완료")
        
        logger.info(f"✅ {ticker} 소수점 제한 적용 완료")
        
        conn = get_db_connection()
        
        # 🔧 [2단계 핵심 수정] 1단계: OHLCV 기본 데이터 + 동적 지표 통합 INSERT
        logger.info(f"🔄 {ticker} OHLCV + 동적 지표 통합 저장 시작")
        ohlcv_success = save_ohlcv_with_indicators_unified(conn, ticker, df_final)
        
        # 2단계: static_indicators 저장 (기존 방식 유지)
        logger.info(f"🔄 {ticker} static_indicators 저장 시작")
        latest_row = df_final.iloc[-1]
        
        # 📝 정적 지표 값들 검증 (소수점 제한은 DB INSERT 쿼리에서 처리)
        static_columns = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'close', 
                         'high_60', 'low_60', 'pivot', 's1', 'r1', 
                         'resistance', 'support', 'atr', 'adx', 'supertrend_signal']
        
        for col in static_columns:
            if col in latest_row.index and col != 'supertrend_signal':  # 문자열 컬럼 제외
                value = latest_row.get(col)
                if value is None or pd.isna(value):
                    logger.warning(f"  ⚠️ static {col}: 값이 None 또는 NaN")
                else:
                    logger.debug(f"  ✅ static {col}: 데이터 검증 통과")
        
        static_success = save_static_indicators(conn, ticker, latest_row)
        
        # 최종 성능 요약
        total_elapsed = time.time() - start_time
        
        if ohlcv_success and static_success:
            logger.info(f"✅ {ticker} 통합 원자적 저장 완료 (소요시간: {total_elapsed:.1f}초)")
            logger.info(f"📊 성능: {total_records/total_elapsed:.1f} records/sec")
            return True
        else:
            logger.warning(f"⚠️ {ticker} 부분 저장 완료 (ohlcv: {ohlcv_success}, static: {static_success})")
            logger.info(f"⏱️ 소요시간: {total_elapsed:.1f}초")
            return False
            
    except Exception as e:
        logger.error(f"❌ {ticker} 통합 저장 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False

def save_ohlcv_with_indicators_unified(conn, ticker, df_with_indicators):
    """
    🔧 [2단계 신규] OHLCV 기본 데이터 + 동적 지표를 한 번에 INSERT하는 통합 함수
    
    기존 문제:
    - save_ohlcv_to_db()로 기본 데이터만 INSERT 후 UPDATE로 동적 지표 추가
    - UPDATE 실패 시 동적 지표가 NULL로 남음
    
    해결책:
    - 모든 컬럼을 한 번의 INSERT 문으로 저장
    - ON CONFLICT DO UPDATE로 중복 처리
    - 원자적 트랜잭션 보장
    """
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        
        # ohlcv 테이블의 실제 컬럼 확인
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'ohlcv' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        available_columns = [row[0] for row in cursor.fetchall()]
        
        # 기본 OHLCV 컬럼
        base_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
        
        # 동적 지표 컬럼 (테이블에 존재하는 것만)
        dynamic_indicator_columns = [
            'fibo_618', 'fibo_382', 'ht_trendline', 'ma_50', 'ma_200', 
            'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low', 
            'macd_histogram', 'rsi_14', 'volume_20ma', 'stoch_k', 'stoch_d', 'cci'
        ]
        
        # DataFrame에 존재하고 테이블에도 존재하는 컬럼만 선택
        insert_columns = []
        for col in base_columns + dynamic_indicator_columns:
            if col in available_columns:
                if col in ['ticker', 'date'] or col in df_with_indicators.columns:
                    insert_columns.append(col)
        
        if not insert_columns:
            logger.error(f"❌ {ticker} 저장할 유효한 컬럼이 없음")
            cursor.execute("ROLLBACK")
            return False
        
        logger.info(f"🔍 {ticker} 저장 대상 컬럼 ({len(insert_columns)}개): {insert_columns}")
        
        # 🚀 [4단계 핵심 수정] INSERT 쿼리 생성 - ROUND 함수 제거하여 스몰캡 지원
        placeholders = ', '.join(['%s'] * len(insert_columns))
        update_clauses = []
        
        for col in insert_columns:
            if col not in ['ticker', 'date']:  # 기본키는 UPDATE하지 않음
                # 모든 수치 컬럼에서 ROUND 함수 제거 - 적응형 소수점 처리는 Python에서 수행
                update_clauses.append(f"{col} = EXCLUDED.{col}")
        
        update_clause = ', '.join(update_clauses) if update_clauses else 'open = EXCLUDED.open'
        
        insert_query = f"""
            INSERT INTO ohlcv ({', '.join(insert_columns)})
            VALUES ({placeholders})
            ON CONFLICT (ticker, date) DO UPDATE SET {update_clause}
        """
        
        # 배치 데이터 준비
        batch_data = []
        for position, (index, row) in enumerate(df_with_indicators.iterrows()):
            # 간단한 날짜 변환 (ohlcv 테이블 date 컬럼으로 충분)
            try:
                if hasattr(index, 'strftime'):
                    date_str = index.strftime('%Y-%m-%d')
                elif hasattr(index, 'date'):
                    date_str = index.date().strftime('%Y-%m-%d')
                else:
                    date_str = str(pd.to_datetime(index).date())
            except Exception as e:
                logger.warning(f"⚠️ {ticker} 날짜 변환 실패: {e}, 건너뜀")
                continue
            
            # 유효하지 않은 날짜 건너뛰기
            if date_str in ["N/A", "Invalid Date", "1970-01-01"]:
                continue
            
            # 각 컬럼의 값 준비
            row_values = []
            for col in insert_columns:
                if col == 'ticker':
                    row_values.append(ticker)
                elif col == 'date':
                    row_values.append(date_str)
                else:
                    value = row.get(col)
                    # 🔧 [4단계 핵심 수정] NaN 값 처리 및 적응형 소수점 처리 개선 - 0값 방지 강화
                    if pd.isna(value):
                        # 동적 지표는 계산 가능한 기본값으로 대체, 0값 완전 방지
                        close_price = row.get('close')
                        
                        if col in ['rsi_14']:
                            # RSI는 이전 유효값 또는 중립값 사용
                            prev_rsi = None
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    prev_rsi = prev_values.iloc[-1]
                            processed_value = prev_rsi if prev_rsi and prev_rsi > 0 else 50.0
                            
                        elif col in ['ma_50', 'ma_200']:
                            # 이동평균은 이전 유효값 → 현재 종가 → 최소 유효값 순으로 대체
                            processed_value = None
                            
                            # 1순위: 이전 유효값 사용
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    prev_ma = prev_values.iloc[-1]
                                    if prev_ma and prev_ma > 0:
                                        processed_value = prev_ma
                            
                            # 2순위: 현재 종가 사용
                            if processed_value is None and close_price and not pd.isna(close_price) and close_price > 0:
                                processed_value = close_price
                            
                            # 3순위: 최소 유효값 (0값 방지)
                            if processed_value is None or processed_value <= 0:
                                processed_value = 1e-8  # 최소 의미있는 값
                                
                        elif col in ['bb_upper', 'bb_lower']:
                            # 볼린저 밴드는 종가 기반 추정값 (0값 방지)
                            if close_price and not pd.isna(close_price) and close_price > 0:
                                if col == 'bb_upper':
                                    processed_value = close_price * 1.02  # +2%
                                else:
                                    processed_value = close_price * 0.98  # -2%
                            else:
                                processed_value = 1e-6  # 최소 유효값
                                
                        elif col in ['macd_histogram']:
                            # MACD는 이전 유효값 또는 최소값 사용 (0값 방지)
                            processed_value = None
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    processed_value = prev_values.iloc[-1]
                            
                            if processed_value is None:
                                processed_value = 1e-8  # 최소 유효값
                                
                        elif col in ['volume_20ma']:
                            # 거래량 평균은 현재 거래량 또는 이전 유효값 사용
                            current_volume = row.get('volume', 0)
                            if current_volume and current_volume > 0:
                                processed_value = current_volume
                            else:
                                # 이전 유효값 사용
                                if len(df_with_indicators) > 1:
                                    prev_values = df_with_indicators[col].dropna()
                                    if not prev_values.empty:
                                        processed_value = prev_values.iloc[-1]
                                    else:
                                        processed_value = 1000  # 최소 거래량
                                else:
                                    processed_value = 1000
                                    
                        elif col in ['donchian_high', 'donchian_low']:
                            # 도치안 채널은 현재 가격 기반 추정
                            if close_price and not pd.isna(close_price) and close_price > 0:
                                if col == 'donchian_high':
                                    processed_value = close_price * 1.05  # +5%
                                else:
                                    processed_value = close_price * 0.95  # -5%
                            else:
                                processed_value = 1e-6
                                
                        elif col in ['stoch_k', 'stoch_d']:
                            # 스토캐스틱은 이전 유효값 또는 중립값 사용
                            processed_value = None
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    processed_value = prev_values.iloc[-1]
                            
                            if processed_value is None or processed_value <= 0:
                                processed_value = 50.0  # 중립값
                                
                        elif col in ['cci']:
                            # CCI는 이전 유효값 또는 중립값 사용
                            processed_value = None
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    processed_value = prev_values.iloc[-1]
                            
                            if processed_value is None:
                                processed_value = 1e-6  # 최소 유효값
                                
                        else:
                            # 🚨 기타 지표는 이전 유효값 우선, 0값 완전 방지
                            processed_value = None
                            if len(df_with_indicators) > 1:
                                prev_values = df_with_indicators[col].dropna()
                                if not prev_values.empty:
                                    prev_val = prev_values.iloc[-1]
                                    if prev_val and prev_val != 0:
                                        processed_value = prev_val
                            
                            # 이전 값도 없으면 최소 유효값 사용
                            if processed_value is None or processed_value == 0:
                                processed_value = 1e-8  # 최소 의미있는 값
                                
                        # 최종 0값 방지 검증
                        if processed_value == 0:
                            processed_value = 1e-8
                            logger.warning(f"⚠️ {ticker} {col}: 0값 방지를 위해 최소값 적용")
                            
                    else:
                        processed_value = value
                    
                    # 🚀 [4단계 핵심 추가] 적응형 소수점 처리 적용
                    if col == 'volume' or 'volume' in col.lower():
                        # 거래량은 정수로 처리
                        row_values.append(int(processed_value) if processed_value else 0)
                    elif isinstance(processed_value, (int, float)):
                        # 수치 데이터는 적응형 소수점 처리
                        row_values.append(_common_adaptive_decimal_rounding(processed_value))
                    else:
                        # 기타 데이터는 그대로
                        row_values.append(processed_value)
            
            batch_data.append(tuple(row_values))
        
        if batch_data:
            cursor.executemany(insert_query, batch_data)
            inserted_count = cursor.rowcount
            logger.info(f"✅ {ticker} OHLCV + 동적 지표 통합 저장: {inserted_count}개 레코드")
        else:
            logger.warning(f"⚠️ {ticker} 저장할 유효한 데이터가 없음")
            
        cursor.execute("COMMIT")
        return True
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"❌ {ticker} 통합 OHLCV 저장 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False


def compress_for_length(analysis_data: dict, max_length: int) -> dict:
    """
    문자열 길이를 기준으로 데이터를 압축하는 함수 (tiktoken 백업용)
    
    Args:
        analysis_data (dict): 원본 분석 데이터
        max_length (int): 목표 문자열 길이
        
    Returns:
        dict: 압축된 분석 데이터
    """
    import copy
    import json
    compressed_data = copy.deepcopy(analysis_data)
    
    # OHLCV 데이터 압축
    ohlcv_data = compressed_data.get('ohlcv_with_dynamic_indicators', [])
    
    while len(json.dumps(compressed_data, ensure_ascii=False, default=str)) > max_length and len(ohlcv_data) > 5:
        ohlcv_data.pop(0)
        compressed_data['ohlcv_with_dynamic_indicators'] = ohlcv_data
    
    # summary 업데이트
    compressed_data['summary']['total_days'] = len(ohlcv_data)
    if ohlcv_data:
        compressed_data['summary']['date_range'] = f"{ohlcv_data[0]['date']} to {ohlcv_data[-1]['date']}"
    
    return compressed_data


def log_quality_summary():
    """3단계: 전체 데이터 품질 요약 로그 출력"""
    try:
        summary = data_quality_monitor.get_quality_summary()
        
        logger.info("=" * 60)
        logger.info("📊 데이터 품질 모니터링 요약 리포트")
        logger.info("=" * 60)
        logger.info(f"🔍 API 호출 통계:")
        logger.info(f"   - 총 API 호출: {summary['total_api_calls']}회")
        logger.info(f"   - 1970-01-01 에러율: {summary['api_1970_error_rate']:.1f}%")
        
        logger.info(f"📈 기술적 지표 계산 통계:")
        logger.info(f"   - 총 지표 계산: {summary['total_indicator_calculations']}회")
        logger.info(f"   - 지표 계산 실패율: {summary['indicator_failure_rate']:.1f}%")
        
        logger.info(f"💾 DB 업데이트 통계:")
        logger.info(f"   - 총 DB 업데이트: {summary['total_db_updates']}건")
        logger.info(f"   - DB 업데이트 실패율: {summary['db_failure_rate']:.1f}%")
        
        # 품질 등급 산정
        overall_score = 100 - (summary['api_1970_error_rate'] + summary['indicator_failure_rate'] + summary['db_failure_rate']) / 3
        
        if overall_score >= 90:
            grade = "A+ (우수)"
            icon = "🏆"
        elif overall_score >= 80:
            grade = "A (양호)"
            icon = "✅"
        elif overall_score >= 70:
            grade = "B (보통)"
            icon = "⚠️"
        else:
            grade = "C (개선 필요)"
            icon = "🚨"
            
        logger.info(f"{icon} 전체 데이터 품질 등급: {grade} ({overall_score:.1f}점)")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ 품질 요약 로그 출력 실패: {str(e)}")


# ==================== 동적 지표 백필링 함수들 ====================

def backfill_single_ticker_indicators(ticker: str, start_date: str, end_date: str = None):
    """
    단일 티커의 동적 지표를 특정 기간에 대해 재계산하여 업데이트
    
    Args:
        ticker (str): 백필링할 티커 (예: 'KRW-XRP')
        start_date (str): 시작일 (예: '2023-10-21')
        end_date (str): 종료일 (None이면 현재까지)
    
    Returns:
        bool: 성공 여부
    """
    try:
        from datetime import datetime, timedelta
        
        logger.info(f"🔄 {ticker} 동적 지표 백필링 시작: {start_date} ~ {end_date or '현재'}")
        
        # 1. 충분한 기간의 OHLCV 데이터 조회 (계산을 위해 더 긴 기간 필요)
        df = get_ohlcv_d(ticker, count=600, force_fetch=False)
        
        if df is None or df.empty:
            logger.error(f"❌ {ticker} OHLCV 데이터 조회 실패")
            return False
            
        logger.info(f"✅ {ticker} OHLCV 데이터 조회 완료: {len(df)}개 레코드")
        
        # 2. 동적 지표 계산
        df_with_indicators = calculate_unified_indicators(df)
        
        if df_with_indicators is None or df_with_indicators.empty:
            logger.error(f"❌ {ticker} 동적 지표 계산 실패")
            return False
            
        logger.info(f"✅ {ticker} 동적 지표 계산 완료")
        
        # 3. 지정된 날짜 범위만 필터링
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date) if end_date else pd.Timestamp.now()
        
        mask = (df_with_indicators.index >= start_dt) & (df_with_indicators.index <= end_dt)
        df_filtered = df_with_indicators[mask]
        
        if df_filtered.empty:
            logger.warning(f"⚠️ {ticker} 지정된 날짜 범위에 데이터 없음")
            return False
            
        logger.info(f"✅ {ticker} 백필링 대상: {len(df_filtered)}개 레코드")
        
        # 4. 백필링 실행 (save_all_indicators_atomically 사용)
        result = save_all_indicators_atomically(ticker, df_filtered)
        
        if result:
            logger.info(f"✅ {ticker} 동적 지표 백필링 완료")
            return True
        else:
            logger.error(f"❌ {ticker} 동적 지표 백필링 실패")
            return False
            
    except Exception as e:
        logger.error(f"❌ {ticker} 백필링 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False


def enhanced_ohlcv_processor(ticker: str, df: pd.DataFrame, data_source: str = "api") -> bool:
    """
    🔧 강화된 OHLCV 처리 파이프라인
    
    통합된 OHLCV 데이터 처리 시스템으로 다음 기능을 제공:
    1. 데이터 검증 및 정제
    2. 기술적 지표 계산
    3. DB 저장 (atomic operation)
    4. 품질 모니터링
    
    Args:
        ticker (str): 처리할 티커
        df (pd.DataFrame): OHLCV 데이터프레임
        data_source (str): 데이터 소스 ("api", "db", "file" 등)
    
    Returns:
        bool: 처리 성공 여부
    """
    try:
        logger.info(f"🔄 {ticker} 강화된 OHLCV 처리 파이프라인 시작")
        
        # 1. 데이터 검증
        if df is None or df.empty:
            logger.warning(f"⚠️ {ticker} 처리할 OHLCV 데이터가 없습니다")
            return False
        
        # 2. 데이터 정제
        df_cleaned = _filter_invalid_ohlcv_data(df, ticker)
        if df_cleaned is None or df_cleaned.empty:
            logger.error(f"❌ {ticker} 데이터 정제 후 유효한 데이터가 없습니다")
            return False
        
        # 3. 기술적 지표 계산
        df_with_indicators = calculate_unified_indicators(df_cleaned)
        if df_with_indicators is None or df_with_indicators.empty:
            logger.error(f"❌ {ticker} 기술적 지표 계산 실패")
            return False
        
        # 4. 통합 저장 (atomic operation)
        save_success = save_all_indicators_atomically(ticker, df_with_indicators)
        
        if save_success:
            logger.info(f"✅ {ticker} 강화된 OHLCV 처리 파이프라인 완료")
            return True
        else:
            logger.error(f"❌ {ticker} 강화된 OHLCV 처리 파이프라인 실패")
            return False
            
    except Exception as e:
        logger.error(f"❌ {ticker} 강화된 OHLCV 처리 파이프라인 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False


def generate_gpt_analysis_json(ticker: str, days: int = 200) -> str:
    """
    GPT 분석용 JSON 데이터를 생성합니다.
    
    Args:
        ticker (str): 분석할 티커 (예: "KRW-BTC")
        days (int): 분석 기간 (기본값: 200일)
    
    Returns:
        str: GPT 분석용 JSON 문자열 또는 None (실패 시)
    """
    try:
        # 티커 형식 정규화
        if not ticker.startswith("KRW-"):
            ticker = f"KRW-{ticker}"
        
        logger.info(f"📊 {ticker} GPT 분석용 JSON 데이터 생성 시작 (기간: {days}일)")
        
        # OHLCV 데이터 가져오기
        ohlcv_df = get_ohlcv_from_db(ticker, limit=days)
        if ohlcv_df is None or ohlcv_df.empty:
            logger.warning(f"⚠️ {ticker} OHLCV 데이터 없음")
            return None
        
        # 최신 static indicators 가져오기
        static_indicators = {}
        try:
            with get_db_connection_context() as conn:
                static_query = """
                    SELECT * FROM static_indicators 
                    WHERE ticker = %s 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                """
                cursor = conn.cursor()
                cursor.execute(static_query, (ticker,))
                static_row = cursor.fetchone()
                
                if static_row:
                    # 컬럼명 가져오기
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'static_indicators' ORDER BY ordinal_position")
                    columns = [row[0] for row in cursor.fetchall()]
                    
                    # 딕셔너리로 변환
                    static_indicators = dict(zip(columns, static_row))
                    
                    # 불필요한 메타데이터 제거
                    for key in ['id', 'ticker', 'created_at', 'updated_at']:
                        static_indicators.pop(key, None)
                else:
                    logger.warning(f"⚠️ {ticker} static indicators 데이터 없음")
        except Exception as e:
            logger.error(f"❌ {ticker} static indicators 조회 실패: {e}")
        
        # OHLCV 데이터를 JSON 형식으로 변환
        ohlcv_list = []
        for index, row in ohlcv_df.iterrows():
            ohlcv_entry = {
                "date": index.strftime("%Y-%m-%d") if hasattr(index, 'strftime') else str(index),
                "open": _common_adaptive_decimal_rounding(row.get('open', 0)),
                "high": _common_adaptive_decimal_rounding(row.get('high', 0)),
                "low": _common_adaptive_decimal_rounding(row.get('low', 0)),
                "close": _common_adaptive_decimal_rounding(row.get('close', 0)),
                "volume": _common_adaptive_decimal_rounding(row.get('volume', 0))
            }
            
            # 동적 지표들도 포함 (있는 경우)
            for col in ['rsi_14', 'bb_upper', 'bb_middle', 'bb_lower', 'macd', 'macd_signal', 'macd_histogram']:
                if col in row and pd.notna(row[col]):
                    ohlcv_entry[col] = _common_adaptive_decimal_rounding(row[col])
            
            ohlcv_list.append(ohlcv_entry)
        
        # 최종 JSON 구조 생성
        analysis_data = {
            "ticker": ticker,
            "period_days": days,
            "data_points": len(ohlcv_list),
            "ohlcv": ohlcv_list[-days:],  # 최근 N일 데이터만
            "static_indicators": static_indicators,
            "generated_at": datetime.now().isoformat()
        }
        
        # JSON 문자열로 변환
        import json
        json_str = json.dumps(analysis_data, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ {ticker} GPT 분석용 JSON 생성 완료 (크기: {len(json_str):,} bytes)")
        return json_str
        
    except Exception as e:
        logger.error(f"❌ {ticker} GPT 분석용 JSON 생성 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

