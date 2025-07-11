import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()
import os
import pandas as pd
import pandas_ta as ta
import talib
from datetime import datetime
import logging
import concurrent.futures
import yaml
import inspect  # ratio 호환성 래퍼를 위해 추가
from data_fetcher import (
    calculate_technical_indicators,
    save_chart_image,
    get_ohlcv_d
)
from utils import load_blacklist, setup_logger

# 필터링 규칙 모듈들 import
from filter_rules.rule_price import price_above_ma200, price_above_high60, has_valid_price_data
from filter_rules.rule_momentum import macd_positive, adx_strength, golden_cross, has_valid_momentum_data, supertrend_bullish
from filter_rules.rule_volume import volume_surge, has_valid_volume_data

# 1. 모드 프리셋 불러오기 추가
from config.mode_presets import MODE_PRESETS

# ratio 인자 유무에 따라 함수 호환성 보장 래퍼
def wrap_with_ratio_support(func):
    if 'ratio' not in inspect.signature(func).parameters:
        def wrapped(row, ratio=1.0):
            return func(row)
        return wrapped
    return func

# 함수에 래퍼 적용
price_above_ma200 = wrap_with_ratio_support(price_above_ma200)
price_above_high60 = wrap_with_ratio_support(price_above_high60)

# 로거 초기화
logger = setup_logger()

# 1억원 단위 전역변수 추가
ONE_HMIL_KRW = 100000000  # 1억원

def load_filter_config(config_path: str = "config/filter_rules_config.yaml") -> dict:
    """
    필터링 규칙 설정 파일을 로드합니다.
    
    Args:
        config_path (str): 설정 파일 경로
        
    Returns:
        dict: 설정 값들
    """
    try:
        # 1. YAML 파일 로딩
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file) or {}
            
        # 2. mode 값 추출 (기본값은 'tight')
        mode_key = config.get("mode", "tight")
        
        # 3. YAML의 modes 섹션에서 해당 모드 설정 추출
        yaml_mode_config = {}
        if "modes" in config and mode_key in config["modes"]:
            yaml_mode_config = config["modes"][mode_key]
            logger.info(f"🔧 YAML에서 {mode_key} 모드 설정 로드: {yaml_mode_config}")
        
        # 4. MODE_PRESETS에서 해당 프리셋 불러오기 (fallback용)
        preset = MODE_PRESETS.get(mode_key, {})
        
        # 5. 우선순위: YAML modes > MODE_PRESETS > YAML 최상위 설정
        # - 기본 프리셋으로 시작
        merged_config = preset.copy()
        # - YAML 최상위 설정으로 업데이트 (modes, mode 키 제외)
        for key, value in config.items():
            if key not in ["modes", "mode"]:
                merged_config[key] = value
        # - YAML modes 설정으로 최종 업데이트 (최고 우선순위)
        merged_config.update(yaml_mode_config)
        # - mode 키 보존
        merged_config["mode"] = mode_key
        
        logger.info(f"✅ 필터링 설정 파일 로드 완료: {config_path}, 모드: {mode_key}")
        logger.info(f"🔧 최종 적용 설정: ADX임계값={merged_config.get('adx_threshold')}, 최소보조조건={merged_config.get('min_optional_conditions_passed')}")
        
        # 6. 병합 결과 반환
        return merged_config
        
    except FileNotFoundError:
        logger.warning(f"⚠️ 설정 파일을 찾을 수 없습니다: {config_path}. 기본값 사용")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML 파일 파싱 오류: {e}. 기본값 사용")
        return {}
    except Exception as e:
        logger.error(f"❌ 설정 파일 로드 중 오류 발생: {e}. 기본값 사용")
        return {}

def is_peak_trading_hour():
    now = datetime.now().hour
    return 0 <= now < 6 or 22 <= now < 24  # Peak trading hours in KST

DB_PATH = "makenaide.db"

def fetch_ohlcv_data():
    """ 데이터베이스에서 OHLCV 데이터를 로드하여 DataFrame으로 반환 """
    engine = create_engine(
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )
    query = "SELECT * FROM ohlcv"
    df = pd.read_sql_query(query, engine)

    # 날짜 컬럼을 datetime 형식으로 변환 후 인덱스로 설정
    df['date'] = pd.to_datetime(df['date'])
    df.set_index(['ticker', 'date'], inplace=True)
    df.index.set_names(["ticker", "datetime"], inplace=True)

    return df

def filter_breakout_candidates(market_df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    추세추종 기반 돌파 매매 전략에 따라 상승 초기 구간의 종목을 선별합니다.
    스탠 와인스타인의 1단계(바닥) → 2단계(상승초기) 구간 진입 또는 전고점 돌파 구간 검출
    
    [필수 조건] - 둘 다 만족해야 통과
    1. 현재가가 200일 이평선 위에 있어야 함: price > ma_200
    2. 현재가가 최근 60일 고점을 상향 돌파: price > high_60
    
    [보조 조건] - 설정된 최소 개수 이상 만족
    1. MACD > 0: 양봉 전환 초기 여부
    2. ADX >= threshold: 추세 강도 판단
    3. ma_50 > ma_200: 골든크로스 여부
    4. volume > volume_20ma: 거래량 증가 여부
    
    Args:
        market_df (pd.DataFrame): 일봉 시장 데이터 (기술지표 포함)
        config (dict): 필터링 설정 값 (None일 경우 YAML 파일에서 로드)
        
    Returns:
        pd.DataFrame: 필터링된 종목 데이터
    """
    try:
        if market_df.empty:
            logger.warning("⚠️ 시장 데이터가 비어있습니다.")
            return pd.DataFrame()
            
        # 설정값 로드 (config가 None이면 YAML 파일에서 로드)
        if config is None:
            config = load_filter_config()
            
        # 기본 설정값 (YAML 로드 실패시 또는 누락된 값들을 위한 fallback)
        default_config = {
            "require_price_above_ma200": True,
            "require_price_above_high60": True,
            "check_macd_positive": True,
            "check_adx_strength": True,
            "check_golden_cross": True,
            "check_volume_surge": True,
            "check_supertrend_bullish": True,
            "supertrend_signal_value": 'bull',
            "adx_threshold": 20,
            "min_optional_conditions_passed": 2,
            "max_filtered_tickers": 20,
            "enable_debug_logging": False
        }
        
        # 기본값과 로드된 설정값 병합
        for key, default_value in default_config.items():
            if key not in config:
                config[key] = default_value
                
        # 디버그 로깅 설정 적용
        debug_enabled = config.get("enable_debug_logging", False)
            
        # 블랙리스트 로드
        blacklist = load_blacklist()
        filtered_data = []
        
        # 📊 필터링 리포트 추적용 딕셔너리
        filter_report = {}
        
        logger.info(f"🔍 추세추종 돌파 필터링 시작: 총 {len(market_df)} 종목 검사")
        
        # 현재 적용된 필터링 모드 로그 출력
        mode_key = config.get("mode", "default")
        logger.info(f"🧩 현재 적용된 필터링 모드: {mode_key}")
        
        logger.info(f"📋 적용된 설정: ADX임계값={config['adx_threshold']}, 최소보조조건={config['min_optional_conditions_passed']}")
        
        for ticker in market_df.index:
            try:
                # 블랙리스트 체크
                if ticker in blacklist:
                    if debug_enabled:
                        logger.debug(f"⛔️ {ticker} 블랙리스트 제외")
                    # 블랙리스트 종목도 리포트에 기록
                    filter_report[ticker] = {
                        "price_above_ma200": None,
                        "price_above_high60": None,
                        "macd_positive": None,
                        "adx_strength": None,
                        "golden_cross": None,
                        "volume_surge": None,
                        "supertrend_bullish": None,
                        "passed": False,
                        "reason": "blacklisted"
                    }
                    continue
                    
                row = market_df.loc[ticker]
                
                # === [1] 필수 조건 검사 - 규칙 함수 사용 ===
                # 데이터 유효성 검사
                price_data_validity = has_valid_price_data(row)
                
                # 필수 조건 1: 현재가 > 200일 이동평균선
                must_pass_price_ma200 = False
                if config.get("require_price_above_ma200", True):
                    if not price_data_validity['has_price_data'] or not price_data_validity['has_ma200_data']:
                        if debug_enabled:
                            logger.debug(f"❌ {ticker} price 또는 ma_200 데이터 없음")
                        # 데이터 부족 종목도 리포트에 기록
                        filter_report[ticker] = {
                            "price_above_ma200": None,
                            "price_above_high60": None,
                            "macd_positive": None,
                            "adx_strength": None,
                            "golden_cross": None,
                            "volume_surge": None,
                            "supertrend_bullish": None,
                            "passed": False,
                            "reason": "insufficient_data"
                        }
                        continue
                    # (1) 현재가 > 200일 이동평균선 확인
                    ratio_ma200 = config.get("price_above_ma200_ratio", 1.0)
                    must_pass_price_ma200 = price_above_ma200(row, ratio_ma200)
                    if not must_pass_price_ma200:
                        if debug_enabled:
                            logger.debug(f"❌ {ticker} price({row['price']:.2f}) <= ma_200({row['ma_200']:.2f}) * ratio({ratio_ma200})")
                else:
                    must_pass_price_ma200 = True  # 조건 비활성화시 통과
                
                # 필수 조건 2: 현재가 > 60일 고점 (돌파)
                must_pass_high60 = False
                if config.get("require_price_above_high60", True):
                    if not price_data_validity['has_high60_data']:
                        if debug_enabled:
                            logger.debug(f"❌ {ticker} high_60 데이터 없음")
                        # 데이터 부족 종목도 리포트에 기록
                        filter_report[ticker] = {
                            "price_above_ma200": must_pass_price_ma200,
                            "price_above_high60": None,
                            "macd_positive": None,
                            "adx_strength": None,
                            "golden_cross": None,
                            "volume_surge": None,
                            "passed": False,
                            "reason": "insufficient_data"
                        }
                        continue
                    # (2) 현재가 > 60일 고점 확인
                    ratio_high60 = config.get("price_near_high60_ratio", 1.0)
                    must_pass_high60 = price_above_high60(row, ratio_high60)
                    if not must_pass_high60:
                        if debug_enabled:
                            logger.debug(f"❌ {ticker} price({row['price']:.2f}) <= high_60({row['high_60']:.2f}) * ratio({ratio_high60})")
                else:
                    must_pass_high60 = True  # 조건 비활성화시 통과
                
                # === [2] 보조 조건 검사 - 규칙 함수 사용 ===
                optional_score = 0
                optional_details = []
                
                # 보조 조건 1: MACD > 0 (양봉 전환 초기)
                optional_macd_positive = False
                if config.get("check_macd_positive", True):
                    optional_macd_positive = macd_positive(row)
                    if optional_macd_positive:
                        optional_score += 1
                        optional_details.append("MACD양전환")
                        if debug_enabled:
                            logger.debug(f"✅ {ticker} MACD 양전환 ({row['macd']:.4f})")
                
                # 보조 조건 2: ADX >= threshold (추세 강도)
                optional_adx_strength = False
                if config.get("check_adx_strength", True):
                    adx_threshold = config.get("adx_threshold", 20)
                    optional_adx_strength = adx_strength(row, threshold=adx_threshold)
                    if optional_adx_strength:
                        optional_score += 1
                        optional_details.append("ADX강세")
                        if debug_enabled:
                            logger.debug(f"✅ {ticker} ADX 강세 ({row['adx']:.2f} >= {adx_threshold})")
                
                # 보조 조건 3: ma_50 > ma_200 (골든크로스)
                optional_golden_cross = False
                if config.get("check_golden_cross", True):
                    optional_golden_cross = golden_cross(row)
                    if optional_golden_cross:
                        optional_score += 1
                        optional_details.append("골든크로스")
                        if debug_enabled:
                            logger.debug(f"✅ {ticker} 골든크로스")
                
                # 보조 조건 4: volume > volume_20ma (거래량 증가)
                optional_volume_surge = False
                if config.get("check_volume_surge", True):
                    optional_volume_surge = volume_surge(row)
                    if optional_volume_surge:
                        optional_score += 1
                        optional_details.append("거래량증가")
                        if debug_enabled:
                            logger.debug(f"✅ {ticker} 거래량 증가")
                
                # 보조 조건 5: Supertrend 상승 신호
                optional_supertrend_bullish = False
                if config.get("check_supertrend_bullish", True):
                    signal_value = config.get("supertrend_signal_value", 'bull')
                    optional_supertrend_bullish = supertrend_bullish(row, signal_value)
                    if optional_supertrend_bullish:
                        optional_score += 1
                        optional_details.append("Supertrend상승")
                        if debug_enabled:
                            logger.debug(f"✅ {ticker} Supertrend 상승 신호 ({signal_value})")
                
                # 전체 필수 조건 통과 여부
                must_pass_all = must_pass_price_ma200 and must_pass_high60
                
                # 최소 보조 조건 확인
                min_optional = config.get("min_optional_conditions_passed", 2)
                optional_pass = optional_score >= min_optional
                
                # 최종 통과 여부
                final_passed = must_pass_all and optional_pass
                
                # 📊 리포트에 종목별 조건 통과 여부 기록
                filter_report[ticker] = {
                    "price_above_ma200": must_pass_price_ma200,
                    "price_above_high60": must_pass_high60,
                    "macd_positive": optional_macd_positive,
                    "adx_strength": optional_adx_strength,
                    "golden_cross": optional_golden_cross,
                    "volume_surge": optional_volume_surge,
                    "supertrend_bullish": optional_supertrend_bullish,
                    "passed": final_passed,
                    "optional_score": optional_score,
                    "min_optional_required": min_optional,
                    "reason": "passed" if final_passed else ("must_conditions_failed" if not must_pass_all else "optional_conditions_insufficient")
                }
                
                if final_passed:
                    # 필터 통과 데이터 저장 (조건별 상세 정보 포함)
                    ticker_data = row.copy()
                    ticker_data['optional_score'] = optional_score
                    ticker_data['optional_details'] = ', '.join(optional_details)
                    
                    # 조건별 통과 여부 추가 (추후 분석용)
                    ticker_data['must_pass_price_ma200'] = must_pass_price_ma200
                    ticker_data['must_pass_high60'] = must_pass_high60
                    ticker_data['optional_macd_positive'] = optional_macd_positive
                    ticker_data['optional_adx_strength'] = optional_adx_strength
                    ticker_data['optional_golden_cross'] = optional_golden_cross
                    ticker_data['optional_volume_surge'] = optional_volume_surge
                    ticker_data['optional_supertrend_bullish'] = optional_supertrend_bullish
                    
                    filtered_data.append(ticker_data)
                    
                    logger.info(f"✨ {ticker} 돌파 필터 통과 (보조조건: {optional_score}/{min_optional}, 상세: {', '.join(optional_details)})")
                else:
                    if debug_enabled:
                        if not must_pass_all:
                            logger.debug(f"❌ {ticker} 필수조건 미통과")
                        else:
                            logger.debug(f"❌ {ticker} 보조조건 부족 ({optional_score}/{min_optional})")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} 필터링 중 오류 발생: {e}")
                # 오류 발생 종목도 리포트에 기록
                filter_report[ticker] = {
                    "price_above_ma200": None,
                    "price_above_high60": None,
                    "macd_positive": None,
                    "adx_strength": None,
                    "golden_cross": None,
                    "volume_surge": None,
                    "supertrend_bullish": None,
                    "passed": False,
                    "reason": f"error: {str(e)}"
                }
                continue
        
        # 📊 필터링 리포트 출력
        generate_filter_report(filter_report, config)
                
        # DataFrame 생성
        if filtered_data:
            result_df = pd.DataFrame(filtered_data)
            result_df.index = [data.name for data in filtered_data]  # ticker를 인덱스로 설정
            
            # 보조 점수 기준 내림차순 정렬
            result_df = result_df.sort_values('optional_score', ascending=False)
            
            # 최대 종목 수 제한
            max_tickers = config.get("max_filtered_tickers", 20)
            if len(result_df) > max_tickers:
                result_df = result_df.head(max_tickers)
                logger.info(f"📊 상위 {max_tickers}개 종목으로 제한")
            
            logger.info(f"📊 추세추종 돌파 필터링 결과: 총 {len(market_df)} 종목 중 {len(result_df)}개 통과")
            logger.info(f"✅ 통과 종목 목록: {', '.join(result_df.index.tolist())}")
            
            return result_df
        else:
            logger.warning("⚠️ 필터링 조건을 만족하는 종목이 없습니다.")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"❌ filter_breakout_candidates 실행 중 오류 발생: {e}")
        return pd.DataFrame()

def validate_data_consistency(static_df: pd.DataFrame, dynamic_df: pd.DataFrame) -> dict:
    """
    정적+동적 데이터 간 시간 동기화 및 일관성을 검증합니다.
    
    Args:
        static_df: static_indicators 테이블 데이터
        dynamic_df: ohlcv 테이블 데이터 (최신 데이터)
        
    Returns:
        dict: 검증 결과 및 상세 정보
    """
    validation_result = {
        'is_valid': True,
        'warnings': [],
        'errors': [],
        'ticker_consistency': {},
        'data_freshness': {},
        'column_completeness': {}
    }
    
    try:
        # 1. 티커 일치성 확인
        static_tickers = set(static_df.index) if not static_df.empty else set()
        dynamic_tickers = set(dynamic_df.index) if not dynamic_df.empty else set()
        
        common_tickers = static_tickers & dynamic_tickers
        static_only = static_tickers - dynamic_tickers
        dynamic_only = dynamic_tickers - static_tickers
        
        validation_result['ticker_consistency'] = {
            'common_count': len(common_tickers),
            'static_only_count': len(static_only),
            'dynamic_only_count': len(dynamic_only),
            'coverage_ratio': len(common_tickers) / len(static_tickers) if static_tickers else 0
        }
        
        # 경고: 커버리지가 낮은 경우
        if validation_result['ticker_consistency']['coverage_ratio'] < 0.7:
            validation_result['warnings'].append(
                f"낮은 데이터 커버리지: {validation_result['ticker_consistency']['coverage_ratio']:.1%}"
            )
        
        # 2. 데이터 freshness 검증 (최신성)
        current_time = datetime.now()
        
        # static_indicators 최신성 확인
        if not static_df.empty and 'updated_at' in static_df.columns:
            static_updates = pd.to_datetime(static_df['updated_at'], errors='coerce')
            latest_static = static_updates.max()
            if pd.notnull(latest_static):
                static_age_hours = (current_time - latest_static).total_seconds() / 3600
                validation_result['data_freshness']['static_age_hours'] = static_age_hours
                
                if static_age_hours > 24:  # 24시간 이상 오래된 데이터
                    validation_result['warnings'].append(f"정적 지표 데이터가 오래됨: {static_age_hours:.1f}시간 전")
        
        # dynamic 데이터는 날짜 기준으로 최신성 확인 (date 컬럼 없이 조회되므로 현재 시점 기준)
        validation_result['data_freshness']['dynamic_assumed_current'] = True
        
        # 3. 필수 컬럼 존재 여부 확인
        required_static_columns = ['price', 'high_60', 'resistance', 'support', 'atr', 'adx']
        required_dynamic_columns = ['rsi_14', 'macd_histogram', 'bb_upper', 'bb_lower', 'volume_20ma']
        
        # 정적 데이터 컬럼 검증
        if not static_df.empty:
            missing_static = [col for col in required_static_columns if col not in static_df.columns]
            if missing_static:
                validation_result['errors'].append(f"정적 데이터 필수 컬럼 누락: {missing_static}")
                validation_result['is_valid'] = False
            
            # 컬럼별 완성도 확인
            static_completeness = {}
            for col in required_static_columns:
                if col in static_df.columns:
                    non_null_ratio = static_df[col].notna().sum() / len(static_df)
                    static_completeness[col] = non_null_ratio
                    if non_null_ratio < 0.8:  # 80% 미만 완성도
                        validation_result['warnings'].append(f"정적 컬럼 '{col}' 완성도 낮음: {non_null_ratio:.1%}")
            
            validation_result['column_completeness']['static'] = static_completeness
        
        # 동적 데이터 컬럼 검증
        if not dynamic_df.empty:
            missing_dynamic = [col for col in required_dynamic_columns if col not in dynamic_df.columns]
            if missing_dynamic:
                validation_result['warnings'].append(f"동적 데이터 컬럼 누락: {missing_dynamic}")
            
            # 컬럼별 완성도 확인
            dynamic_completeness = {}
            for col in required_dynamic_columns:
                if col in dynamic_df.columns:
                    non_null_ratio = dynamic_df[col].notna().sum() / len(dynamic_df)
                    dynamic_completeness[col] = non_null_ratio
                    if non_null_ratio < 0.5:  # 50% 미만 완성도 (동적 데이터는 더 관대)
                        validation_result['warnings'].append(f"동적 컬럼 '{col}' 완성도 낮음: {non_null_ratio:.1%}")
            
            validation_result['column_completeness']['dynamic'] = dynamic_completeness
        
        # 4. 전체 검증 상태 결정
        if validation_result['errors']:
            validation_result['is_valid'] = False
        
        logger.info(f"📋 데이터 일관성 검증 완료: {'✅ 통과' if validation_result['is_valid'] else '❌ 실패'}")
        if validation_result['warnings']:
            logger.warning(f"⚠️ 검증 경고 {len(validation_result['warnings'])}개: {validation_result['warnings'][:2]}")
        if validation_result['errors']:
            logger.error(f"❌ 검증 오류 {len(validation_result['errors'])}개: {validation_result['errors']}")
            
    except Exception as e:
        validation_result['is_valid'] = False
        validation_result['errors'].append(f"검증 중 오류 발생: {str(e)}")
        logger.error(f"❌ 데이터 검증 중 오류: {e}")
    
    return validation_result


def calculate_adaptive_weights(has_dynamic_data: bool, config: dict = None) -> dict:
    """
    동적 데이터 유무에 따라 적응적 가중치를 계산합니다.
    
    Args:
        has_dynamic_data: 동적 데이터 존재 여부
        config: 설정 값 (가중치 설정 포함)
        
    Returns:
        dict: 정적/동적 지표 가중치
    """
    if config is None:
        config = {}
    
    if has_dynamic_data:
        # 동적 데이터가 있을 때의 가중치 (설정값 또는 기본값)
        static_weight = config.get('static_weight', 0.6)
        dynamic_weight = config.get('dynamic_weight', 0.4) 
        
        # 가중치 정규화 (합계가 1이 되도록)
        total_weight = static_weight + dynamic_weight
        if total_weight > 0:
            static_weight /= total_weight
            dynamic_weight /= total_weight
        
        return {
            "static": static_weight,
            "dynamic": dynamic_weight,
            "mode": "hybrid"
        }
    else:
        # 동적 데이터가 없을 때는 정적 데이터만 사용
        return {
            "static": 1.0,
            "dynamic": 0.0,
            "mode": "static_only"
        }


def calculate_hybrid_score(row, weights: dict, config: dict) -> tuple:
    """
    정적+동적 지표를 가중치에 따라 조합하여 하이브리드 점수를 계산합니다.
    
    Args:
        row: 종목 데이터 행
        weights: 적응적 가중치
        config: 필터링 설정
        
    Returns:
        tuple: (총점, 상세정보)
    """
    static_score = 0
    dynamic_score = 0
    score_details = []
    
    # === 정적 지표 점수 계산 ===
    if weights["static"] > 0:
        # 1. 가격 > MA200 (필수 조건이므로 높은 가중치)
        try:
            price = row.get('price', 0)
            ma_200 = row.get('ma_200', 0) or row.get('ma200_slope', 0)
            if price > 0 and ma_200 > 0 and price > ma_200:
                static_score += 2
                score_details.append("MA200상향")
        except:
            pass
        
        # 2. 가격 > 60일 고점
        try:
            price = row.get('price', 0)
            high_60 = row.get('high_60', 0)
            if price > 0 and high_60 > 0 and price > high_60:
                static_score += 2
                score_details.append("고점돌파")
        except:
            pass
        
        # 3. 저항선 근접도
        try:
            price = row.get('price', 0)
            resistance = row.get('resistance', 0)
            if price > 0 and resistance > 0:
                proximity = price / resistance
                if proximity >= 0.95:
                    static_score += 1
                    score_details.append(f"저항근접:{proximity:.3f}")
        except:
            pass
    
    # === 동적 지표 점수 계산 ===
    if weights["dynamic"] > 0:
        # 1. RSI 범위 (30-70)
        try:
            rsi = row.get('rsi_14')
            if rsi and config.get('rsi_min', 30) <= rsi <= config.get('rsi_max', 70):
                dynamic_score += 1
                score_details.append(f"RSI:{rsi:.1f}")
        except:
            pass
        
        # 2. 볼린저 밴드 상단 근접
        try:
            current_close = row.get('current_close') or row.get('price', 0)
            bb_upper = row.get('bb_upper')
            if bb_upper and current_close:
                proximity = current_close / bb_upper
                if proximity >= config.get('bb_proximity_ratio', 0.95):
                    dynamic_score += 1
                    score_details.append(f"BB상단:{proximity:.3f}")
        except:
            pass
        
        # 3. MACD 히스토그램 양수
        try:
            macd_histogram = row.get('macd_histogram')
            if macd_histogram and macd_histogram >= config.get('macd_histogram_min', 0):
                dynamic_score += 1
                score_details.append(f"MACD:{macd_histogram:.4f}")
        except:
            pass
    
    # === 가중치 적용 점수 계산 ===
    weighted_score = (static_score * weights["static"]) + (dynamic_score * weights["dynamic"])
    
    return weighted_score, score_details


def filter_comprehensive_candidates(combined_df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    정적+동적 지표를 조합한 하이브리드 필터링으로 돌파 매매 후보를 선별합니다.
    
    데이터 소스:
    - static_indicators: 정적 지표 (resistance, support, atr, adx, price, high_60, ma200_slope 등)
    - ohlcv: 동적 지표 (rsi_14, macd_histogram, bb_upper, bb_lower, volume_20ma 등)
    
    Args:
        combined_df (pd.DataFrame): 정적+동적 지표가 조합된 데이터프레임
        config (dict): 필터링 설정 값
        
    Returns:
        pd.DataFrame: 하이브리드 필터링을 통과한 종목 데이터
    """
    try:
        if combined_df.empty:
            logger.warning("⚠️ 조합된 데이터가 비어있습니다.")
            return pd.DataFrame()
            
        if config is None:
            config = load_filter_config()
            
        # 기본 설정값
        default_config = {
            "require_price_above_ma200": True,
            "require_price_above_high60": True,
            "check_rsi_range": True,
            "check_bollinger_breakout": True, 
            "check_macd_signal_cross": True,
            "rsi_min": 30,
            "rsi_max": 70,
            "bb_proximity_ratio": 0.95,
            "macd_histogram_min": 0,
            "static_weight": 0.6,
            "dynamic_weight": 0.4,
            "min_optional_conditions_passed": 2,
            "max_filtered_tickers": 20,
            "enable_debug_logging": False
        }
        
        # 설정값 병합
        for key, default_value in default_config.items():
            if key not in config:
                config[key] = default_value
                
        debug_enabled = config.get("enable_debug_logging", False)
        blacklist = load_blacklist()
        filtered_data = []
        
        logger.info(f"🔍 하이브리드 필터링 시작: 총 {len(combined_df)} 종목 검사")
        
        for ticker in combined_df.index:
            try:
                # 블랙리스트 체크
                if ticker in blacklist:
                    if debug_enabled:
                        logger.debug(f"⛔️ {ticker} 블랙리스트 제외")
                    continue
                    
                row = combined_df.loc[ticker]
                
                # === 1. 동적 데이터 유무 확인 ===
                dynamic_columns = ['rsi_14', 'macd_histogram', 'bb_upper', 'bb_lower', 'volume_20ma']
                has_dynamic_data = any(pd.notnull(row.get(col)) for col in dynamic_columns)
                
                # === 2. 적응적 가중치 계산 ===
                weights = calculate_adaptive_weights(has_dynamic_data, config)
                
                # === 3. 필수 조건 검사 (정적 지표 기반) ===
                # 현재가 > 200일 이평선
                price_above_ma200_check = True
                if config.get("require_price_above_ma200", True):
                    try:
                        price = row.get('price', 0)
                        ma_200 = row.get('ma_200', 0) or row.get('ma200_slope', 0)
                        price_above_ma200_check = price > ma_200 if ma_200 > 0 else False
                    except:
                        price_above_ma200_check = False
                
                # 현재가 > 60일 고점
                price_above_high60_check = True
                if config.get("require_price_above_high60", True):
                    try:
                        price = row.get('price', 0)
                        high_60 = row.get('high_60', 0)
                        price_above_high60_check = price > high_60 if high_60 > 0 else False
                    except:
                        price_above_high60_check = False
                
                # 필수 조건 통과 검사
                if not (price_above_ma200_check and price_above_high60_check):
                    if debug_enabled:
                        logger.debug(f"❌ {ticker} 필수 조건 미통과 (MA200: {price_above_ma200_check}, High60: {price_above_high60_check})")
                    continue
                
                # === 4. 하이브리드 점수 계산 ===
                hybrid_score, score_details = calculate_hybrid_score(row, weights, config)
                
                # === 5. 최소 점수 기준 확인 ===
                # 동적 데이터가 있는 경우 더 높은 기준 적용
                min_score = config.get("min_hybrid_score", 2.0 if has_dynamic_data else 1.5)
                
                if hybrid_score < min_score:
                    if debug_enabled:
                        logger.debug(f"❌ {ticker} 점수 미달 ({hybrid_score:.2f}/{min_score}, 모드: {weights['mode']})")
                    continue
                
                # 통과한 종목 데이터 수집
                result_row = row.copy()
                result_row['hybrid_score'] = hybrid_score
                result_row['score_details'] = ', '.join(score_details)
                result_row['data_source'] = 'hybrid'
                result_row['weight_mode'] = weights['mode']
                result_row['static_weight'] = weights['static']
                result_row['dynamic_weight'] = weights['dynamic']
                result_row['has_dynamic_data'] = has_dynamic_data
                
                filtered_data.append(result_row)
                
                if debug_enabled:
                    logger.debug(f"✅ {ticker} 통과 (점수: {hybrid_score:.2f}, 모드: {weights['mode']}, 조건: {score_details})")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} 하이브리드 필터링 중 오류: {e}")
                continue
        
        # 결과 DataFrame 생성
        if not filtered_data:
            logger.warning("⚠️ 하이브리드 필터링 조건을 만족하는 종목이 없습니다.")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(filtered_data)
        
        # 하이브리드 점수 순으로 정렬
        result_df.sort_values('hybrid_score', ascending=False, inplace=True)
        
        # 최대 종목 수 제한
        max_tickers = config.get("max_filtered_tickers", 20)
        if len(result_df) > max_tickers:
            result_df = result_df.head(max_tickers)
        
        # 결과 통계 생성
        hybrid_count = (result_df['weight_mode'] == 'hybrid').sum()
        static_only_count = (result_df['weight_mode'] == 'static_only').sum()
        
        logger.info(f"✅ 하이브리드 필터링 완료: {len(result_df)}개 종목 선별")
        logger.info(f"📊 선별된 종목: {', '.join(result_df.index.tolist())}")
        logger.info(f"🎯 필터링 모드: 하이브리드 {hybrid_count}개, 정적전용 {static_only_count}개")
        
        if debug_enabled and not result_df.empty:
            logger.debug(f"🏆 상위 3개 종목 점수:")
            for i, (ticker, row) in enumerate(result_df.head(3).iterrows()):
                logger.debug(f"  {i+1}. {ticker}: {row['hybrid_score']:.2f} ({row['weight_mode']}) - {row['score_details']}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"❌ 하이브리드 필터링 중 오류 발생: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return pd.DataFrame()

def generate_filter_report(filter_report: dict, config: dict):
    """
    필터링 결과 리포트를 생성하고 출력합니다.
    
    Args:
        filter_report (dict): 종목별 필터링 결과
        config (dict): 필터링 설정
    """
    try:
        # 통계 계산
        total_tickers = len(filter_report)
        passed_tickers = sum(1 for data in filter_report.values() if data['passed'])
        failed_tickers = total_tickers - passed_tickers
        
        # 조건별 통과율 계산
        condition_stats = {
            "price_above_ma200": {"passed": 0, "total": 0},
            "price_above_high60": {"passed": 0, "total": 0},
            "macd_positive": {"passed": 0, "total": 0},
            "adx_strength": {"passed": 0, "total": 0},
            "golden_cross": {"passed": 0, "total": 0},
            "volume_surge": {"passed": 0, "total": 0},
            "supertrend_bullish": {"passed": 0, "total": 0}
        }
        
        for ticker_data in filter_report.values():
            for condition in condition_stats.keys():
                if ticker_data[condition] is not None:
                    condition_stats[condition]["total"] += 1
                    if ticker_data[condition]:
                        condition_stats[condition]["passed"] += 1
        
        # 리포트 출력
        logger.info("=" * 60)
        logger.info("📊 필터링 결과 리포트")
        logger.info("=" * 60)
        logger.info(f"📈 전체 검사 종목: {total_tickers}개")
        logger.info(f"✅ 필터 통과: {passed_tickers}개 ({passed_tickers/total_tickers*100:.1f}%)")
        logger.info(f"❌ 필터 실패: {failed_tickers}개 ({failed_tickers/total_tickers*100:.1f}%)")
        logger.info("-" * 60)
        
        # 조건별 통과율 출력
        logger.info("📋 조건별 통과율:")
        for condition, stats in condition_stats.items():
            if stats["total"] > 0:
                pass_rate = stats["passed"] / stats["total"] * 100
                condition_name = {
                    "price_above_ma200": "현재가 > MA200",
                    "price_above_high60": "현재가 > 60일고점",
                    "macd_positive": "MACD 양전환",
                    "adx_strength": "ADX 강세",
                    "golden_cross": "골든크로스",
                    "volume_surge": "거래량 증가",
                    "supertrend_bullish": "Supertrend 상승"
                }.get(condition, condition)
                logger.info(f"  • {condition_name}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%)")
        
        logger.info("-" * 60)
        
        # 통과 종목 상세 정보
        if passed_tickers > 0:
            logger.info("✨ 필터 통과 종목 상세:")
            for ticker, data in filter_report.items():
                if data['passed']:
                    conditions_met = []
                    if data.get('price_above_ma200'): conditions_met.append("MA200↑")
                    if data.get('price_above_high60'): conditions_met.append("60일고점↑")
                    if data.get('macd_positive'): conditions_met.append("MACD+")
                    if data.get('adx_strength'): conditions_met.append("ADX강세")
                    if data.get('golden_cross'): conditions_met.append("골든크로스")
                    if data.get('volume_surge'): conditions_met.append("거래량↑")
                    if data.get('supertrend_bullish'): conditions_met.append("Supertrend↑")
                    
                    optional_score = data.get('optional_score', 0)
                    min_required = data.get('min_optional_required', 0)
                    logger.info(f"  • {ticker}: 보조조건 {optional_score}/{min_required} [{', '.join(conditions_met)}]")
        
        logger.info("=" * 60)
        
        # DataFrame으로도 리포트 생성 (선택적)
        if config.get("enable_debug_logging", False):
            report_df = pd.DataFrame.from_dict(filter_report, orient='index')
            logger.debug("📊 상세 필터링 리포트 DataFrame:")
            logger.debug(f"\n{report_df.to_string()}")
            
    except Exception as e:
        logger.error(f"❌ 필터링 리포트 생성 중 오류 발생: {e}")

# UNUSED: 오래된 OHLCV 데이터 삭제 함수 - 현재 파이프라인에서 사용되지 않음
# def clean_old_data(days=400):
#     conn = psycopg2.connect(
#         host=os.getenv("PG_HOST"),
#         port=os.getenv("PG_PORT"),
#         dbname=os.getenv("PG_DATABASE"),
#         user=os.getenv("PG_USER"),
#         password=os.getenv("PG_PASSWORD")
#     )
#     cursor = conn.cursor()
# 
#     cursor.execute("""
#         DELETE FROM ohlcv
#         WHERE date < (
#             SELECT MAX(date) FROM ohlcv t2 WHERE t2.ticker = ohlcv.ticker
#         ) - INTERVAL %s
#     """, (f'{days} days',))
# 
#     conn.commit()
#     conn.close()

def fetch_market_data_4h():
    """
    4시간봉 시장 데이터를 가져옵니다.
    Returns:
        pd.DataFrame: 4시간봉 시장 데이터
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        
        # 4시간봉 시장 데이터 조회
        df = pd.read_sql_query("SELECT * FROM market_data_4h", conn)
        
        if df is not None and not df.empty:
            df.set_index('ticker', inplace=True)
            logger.info(f"✅ 4시간봉 시장 데이터 {len(df)}개 조회 완료")
        else:
            pass
            
        return df
        
    except Exception as e:
        logger.error(f"❌ 4시간봉 시장 데이터 조회 중 오류 발생: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_static_indicators_data():
    """
    static_indicators 테이블에서 일봉 지표 데이터를 가져오는 함수
    
    Returns:
        pd.DataFrame: 일봉 지표 데이터
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        
        # static_indicators 테이블에서 데이터 조회
        static_data = pd.read_sql_query(
            "SELECT * FROM static_indicators",
            conn
        )
        
        if not static_data.empty:
            static_data.set_index('ticker', inplace=True)
            logger.info(f"✅ 일봉 지표 데이터 조회 완료: {len(static_data)} 개의 티커")
        else:
            logger.warning("⚠️ 조회된 일봉 지표 데이터가 없습니다.")
        
        return static_data
        
    except Exception as e:
        logger.error(f"❌ 일봉 지표 데이터 조회 중 오류 발생: {e}")
        return pd.DataFrame()
        
    finally:
        if 'conn' in locals():
            conn.close()

def process_single_ticker(ticker):
    """
    단일 티커의 데이터를 처리하는 함수
    
    Args:
        ticker (str): 처리할 티커
        
    Returns:
        str or None: 처리된 티커 또는 None (실패 시)
    """
    try:
        logger.info(f"🔄 {ticker} 처리 시작")
        
        # 1. OHLCV 데이터 가져오기
        ohlcv_data = get_ohlcv_d(ticker)
        if ohlcv_data is None or ohlcv_data.empty:
            logger.warning(f"⚠️ {ticker} OHLCV 데이터 없음")
            return None
            
        # 2. 기술적 지표 계산
        try:
            df_with_indicators = calculate_technical_indicators(ohlcv_data)
            if df_with_indicators is None:
                logger.warning(f"⚠️ {ticker} 기술적 지표 계산 실패")
                return None
        except Exception as e:
            logger.error(f"❌ {ticker} 기술적 지표 계산 중 오류 발생: {e}")
            return None
            
        # 3. DB에 저장 (market_data 테이블이 제거됨)
        # save_market_data_to_db 함수는 더 이상 사용하지 않음
        logger.debug(f"ℹ️ {ticker} DB 저장 단계 건너뜀 (market_data 테이블 제거됨)")
            
        # 4. 차트 이미지 생성
        try:
            save_chart_image(ticker, df_with_indicators)
        except Exception as e:
            logger.error(f"❌ {ticker} 차트 이미지 생성 중 오류 발생: {e}")
            # 차트 생성 실패는 치명적이지 않으므로 계속 진행
            
        logger.info(f"✅ {ticker} 처리 완료")
        return ticker
        
    except Exception as e:
        logger.error(f"❌ {ticker} 처리 중 예상치 못한 오류 발생: {e}")
        return None

def filter_by_volume(tickers: list = None, min_trade_price_krw: int = ONE_HMIL_KRW * 3) -> list:
    """최신 24시간 거래대금이 min_trade_price_krw(기본 3억원) 이상인 티커만 반환합니다."""
    try:
        logger.info(f"🔍 24시간 실시간 거래대금 필터링 시작 (기준: {min_trade_price_krw} KRW)")
        import pyupbit
        import requests
        import time

        krw_tickers = tickers if tickers is not None else pyupbit.get_tickers(fiat="KRW")
        logger.info(f"📊 KRW 마켓 티커 {len(krw_tickers)}개 조회 완료")
        filtered_tickers = []

        # 개별 티커별로 요청 (per-ticker)
        for ticker in krw_tickers:
            url = f"https://api.upbit.com/v1/ticker?markets={ticker}"
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        item = data[0]
                        trade_price_24h = item.get('acc_trade_price_24h', 0)
                        if isinstance(trade_price_24h, list):
                            trade_price_24h = trade_price_24h[0] if trade_price_24h else 0
                        if trade_price_24h >= min_trade_price_krw:
                            filtered_tickers.append(ticker)
                            logger.info(f"✅ {ticker}: 24시간 거래대금 {trade_price_24h:.0f} KRW (≥ {min_trade_price_krw} KRW)")
                        else:
                            logger.debug(f"❌ {ticker}: 24시간 거래대금 {trade_price_24h:.0f} KRW (< {min_trade_price_krw} KRW)")
                    else:
                        logger.warning(f"⚠️ {ticker} 응답 데이터 비정상: {data}")
                else:
                    logger.warning(f"⚠️ API 요청 실패: status_code={response.status_code}")
            except Exception as e:
                logger.error(f"❌ {ticker} 거래대금 조회 중 오류: {str(e)}")
            time.sleep(0.1)  # API rate limit 보호

        logger.info(f"📊 24시간 거래대금 필터링 결과: {len(filtered_tickers)}/{len(krw_tickers)} 종목 통과")
        return filtered_tickers
    except Exception as e:
        logger.error(f"❌ 24시간 거래대금 필터링 중 오류 발생: {str(e)}")
        return []

def filter_by_monthly_data_length(tickers: list, min_months: int = 14) -> list:
    """
    pyupbit API를 사용하여 월봉 데이터가 최소 min_months개 존재하는 티커만 필터링
    """
    import pyupbit
    import time

    logger.info(f"🔍 [pyupbit] 월봉 데이터 필터링 시작 (최소 {min_months}개월 기준)")
    passed = []

    for ticker in tickers:
        try:
            df = pyupbit.get_ohlcv(ticker, interval="month", count=min_months)
            if df is not None and len(df) >= min_months:
                passed.append(ticker)
                logger.debug(f"✅ {ticker}: {len(df)}개월치 월봉 보유")
            else:
                logger.debug(f"❌ {ticker}: 월봉 부족 or 없음")
        except Exception as e:
            logger.warning(f"⚠️ {ticker} 월봉 조회 실패: {e}")
        time.sleep(0.1)  # rate limit 보호

    logger.info(f"📊 월봉 데이터 필터링 결과: {len(passed)}/{len(tickers)}개 종목 통과")
    logger.info("✅ 통과된 종목 리스트:\n" + ", ".join(passed))
    return passed

def safe_len(value):
    """
    안전하게 len() 함수를 호출하는 헬퍼 함수
    
    Args:
        value: 길이를 확인할 객체
        
    Returns:
        int: 객체의 길이 또는 0 (길이를 확인할 수 없는 경우)
    """
    if hasattr(value, '__len__'):
        return len(value)
    return 0

def check_sell_conditions(ticker, market_data, config):
    """
    매도 조건을 점검합니다.
    
    Args:
        ticker (str): 티커 심볼
        market_data (pd.DataFrame): 시장 데이터
        config (dict): 설정
        
    Returns:
        bool: 매도 조건 충족 여부
    """
    try:
        if market_data is None or market_data.empty:
            logger.warning(f"⚠️ {ticker} 매도 조건 점검 실패: 시장 데이터 없음")
            return False
            
        # 시장 데이터 길이 확인
        if safe_len(market_data) == 0:
            logger.warning(f"⚠️ {ticker} 매도 조건 점검 실패: 시장 데이터 길이 0")
            return False
            
        # 현재가 확인
        current_price = market_data.loc[ticker, 'price'] if ticker in market_data.index else None
        if current_price is None:
            logger.warning(f"⚠️ {ticker} 매도 조건 점검 실패: 현재가 없음")
            return False
            
        # 이동평균선 확인
        ma_50 = market_data.loc[ticker, 'ma_50'] if 'ma_50' in market_data.columns else None
        ma_200 = market_data.loc[ticker, 'ma_200'] if 'ma_200' in market_data.columns else None
        
        if ma_50 is None or ma_200 is None:
            logger.warning(f"⚠️ {ticker} 매도 조건 점검 실패: 이동평균선 데이터 없음")
            return False
            
        # 매도 조건 점검
        if current_price < ma_50:  # 50일 이동평균선 아래로 하락
            logger.info(f"🔴 {ticker} 매도 신호: 현재가가 50일 이동평균선 아래로 하락")
            return True
            
        if current_price < ma_200:  # 200일 이동평균선 아래로 하락
            logger.info(f"🔴 {ticker} 매도 신호: 현재가가 200일 이동평균선 아래로 하락")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"❌ {ticker} 매도 조건 점검 중 오류 발생: {e}")
        return False


def apply_timing_filter_4h(market_df_4h, config=None):
    """
    🎯 Makenaide 4시간봉 마켓타이밍 필터 (추세 돌파 전략)
    
    개별 종목의 단기 흐름에서 상승 전환이 임박했거나 강한 추세 진입 구간을 탐지하여 
    최적의 매수 시점을 포착합니다.
    
    📊 [핵심 전환점 탐지 지표] - 7개 지표 점수제 (score >= min_score)
    1. 📈 MACD Signal 상향 돌파: macd > macds AND macdh > 0
    2. 🔄 Stochastic 상승: stochastic_k > stochastic_d AND stochastic_k > 20
    3. 💫 CCI 돌파: cci > 100 (중립에서 상승세 진입)
    4. 📊 ADX 추세 강도: adx > 25 AND plus_di > minus_di
    5. 🚀 MA200 돌파: price > ma_200 (장기 상승추세 유지)
    6. ⚡ Supertrend 상승: supertrend_signal == 'up'
    7. 🎈 Bollinger Band 상단 돌파: price > bb_upper (변동성 확산)
    
    ⚠️ [추가 안전장치]
    - RSI 과열 방지: rsi_14 < 80 (과매수 구간 제외)
    - 볼린저밴드 압축 후 확산: bb_upper - bb_lower > 임계값
    
    Args:
        market_df_4h (pd.DataFrame): 4시간봉 시장 데이터
        config (dict): 필터링 설정
            - min_score (int): 핵심지표 최소 통과 점수 (기본값: 5/7)
            - rsi_max (int): RSI 과열 임계값 (기본값: 80)
            
    Returns:
        list: 마켓타이밍 필터를 통과한 최종 후보 티커 목록
    """
    try:
        if market_df_4h.empty:
            logger.warning("⚠️ 4시간봉 시장 데이터가 비어있습니다.")
            return []
            
        # 기본 설정값
        if config is None:
            config = {
                "min_score": 5,     # 7개 지표 중 5개 이상 통과
                "rsi_max": 80       # RSI 과열 임계값
            }
        final_candidates = []
        
        for ticker in market_df_4h.index:
            try:
                row = market_df_4h.loc[ticker]
                
                # === [0] 안전장치 사전 체크 ===
                safety_checks = []
                rsi_max = config.get("rsi_max", 80)
                
                # RSI 과열 방지
                if 'rsi_14' in row and pd.notna(row['rsi_14']):
                    if row['rsi_14'] < rsi_max:
                        safety_checks.append("RSI안전")
                    else:
                        logger.debug(f"⏭️ {ticker} RSI 과열 ({row['rsi_14']:.1f} >= {rsi_max})")
                        continue
                
                # === [1] 핵심 전환점 탐지 지표 (7개 점수제) ===
                score = 0
                passed_indicators = []
                
                # 1. 📈 MACD Signal 상향 돌파
                try:
                    if (pd.notna(row['macd']) and pd.notna(row['macds']) and pd.notna(row['macdh'])):
                        if row['macd'] > row['macds'] and row['macdh'] > 0:
                            score += 1
                            passed_indicators.append("MACD돌파")
                except:
                    pass
                
                # 2. 🔄 Stochastic 상승
                try:
                    if (pd.notna(row['stochastic_k']) and pd.notna(row['stochastic_d'])):
                        if row['stochastic_k'] > row['stochastic_d'] and row['stochastic_k'] > 20:
                            score += 1
                            passed_indicators.append("Stoch상승")
                except:
                    pass
                
                # 3. 💫 CCI 돌파
                try:
                    if pd.notna(row['cci']) and row['cci'] > 100:
                        score += 1
                        passed_indicators.append("CCI돌파")
                except:
                    pass
                
                # 4. 📊 ADX 추세 강도
                try:
                    if (pd.notna(row['adx']) and pd.notna(row['plus_di']) and pd.notna(row['minus_di'])):
                        if row['adx'] > 25 and row['plus_di'] > row['minus_di']:
                            score += 1
                            passed_indicators.append("ADX강세")
                except:
                    pass
                
                # 5. 🚀 MA200 돌파
                try:
                    if pd.notna(row['price']) and pd.notna(row['ma_200']):
                        if row['price'] > row['ma_200']:
                            score += 1
                            passed_indicators.append("MA200돌파")
                except:
                    pass
                
                # 6. ⚡ Supertrend 상승
                try:
                    if pd.notna(row['supertrend_signal']) and row['supertrend_signal'] == 'up':
                        score += 1
                        passed_indicators.append("Supertrend상승")
                except:
                    pass
                
                # 7. 🎈 Bollinger Band 상단 돌파
                try:
                    if pd.notna(row['price']) and pd.notna(row['bb_upper']):
                        if row['price'] > row['bb_upper']:
                            score += 1
                            passed_indicators.append("BB상단돌파")
                except:
                    pass
                
                # === [2] 최소 점수 조건 확인 ===
                min_score = config.get("min_score", 5)
                if score >= min_score:
                    final_candidates.append(ticker)
                    logger.info(f"✨ {ticker} 마켓타이밍 필터 통과 (점수: {score}/{min_score}) - 통과지표: {', '.join(passed_indicators)}")
                else:
                    logger.debug(f"⏭️ {ticker} 점수 부족 ({score}/{min_score}) - 통과지표: {', '.join(passed_indicators)}")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} 마켓타이밍 필터링 중 오류 발생: {e}")
                continue
                
        # === [3] 결과 요약 ===
        logger.info(f"🎯 Makenaide 마켓타이밍 필터링 결과: {len(final_candidates)}개 종목 통과")
        if final_candidates:
            logger.info(f"   ✅ 통과 종목: {', '.join(final_candidates)}")
            logger.info(f"   📊 필터 설정: 최소점수 {config.get('min_score', 5)}/7, RSI최대 {config.get('rsi_max', 80)}")
        else:
            logger.info("   ❌ 마켓타이밍 조건을 만족하는 종목이 없습니다.")
            
        return final_candidates
        
    except Exception as e:
        logger.error(f"❌ 마켓타이밍 필터 적용 중 오류 발생: {e}")
        return []
