"""
모멘텀 관련 필터링 조건들 (하이브리드 데이터 지원)
"""
import pandas as pd
import logging

# 로거 설정
logger = logging.getLogger(__name__)


def macd_positive(row):
    """
    MACD가 양수인지 확인 (양봉 전환 초기)
    하이브리드 데이터에서는 macd_histogram를 우선적으로 사용
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        
    Returns:
        bool: 조건 만족 여부
    """
    # 하이브리드 데이터: macd_histogram 우선 사용 (ohlcv 테이블)
    macd_value = row.get('macd_histogram') or row.get('macd', 0)
    
    if pd.isnull(macd_value):
        return False
    return macd_value > 0


def rsi_in_range(row, min_rsi=30, max_rsi=70):
    """
    RSI가 지정된 범위 내에 있는지 확인 (과매수/과매도 구간 제외)
    
    Args:
        row: 종목 데이터 행
        min_rsi: RSI 최소값 (기본값: 30)
        max_rsi: RSI 최대값 (기본값: 70)
        
    Returns:
        bool: 조건 만족 여부
    """
    rsi = row.get('rsi_14')
    if pd.isnull(rsi):
        return False
    return min_rsi <= rsi <= max_rsi


def bollinger_breakout(row, proximity_ratio=0.95):
    """
    볼린저 밴드 상단 근접 또는 돌파 확인
    
    Args:
        row: 종목 데이터 행
        proximity_ratio: 상단 근접 비율 (기본값: 0.95 = 95%)
        
    Returns:
        bool: 조건 만족 여부
    """
    current_price = row.get('current_close') or row.get('price', 0)
    bb_upper = row.get('bb_upper')
    
    if pd.isnull(bb_upper) or current_price <= 0:
        return False
        
    proximity = current_price / bb_upper
    return proximity >= proximity_ratio


def adx_strength(row, threshold=20):
    """
    ADX가 임계값 이상인지 확인 (추세 강도)
    
    Args:
        row: 종목 데이터 행
        threshold: ADX 임계값 (기본값: 20)
        
    Returns:
        bool: 조건 만족 여부
    """
    if pd.isnull(row.get('adx')):
        return False
    return row['adx'] >= threshold


def golden_cross(row):
    """
    골든크로스 여부 확인 (MA50 > MA200)
    
    Args:
        row: 종목 데이터 행
        
    Returns:
        bool: 조건 만족 여부
    """
    if pd.isnull(row.get('ma_50')) or pd.isnull(row.get('ma_200')):
        return False
    return row['ma_50'] > row['ma_200']


def supertrend_bullish(row, signal_value='bull'):
    """
    Supertrend 상승 신호 확인
    
    Args:
        row: 종목 데이터 행
        signal_value: 확인할 신호 값 (기본값: 'bull')
        
    Returns:
        bool: 조건 만족 여부
    """
    try:
        supertrend_signal = row.get('supertrend_signal')
        
        if pd.isna(supertrend_signal):
            return False
        
        # 문자열 값 비교 (대소문자 무시)
        if isinstance(supertrend_signal, str):
            return supertrend_signal.lower() == signal_value.lower()
        
        # 숫자 값 처리 (기존 호환성)
        if isinstance(supertrend_signal, (int, float)):
            if signal_value.lower() == 'bull':
                return supertrend_signal > 0.5
            elif signal_value.lower() == 'bear':
                return supertrend_signal < 0.5
            elif signal_value.lower() == 'neutral':
                return supertrend_signal == 0.5
        
        return False
        
    except Exception as e:
        logger.error(f"Supertrend 신호 확인 중 오류: {e}")
        return False


def has_valid_momentum_data(row):
    """
    하이브리드 모멘텀 데이터의 유효성을 확인 (정적+동적 지표)
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        
    Returns:
        dict: 각 데이터의 유효성 정보
    """
    return {
        'has_macd_data': pd.notnull(row.get('macd_histogram')) or pd.notnull(row.get('macd')),
        'has_rsi_data': pd.notnull(row.get('rsi_14')),
        'has_bb_data': pd.notnull(row.get('bb_upper')) and pd.notnull(row.get('bb_lower')),
        'has_adx_data': pd.notnull(row.get('adx')),
        'has_ma50_data': pd.notnull(row.get('ma_50')),
        'has_ma200_data': pd.notnull(row.get('ma_200')),
        'data_source': row.get('data_source', 'unknown')
    } 