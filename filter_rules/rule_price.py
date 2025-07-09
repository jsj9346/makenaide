"""
가격 관련 필터링 조건들 (하이브리드 데이터 지원) 
"""
import pandas as pd


def price_above_ma200(row, ratio=1.0):
    """
    현재가가 200일 이동평균선(ratio × ma_200) 이상인지 확인합니다.
    하이브리드 데이터에서는 static_indicators의 price와 ohlcv의 ma_200을 사용
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        ratio (float): 이동평균선에 적용할 비율 (기본값: 1.0)
        
    Returns:
        bool: 조건 만족 여부
    """
    # 가격: static_indicators 테이블의 price 우선 사용
    price = row.get("price", 0)
    
    # 200일 이평선: ohlcv 테이블의 ma_200 우선, fallback으로 ma200_slope 활용
    ma_200 = row.get("ma_200", 0)
    if ma_200 <= 0:
        # ma200_slope가 양수면 상승추세로 간주하여 완화된 조건 적용
        ma200_slope = row.get("ma200_slope", -999)
        if ma200_slope > 0:
            ma_200 = price * 0.95  # 현재가의 95% 수준으로 설정
    
    return (
        pd.notnull(price) and price > 0 and
        pd.notnull(ma_200) and ma_200 > 0 and
        price >= ma_200 * ratio
    )


def price_above_high60(row, ratio=1.0):
    """
    현재가가 60일 고점(ratio × high_60) 이상인지 확인합니다.
    하이브리드 데이터에서는 static_indicators의 price와 high_60을 사용
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        ratio (float): 60일 고점에 적용할 비율 (기본값: 1.0)
        
    Returns:
        bool: 조건 만족 여부
    """
    # 가격: static_indicators 테이블의 price 사용
    price = row.get("price", 0)
    
    # 60일 고점: static_indicators 테이블의 high_60 사용
    high_60 = row.get("high_60", 0)
    
    return (
        pd.notnull(price) and price > 0 and
        pd.notnull(high_60) and high_60 > 0 and
        price >= high_60 * ratio
    )


def price_near_resistance(row, proximity_ratio=0.95):
    """
    현재가가 저항선에 근접했는지 확인 (돌파 직전 상황)
    
    Args:
        row: 종목 데이터 행
        proximity_ratio: 저항선 근접 비율 (기본값: 0.95 = 95%)
        
    Returns:
        bool: 조건 만족 여부
    """
    price = row.get("price", 0)
    resistance = row.get("resistance", 0)
    
    if pd.isnull(price) or pd.isnull(resistance) or resistance <= 0:
        return False
        
    proximity = price / resistance
    return proximity >= proximity_ratio


def has_valid_price_data(row):
    """
    하이브리드 가격 데이터의 유효성을 확인 (정적+동적 지표)
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        
    Returns:
        dict: 각 데이터의 유효성 정보
    """
    return {
        'has_price_data': pd.notnull(row.get('price')),
        'has_ma200_data': pd.notnull(row.get('ma_200')) or pd.notnull(row.get('ma200_slope')),
        'has_high60_data': pd.notnull(row.get('high_60')),
        'has_resistance_data': pd.notnull(row.get('resistance')),
        'has_support_data': pd.notnull(row.get('support')),
        'data_source': row.get('data_source', 'unknown'),
        'static_indicators_available': all([
            pd.notnull(row.get('price')),
            pd.notnull(row.get('high_60'))
        ])
    } 