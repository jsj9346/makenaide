"""
거래량 관련 필터링 조건들 (하이브리드 데이터 지원)
"""
import pandas as pd


def volume_surge(row, surge_ratio=1.0):
    """
    거래량이 20일 평균 거래량보다 많은지 확인
    하이브리드 데이터에서는 ohlcv 테이블의 volume_20ma를 사용
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        surge_ratio: 거래량 증가 비율 (기본값: 1.0 = 100%)
        
    Returns:
        bool: 조건 만족 여부
    """
    current_volume = row.get('volume', 0)
    volume_20ma = row.get('volume_20ma', 0)
    
    if pd.isnull(current_volume) or pd.isnull(volume_20ma) or volume_20ma <= 0:
        return False
        
    volume_ratio = current_volume / volume_20ma
    return volume_ratio > surge_ratio


def volume_breakout_strength(row, min_ratio=1.5):
    """
    거래량 돌파 강도를 확인 (평균 거래량의 1.5배 이상)
    
    Args:
        row: 종목 데이터 행
        min_ratio: 최소 거래량 비율 (기본값: 1.5배)
        
    Returns:
        bool: 조건 만족 여부
    """
    current_volume = row.get('volume', 0)
    volume_20ma = row.get('volume_20ma', 0)
    
    if pd.isnull(current_volume) or pd.isnull(volume_20ma) or volume_20ma <= 0:
        return False
        
    return (current_volume / volume_20ma) >= min_ratio


def has_valid_volume_data(row):
    """
    하이브리드 거래량 데이터의 유효성을 확인
    
    Args:
        row: 종목 데이터 행 (정적+동적 지표 포함)
        
    Returns:
        dict: 각 데이터의 유효성 정보
    """
    return {
        'has_volume_data': pd.notnull(row.get('volume')),
        'has_volume_20ma_data': pd.notnull(row.get('volume_20ma')),
        'data_source': row.get('data_source', 'unknown'),
        'volume_availability': 'ohlcv' if pd.notnull(row.get('volume_20ma')) else 'none'
    } 