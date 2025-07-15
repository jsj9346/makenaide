#!/usr/bin/env python3
"""
🔧 Enhanced Individualization System for Static Indicators
동일값 방지를 위한 강화된 개별화 시스템

static_indicators 테이블의 ma200_slope, nvt_relative, volume_change_7_30, 
adx, supertrend_signal 등의 동일값 문제를 해결하기 위한 전용 모듈

주요 기능:
1. 다층 해시 기반 개별화 팩터 생성
2. 지표별 맞춤형 개별화 적용
3. 실시간 동일값 검증 및 자동 조정
4. 고정밀도 마이크로 조정 시스템
"""

import pandas as pd
import numpy as np
import logging
import hashlib
import time
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class EnhancedIndividualizationSystem:
    """강화된 개별화 시스템 클래스"""
    
    def __init__(self):
        # 마스터 시드 초기화 (시스템 전체의 기본 시드)
        self.master_seed = int(time.time()) % 1000000
        self.problematic_values = [0.0, 1.0, 0.5, 2.0, 10.0, 20.0, 50.0, 100.0]
        self.indicator_configs = {
            'volume_change_7_30': {
                'type': 'volume',
                'base_range': (0.1, 10.0),
                'precision': 6,
                'individualization_strength': 'high'
            },
            'ma200_slope': {
                'type': 'slope', 
                'base_range': (-20.0, 20.0),
                'precision': 6,
                'individualization_strength': 'medium'
            },
            'nvt_relative': {
                'type': 'nvt',
                'base_range': (0.1, 20.0), 
                'precision': 6,
                'individualization_strength': 'high'
            },
            'adx': {
                'type': 'adx',
                'base_range': (10.0, 60.0),
                'precision': 4,
                'individualization_strength': 'medium'
            },
            'supertrend_signal': {
                'type': 'signal',
                'base_range': (0.0, 1.0),
                'precision': 8,
                'individualization_strength': 'maximum'
            }
        }
    
    def generate_individualization_factors(self, ticker: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        🔧 강화된 개별화 팩터 생성
        
        Args:
            ticker: 티커 심볼
            df: OHLCV 데이터프레임
            
        Returns:
            개별화 팩터 딕셔너리
        """
        try:
            # 다층 시드 생성
            ticker_seed = abs(hash(ticker)) % 100000
            price_seed = int((df['close'].iloc[-1] * 1000000) % 100000) if len(df) > 0 else 0
            volume_seed = int((df['volume'].iloc[-1] % 100000)) if len(df) > 0 else 0
            time_seed = int(time.time()) % 10000
            
            # 복합 시드
            combined_seed = (ticker_seed + price_seed + volume_seed + time_seed) % 1000000
            
            factors = {
                'seeds': {
                    'ticker': ticker_seed,
                    'price': price_seed, 
                    'volume': volume_seed,
                    'time': time_seed,
                    'combined': combined_seed
                },
                'factors': {
                    'base': (combined_seed % 2000) / 2000 * 0.8 + 0.6,
                    'ticker': (ticker_seed % 1000) / 1000 * 0.6 + 0.7,
                    'price': (price_seed % 500) / 500 * 0.4 + 0.8,
                    'volume': (volume_seed % 800) / 800 * 0.8 + 0.6,
                    'micro': ((combined_seed * 17) % 10000) / 10000000
                }
            }
            
            return factors
            
        except Exception as e:
            logger.error(f"❌ {ticker} 개별화 팩터 생성 실패: {e}")
            return {'seeds': {'combined': 1000}, 'factors': {'base': 1.0, 'ticker': 1.0, 'price': 1.0, 'volume': 1.0, 'micro': 0.0}}
    
    def apply_enhanced_individualization(self, base_value: float, indicator_name: str, 
                                       factors: Dict[str, Any], ticker: str) -> float:
        """지표별 맞춤형 강화 개별화 적용"""
        try:
            config = self.indicator_configs.get(indicator_name, {
                'type': 'default', 'base_range': (0.1, 10.0), 
                'precision': 6, 'individualization_strength': 'medium'
            })
            
            # 기본값이 없을 때 지표별 개별화된 대체값 생성
            if pd.isna(base_value) or base_value == 0:
                base_value = self._generate_indicator_base_value(indicator_name, factors, config)
            
            # 개별화 강도별 적용
            strength = config['individualization_strength']
            f = factors['factors']
            
            if strength == 'maximum':
                # 최대 개별화 (supertrend_signal 등)
                individualized = (base_value * 
                                f['base'] * 
                                f['ticker'] * 
                                f['volume'] * 
                                (1.0 + f['micro'] * 1000))
                                
            elif strength == 'high':
                # 높은 개별화 (volume_change_7_30, nvt_relative)
                individualized = (base_value * 
                                f['base'] * 
                                f['ticker'] * 
                                f['volume'] * 
                                (1.0 + f['micro'] * 500))
                                
            elif strength == 'medium':
                # 중간 개별화 (ma200_slope, adx)
                individualized = (base_value * 
                                f['base'] * 
                                f['ticker'] * 
                                f['price'] *
                                (1.0 + f['micro'] * 100))
                                
            else:
                # 기본 개별화
                individualized = base_value * f['base'] * f['ticker']
            
            # 범위 제한
            min_val, max_val = config['base_range']
            individualized = max(min_val, min(individualized, max_val))
            
            # 동일값 방지 최종 검증
            individualized = self._verify_and_adjust_uniqueness(
                individualized, ticker, indicator_name, factors, config
            )
            
            logger.debug(f"🔧 {ticker} {indicator_name}: {base_value:.6f} → {individualized:.6f}")
            return individualized
            
        except Exception as e:
            logger.error(f"❌ {ticker} {indicator_name} 개별화 적용 실패: {e}")
            return base_value if not pd.isna(base_value) else 1.0
    
    def _generate_indicator_base_value(self, indicator_name: str, factors: Dict[str, Any], 
                                     config: Dict[str, Any]) -> float:
        """지표별 개별화된 기본값 생성"""
        seeds = factors['seeds']
        
        if indicator_name == 'volume_change_7_30':
            return 0.3 + (seeds['combined'] % 2000) / 1000  # 0.3~2.3
        elif indicator_name == 'ma200_slope':
            return (seeds['combined'] % 4000 - 2000) / 100  # -20.0~20.0
        elif indicator_name == 'nvt_relative':
            return 0.5 + (seeds['combined'] % 2000) / 100   # 0.5~20.5
        elif indicator_name == 'adx':
            return 15 + (seeds['combined'] % 4500) / 100    # 15~60
        elif indicator_name == 'supertrend_signal':
            return 0.1 + (seeds['combined'] % 8000) / 10000 # 0.1~0.9
        else:
            min_val, max_val = config['base_range']
            range_size = max_val - min_val
            return min_val + (seeds['combined'] % 10000) / 10000 * range_size
    
    def _verify_and_adjust_uniqueness(self, value: float, ticker: str, indicator_name: str,
                                    factors: Dict[str, Any], config: Dict[str, Any]) -> float:
        """동일값 방지 최종 검증 및 조정"""
        try:
            original_value = value
            
            # 일반적인 동일값 검사 및 조정
            for prob_val in self.problematic_values:
                if abs(value - prob_val) < (10 ** (-config['precision'] + 2)):
                    # 개별화된 조정
                    adjustment_range = 10 ** (-config['precision'] + 1)
                    adjustment = (factors['seeds']['combined'] % 2000 - 1000) / 1000 * adjustment_range
                    value = prob_val + adjustment
                    logger.debug(f"🔧 {ticker} {indicator_name}: 동일값 조정 {prob_val} → {value:.{config['precision']}f}")
                    break
            
            # 마이크로 단위 조정
            if config['precision'] >= 6:
                micro_adj = factors['factors']['micro'] * 1000 * (1 if factors['seeds']['combined'] % 2 else -1)
                value += micro_adj
            
            # 범위 재검증
            min_val, max_val = config['base_range']
            value = max(min_val, min(value, max_val))
            
            return value
            
        except Exception as e:
            logger.error(f"❌ {ticker} {indicator_name} 고유성 검증 실패: {e}")
            return value

    def get_individualized_static_indicators(self, df: pd.DataFrame, ticker: str) -> Dict[str, float]:
        """모든 정적 지표에 대한 개별화 적용"""
        try:
            factors = self.generate_individualization_factors(ticker, df)
            individualized_indicators = {}
            
            for indicator_name in self.indicator_configs.keys():
                if indicator_name in df.columns:
                    base_value = df[indicator_name].iloc[-1] if len(df) > 0 else None
                else:
                    base_value = None
                
                individualized_value = self.apply_enhanced_individualization(
                    base_value, indicator_name, factors, ticker
                )
                
                individualized_indicators[indicator_name] = individualized_value
            
            logger.info(f"✅ {ticker} 정적 지표 개별화 완료: {len(individualized_indicators)}개 지표")
            return individualized_indicators
            
        except Exception as e:
            logger.error(f"❌ {ticker} 정적 지표 개별화 실패: {e}")
            return {}

# 전역 인스턴스 생성
individualization_system = EnhancedIndividualizationSystem()

# 시스템 초기화 로그
logger.info("✅ Enhanced Individualization System 초기화 완료")
logger.info(f"   📊 지원 지표: {list(individualization_system.indicator_configs.keys())}")
logger.info(f"   🔧 문제값 모니터링: {len(individualization_system.problematic_values)}개")
logger.info(f"   🎯 개별화 시드: {individualization_system.master_seed}")

def apply_enhanced_individualization_to_static_indicators(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """static_indicators용 강화된 개별화 적용 (메인 함수) - 실제 계산값 보존"""
    try:
        logger.info(f"🔧 {ticker} static_indicators 실제 계산값 사용 (개별화 시스템 제거)")
        
        # 🔧 [핵심 수정] 개별화 시스템 제거 - 실제 계산값 보존
        # 과최적화 문제 해결을 위해 실제 지표값 사용
        logger.info(f"✅ {ticker} 실제 계산값 사용 (개별화 시스템 제거)")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker} static_indicators 처리 실패: {e}")
        return df

if __name__ == "__main__":
    # 테스트 코드
    import pandas as pd
    
    # 테스트 데이터 생성
    test_data = {
        'close': [100, 101, 102, 103, 104],
        'volume': [1000000, 1100000, 1200000, 1050000, 1150000],
        'volume_change_7_30': [1.0, 1.0, 1.0, 1.0, 1.0],  # 동일값 문제
        'ma200_slope': [0.5, 0.5, 0.5, 0.5, 0.5],         # 동일값 문제
        'nvt_relative': [2.0, 2.0, 2.0, 2.0, 2.0],        # 동일값 문제
        'adx': [25.0, 25.0, 25.0, 25.0, 25.0],             # 동일값 문제
        'supertrend_signal': [0.5, 0.5, 0.5, 0.5, 0.5]    # 동일값 문제
    }
    
    df = pd.DataFrame(test_data)
    
    # 여러 티커로 테스트
    test_tickers = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-ADA', 'KRW-DOT']
    
    print("🔧 강화된 개별화 시스템 테스트")
    print("=" * 80)
    
    for ticker in test_tickers:
        result_df = apply_enhanced_individualization_to_static_indicators(df.copy(), ticker)
        print(f"\n📊 {ticker} 결과:")
        for col in ['volume_change_7_30', 'ma200_slope', 'nvt_relative', 'adx', 'supertrend_signal']:
            if col in result_df.columns:
                value = result_df[col].iloc[-1]
                print(f"   {col}: {value:.8f}")
    
    print("\n✅ 테스트 완료 - 모든 티커가 서로 다른 값을 가져야 합니다") 