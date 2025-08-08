#!/usr/bin/env python3
"""
전략 레지스트리 모듈

백테스트 전략들을 등록, 관리, 실행하는 중앙 집중식 레지스트리입니다.
기존 backtester.py의 StrategyRegistry 클래스를 분리했습니다.

Author: Backtesting Refactoring
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Callable, Optional, Any, Tuple
from datetime import datetime, timedelta
import inspect
from functools import wraps

from .backtest_types import StrategyConfig, BacktestResult

logger = logging.getLogger(__name__)

class StrategyRegistry:
    """전략 등록 및 관리 클래스"""
    
    def __init__(self):
        self._strategies: Dict[str, Dict[str, Any]] = {}
        self._strategy_cache: Dict[str, Any] = {}
        
        # 기본 전략들 등록
        self._register_default_strategies()
    
    def register_strategy(self, name: str, strategy_func: Callable, 
                         description: str = "", category: str = "custom",
                         parameters: Optional[Dict] = None) -> bool:
        """
        전략 등록
        
        Args:
            name: 전략 이름
            strategy_func: 전략 함수 (DataFrame -> str 시그널)
            description: 전략 설명
            category: 전략 카테고리
            parameters: 기본 파라미터
            
        Returns:
            bool: 등록 성공 여부
        """
        try:
            # 함수 유효성 검증
            if not callable(strategy_func):
                logger.error(f"❌ '{name}': 전략 함수가 callable하지 않습니다")
                return False
            
            # 시그니처 검증
            sig = inspect.signature(strategy_func)
            if len(sig.parameters) < 1:
                logger.error(f"❌ '{name}': 전략 함수는 최소 1개 파라미터가 필요합니다")
                return False
            
            strategy_info = {
                'function': strategy_func,
                'description': description,
                'category': category,
                'parameters': parameters or {},
                'registered_at': datetime.now(),
                'usage_count': 0,
                'last_used': None,
                'performance_history': []
            }
            
            self._strategies[name] = strategy_info
            logger.info(f"✅ 전략 '{name}' 등록 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 전략 '{name}' 등록 실패: {e}")
            return False
    
    def get_strategy(self, name: str) -> Optional[Callable]:
        """등록된 전략 함수 반환"""
        if name in self._strategies:
            self._strategies[name]['usage_count'] += 1
            self._strategies[name]['last_used'] = datetime.now()
            return self._strategies[name]['function']
        
        logger.warning(f"⚠️ 전략 '{name}'을 찾을 수 없습니다")
        return None
    
    def list_strategies(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """등록된 전략 목록 조회"""
        strategies = []
        
        for name, info in self._strategies.items():
            if category is None or info['category'] == category:
                strategy_summary = {
                    'name': name,
                    'description': info['description'],
                    'category': info['category'],
                    'parameters': list(info['parameters'].keys()),
                    'usage_count': info['usage_count'],
                    'last_used': info['last_used'],
                    'registered_at': info['registered_at']
                }
                strategies.append(strategy_summary)
        
        # 사용 빈도순 정렬
        strategies.sort(key=lambda x: x['usage_count'], reverse=True)
        return strategies
    
    def get_categories(self) -> List[str]:
        """전략 카테고리 목록 반환"""
        categories = set(info['category'] for info in self._strategies.values())
        return sorted(list(categories))
    
    def remove_strategy(self, name: str) -> bool:
        """전략 제거"""
        if name in self._strategies:
            del self._strategies[name]
            if name in self._strategy_cache:
                del self._strategy_cache[name]
            logger.info(f"🗑️ 전략 '{name}' 제거 완료")
            return True
        
        logger.warning(f"⚠️ 제거할 전략 '{name}'을 찾을 수 없습니다")
        return False
    
    def update_performance_history(self, strategy_name: str, result: BacktestResult):
        """전략 성과 이력 업데이트"""
        if strategy_name in self._strategies:
            performance_entry = {
                'date': datetime.now(),
                'avg_return': result.avg_return,
                'win_rate': result.win_rate,
                'mdd': result.mdd,
                'total_trades': result.total_trades,
                'sharpe_ratio': result.sharpe_ratio,
                'kelly_fraction': result.kelly_fraction
            }
            
            self._strategies[strategy_name]['performance_history'].append(performance_entry)
            
            # 최근 10개만 유지
            if len(self._strategies[strategy_name]['performance_history']) > 10:
                self._strategies[strategy_name]['performance_history'] = \
                    self._strategies[strategy_name]['performance_history'][-10:]
    
    def get_strategy_performance_summary(self, strategy_name: str) -> Optional[Dict]:
        """전략 성과 요약 조회"""
        if strategy_name not in self._strategies:
            return None
        
        history = self._strategies[strategy_name]['performance_history']
        if not history:
            return None
        
        returns = [h['avg_return'] for h in history]
        win_rates = [h['win_rate'] for h in history]
        mdds = [h['mdd'] for h in history]
        
        return {
            'strategy_name': strategy_name,
            'total_backtests': len(history),
            'avg_return_mean': np.mean(returns),
            'avg_return_std': np.std(returns),
            'win_rate_mean': np.mean(win_rates),
            'win_rate_std': np.std(win_rates),
            'mdd_mean': np.mean(mdds),
            'mdd_std': np.std(mdds),
            'consistency_score': 1 - (np.std(returns) / (abs(np.mean(returns)) + 0.01)),
            'last_performance': history[-1]
        }
    
    def _register_default_strategies(self):
        """기본 전략들 등록"""
        
        # 1. 돈치안 채널 + SuperTrend 전략 (긴급 완화 버전)
        @self._strategy_decorator("technical")
        def donchian_supertrend_strategy(data: pd.DataFrame) -> str:
            """완화된 돈치안 채널과 SuperTrend 추세 추종 전략"""
            if len(data) < 10:  # 20 -> 10으로 완화
                return 'HOLD'
            
            current = data.iloc[-1]
            recent = data.tail(10)  # 20 -> 10으로 완화
            
            # 단순화된 돈치안 채널 (10일)
            donchian_high = recent['high'].max()
            donchian_low = recent['low'].min()
            donchian_mid = (donchian_high + donchian_low) / 2
            
            # 단순화된 ATR
            high_low = recent['high'] - recent['low']
            atr = high_low.mean()
            
            # 단순 이동평균
            ma_5 = recent['close'].tail(5).mean()
            ma_10 = recent['close'].mean()
            
            # 완화된 매수 조건
            if (current['close'] > donchian_mid and           # 돈치안 중간선 위 (완화)
                current['close'] > ma_5 and                  # 단기 이평 위
                ma_5 > ma_10 * 0.995):                       # 이평 상승 추세 (완화)
                return 'BUY'
            
            # 완화된 매도 조건  
            elif (current['close'] < donchian_mid or          # 돈치안 중간선 아래
                  current['close'] < ma_10 * 0.95):          # 이평 대비 5% 하락
                return 'SELL'
            
            return 'HOLD'
        
        self.register_strategy(
            "Static_Donchian_Supertrend",
            donchian_supertrend_strategy,
            "돈치안 채널과 SuperTrend 지표를 활용한 추세 추종 전략",
            "technical",
            {"donchian_period": 20, "supertrend_multiplier": 3.0}
        )
        
        # 2. RSI + MACD 전략 (긴급 완화 버전)
        @self._strategy_decorator("oscillator")
        def rsi_macd_strategy(data: pd.DataFrame) -> str:
            """완화된 RSI와 MACD 동적 매매 전략"""
            if len(data) < 14:  # 26 -> 14로 완화
                return 'HOLD'
            
            current = data.iloc[-1]
            recent = data.tail(14)
            
            # 단순화된 RSI (7일)
            delta = recent['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(7).mean().iloc[-1]
            loss = -delta.where(delta < 0, 0).rolling(7).mean().iloc[-1]
            
            if loss == 0:
                rsi = 100
            else:
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
            
            # 단순화된 MACD
            ma_fast = recent['close'].tail(6).mean()  # 12 -> 6
            ma_slow = recent['close'].mean()          # 26 -> 14
            macd_line = ma_fast - ma_slow
            
            # 이동평균 추세
            ma_7 = recent['close'].tail(7).mean()
            
            # 완화된 매수 조건
            if (rsi < 60 and                          # 40 -> 60으로 완화
                macd_line > 0 and                     # MACD 양수
                current['close'] > ma_7):             # 단기 이평 위
                return 'BUY'
            
            # 완화된 매도 조건
            elif (rsi > 70 or                         # 65 -> 70으로 완화
                  macd_line < -ma_slow * 0.01 or     # MACD 크게 음수
                  current['close'] < ma_7 * 0.97):   # 단기 이평 대비 3% 하락
                return 'SELL'
            
            return 'HOLD'
        
        self.register_strategy(
            "Dynamic_RSI_MACD", 
            rsi_macd_strategy,
            "RSI와 MACD의 조합을 통한 동적 매매 전략",
            "oscillator",
            {"rsi_period": 14, "macd_fast": 12, "macd_slow": 26, "macd_signal": 9}
        )
        
        # 3. VCP + 모멘텀 전략 (긴급 완화 버전)
        @self._strategy_decorator("momentum")
        def vcp_momentum_strategy(data: pd.DataFrame) -> str:
            """완화된 VCP 모멘텀 하이브리드 전략"""
            if len(data) < 20:  # 50 -> 20으로 완화
                return 'HOLD'
            
            current = data.iloc[-1]
            recent = data.tail(20)
            
            # 단순화된 변동성 체크
            price_change = recent['close'].pct_change()
            volatility = price_change.std()
            
            # 단순화된 모멘텀
            momentum_5 = (current['close'] - recent.iloc[0]['close']) / recent.iloc[0]['close']
            
            # 이동평균
            ma_5 = recent['close'].tail(5).mean()
            ma_10 = recent['close'].tail(10).mean()
            
            # 거래량 비율
            avg_volume = recent['volume'].mean()
            volume_ratio = current['volume'] / avg_volume if avg_volume > 0 else 1
            
            # 완화된 매수 조건
            if (momentum_5 > -0.1 and                 # -5% -> -10%로 완화
                current['close'] > ma_5 and          # 단기 이평 위
                ma_5 > ma_10 * 0.98 and              # 이평 상승 (완화)
                volume_ratio > 1.2):                 # 거래량 20% 증가 (완화)
                return 'BUY'
            
            # 완화된 매도 조건
            elif (momentum_5 < -0.15 or              # -15% 하락
                  current['close'] < ma_10 * 0.95 or # 중기 이평 대비 5% 하락
                  volume_ratio > 4.0):               # 과도한 거래량
                return 'SELL'
            
            return 'HOLD'
        
        self.register_strategy(
            "Hybrid_VCP_Momentum",
            vcp_momentum_strategy, 
            "변동성 수축 패턴과 모멘텀을 결합한 하이브리드 전략",
            "momentum",
            {"volatility_threshold": 0.7, "pullback_min": 0.05, "pullback_max": 0.25}
        )
        
        # 4. 간단한 이동평균 전략 (긴급 완화 버전)
        @self._strategy_decorator("simple")
        def simple_ma_crossover(data: pd.DataFrame) -> str:
            """완화된 이동평균 크로스오버 전략"""
            if len(data) < 15:  # 50 -> 15로 완화
                return 'HOLD'
            
            recent = data.tail(15)
            ma_5 = recent['close'].tail(5).mean()     # 20 -> 5
            ma_15 = recent['close'].mean()            # 50 -> 15
            
            # 이전 값들
            if len(recent) >= 6:
                prev_ma_5 = recent['close'].iloc[-6:-1].mean()
                prev_ma_15 = recent['close'].iloc[:-1].mean()
            else:
                return 'HOLD'
            
            # 완화된 크로스오버 조건
            if (ma_5 > ma_15 and prev_ma_5 <= prev_ma_15):      # 골든 크로스
                return 'BUY'
            elif (ma_5 < ma_15 and prev_ma_5 >= prev_ma_15):    # 데드 크로스
                return 'SELL'
            elif (ma_5 > ma_15 * 1.02):                        # 5일선이 15일선보다 2% 위
                return 'BUY'
            elif (ma_5 < ma_15 * 0.98):                        # 5일선이 15일선보다 2% 아래
                return 'SELL'
            
            return 'HOLD'
        
        self.register_strategy(
            "Simple_MA_Crossover",
            simple_ma_crossover,
            "20일/50일 이동평균 크로스오버 전략",
            "simple",
            {"ma_short": 20, "ma_long": 50}
        )
        
        logger.info("✅ 기본 전략 4개 등록 완료")
    
    def _strategy_decorator(self, category: str):
        """전략 함수 데코레이터"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"❌ 전략 '{func.__name__}' 실행 오류: {e}")
                    return 'HOLD'
            return wrapper
        return decorator
    
    def validate_strategy_function(self, strategy_func: Callable, 
                                 test_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """전략 함수 유효성 검증"""
        validation_result = {
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'test_results': {}
        }
        
        try:
            # 1. 시그니처 검증
            sig = inspect.signature(strategy_func)
            if len(sig.parameters) < 1:
                validation_result['errors'].append("전략 함수는 최소 1개 파라미터가 필요합니다")
            
            # 2. 테스트 데이터로 실행 검증
            if test_data is not None:
                try:
                    # 다양한 데이터 크기로 테스트
                    test_sizes = [10, 30, 100]
                    test_results = {}
                    
                    for size in test_sizes:
                        if len(test_data) >= size:
                            test_subset = test_data.tail(size)
                            result = strategy_func(test_subset)
                            
                            test_results[f'size_{size}'] = {
                                'signal': result,
                                'valid_signal': result in ['BUY', 'SELL', 'HOLD'],
                                'execution_time': None  # 실제로는 시간 측정 가능
                            }
                            
                            if result not in ['BUY', 'SELL', 'HOLD']:
                                validation_result['errors'].append(
                                    f"잘못된 시그널 반환: {result} (데이터 크기: {size})"
                                )
                    
                    validation_result['test_results'] = test_results
                    
                except Exception as e:
                    validation_result['errors'].append(f"전략 함수 실행 오류: {str(e)}")
            
            # 3. 최종 검증 결과
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f"검증 과정 오류: {str(e)}")
        
        return validation_result
    
    def get_strategy_recommendations(self, market_condition: str = "normal") -> List[str]:
        """시장 상황에 따른 전략 추천"""
        recommendations = []
        
        if market_condition == "bull":  # 상승장
            recommendations = [
                "Hybrid_VCP_Momentum",
                "Static_Donchian_Supertrend", 
                "Simple_MA_Crossover"
            ]
        elif market_condition == "bear":  # 하락장
            recommendations = [
                "Dynamic_RSI_MACD"
            ]
        elif market_condition == "sideways":  # 횡보장
            recommendations = [
                "Dynamic_RSI_MACD"
            ]
        else:  # 일반 상황
            recommendations = [
                "Static_Donchian_Supertrend",
                "Dynamic_RSI_MACD",
                "Hybrid_VCP_Momentum"
            ]
        
        # 등록된 전략만 필터링
        available_recommendations = [
            name for name in recommendations 
            if name in self._strategies
        ]
        
        return available_recommendations
    
    def export_strategies(self) -> Dict[str, Any]:
        """전략 정보 내보내기 (함수 제외)"""
        export_data = {}
        
        for name, info in self._strategies.items():
            export_data[name] = {
                'description': info['description'],
                'category': info['category'],
                'parameters': info['parameters'],
                'registered_at': info['registered_at'].isoformat(),
                'usage_count': info['usage_count'],
                'last_used': info['last_used'].isoformat() if info['last_used'] else None,
                'performance_history': info['performance_history']
            }
        
        return export_data
    
    def get_strategy_statistics(self) -> Dict[str, Any]:
        """전략 통계 정보"""
        total_strategies = len(self._strategies)
        categories = {}
        usage_stats = []
        
        for name, info in self._strategies.items():
            # 카테고리별 집계
            category = info['category']
            if category not in categories:
                categories[category] = 0
            categories[category] += 1
            
            # 사용 통계
            usage_stats.append({
                'name': name,
                'usage_count': info['usage_count'],
                'category': category
            })
        
        # 가장 많이 사용된 전략
        most_used = max(usage_stats, key=lambda x: x['usage_count']) if usage_stats else None
        
        return {
            'total_strategies': total_strategies,
            'categories': categories,
            'most_used_strategy': most_used['name'] if most_used else None,
            'total_usage': sum(info['usage_count'] for info in self._strategies.values()),
            'average_usage': sum(info['usage_count'] for info in self._strategies.values()) / total_strategies if total_strategies > 0 else 0
        }