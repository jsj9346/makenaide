#!/usr/bin/env python3
"""
백테스트 타입 정의 모듈

백테스트 시스템에서 사용하는 데이터 클래스들과 타입 정의를 포함합니다.
기존 backtester.py의 BacktestResult, KellyBacktestResult, StrategyConfig 클래스들을 분리했습니다.

Author: Backtesting Refactoring
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date

@dataclass
class BacktestResult:
    """백테스트 결과 데이터 클래스"""
    strategy_name: str
    combo_name: Optional[str] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    
    # 성과 지표
    win_rate: float = 0.0
    avg_return: float = 0.0
    mdd: float = 0.0  # Max Drawdown
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    # 켈리 공식 관련
    kelly_fraction: float = 0.0
    kelly_1_2: float = 0.0  # 켈리의 1/2
    
    # 추가 지표
    b_value: float = 0.0  # 평균 수익/평균 손실 비율
    swing_score: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    profit_factor: float = 0.0
    
    # 메타데이터
    parameters: Dict[str, Any] = None
    trades: List[Dict] = None
    created_at: Optional[datetime] = None
    
    # 시간대별 분석 지원
    timezone_enhanced: bool = False
    timezone_analysis: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}
        if self.trades is None:
            self.trades = []
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.timezone_analysis is None:
            self.timezone_analysis = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        result_dict = {
            'strategy_name': self.strategy_name,
            'combo_name': self.combo_name,
            'period_start': self.period_start,
            'period_end': self.period_end,
            'win_rate': self.win_rate,
            'avg_return': self.avg_return,
            'mdd': self.mdd,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'kelly_fraction': self.kelly_fraction,
            'kelly_1_2': self.kelly_1_2,
            'b_value': self.b_value,
            'swing_score': self.swing_score,
            'sharpe_ratio': self.sharpe_ratio,
            'sortino_ratio': self.sortino_ratio,
            'profit_factor': self.profit_factor,
            'parameters': self.parameters,
            'trades': self.trades,
            'created_at': self.created_at,
            'timezone_enhanced': self.timezone_enhanced,
            'timezone_analysis': self.timezone_analysis
        }
        
        # Dict serialization 문제를 방지하기 위해 복잡한 객체는 JSON 호환 형태로 변환
        if isinstance(result_dict['parameters'], dict):
            # 날짜/시간 객체 등을 문자열로 변환
            result_dict['parameters'] = self._serialize_dict(result_dict['parameters'])
        
        if isinstance(result_dict['timezone_analysis'], dict):
            result_dict['timezone_analysis'] = self._serialize_dict(result_dict['timezone_analysis'])
            
        return result_dict
    
    def _serialize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """딕셔너리의 JSON 직렬화 호환성 보장"""
        serialized = {}
        for key, value in data.items():
            if isinstance(value, (datetime, date)):
                serialized[key] = value.isoformat()
            elif isinstance(value, dict):
                serialized[key] = self._serialize_dict(value)
            elif isinstance(value, list):
                serialized[key] = [
                    item.isoformat() if isinstance(item, (datetime, date)) else item 
                    for item in value
                ]
            else:
                serialized[key] = value
        return serialized
    
    def get_composite_score(self, weights: Optional[Dict[str, float]] = None) -> float:
        """
        복합 성과 점수 계산
        
        Args:
            weights: 각 지표의 가중치 {'return': 0.3, 'win_rate': 0.2, ...}
        
        Returns:
            float: 0-1 사이의 정규화된 복합 점수
        """
        if weights is None:
            weights = {
                'return': 0.35,      # 수익률 (35%)
                'win_rate': 0.25,    # 승률 (25%)
                'kelly': 0.20,       # 켈리 비율 (20%)
                'sharpe': 0.15,      # 샤프 비율 (15%)
                'mdd_penalty': 0.05  # 최대 낙폭 페널티 (5%)
            }
        
        # 각 지표를 0-1로 정규화
        normalized_return = max(0, min(1, (self.avg_return + 0.2) / 0.4))  # -20% ~ +20% 범위
        normalized_win_rate = max(0, min(1, self.win_rate))  # 0% ~ 100%
        normalized_kelly = max(0, min(1, (self.kelly_fraction + 0.1) / 0.6))  # -10% ~ 50% 범위
        normalized_sharpe = max(0, min(1, (self.sharpe_ratio + 1) / 4))  # -1 ~ 3 범위
        mdd_penalty = max(0, min(1, abs(self.mdd) / 0.5))  # 0% ~ 50% MDD
        
        composite_score = (
            normalized_return * weights['return'] +
            normalized_win_rate * weights['win_rate'] +
            normalized_kelly * weights['kelly'] +
            normalized_sharpe * weights['sharpe'] -
            mdd_penalty * weights['mdd_penalty']
        )
        
        return max(0.0, min(1.0, composite_score))


@dataclass
class KellyBacktestResult:
    """켈리 공식 특화 백테스트 결과"""
    strategy_name: str
    period: Tuple[date, date]
    
    # 켈리 공식 핵심 지표
    kelly_fraction: float = 0.0
    kelly_1_2: float = 0.0
    kelly_1_4: float = 0.0
    
    # 성과 지표
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    b_value: float = 0.0  # avg_win / abs(avg_loss)
    
    # 리스크 지표
    max_drawdown: float = 0.0
    volatility: float = 0.0
    var_95: float = 0.0  # Value at Risk (95%)
    
    # 거래 통계
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    # 백테스트 메타데이터
    parameters: Dict[str, Any] = None
    trades_detail: List[Dict] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}
        if self.trades_detail is None:
            self.trades_detail = []
        
        # 파생 지표 계산
        if self.losing_trades > 0:
            self.b_value = abs(self.avg_win / self.avg_loss) if self.avg_loss != 0 else 0
        
        # 켈리의 1/2, 1/4 계산
        self.kelly_1_2 = self.kelly_fraction * 0.5
        self.kelly_1_4 = self.kelly_fraction * 0.25
    
    def calculate_kelly_metrics(self, returns: List[float]) -> Dict[str, float]:
        """켈리 공식 관련 지표 재계산"""
        if not returns:
            return {}
            
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        
        if not wins or not losses:
            return {}
        
        p = len(wins) / len(returns)  # 승률
        q = 1 - p  # 패률
        b = np.mean(wins) / abs(np.mean(losses))  # 평균 승리/평균 패배 비율
        
        # 켈리 공식: f* = (bp - q) / b
        kelly_optimal = (b * p - q) / b if b > 0 else 0
        
        return {
            'kelly_fraction': kelly_optimal,
            'kelly_1_2': kelly_optimal * 0.5,
            'kelly_1_4': kelly_optimal * 0.25,
            'win_rate': p,
            'b_value': b,
            'avg_win': np.mean(wins),
            'avg_loss': np.mean(losses)
        }


@dataclass
class StrategyConfig:
    """백테스트 전략 설정 클래스"""
    name: str
    description: Optional[str] = None
    
    # 기본 파라미터
    initial_cash: float = 10_000_000  # 초기 자금 (1천만원)
    position_size_method: str = "fixed"  # "fixed", "percent", "kelly"
    position_size_value: float = 0.1  # 10% 또는 고정 금액
    
    # 리스크 관리
    stop_loss_pct: float = 0.08  # 8% 손절
    take_profit_pct: Optional[float] = None  # 익절 (None이면 무제한)
    max_positions: int = 10  # 최대 동시 보유 종목 수
    
    # 거래 조건
    min_volume: float = 1_000_000  # 최소 거래량
    commission_rate: float = 0.0005  # 수수료율 0.05%
    slippage_rate: float = 0.001  # 슬리피지 0.1%
    
    # 기술적 지표 파라미터
    technical_indicators: Dict[str, Any] = None
    
    # 필터링 조건
    filters: Dict[str, Any] = None
    
    # 시간대별 설정 (선택적)
    timezone_settings: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.technical_indicators is None:
            self.technical_indicators = {
                'ma_periods': [20, 50, 200],
                'rsi_period': 14,
                'macd_params': (12, 26, 9),
                'bollinger_period': 20,
                'atr_period': 14
            }
        
        if self.filters is None:
            self.filters = {
                'min_price': 1000,  # 최소 가격 1000원
                'max_price': 500000,  # 최대 가격 50만원
                'volume_ma_ratio': 1.5,  # 거래량 이동평균 대비 1.5배
                'exclude_tickers': ['USDT-KRW', 'BTC-KRW']  # 제외할 종목
            }
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'name': self.name,
            'description': self.description,
            'initial_cash': self.initial_cash,
            'position_size_method': self.position_size_method,
            'position_size_value': self.position_size_value,
            'stop_loss_pct': self.stop_loss_pct,
            'take_profit_pct': self.take_profit_pct,
            'max_positions': self.max_positions,
            'min_volume': self.min_volume,
            'commission_rate': self.commission_rate,
            'slippage_rate': self.slippage_rate,
            'technical_indicators': self.technical_indicators,
            'filters': self.filters,
            'timezone_settings': self.timezone_settings
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategyConfig':
        """딕셔너리에서 생성"""
        return cls(**data)
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if self.initial_cash <= 0:
            errors.append("초기 자금은 0보다 커야 합니다")
        
        if self.position_size_method not in ["fixed", "percent", "kelly"]:
            errors.append("포지션 크기 방법은 'fixed', 'percent', 'kelly' 중 하나여야 합니다")
        
        if self.position_size_method == "percent" and not (0 < self.position_size_value <= 1):
            errors.append("퍼센트 방식의 포지션 크기는 0과 1 사이여야 합니다")
        
        if self.stop_loss_pct <= 0 or self.stop_loss_pct >= 1:
            errors.append("손절 비율은 0과 1 사이여야 합니다")
        
        if self.take_profit_pct is not None and (self.take_profit_pct <= 0 or self.take_profit_pct >= 5):
            errors.append("익절 비율은 0과 5 사이여야 합니다")
        
        if self.max_positions <= 0:
            errors.append("최대 포지션 수는 0보다 커야 합니다")
        
        if self.commission_rate < 0 or self.commission_rate > 0.01:
            errors.append("수수료율은 0과 0.01 사이여야 합니다")
        
        return errors


@dataclass
class BacktestSummary:
    """백테스트 전체 요약 결과"""
    session_name: str
    period: Tuple[date, date]
    total_strategies: int
    
    # 최고 성과
    best_strategy: str
    best_return: float
    best_sharpe: float
    best_kelly: float
    
    # 평균 성과
    avg_return: float
    avg_win_rate: float
    avg_mdd: float
    avg_trades: float
    
    # 전략별 순위
    strategy_rankings: List[Dict[str, Any]]
    
    # 실행 정보
    execution_time: float
    strategies_tested: int
    optimization_applied: bool = False
    hybrid_filtering_enabled: bool = False
    
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'session_name': self.session_name,
            'period_start': self.period[0],
            'period_end': self.period[1],
            'total_strategies': self.total_strategies,
            'best_strategy': self.best_strategy,
            'best_return': self.best_return,
            'best_sharpe': self.best_sharpe,
            'best_kelly': self.best_kelly,
            'avg_return': self.avg_return,
            'avg_win_rate': self.avg_win_rate,
            'avg_mdd': self.avg_mdd,
            'avg_trades': self.avg_trades,
            'strategy_rankings': self.strategy_rankings,
            'execution_time': self.execution_time,
            'strategies_tested': self.strategies_tested,
            'optimization_applied': self.optimization_applied,
            'hybrid_filtering_enabled': self.hybrid_filtering_enabled,
            'created_at': self.created_at
        }


# 유틸리티 함수들
def create_default_strategy_configs() -> List[StrategyConfig]:
    """기본 전략 설정들 생성"""
    configs = []
    
    # 1. Donchian + SuperTrend 전략
    donchian_config = StrategyConfig(
        name="Static_Donchian_Supertrend",
        description="돈치안 채널과 SuperTrend 지표를 활용한 추세 추종 전략",
        position_size_method="percent",
        position_size_value=0.15,
        stop_loss_pct=0.07,
        technical_indicators={
            'donchian_period': 20,
            'supertrend_period': 10,
            'supertrend_multiplier': 3.0,
            'ma_periods': [20, 50, 200]
        }
    )
    configs.append(donchian_config)
    
    # 2. RSI + MACD 전략  
    rsi_macd_config = StrategyConfig(
        name="Dynamic_RSI_MACD",
        description="RSI와 MACD의 조합을 통한 동적 매매 전략",
        position_size_method="kelly",
        position_size_value=0.25,
        stop_loss_pct=0.08,
        take_profit_pct=0.25,
        technical_indicators={
            'rsi_period': 14,
            'rsi_overbought': 70,
            'rsi_oversold': 30,
            'macd_params': (12, 26, 9),
            'ma_periods': [20, 50]
        }
    )
    configs.append(rsi_macd_config)
    
    # 3. 하이브리드 VCP + 모멘텀 전략
    vcp_momentum_config = StrategyConfig(
        name="Hybrid_VCP_Momentum",
        description="변동성 수축 패턴과 모멘텀을 결합한 하이브리드 전략",
        position_size_method="percent",
        position_size_value=0.12,
        stop_loss_pct=0.075,
        take_profit_pct=0.30,
        max_positions=8,
        technical_indicators={
            'volatility_period': 20,
            'momentum_period': 10,
            'volume_ma_period': 30,
            'breakout_threshold': 0.02,
            'ma_periods': [10, 20, 50]
        },
        filters={
            'min_price': 5000,
            'volume_ma_ratio': 2.0,
            'volatility_min': 0.02,
            'volatility_max': 0.15
        }
    )
    configs.append(vcp_momentum_config)
    
    return configs


def validate_backtest_result(result: BacktestResult) -> List[str]:
    """백테스트 결과 유효성 검증"""
    errors = []
    
    if not result.strategy_name:
        errors.append("전략명이 필요합니다")
    
    if result.total_trades < 0:
        errors.append("총 거래 수는 음수일 수 없습니다")
    
    if result.winning_trades + result.losing_trades != result.total_trades:
        errors.append("승리 + 패배 거래 수가 총 거래 수와 일치하지 않습니다")
    
    if not (0 <= result.win_rate <= 1):
        errors.append("승률은 0과 1 사이여야 합니다")
    
    if result.mdd > 0:
        errors.append("최대 낙폭(MDD)은 음수 또는 0이어야 합니다")
    
    return errors