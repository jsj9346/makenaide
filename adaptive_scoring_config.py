#!/usr/bin/env python3
"""
적응형 점수제 설정 시스템
시장 상황과 사용자 선호도에 따라 동적으로 필터링 기준을 조정

🎯 핵심 아이디어:
- 시장 상황별 임계점 조정 (강세장 vs 약세장)
- 사용자 선호도 반영 (보수적 vs 공격적)
- 백테스트 기반 최적화
- 실시간 성과 피드백 반영
"""

import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class MarketRegime(Enum):
    """시장 상황"""
    BULL = "bull"          # 강세장
    BEAR = "bear"          # 약세장
    SIDEWAYS = "sideways"  # 횡보장
    VOLATILE = "volatile"  # 변동성 장

class InvestorProfile(Enum):
    """투자 성향"""
    CONSERVATIVE = "conservative"  # 보수적
    MODERATE = "moderate"         # 중도적
    AGGRESSIVE = "aggressive"     # 공격적

@dataclass
class AdaptiveConfig:
    """적응형 설정"""
    # 기본 임계점
    base_pass_threshold: float = 60.0
    base_buy_threshold: float = 80.0
    base_strong_buy_threshold: float = 90.0

    # 시장 상황별 조정 계수
    market_adjustments: Dict[str, float] = None

    # 투자 성향별 조정 계수
    profile_adjustments: Dict[str, float] = None

    # 가중치 조정
    dynamic_weights: Dict[str, float] = None

    def __post_init__(self):
        if self.market_adjustments is None:
            self.market_adjustments = {
                MarketRegime.BULL.value: -5.0,      # 강세장: 기준 완화
                MarketRegime.BEAR.value: +10.0,     # 약세장: 기준 강화
                MarketRegime.SIDEWAYS.value: 0.0,   # 횡보장: 기준 유지
                MarketRegime.VOLATILE.value: +5.0   # 변동성장: 기준 약간 강화
            }

        if self.profile_adjustments is None:
            self.profile_adjustments = {
                InvestorProfile.CONSERVATIVE.value: +10.0,  # 보수적: 높은 기준
                InvestorProfile.MODERATE.value: 0.0,        # 중도적: 기본 기준
                InvestorProfile.AGGRESSIVE.value: -10.0     # 공격적: 낮은 기준
            }

        if self.dynamic_weights is None:
            self.dynamic_weights = {
                "stage_weight": 0.25,
                "ma_weight": 0.20,
                "rs_weight": 0.25,
                "volume_weight": 0.15,
                "momentum_weight": 0.15
            }

class AdaptiveScoringManager:
    """적응형 점수제 관리자"""

    def __init__(self, config_file: str = "adaptive_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.performance_history = []

    def load_config(self) -> AdaptiveConfig:
        """설정 로드"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return AdaptiveConfig(**data)
        except FileNotFoundError:
            # 기본 설정 생성
            config = AdaptiveConfig()
            self.save_config(config)
            return config

    def save_config(self, config: AdaptiveConfig):
        """설정 저장"""
        data = {
            'base_pass_threshold': config.base_pass_threshold,
            'base_buy_threshold': config.base_buy_threshold,
            'base_strong_buy_threshold': config.base_strong_buy_threshold,
            'market_adjustments': config.market_adjustments,
            'profile_adjustments': config.profile_adjustments,
            'dynamic_weights': config.dynamic_weights
        }

        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get_adaptive_thresholds(self,
                               market_regime: MarketRegime,
                               investor_profile: InvestorProfile) -> Dict[str, float]:
        """적응형 임계점 계산"""

        market_adj = self.config.market_adjustments[market_regime.value]
        profile_adj = self.config.profile_adjustments[investor_profile.value]

        total_adjustment = market_adj + profile_adj

        return {
            'pass_threshold': max(30.0, self.config.base_pass_threshold + total_adjustment),
            'buy_threshold': max(50.0, self.config.base_buy_threshold + total_adjustment),
            'strong_buy_threshold': max(70.0, self.config.base_strong_buy_threshold + total_adjustment)
        }

    def get_adaptive_weights(self, market_regime: MarketRegime) -> Dict[str, float]:
        """시장 상황별 적응형 가중치"""
        base_weights = self.config.dynamic_weights.copy()

        # 시장 상황별 가중치 조정
        if market_regime == MarketRegime.BULL:
            # 강세장: 모멘텀과 상대강도 중시
            base_weights['momentum_weight'] *= 1.2
            base_weights['rs_weight'] *= 1.1
            base_weights['stage_weight'] *= 0.9

        elif market_regime == MarketRegime.BEAR:
            # 약세장: Stage와 MA 정배열 중시
            base_weights['stage_weight'] *= 1.2
            base_weights['ma_weight'] *= 1.1
            base_weights['momentum_weight'] *= 0.8

        elif market_regime == MarketRegime.VOLATILE:
            # 변동성장: 거래량과 기술적 안정성 중시
            base_weights['volume_weight'] *= 1.3
            base_weights['ma_weight'] *= 1.1
            base_weights['momentum_weight'] *= 0.9

        # 가중치 정규화
        total_weight = sum(base_weights.values())
        normalized_weights = {k: v/total_weight for k, v in base_weights.items()}

        return normalized_weights

    def detect_market_regime(self) -> MarketRegime:
        """시장 상황 자동 감지 (임시 구현)"""
        # 실제로는 BTC, 코스피 등 주요 지수 분석 필요
        # 현재는 임시로 SIDEWAYS 반환
        return MarketRegime.SIDEWAYS

    def optimize_thresholds_from_backtest(self, backtest_results: List[Dict]):
        """백테스트 결과 기반 임계점 최적화"""
        # 백테스트 결과를 분석하여 최적의 임계점 찾기
        best_threshold = 60.0
        best_performance = 0.0

        for threshold in range(40, 90, 5):
            performance = self._calculate_performance_at_threshold(
                backtest_results, threshold
            )

            if performance > best_performance:
                best_performance = performance
                best_threshold = threshold

        # 설정 업데이트
        self.config.base_pass_threshold = best_threshold
        self.save_config(self.config)

        return best_threshold, best_performance

    def _calculate_performance_at_threshold(self,
                                          results: List[Dict],
                                          threshold: float) -> float:
        """특정 임계점에서의 성과 계산"""
        # 승률, 수익률, 샤프비율 등을 종합한 성과 지표
        # 임시 구현
        return 0.5

    def update_from_live_performance(self, trade_results: List[Dict]):
        """실제 거래 성과 기반 설정 업데이트"""
        self.performance_history.extend(trade_results)

        # 최근 30건 거래 성과 분석
        recent_trades = self.performance_history[-30:]

        if len(recent_trades) >= 10:
            win_rate = sum(1 for t in recent_trades if t['profit'] > 0) / len(recent_trades)
            avg_profit = sum(t['profit'] for t in recent_trades) / len(recent_trades)

            # 성과가 좋지 않으면 기준 강화
            if win_rate < 0.5 and avg_profit < 0:
                self.config.base_pass_threshold = min(80.0, self.config.base_pass_threshold + 2.0)
            # 성과가 좋으면 기준 완화
            elif win_rate > 0.7 and avg_profit > 0.05:
                self.config.base_pass_threshold = max(40.0, self.config.base_pass_threshold - 2.0)

            self.save_config(self.config)

def create_scoring_strategies():
    """다양한 점수제 전략 프리셋"""

    strategies = {
        "conservative": {
            "name": "보수적 전략",
            "pass_threshold": 75.0,
            "buy_threshold": 85.0,
            "weights": {
                "stage_weight": 0.30,  # Stage 비중 높임
                "ma_weight": 0.25,     # MA 정배열 비중 높임
                "rs_weight": 0.20,
                "volume_weight": 0.15,
                "momentum_weight": 0.10  # 모멘텀 비중 낮춤
            },
            "description": "안정적인 Stage 2+ 종목 위주, 높은 기준"
        },

        "growth": {
            "name": "성장주 전략",
            "pass_threshold": 65.0,
            "buy_threshold": 80.0,
            "weights": {
                "stage_weight": 0.20,
                "ma_weight": 0.15,
                "rs_weight": 0.30,     # RS Rating 비중 높임
                "volume_weight": 0.20,  # 거래량 비중 높임
                "momentum_weight": 0.15
            },
            "description": "상대강도와 거래량 중시, 성장 모멘텀 포착"
        },

        "momentum": {
            "name": "모멘텀 전략",
            "pass_threshold": 60.0,
            "buy_threshold": 75.0,
            "weights": {
                "stage_weight": 0.15,
                "ma_weight": 0.15,
                "rs_weight": 0.25,
                "volume_weight": 0.20,
                "momentum_weight": 0.25  # 모멘텀 비중 대폭 높임
            },
            "description": "단기 모멘텀과 거래량 급증 포착"
        },

        "quality": {
            "name": "품질 우선 전략",
            "pass_threshold": 70.0,
            "buy_threshold": 85.0,
            "weights": {
                "stage_weight": 0.25,
                "ma_weight": 0.30,     # MA 정배열 최우선
                "rs_weight": 0.25,
                "volume_weight": 0.10,
                "momentum_weight": 0.10
            },
            "description": "기술적 안정성 최우선, 완벽한 정배열 종목"
        }
    }

    return strategies

def test_adaptive_system():
    """적응형 시스템 테스트"""
    print("🧪 적응형 점수제 시스템 테스트")
    print("=" * 50)

    manager = AdaptiveScoringManager()

    # 다양한 시장 상황과 투자 성향 조합 테스트
    scenarios = [
        (MarketRegime.BULL, InvestorProfile.AGGRESSIVE),
        (MarketRegime.BEAR, InvestorProfile.CONSERVATIVE),
        (MarketRegime.SIDEWAYS, InvestorProfile.MODERATE),
        (MarketRegime.VOLATILE, InvestorProfile.CONSERVATIVE)
    ]

    print("📊 시나리오별 적응형 임계점:")
    print("-" * 60)
    print(f"{'시나리오':<20} {'통과':<8} {'매수':<8} {'강매수':<8}")
    print("-" * 60)

    for market, profile in scenarios:
        thresholds = manager.get_adaptive_thresholds(market, profile)
        weights = manager.get_adaptive_weights(market)

        scenario_name = f"{market.value}+{profile.value}"
        print(f"{scenario_name:<20} "
              f"{thresholds['pass_threshold']:<8.1f} "
              f"{thresholds['buy_threshold']:<8.1f} "
              f"{thresholds['strong_buy_threshold']:<8.1f}")

    print("\n🎯 전략별 프리셋:")
    strategies = create_scoring_strategies()

    for key, strategy in strategies.items():
        print(f"\n📈 {strategy['name']}:")
        print(f"   임계점: 통과 {strategy['pass_threshold']:.1f}, 매수 {strategy['buy_threshold']:.1f}")
        print(f"   특징: {strategy['description']}")
        weights = strategy['weights']
        print(f"   가중치: Stage {weights['stage_weight']:.2f}, "
              f"MA {weights['ma_weight']:.2f}, "
              f"RS {weights['rs_weight']:.2f}, "
              f"Vol {weights['volume_weight']:.2f}, "
              f"Mom {weights['momentum_weight']:.2f}")

if __name__ == "__main__":
    test_adaptive_system()