#!/usr/bin/env python3
"""
기본 Scoring Modules 구현
LayeredScoringEngine에서 사용할 핵심 점수 모듈들

🏛️ Layer별 모듈 구성:
- Macro Layer (25점): MarketRegime, VolumeProfile, PriceAction
- Structural Layer (45점): StageAnalysis, MovingAverage, RelativeStrength
- Micro Layer (30점): PatternRecognition, VolumeSpike, Momentum

🎯 설계 원칙:
- 각 모듈은 0-100점 스케일로 점수 계산
- 신뢰도(confidence) 함께 제공
- 상세 분석 정보(details) 포함
- 데이터 검증 로직 내장
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from layered_scoring_engine import ScoringModule, ModuleScore, LayerType
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# MACRO LAYER MODULES (25점)
# =============================================================================

class MarketRegimeModule(ScoringModule):
    """시장 상황 분석 모듈 (5점 기여)"""

    def __init__(self):
        super().__init__("MarketRegime", LayerType.MACRO, weight=0.2)  # 25점 중 20% = 5점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """시장 상황 기반 점수 계산"""
        try:
            if len(data) < 20:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            # BTC 동향 대용으로 전체적인 상승/하락 추세 분석
            recent_data = data.tail(20)

            # 최근 20일 수익률 계산
            recent_return = (recent_data['close'].iloc[-1] / recent_data['close'].iloc[0] - 1) * 100

            # MA20 기울기로 단기 추세 판단
            ma20_slope = 0.0
            if 'ma20' in recent_data.columns and recent_data['ma20'].notna().sum() >= 10:
                ma20_values = recent_data['ma20'].dropna()
                if len(ma20_values) >= 2:
                    ma20_slope = (ma20_values.iloc[-1] / ma20_values.iloc[0] - 1) * 100

            # 점수 계산 로직
            score = 50.0  # 기본 점수

            # 최근 수익률 반영
            if recent_return > 10:      # 강한 상승
                score += 30
            elif recent_return > 5:     # 중간 상승
                score += 15
            elif recent_return > 0:     # 약한 상승
                score += 5
            elif recent_return > -5:    # 약한 하락
                score -= 5
            elif recent_return > -10:   # 중간 하락
                score -= 15
            else:                       # 강한 하락
                score -= 30

            # MA20 기울기 반영
            if ma20_slope > 5:
                score += 15
            elif ma20_slope > 0:
                score += 5
            elif ma20_slope < -5:
                score -= 15
            elif ma20_slope < 0:
                score -= 5

            # 점수 범위 조정
            score = max(0.0, min(100.0, score))

            # 신뢰도 계산 (데이터 품질 기반)
            confidence = min(1.0, len(recent_data) / 20 * 0.8 + 0.2)

            details = {
                "recent_20d_return": round(recent_return, 2),
                "ma20_slope": round(ma20_slope, 2),
                "market_sentiment": "bullish" if score > 60 else "bearish" if score < 40 else "neutral"
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"MarketRegimeModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'ma20']


class VolumeProfileModule(ScoringModule):
    """거래량 프로파일 분석 모듈 (10점 기여)"""

    def __init__(self):
        super().__init__("VolumeProfile", LayerType.MACRO, weight=0.4)  # 25점 중 40% = 10점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """거래량 패턴 기반 점수 계산"""
        try:
            if len(data) < 30 or 'volume' not in data.columns:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_volume_data"}, self.name)

            # 최근 30일 거래량 분석
            recent_data = data.tail(30)
            volumes = recent_data['volume'].dropna()

            if len(volumes) < 20:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_volume_data"}, self.name)

            # 거래량 통계
            avg_volume = volumes.mean()
            recent_5d_avg = volumes.tail(5).mean()
            volume_trend = recent_5d_avg / avg_volume if avg_volume > 0 else 1.0

            # 거래량 급증 감지
            volume_spikes = (volumes > avg_volume * 2).sum()
            spike_ratio = volume_spikes / len(volumes)

            # 거래량 상승 지속성
            volume_consistency = 0.0
            if len(volumes) >= 10:
                recent_10d = volumes.tail(10)
                above_avg_days = (recent_10d > avg_volume).sum()
                volume_consistency = above_avg_days / 10

            # 점수 계산
            score = 50.0

            # 거래량 트렌드 반영
            if volume_trend > 1.5:      # 50% 이상 증가
                score += 25
            elif volume_trend > 1.2:    # 20% 이상 증가
                score += 15
            elif volume_trend > 1.0:    # 증가
                score += 5
            elif volume_trend > 0.8:    # 소폭 감소
                score -= 5
            elif volume_trend > 0.5:    # 중간 감소
                score -= 15
            else:                       # 큰 감소
                score -= 25

            # 거래량 급증 빈도 반영
            if spike_ratio > 0.2:       # 20% 이상 급증일
                score += 15
            elif spike_ratio > 0.1:     # 10% 이상 급증일
                score += 10
            elif spike_ratio > 0.05:    # 5% 이상 급증일
                score += 5

            # 거래량 일관성 반영
            score += volume_consistency * 10

            score = max(0.0, min(100.0, score))

            # 신뢰도 계산
            confidence = min(1.0, len(volumes) / 30 * 0.7 + 0.3)

            details = {
                "avg_volume": int(avg_volume),
                "recent_5d_avg": int(recent_5d_avg),
                "volume_trend": round(volume_trend, 2),
                "spike_ratio": round(spike_ratio, 2),
                "volume_consistency": round(volume_consistency, 2)
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"VolumeProfileModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['volume']


class PriceActionModule(ScoringModule):
    """가격 행동 품질 분석 모듈 (10점 기여)"""

    def __init__(self):
        super().__init__("PriceAction", LayerType.MACRO, weight=0.4)  # 25점 중 40% = 10점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """가격 행동 패턴 기반 점수 계산"""
        try:
            if len(data) < 20:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            recent_data = data.tail(20)

            # 가격 변동성 분석
            returns = recent_data['close'].pct_change().dropna()
            volatility = returns.std() * np.sqrt(252) * 100  # 연간 변동성

            # 상승 지속성 분석
            positive_days = (returns > 0).sum()
            up_ratio = positive_days / len(returns) if len(returns) > 0 else 0

            # 캔들 패턴 강도 (고가-저가 대비 종가 위치)
            hl_ranges = recent_data['high'] - recent_data['low']
            close_positions = (recent_data['close'] - recent_data['low']) / hl_ranges
            avg_close_position = close_positions.mean()  # 0.5 이상이면 상단 마감

            # 52주 고점 대비 현재 위치
            max_high = data['high'].max()
            current_price = recent_data['close'].iloc[-1]
            high_proximity = current_price / max_high if max_high > 0 else 0

            # 점수 계산
            score = 50.0

            # 변동성 점수 (적당한 변동성이 좋음)
            if 15 <= volatility <= 40:     # 적정 변동성
                score += 15
            elif 10 <= volatility <= 50:   # 허용 범위
                score += 10
            elif volatility > 60:          # 너무 높은 변동성
                score -= 15

            # 상승 지속성 점수
            if up_ratio > 0.6:             # 60% 이상 상승일
                score += 15
            elif up_ratio > 0.5:           # 50% 이상 상승일
                score += 10
            elif up_ratio < 0.3:           # 30% 미만 상승일
                score -= 15

            # 캔들 강도 점수
            if avg_close_position > 0.6:   # 상단 마감 강세
                score += 10
            elif avg_close_position > 0.5: # 중상단 마감
                score += 5
            elif avg_close_position < 0.3: # 하단 마감 약세
                score -= 10

            # 고점 근접성 점수
            if high_proximity > 0.9:       # 고점 90% 이상
                score += 10
            elif high_proximity > 0.8:     # 고점 80% 이상
                score += 5
            elif high_proximity < 0.5:     # 고점 50% 미만
                score -= 10

            score = max(0.0, min(100.0, score))

            # 신뢰도 계산
            confidence = min(1.0, len(recent_data) / 20 * 0.8 + 0.2)

            details = {
                "volatility": round(volatility, 1),
                "up_ratio": round(up_ratio, 2),
                "avg_close_position": round(avg_close_position, 2),
                "high_proximity": round(high_proximity, 2)
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"PriceActionModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['open', 'high', 'low', 'close']


# =============================================================================
# STRUCTURAL LAYER MODULES (45점)
# =============================================================================

class StageAnalysisModule(ScoringModule):
    """Weinstein Stage 분석 모듈 (15점 기여)"""

    def __init__(self):
        super().__init__("StageAnalysis", LayerType.STRUCTURAL, weight=0.333)  # 45점 중 33.3% = 15점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """Weinstein 4단계 기반 점수 계산"""
        try:
            if len(data) < 50:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            # MA200 데이터 확인
            if 'ma200' not in data.columns:
                return ModuleScore(0.0, 0.0, {"error": "missing_ma200"}, self.name)

            recent_data = data.tail(50)
            current_price = recent_data['close'].iloc[-1]

            # MA200 관련 계산
            ma200_values = recent_data['ma200'].dropna()
            if len(ma200_values) < 10:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_ma200_data"}, self.name)

            current_ma200 = ma200_values.iloc[-1]
            ma200_slope = 0.0

            if len(ma200_values) >= 20:
                # 20일간 MA200 기울기 계산
                ma200_slope = (ma200_values.iloc[-1] / ma200_values.iloc[-20] - 1) * 100

            # Stage 판정
            stage = 1
            stage_confidence = 0.5

            # 현재가 vs MA200 위치
            price_vs_ma200 = current_price / current_ma200 if current_ma200 > 0 else 1.0

            if price_vs_ma200 > 1.0 and ma200_slope > 0.5:
                stage = 2  # 상승 돌파 단계 (핵심 매수 구간)
                stage_confidence = 0.8
            elif price_vs_ma200 > 1.0 and ma200_slope > 0:
                stage = 2  # 약한 상승 돌파
                stage_confidence = 0.6
            elif price_vs_ma200 > 1.0:
                stage = 3  # 분배 단계 가능성
                stage_confidence = 0.4
            elif ma200_slope < -0.5:
                stage = 4  # 하락 단계
                stage_confidence = 0.7
            else:
                stage = 1  # 기반 구축 단계
                stage_confidence = 0.6

            # 점수 계산 (Stage 2가 최고점)
            stage_scores = {1: 30, 2: 100, 3: 50, 4: 0}
            base_score = stage_scores.get(stage, 0)

            # 신뢰도에 따른 조정
            final_score = base_score * stage_confidence

            details = {
                "stage": stage,
                "stage_confidence": round(stage_confidence, 2),
                "price_vs_ma200": round(price_vs_ma200, 3),
                "ma200_slope": round(ma200_slope, 2),
                "current_price": current_price,
                "current_ma200": current_ma200
            }

            return ModuleScore(final_score, stage_confidence, details, self.name)

        except Exception as e:
            logger.error(f"StageAnalysisModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'ma200']


class MovingAverageModule(ScoringModule):
    """이동평균 정배열 분석 모듈 (15점 기여)"""

    def __init__(self):
        super().__init__("MovingAverage", LayerType.STRUCTURAL, weight=0.333)  # 45점 중 33.3% = 15점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """이동평균 정배열 강도 기반 점수 계산"""
        try:
            if len(data) < 30:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            recent_data = data.tail(20)
            current_price = recent_data['close'].iloc[-1]

            # 이동평균 값들 추출
            ma_columns = ['ma5', 'ma20', 'ma60', 'ma120', 'ma200']
            ma_values = {}
            ma_available = []

            for ma_col in ma_columns:
                if ma_col in recent_data.columns:
                    ma_data = recent_data[ma_col].dropna()
                    if len(ma_data) > 0:
                        ma_values[ma_col] = ma_data.iloc[-1]
                        ma_available.append(ma_col)

            if len(ma_available) < 3:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_ma_data"}, self.name)

            # 정배열 점수 계산
            score = 0.0
            total_checks = 0
            alignment_details = {}

            # 현재가 vs 이동평균 (25점)
            current_vs_ma_score = 0
            if 'ma20' in ma_values:
                if current_price > ma_values['ma20']:
                    current_vs_ma_score += 15
                alignment_details['price_vs_ma20'] = current_price > ma_values['ma20']

            if 'ma60' in ma_values:
                if current_price > ma_values['ma60']:
                    current_vs_ma_score += 10
                alignment_details['price_vs_ma60'] = current_price > ma_values['ma60']

            score += min(25, current_vs_ma_score)

            # 이동평균간 정배열 (50점)
            ma_pairs = [
                ('ma5', 'ma20', 15),
                ('ma20', 'ma60', 15),
                ('ma60', 'ma120', 10),
                ('ma120', 'ma200', 10)
            ]

            alignment_score = 0
            for short_ma, long_ma, points in ma_pairs:
                if short_ma in ma_values and long_ma in ma_values:
                    is_aligned = ma_values[short_ma] > ma_values[long_ma]
                    if is_aligned:
                        alignment_score += points
                    alignment_details[f'{short_ma}_vs_{long_ma}'] = is_aligned
                    total_checks += 1

            score += min(50, alignment_score)

            # 이동평균 기울기 (25점)
            slope_score = 0
            for ma_col in ['ma20', 'ma60']:
                if ma_col in recent_data.columns:
                    ma_series = recent_data[ma_col].dropna()
                    if len(ma_series) >= 10:
                        slope = (ma_series.iloc[-1] / ma_series.iloc[-10] - 1) * 100
                        if slope > 1:
                            slope_score += 12.5
                        elif slope > 0:
                            slope_score += 6.25
                        alignment_details[f'{ma_col}_slope'] = round(slope, 2)

            score += min(25, slope_score)

            # 신뢰도 계산
            confidence = min(1.0, len(ma_available) / 5 * 0.7 + total_checks / 4 * 0.3)

            details = {
                "alignment_score": round(score, 1),
                "ma_available": ma_available,
                "current_price": current_price,
                **alignment_details,
                **ma_values
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"MovingAverageModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'ma5', 'ma20', 'ma60', 'ma120', 'ma200']


class RelativeStrengthModule(ScoringModule):
    """상대강도 분석 모듈 (15점 기여)"""

    def __init__(self):
        super().__init__("RelativeStrength", LayerType.STRUCTURAL, weight=0.334)  # 45점 중 33.4% = 15점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """상대강도 기반 점수 계산"""
        try:
            if len(data) < 50:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            # 다양한 기간 수익률 계산
            current_price = data['close'].iloc[-1]
            returns = {}

            periods = [7, 30, 90, 180]
            for period in periods:
                if len(data) >= period:
                    past_price = data['close'].iloc[-period]
                    period_return = (current_price / past_price - 1) * 100 if past_price > 0 else 0
                    returns[f'{period}d_return'] = period_return

            if not returns:
                return ModuleScore(0.0, 0.0, {"error": "no_returns_calculated"}, self.name)

            # RSI 분석
            rsi_score = 0
            rsi_value = None
            if 'rsi' in data.columns:
                rsi_data = data['rsi'].dropna()
                if len(rsi_data) > 0:
                    rsi_value = rsi_data.iloc[-1]
                    # RSI 50-70 구간이 상승 모멘텀 최적
                    if 50 <= rsi_value <= 70:
                        rsi_score = 25
                    elif 45 <= rsi_value <= 75:
                        rsi_score = 15
                    elif 40 <= rsi_value <= 80:
                        rsi_score = 5
                    # 과매수/과매도 구간은 감점
                    elif rsi_value > 80:
                        rsi_score = -10
                    elif rsi_value < 30:
                        rsi_score = -5

            # 수익률 기반 점수
            return_score = 0

            # 단기 수익률 (7일, 30일) - 40점
            if '7d_return' in returns:
                if returns['7d_return'] > 10:
                    return_score += 20
                elif returns['7d_return'] > 5:
                    return_score += 15
                elif returns['7d_return'] > 0:
                    return_score += 5
                elif returns['7d_return'] < -10:
                    return_score -= 15

            if '30d_return' in returns:
                if returns['30d_return'] > 20:
                    return_score += 20
                elif returns['30d_return'] > 10:
                    return_score += 15
                elif returns['30d_return'] > 0:
                    return_score += 5
                elif returns['30d_return'] < -15:
                    return_score -= 15

            # 중장기 수익률 (90일, 180일) - 35점
            for period, weight in [('90d_return', 15), ('180d_return', 20)]:
                if period in returns:
                    ret = returns[period]
                    if ret > 50:
                        return_score += weight
                    elif ret > 30:
                        return_score += weight * 0.8
                    elif ret > 15:
                        return_score += weight * 0.5
                    elif ret > 0:
                        return_score += weight * 0.2
                    elif ret < -20:
                        return_score -= weight * 0.5

            # 최종 점수 (RSI + 수익률)
            total_score = rsi_score + return_score
            total_score = max(0.0, min(100.0, total_score))

            # 신뢰도 계산
            confidence = min(1.0, len(returns) / 4 * 0.6 + (0.4 if rsi_value else 0))

            details = {
                "rsi_value": rsi_value,
                "rsi_score": rsi_score,
                "return_score": return_score,
                **returns
            }

            return ModuleScore(total_score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"RelativeStrengthModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'rsi']


# =============================================================================
# MICRO LAYER MODULES (30점)
# =============================================================================

class PatternRecognitionModule(ScoringModule):
    """패턴 인식 모듈 (10점 기여)"""

    def __init__(self):
        super().__init__("PatternRecognition", LayerType.MICRO, weight=0.333)  # 30점 중 33.3% = 10점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """기술적 패턴 인식 기반 점수 계산"""
        try:
            if len(data) < 30:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            score = 0.0
            patterns_found = []

            # 최근 30일 데이터
            recent_data = data.tail(30)

            # 1. Cup and Handle 패턴 감지 (간단 버전)
            if self._detect_cup_handle_pattern(recent_data):
                score += 40
                patterns_found.append("cup_handle")

            # 2. 브레이크아웃 패턴
            if self._detect_breakout_pattern(recent_data):
                score += 30
                patterns_found.append("breakout")

            # 3. 상승 삼각형 패턴
            if self._detect_ascending_triangle(recent_data):
                score += 25
                patterns_found.append("ascending_triangle")

            # 4. 지지선 돌파 패턴
            if self._detect_support_break(recent_data):
                score += 20
                patterns_found.append("support_break")

            # 패턴이 없으면 기본 점수
            if not patterns_found:
                score = 20  # 최소 기본 점수

            score = min(100.0, score)

            confidence = min(1.0, len(patterns_found) * 0.3 + 0.4)

            details = {
                "patterns_found": patterns_found,
                "pattern_count": len(patterns_found)
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"PatternRecognitionModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def _detect_cup_handle_pattern(self, data: pd.DataFrame) -> bool:
        """간단한 Cup and Handle 패턴 감지"""
        if len(data) < 20:
            return False

        highs = data['high']
        current_high = highs.iloc[-1]
        max_high = highs.max()

        # 현재가가 고점 근처에 있고, 중간에 하락했다가 다시 올라오는 패턴
        return current_high > max_high * 0.95

    def _detect_breakout_pattern(self, data: pd.DataFrame) -> bool:
        """돌파 패턴 감지"""
        if len(data) < 15:
            return False

        # 최근 5일간 고점 vs 이전 10일간 고점
        recent_5d_high = data['high'].tail(5).max()
        previous_10d_high = data['high'].iloc[-15:-5].max()

        return recent_5d_high > previous_10d_high * 1.02  # 2% 이상 돌파

    def _detect_ascending_triangle(self, data: pd.DataFrame) -> bool:
        """상승 삼각형 패턴 감지"""
        if len(data) < 20:
            return False

        # 고점은 비슷하고 저점은 올라가는 패턴
        highs = data['high']
        lows = data['low']

        recent_high = highs.tail(10).max()
        early_high = highs.iloc[-20:-10].max()

        recent_low = lows.tail(10).min()
        early_low = lows.iloc[-20:-10].min()

        # 고점은 비슷하고 저점은 상승
        return (abs(recent_high / early_high - 1) < 0.05 and
                recent_low > early_low * 1.05)

    def _detect_support_break(self, data: pd.DataFrame) -> bool:
        """지지선 돌파 패턴 감지"""
        if len(data) < 15:
            return False

        # 최근 저점이 이전 저점보다 높은 상승 패턴
        recent_low = data['low'].tail(5).min()
        previous_low = data['low'].iloc[-15:-5].min()

        return recent_low > previous_low * 1.03  # 3% 이상 상승

    def get_required_columns(self) -> List[str]:
        return ['open', 'high', 'low', 'close']


class VolumeSpikeModule(ScoringModule):
    """거래량 급증 감지 모듈 (10점 기여)"""

    def __init__(self):
        super().__init__("VolumeSpike", LayerType.MICRO, weight=0.333)  # 30점 중 33.3% = 10점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """거래량 급증 패턴 기반 점수 계산"""
        try:
            if len(data) < 20 or 'volume' not in data.columns:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_volume_data"}, self.name)

            volumes = data['volume'].dropna()
            if len(volumes) < 15:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_volume_data"}, self.name)

            # 평균 거래량 계산 (최근 20일)
            avg_volume = volumes.tail(20).mean()
            recent_5d_volumes = volumes.tail(5)

            score = 0.0
            spike_details = {}

            # 1. 최근 거래량 급증 감지 (50점)
            max_recent_volume = recent_5d_volumes.max()
            spike_ratio = max_recent_volume / avg_volume if avg_volume > 0 else 1.0

            if spike_ratio > 3.0:        # 3배 이상 급증
                score += 50
                spike_details['spike_level'] = 'extreme'
            elif spike_ratio > 2.0:      # 2배 이상 급증
                score += 35
                spike_details['spike_level'] = 'high'
            elif spike_ratio > 1.5:      # 1.5배 이상 급증
                score += 20
                spike_details['spike_level'] = 'moderate'
            elif spike_ratio > 1.2:      # 1.2배 이상 증가
                score += 10
                spike_details['spike_level'] = 'low'
            else:
                spike_details['spike_level'] = 'none'

            # 2. 거래량 지속성 (30점)
            above_avg_days = (recent_5d_volumes > avg_volume).sum()
            consistency_score = (above_avg_days / 5) * 30
            score += consistency_score

            # 3. 가격과 거래량 동반 상승 (20점)
            recent_prices = data['close'].tail(5)
            if len(recent_prices) >= 2:
                price_change = (recent_prices.iloc[-1] / recent_prices.iloc[0] - 1) * 100
                if price_change > 5 and spike_ratio > 1.5:  # 가격과 거래량 동반 상승
                    score += 20
                elif price_change > 0 and spike_ratio > 1.2:
                    score += 10

            score = min(100.0, score)

            # 신뢰도 계산
            confidence = min(1.0, len(recent_5d_volumes) / 5 * 0.8 + 0.2)

            details = {
                "spike_ratio": round(spike_ratio, 2),
                "avg_volume": int(avg_volume),
                "max_recent_volume": int(max_recent_volume),
                "above_avg_days": above_avg_days,
                "consistency_score": round(consistency_score, 1),
                **spike_details
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"VolumeSpikeModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'volume']


class MomentumModule(ScoringModule):
    """모멘텀 지표 분석 모듈 (10점 기여)"""

    def __init__(self):
        super().__init__("Momentum", LayerType.MICRO, weight=0.334)  # 30점 중 33.4% = 10점

    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """모멘텀 지표 기반 점수 계산"""
        try:
            if len(data) < 20:
                return ModuleScore(0.0, 0.0, {"error": "insufficient_data"}, self.name)

            score = 0.0
            momentum_details = {}

            # 1. 가격 모멘텀 (40점)
            recent_data = data.tail(10)
            price_momentum = (recent_data['close'].iloc[-1] / recent_data['close'].iloc[0] - 1) * 100

            if price_momentum > 10:
                score += 40
            elif price_momentum > 5:
                score += 30
            elif price_momentum > 2:
                score += 20
            elif price_momentum > 0:
                score += 10
            elif price_momentum < -5:
                score -= 10

            momentum_details['price_momentum_10d'] = round(price_momentum, 2)

            # 2. MACD 신호 (30점)
            if 'macd' in data.columns and 'macd_signal' in data.columns:
                macd_data = data[['macd', 'macd_signal']].dropna()
                if len(macd_data) > 0:
                    current_macd = macd_data['macd'].iloc[-1]
                    current_signal = macd_data['macd_signal'].iloc[-1]

                    # MACD 골든크로스 확인
                    if current_macd > current_signal:
                        if current_macd > 0:  # 0선 위에서 골든크로스
                            score += 30
                            momentum_details['macd_signal'] = 'strong_bullish'
                        else:  # 0선 아래에서 골든크로스
                            score += 20
                            momentum_details['macd_signal'] = 'bullish'
                    else:
                        if current_macd < 0 and current_signal < 0:  # 둘 다 음수
                            score -= 10
                            momentum_details['macd_signal'] = 'bearish'
                        else:
                            momentum_details['macd_signal'] = 'neutral'

                    momentum_details['macd_value'] = round(current_macd, 4)
                    momentum_details['macd_signal_value'] = round(current_signal, 4)

            # 3. 단기 추세 강도 (30점)
            if len(data) >= 5:
                short_trend = 0
                prices = data['close'].tail(5)

                # 연속 상승일 계산
                up_days = 0
                for i in range(1, len(prices)):
                    if prices.iloc[i] > prices.iloc[i-1]:
                        up_days += 1

                if up_days >= 4:        # 4일 연속 상승
                    score += 30
                elif up_days >= 3:      # 3일 연속 상승
                    score += 20
                elif up_days >= 2:      # 2일 연속 상승
                    score += 10

                momentum_details['consecutive_up_days'] = up_days

            score = max(0.0, min(100.0, score))

            # 신뢰도 계산
            confidence = min(1.0, len(data) / 20 * 0.7 + 0.3)

            details = {
                "momentum_score": round(score, 1),
                **momentum_details
            }

            return ModuleScore(score, confidence, details, self.name)

        except Exception as e:
            logger.error(f"MomentumModule 계산 오류: {e}")
            return ModuleScore(0.0, 0.0, {"error": str(e)}, self.name)

    def get_required_columns(self) -> List[str]:
        return ['close', 'macd', 'macd_signal']


def test_basic_scoring_modules():
    """기본 Scoring Modules 테스트"""
    print("🧪 기본 Scoring Modules 테스트")
    print("=" * 70)

    # 테스트 데이터 생성 (간단한 상승 패턴)
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    np.random.seed(42)

    base_price = 1000
    prices = []
    volumes = []

    for i in range(100):
        # 상승 추세 + 노이즈
        trend = base_price * (1 + i * 0.01)  # 1% 일일 상승 추세
        noise = np.random.normal(0, 0.02)    # 2% 변동성
        price = trend * (1 + noise)
        prices.append(price)

        # 거래량 (가격 상승시 증가 패턴)
        volume = 1000000 * (1 + noise * 0.5) * (1 + i * 0.005)
        volumes.append(max(100000, volume))

    # DataFrame 생성
    test_data = pd.DataFrame({
        'date': dates,
        'close': prices,
        'high': [p * 1.02 for p in prices],
        'low': [p * 0.98 for p in prices],
        'open': [p * 1.01 for p in prices],
        'volume': volumes
    })

    # 간단한 기술적 지표 추가
    test_data['ma5'] = test_data['close'].rolling(5).mean()
    test_data['ma20'] = test_data['close'].rolling(20).mean()
    test_data['ma60'] = test_data['close'].rolling(60).mean()
    test_data['ma120'] = test_data['close'].rolling(120).mean() if len(test_data) >= 120 else test_data['close']
    test_data['ma200'] = test_data['close'].rolling(min(len(test_data), 200)).mean()

    # RSI 계산 (간단 버전)
    delta = test_data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    test_data['rsi'] = 100 - (100 / (1 + rs))

    # MACD 계산 (간단 버전)
    exp1 = test_data['close'].ewm(span=12).mean()
    exp2 = test_data['close'].ewm(span=26).mean()
    test_data['macd'] = exp1 - exp2
    test_data['macd_signal'] = test_data['macd'].ewm(span=9).mean()

    print(f"📊 테스트 데이터: {len(test_data)}일, 가격 범위 {test_data['close'].min():.0f}~{test_data['close'].max():.0f}")

    # 각 모듈 테스트
    modules = [
        # Macro Layer
        MarketRegimeModule(),
        VolumeProfileModule(),
        PriceActionModule(),

        # Structural Layer
        StageAnalysisModule(),
        MovingAverageModule(),
        RelativeStrengthModule(),

        # Micro Layer
        PatternRecognitionModule(),
        VolumeSpikeModule(),
        MomentumModule()
    ]

    config = {}

    print(f"\n🔍 모듈별 점수 테스트:")
    print("-" * 70)
    print(f"{'모듈명':<20} {'Layer':<12} {'점수':<8} {'신뢰도':<8} {'주요 정보'}")
    print("-" * 70)

    layer_totals = {LayerType.MACRO: 0, LayerType.STRUCTURAL: 0, LayerType.MICRO: 0}
    layer_weights = {LayerType.MACRO: 25, LayerType.STRUCTURAL: 45, LayerType.MICRO: 30}

    for module in modules:
        try:
            result = module.calculate_score(test_data, config)

            # Layer별 실제 점수 계산 (가중치 적용)
            actual_score = (result.score / 100.0) * module.weight * layer_weights[module.layer_type]
            layer_totals[module.layer_type] += actual_score

            key_info = list(result.details.keys())[:2] if result.details else []
            info_str = ', '.join(key_info)

            print(f"{module.name:<20} {module.layer_type.value:<12} {result.score:<8.1f} {result.confidence:<8.2f} {info_str}")

        except Exception as e:
            print(f"{module.name:<20} {module.layer_type.value:<12} ERROR   ERROR   {str(e)[:30]}")

    print("-" * 70)
    total_score = sum(layer_totals.values())
    print(f"\n📊 Layer별 점수 (최대 점수):")
    for layer_type, score in layer_totals.items():
        max_score = layer_weights[layer_type]
        print(f"   {layer_type.value:<12}: {score:.1f} / {max_score}")

    print(f"\n🎯 총 점수: {total_score:.1f} / 100")

    if total_score >= 80:
        recommendation = "STRONG_BUY"
    elif total_score >= 70:
        recommendation = "BUY"
    elif total_score >= 60:
        recommendation = "HOLD"
    else:
        recommendation = "AVOID"

    print(f"🎯 추천사항: {recommendation}")

    print("\n✅ 기본 Scoring Modules 테스트 완료!")


if __name__ == "__main__":
    test_basic_scoring_modules()