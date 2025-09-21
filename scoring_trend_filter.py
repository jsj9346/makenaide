#!/usr/bin/env python3
"""
점수제 추세 필터링 시스템 (Phase 1.5)
기존 이진 필터링을 점수제로 개선하여 더 유연하고 실용적인 필터링 구현

🎯 핵심 개념:
- 필수 조건 (Must-Have): 절대 타협할 수 없는 최소 조건
- 점수 조건 (Scoring): 각 항목별 가중치 기반 점수 계산
- 임계점 시스템 (Threshold): 일정 점수 이상만 통과
- 상대 평가 (Relative): 절대 기준보다 상대적 우위 중시
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import json

# 기존 시스템 임포트
from advanced_trend_analyzer import AdvancedTrendAnalyzer, TrendConfig

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ScoringResult:
    """점수제 분석 결과"""
    ticker: str

    # 필수 조건 (Pass/Fail)
    mandatory_passed: bool
    mandatory_reasons: List[str]

    # 점수 상세
    stage_score: float          # Stage 점수 (0-25)
    ma_alignment_score: float   # 이평선 정배열 점수 (0-20)
    rs_rating_score: float      # 상대강도 점수 (0-25)
    volume_score: float         # 거래량 점수 (0-15)
    momentum_score: float       # 모멘텀 점수 (0-15)

    # 총점 및 등급
    total_score: float          # 총점 (0-100)
    grade: str                  # 등급 (A+, A, B+, B, C+, C, D)
    percentile: float           # 상대 백분위 (0-100)

    # 최종 판정
    passed: bool                # 통과 여부
    recommendation: str         # BUY, HOLD, AVOID
    confidence: float           # 신뢰도 (0-1)

    # 상세 분석
    strengths: List[str]        # 강점 요소
    weaknesses: List[str]       # 약점 요소
    risk_factors: List[str]     # 리스크 요인


class ScoringConfig:
    """점수제 설정"""

    # 필수 조건 (하나라도 실패하면 탈락)
    MANDATORY_MIN_DATA_DAYS = 100      # 최소 100일 데이터
    MANDATORY_MIN_VOLUME = 1000000     # 최소 일일 거래량
    MANDATORY_MAX_PRICE = 1000000      # 최대 가격 (페니스톡 제외)

    # 점수 기준점
    STAGE_EXCELLENT = 4        # Stage 4 = 25점
    STAGE_GOOD = 3            # Stage 3 = 20점
    STAGE_FAIR = 2            # Stage 2 = 15점
    STAGE_POOR = 1            # Stage 1 = 5점

    RS_RATING_EXCELLENT = 90  # 90+ = 25점
    RS_RATING_GOOD = 80       # 80+ = 20점
    RS_RATING_FAIR = 70       # 70+ = 15점
    RS_RATING_POOR = 50       # 50+ = 10점

    # 통과 기준
    PASS_THRESHOLD = 60       # 60점 이상 통과
    BUY_THRESHOLD = 80        # 80점 이상 매수 추천
    STRONG_BUY_THRESHOLD = 90 # 90점 이상 강력 매수

    # 가중치
    STAGE_WEIGHT = 0.25       # 25%
    MA_WEIGHT = 0.20          # 20%
    RS_WEIGHT = 0.25          # 25%
    VOLUME_WEIGHT = 0.15      # 15%
    MOMENTUM_WEIGHT = 0.15    # 15%


class ScoringTrendFilter:
    """점수제 추세 필터링 시스템"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.config = ScoringConfig()
        self.analyzer = AdvancedTrendAnalyzer(db_path)

        # 시장 데이터 캐시 (상대 평가용)
        self.market_cache = {}

        logger.info("🚀 ScoringTrendFilter 초기화 완료")

    def analyze_ticker(self, ticker: str) -> ScoringResult:
        """ticker 점수제 분석"""
        logger.info(f"🔍 {ticker} 점수제 분석 시작")

        try:
            # 1. 데이터 로드
            df = self.analyzer._get_ohlcv_data(ticker)
            if df.empty:
                return self._create_failed_result(ticker, "데이터 없음")

            # 2. 필수 조건 검사
            mandatory_result = self._check_mandatory_conditions(ticker, df)
            if not mandatory_result['passed']:
                return self._create_failed_result(ticker, mandatory_result['reasons'])

            # 3. 개별 점수 계산
            stage_score = self._calculate_stage_score(ticker, df)
            ma_score = self._calculate_ma_alignment_score(ticker, df)
            rs_score = self._calculate_rs_rating_score(ticker, df)
            volume_score = self._calculate_volume_score(ticker, df)
            momentum_score = self._calculate_momentum_score(ticker, df)

            # 4. 총점 계산
            total_score = (
                stage_score * self.config.STAGE_WEIGHT +
                ma_score * self.config.MA_WEIGHT +
                rs_score * self.config.RS_WEIGHT +
                volume_score * self.config.VOLUME_WEIGHT +
                momentum_score * self.config.MOMENTUM_WEIGHT
            ) * 100  # 0-100 스케일로 변환

            # 5. 등급 및 백분위 계산
            grade = self._calculate_grade(total_score)
            percentile = self._calculate_percentile(ticker, total_score)

            # 6. 최종 판정
            passed = total_score >= self.config.PASS_THRESHOLD
            recommendation, confidence = self._make_recommendation(total_score)

            # 7. 강점/약점 분석
            strengths, weaknesses, risks = self._analyze_strengths_weaknesses(
                ticker, df, stage_score, ma_score, rs_score, volume_score, momentum_score
            )

            result = ScoringResult(
                ticker=ticker,
                mandatory_passed=True,
                mandatory_reasons=[],
                stage_score=stage_score * 25,  # 0-25 스케일
                ma_alignment_score=ma_score * 20,  # 0-20 스케일
                rs_rating_score=rs_score * 25,  # 0-25 스케일
                volume_score=volume_score * 15,  # 0-15 스케일
                momentum_score=momentum_score * 15,  # 0-15 스케일
                total_score=total_score,
                grade=grade,
                percentile=percentile,
                passed=passed,
                recommendation=recommendation,
                confidence=confidence,
                strengths=strengths,
                weaknesses=weaknesses,
                risk_factors=risks
            )

            logger.info(f"📊 {ticker} 점수: {total_score:.1f}점 ({grade}) - {recommendation}")
            return result

        except Exception as e:
            logger.error(f"❌ {ticker} 점수제 분석 실패: {e}")
            return self._create_failed_result(ticker, f"분석 오류: {str(e)}")

    def _check_mandatory_conditions(self, ticker: str, df: pd.DataFrame) -> Dict:
        """필수 조건 검사"""
        reasons = []

        # 1. 최소 데이터 기간
        if len(df) < self.config.MANDATORY_MIN_DATA_DAYS:
            reasons.append(f"데이터 부족 ({len(df)}일 < {self.config.MANDATORY_MIN_DATA_DAYS}일)")

        # 2. 최소 거래량
        avg_volume = df['volume'].mean()
        if avg_volume < self.config.MANDATORY_MIN_VOLUME:
            reasons.append(f"거래량 부족 (평균 {avg_volume:,.0f} < {self.config.MANDATORY_MIN_VOLUME:,})")

        # 3. 가격 범위 (페니스톡 제외)
        current_price = df['close'].iloc[-1]
        if current_price > self.config.MANDATORY_MAX_PRICE:
            reasons.append(f"가격 과도 ({current_price:,.0f}원 > {self.config.MANDATORY_MAX_PRICE:,}원)")

        return {
            'passed': len(reasons) == 0,
            'reasons': reasons
        }

    def _calculate_stage_score(self, ticker: str, df: pd.DataFrame) -> float:
        """Stage 점수 계산 (0.0 ~ 1.0)"""
        try:
            stage_result = self.analyzer.stage_analyzer.analyze_stage(df)
            stage = stage_result.stage
            confidence = stage_result.confidence

            # Stage별 기본 점수
            if stage == 4:
                base_score = 1.0
            elif stage == 3:
                base_score = 0.8
            elif stage == 2:
                base_score = 0.6
            else:  # Stage 1
                base_score = 0.2

            # 신뢰도로 보정
            final_score = base_score * confidence

            logger.debug(f"📈 {ticker} Stage 점수: {final_score:.3f} (Stage {stage}, 신뢰도 {confidence:.2f})")
            return final_score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Stage 점수 계산 실패: {e}")
            return 0.2  # 기본값

    def _calculate_ma_alignment_score(self, ticker: str, df: pd.DataFrame) -> float:
        """이평선 정배열 점수 계산 (0.0 ~ 1.0)"""
        try:
            # 이평선 계산
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            ma20 = df['close'].rolling(20).mean().iloc[-1]
            ma60 = df['close'].rolling(60).mean().iloc[-1]
            ma120 = df['close'].rolling(120).mean().iloc[-1]
            ma200 = df['ma200'].iloc[-1] if pd.notna(df['ma200'].iloc[-1]) else df['close'].rolling(200).mean().iloc[-1]

            current_price = df['close'].iloc[-1]

            # 정배열 조건 체크
            alignments = []
            alignments.append(current_price > ma5)      # 현재가 > 5일선
            alignments.append(ma5 > ma20)               # 5일선 > 20일선
            alignments.append(ma20 > ma60)              # 20일선 > 60일선
            alignments.append(ma60 > ma120)             # 60일선 > 120일선
            alignments.append(ma120 > ma200)            # 120일선 > 200일선

            # 정배열 비율로 점수 계산
            alignment_ratio = sum(alignments) / len(alignments)

            # 이평선 기울기도 고려
            ma20_slope = (ma20 - df['close'].rolling(20).mean().iloc[-10]) / ma20 * 100
            ma200_slope = (ma200 - df['close'].rolling(200).mean().iloc[-20]) / ma200 * 100 if pd.notna(ma200) else 0

            slope_bonus = 0
            if ma20_slope > 2:  # 2% 이상 상승
                slope_bonus += 0.1
            if ma200_slope > 1:  # 1% 이상 상승
                slope_bonus += 0.1

            final_score = min(1.0, alignment_ratio + slope_bonus)

            logger.debug(f"📊 {ticker} MA 정배열 점수: {final_score:.3f} (정배열 {sum(alignments)}/5)")
            return final_score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} MA 정배열 점수 계산 실패: {e}")
            return 0.2

    def _calculate_rs_rating_score(self, ticker: str, df: pd.DataFrame) -> float:
        """상대강도 점수 계산 (0.0 ~ 1.0)"""
        try:
            rs_result = self.analyzer.rs_calculator.calculate_rs_rating(ticker, df)
            rs_rating = rs_result.rs_rating

            # RS Rating 기반 점수 계산
            if rs_rating >= 90:
                score = 1.0
            elif rs_rating >= 80:
                score = 0.8
            elif rs_rating >= 70:
                score = 0.6
            elif rs_rating >= 50:
                score = 0.4
            else:
                score = 0.1

            # 52주 고점 근접도 보너스
            if rs_result.high_52w_proximity:
                score = min(1.0, score + 0.1)

            logger.debug(f"📊 {ticker} RS Rating 점수: {score:.3f} (RS {rs_rating})")
            return score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} RS Rating 점수 계산 실패: {e}")
            return 0.3

    def _calculate_volume_score(self, ticker: str, df: pd.DataFrame) -> float:
        """거래량 점수 계산 (0.0 ~ 1.0)"""
        try:
            volume_result = self.analyzer.volume_analyzer.analyze_volume_pattern(df)

            score = 0.3  # 기본 점수

            # VDU (Volume Dry-Up) 감지
            if volume_result.is_vdu:
                score += 0.3

            # Volume Spike 감지
            if volume_result.is_spike:
                score += 0.4

            # 최근 거래량 추세
            recent_volume = df['volume'].tail(5).mean()
            past_volume = df['volume'].tail(20).head(15).mean()

            if recent_volume > past_volume * 1.2:  # 20% 증가
                score += 0.2

            final_score = min(1.0, score)

            logger.debug(f"📊 {ticker} Volume 점수: {final_score:.3f} (VDU:{volume_result.is_vdu}, Spike:{volume_result.is_spike})")
            return final_score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Volume 점수 계산 실패: {e}")
            return 0.3

    def _calculate_momentum_score(self, ticker: str, df: pd.DataFrame) -> float:
        """모멘텀 점수 계산 (0.0 ~ 1.0)"""
        try:
            # RSI 모멘텀
            current_rsi = df['rsi'].iloc[-1] if pd.notna(df['rsi'].iloc[-1]) else 50
            rsi_score = 0

            if 60 <= current_rsi <= 80:  # 적정 상승 모멘텀
                rsi_score = 0.4
            elif 50 <= current_rsi < 60:  # 중립적 모멘텀
                rsi_score = 0.3
            elif current_rsi > 80:  # 과매수 (리스크)
                rsi_score = 0.2
            else:  # 약세 모멘텀
                rsi_score = 0.1

            # 가격 모멘텀 (최근 상승률)
            price_change_5d = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100
            price_change_20d = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100

            price_score = 0
            if price_change_5d > 5:  # 5일간 5% 상승
                price_score += 0.3
            elif price_change_5d > 0:
                price_score += 0.1

            if price_change_20d > 10:  # 20일간 10% 상승
                price_score += 0.3
            elif price_change_20d > 0:
                price_score += 0.1

            final_score = min(1.0, rsi_score + price_score)

            logger.debug(f"📊 {ticker} Momentum 점수: {final_score:.3f} (RSI:{current_rsi:.1f}, 5d:{price_change_5d:.1f}%)")
            return final_score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Momentum 점수 계산 실패: {e}")
            return 0.3

    def _calculate_grade(self, score: float) -> str:
        """점수를 등급으로 변환"""
        if score >= 95:
            return "A+"
        elif score >= 90:
            return "A"
        elif score >= 85:
            return "B+"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C+"
        elif score >= 60:
            return "C"
        else:
            return "D"

    def _calculate_percentile(self, ticker: str, score: float) -> float:
        """상대 백분위 계산 (임시로 고정값 반환)"""
        # 실제로는 전체 시장 대비 상대 백분위를 계산해야 함
        # 현재는 점수 기반 근사치 반환
        return min(100.0, score)

    def _make_recommendation(self, score: float) -> Tuple[str, float]:
        """점수 기반 추천사항 및 신뢰도 계산"""
        if score >= self.config.STRONG_BUY_THRESHOLD:
            return "STRONG_BUY", 0.9
        elif score >= self.config.BUY_THRESHOLD:
            return "BUY", 0.8
        elif score >= self.config.PASS_THRESHOLD:
            return "HOLD", 0.6
        else:
            return "AVOID", 0.4

    def _analyze_strengths_weaknesses(self, ticker: str, df: pd.DataFrame,
                                    stage_score: float, ma_score: float,
                                    rs_score: float, volume_score: float,
                                    momentum_score: float) -> Tuple[List[str], List[str], List[str]]:
        """강점/약점/리스크 분석"""
        strengths = []
        weaknesses = []
        risks = []

        # 강점 분석
        if stage_score > 0.8:
            strengths.append("강력한 Stage 4 상승 추세")
        if ma_score > 0.8:
            strengths.append("완벽한 이평선 정배열")
        if rs_score > 0.8:
            strengths.append("시장 대비 우수한 상대강도")
        if volume_score > 0.7:
            strengths.append("건전한 거래량 패턴")
        if momentum_score > 0.7:
            strengths.append("강력한 상승 모멘텀")

        # 약점 분석
        if stage_score < 0.4:
            weaknesses.append("Stage 1 기반 구축 단계")
        if ma_score < 0.4:
            weaknesses.append("이평선 정배열 미흡")
        if rs_score < 0.4:
            weaknesses.append("시장 대비 상대적 약세")
        if volume_score < 0.4:
            weaknesses.append("거래량 패턴 부족")
        if momentum_score < 0.4:
            weaknesses.append("상승 모멘텀 약화")

        # 리스크 분석
        current_rsi = df['rsi'].iloc[-1] if pd.notna(df['rsi'].iloc[-1]) else 50
        if current_rsi > 80:
            risks.append("RSI 과매수 구간")

        price_change_5d = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100
        if price_change_5d > 15:
            risks.append("단기 급등 후 조정 리스크")

        return strengths, weaknesses, risks

    def _create_failed_result(self, ticker: str, reasons: Union[str, List[str]]) -> ScoringResult:
        """실패 결과 생성"""
        if isinstance(reasons, str):
            reasons = [reasons]

        return ScoringResult(
            ticker=ticker,
            mandatory_passed=False,
            mandatory_reasons=reasons,
            stage_score=0.0,
            ma_alignment_score=0.0,
            rs_rating_score=0.0,
            volume_score=0.0,
            momentum_score=0.0,
            total_score=0.0,
            grade="F",
            percentile=0.0,
            passed=False,
            recommendation="AVOID",
            confidence=0.9,
            strengths=[],
            weaknesses=reasons,
            risk_factors=[]
        )

    def analyze_multiple_tickers(self, tickers: List[str]) -> List[ScoringResult]:
        """여러 ticker 점수제 분석"""
        logger.info(f"🚀 {len(tickers)}개 ticker 점수제 일괄 분석")

        results = []
        for i, ticker in enumerate(tickers, 1):
            try:
                result = self.analyze_ticker(ticker)
                results.append(result)

                status = "✅ 통과" if result.passed else "❌ 탈락"
                logger.info(f"[{i}/{len(tickers)}] {ticker}: {result.total_score:.1f}점 ({result.grade}) {status}")

            except Exception as e:
                logger.error(f"❌ [{i}/{len(tickers)}] {ticker} 분석 실패: {e}")

        # 통과 종목 요약
        passed_results = [r for r in results if r.passed]
        logger.info(f"📊 분석 결과: {len(passed_results)}/{len(results)}개 종목 통과 ({len(passed_results)/len(results)*100:.1f}%)")

        return results


def test_scoring_filter():
    """점수제 필터 테스트"""
    print("🧪 점수제 추세 필터링 시스템 테스트")
    print("=" * 60)

    filter_system = ScoringTrendFilter()
    test_tickers = ['KRW-1INCH', 'KRW-ADA', 'KRW-AAVE', 'KRW-SOL', 'KRW-DOGE']

    results = filter_system.analyze_multiple_tickers(test_tickers)

    print(f"\n📊 상세 결과:")
    print("-" * 80)
    print(f"{'종목':<12} {'총점':<8} {'등급':<6} {'추천':<12} {'Stage':<8} {'MA':<8} {'RS':<8} {'Vol':<8} {'Mom':<8}")
    print("-" * 80)

    for result in results:
        if result.passed or result.total_score > 40:  # 통과 종목 또는 40점 이상만 표시
            print(f"{result.ticker:<12} "
                  f"{result.total_score:<8.1f} "
                  f"{result.grade:<6} "
                  f"{result.recommendation:<12} "
                  f"{result.stage_score:<8.1f} "
                  f"{result.ma_alignment_score:<8.1f} "
                  f"{result.rs_rating_score:<8.1f} "
                  f"{result.volume_score:<8.1f} "
                  f"{result.momentum_score:<8.1f}")

            if result.strengths:
                print(f"  💪 강점: {', '.join(result.strengths)}")
            if result.weaknesses:
                print(f"  ⚠️ 약점: {', '.join(result.weaknesses)}")
            if result.risk_factors:
                print(f"  🚨 리스크: {', '.join(result.risk_factors)}")
            print()

    # 통계 요약
    passed_count = sum(1 for r in results if r.passed)
    avg_score = sum(r.total_score for r in results) / len(results)

    print(f"📈 종합 통계:")
    print(f"   통과율: {passed_count}/{len(results)} ({passed_count/len(results)*100:.1f}%)")
    print(f"   평균 점수: {avg_score:.1f}점")
    print(f"   최고 점수: {max(r.total_score for r in results):.1f}점")


if __name__ == "__main__":
    test_scoring_filter()