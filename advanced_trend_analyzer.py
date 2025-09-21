#!/usr/bin/env python3
"""
Advanced Trend Analyzer - Phase 1
통합 전략 기반 고도화된 추세 필터링 시스템

🎯 핵심 기능:
- 와인스타인-오닐-미너비니 통합 전략 구현
- 3단계 계층적 필터링 (거시적 → 구조적 → 미시적)
- 정교한 주도주 포착 및 타이밍 최적화
- 체계적 리스크 관리

📊 구현 컴포넌트:
1. StageAnalyzer: 정교한 4-Stage 분석 + 이평선 정배열
2. RelativeStrengthCalculator: RS Rating 80+ 주도주 필터링
3. VolumeAnalyzer: VDU + 돌파 거래량 폭증 감지
4. PatternDetector: VCP/Cup&Handle 패턴 인식 (Phase 2)
5. RiskManager: 체계적 손절 및 매도 신호 (Phase 3)
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import json

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# 데이터 클래스 정의
# ============================================================================

@dataclass
class TradingSignal:
    """통합 거래 신호"""
    ticker: str
    action: str  # BUY, HOLD, AVOID, SELL
    confidence: float  # 0.0 ~ 1.0
    stage: int  # 1, 2, 3, 4
    pattern_type: str  # VCP, CUP_HANDLE, BREAKOUT, NONE
    rs_rating: int  # 0 ~ 100
    volume_pattern: str  # VDU, SPIKE, NORMAL
    entry_price: float
    stop_loss: float
    risk_reward_ratio: float
    reasons: List[str]
    quality_score: float

    @classmethod
    def strong_buy(cls, ticker: str, **kwargs) -> 'TradingSignal':
        return cls(
            ticker=ticker, action="BUY", confidence=0.9,
            quality_score=kwargs.get('quality_score', 20.0),
            reasons=kwargs.get('reasons', []), **kwargs
        )

    @classmethod
    def reject(cls, ticker: str, reason: str) -> 'TradingSignal':
        return cls(
            ticker=ticker, action="AVOID", confidence=0.0,
            stage=1, pattern_type="NONE", rs_rating=0,
            volume_pattern="NORMAL", entry_price=0.0, stop_loss=0.0,
            risk_reward_ratio=0.0, reasons=[reason], quality_score=0.0
        )

@dataclass
class StageResult:
    """Stage 분석 결과"""
    stage: int
    confidence: float
    ma_alignment: bool  # 이평선 정배열 여부
    ma_slopes: Dict[str, float]  # 이평선 기울기들
    high_52w_proximity: float  # 52주 고점 근접도 (0-1)
    trend_strength: float  # 추세 강도

@dataclass
class RSResult:
    """상대강도 분석 결과"""
    rs_rating: int  # 0-100
    year_return: float  # 1년 수익률
    market_percentile: float  # 시장 대비 percentile
    high_52w_proximity: bool  # 52주 고점 75% 이내 여부

@dataclass
class VolumeResult:
    """거래량 분석 결과"""
    is_vdu: bool  # Volume Dry-Up 여부
    is_spike: bool  # 거래량 폭증 여부
    vdu_ratio: float  # VDU 비율
    spike_ratio: float  # 폭증 비율
    pattern: str  # VDU, SPIKE, NORMAL

# ============================================================================
# 설정 클래스
# ============================================================================

class TrendConfig:
    """설정 가능한 매개변수"""

    # Stage 분석 설정
    MA_SLOPE_PERIOD: int = 20
    MA_SLOPE_THRESHOLD: float = 1.0  # 1% 이상 상승
    HIGH_52W_THRESHOLD: float = 0.75  # 75% 이내

    # 패턴 감지 설정
    VCP_MIN_CONTRACTIONS: int = 2
    VCP_MAX_CONTRACTIONS: int = 6
    VCP_FINAL_DEPTH_MAX: float = 10.0
    PRIOR_TREND_MIN: float = 30.0

    # 거래량 설정
    VDU_THRESHOLD: float = 0.8  # 80% 이하
    VOLUME_SPIKE_THRESHOLD: float = 1.5  # 150% 이상

    # RS Rating 설정
    RS_RATING_MIN: int = 80  # 상위 20%

    # 리스크 관리
    ONEIL_STOP_LOSS: float = 0.07  # 7%
    TRAILING_STOP: float = 0.15  # 15%

# ============================================================================
# 메인 분석기 클래스
# ============================================================================

class AdvancedTrendAnalyzer:
    """와인스타인-오닐-미너비니 통합 전략 분석기"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.config = TrendConfig()

        # 컴포넌트 초기화
        self.stage_analyzer = StageAnalyzer(db_path)
        self.rs_calculator = RelativeStrengthCalculator(db_path)
        self.volume_analyzer = VolumeAnalyzer()
        # self.pattern_detector = PatternDetector()  # Phase 2에서 구현
        # self.risk_manager = RiskManager()  # Phase 3에서 구현

        self.min_data_points = 200

        logger.info("🚀 AdvancedTrendAnalyzer 초기화 완료")

    def analyze_ticker(self, ticker: str) -> TradingSignal:
        """3단계 계층적 분석 실행"""
        logger.info(f"🔍 {ticker} 고도화 분석 시작")

        try:
            # 데이터 로드
            df = self._get_ohlcv_data(ticker)
            if df.empty or len(df) < self.min_data_points:
                return TradingSignal.reject(ticker, f"데이터 부족 ({len(df)}개)")

            # Stage 1: 거시적 필터
            macro_result = self._macro_filter(ticker, df)
            if not macro_result['passed']:
                return TradingSignal.reject(ticker, macro_result['reason'])

            # Stage 2: 구조적 필터 (Phase 1에서는 기본 구현)
            structural_result = self._structural_filter(ticker, df, macro_result)
            if not structural_result['passed']:
                return TradingSignal.reject(ticker, structural_result['reason'])

            # Stage 3: 미시적 트리거
            trigger_result = self._trigger_filter(ticker, df, structural_result)

            # 최종 신호 생성
            return self._generate_trading_signal(ticker, df, macro_result,
                                                structural_result, trigger_result)

        except Exception as e:
            logger.error(f"❌ {ticker} 분석 실패: {e}")
            return TradingSignal.reject(ticker, f"분석 오류: {str(e)}")

    def _get_ohlcv_data(self, ticker: str, days: int = 250) -> pd.DataFrame:
        """SQLite에서 OHLCV 데이터 조회"""
        try:
            conn = sqlite3.connect(self.db_path)

            query = """
            SELECT ticker, date, open, high, low, close, volume,
                   ma5, ma20, ma60, ma120, ma200, rsi, volume_ratio
            FROM ohlcv_data
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """

            df = pd.read_sql_query(query, conn, params=(ticker, days))
            conn.close()

            if df.empty:
                return pd.DataFrame()

            # 날짜 순으로 정렬 (오래된 것부터)
            df = df.sort_values('date').reset_index(drop=True)
            df['date'] = pd.to_datetime(df['date'])

            return df

        except Exception as e:
            logger.error(f"❌ {ticker} 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def _macro_filter(self, ticker: str, df: pd.DataFrame) -> Dict:
        """Stage 1: 거시적 필터 (시장 국면 + 주도주 스크리닝)"""

        # 1. Stage 분석
        stage_result = self.stage_analyzer.analyze_stage(df)
        if stage_result.stage != 2:
            return {
                'passed': False,
                'reason': f"Stage {stage_result.stage} (Stage 2 필요)",
                'stage_result': stage_result
            }

        # 2. 이평선 정배열 확인
        if not stage_result.ma_alignment:
            return {
                'passed': False,
                'reason': "이평선 정배열 실패",
                'stage_result': stage_result
            }

        # 3. 상대강도 확인
        rs_result = self.rs_calculator.calculate_rs_rating(ticker, df)
        if rs_result.rs_rating < self.config.RS_RATING_MIN:
            return {
                'passed': False,
                'reason': f"RS Rating 부족 ({rs_result.rs_rating} < {self.config.RS_RATING_MIN})",
                'stage_result': stage_result,
                'rs_result': rs_result
            }

        # 4. 52주 고점 근접성 확인
        if not rs_result.high_52w_proximity:
            return {
                'passed': False,
                'reason': "52주 고점에서 너무 멀음 (75% 이내 필요)",
                'stage_result': stage_result,
                'rs_result': rs_result
            }

        return {
            'passed': True,
            'reason': "거시적 필터 통과",
            'stage_result': stage_result,
            'rs_result': rs_result
        }

    def _structural_filter(self, ticker: str, df: pd.DataFrame, macro_result: Dict) -> Dict:
        """Stage 2: 구조적 필터 (베이스 패턴 + 추세 강도)"""

        # Phase 1에서는 기본적인 구조적 필터만 구현
        # Phase 2에서 VCP/Cup&Handle 패턴 감지 추가 예정

        # 1. 사전 상승 추세 확인 (30% 이상)
        if len(df) >= 60:  # 3개월 데이터
            price_60days_ago = df['close'].iloc[-60]
            current_price = df['close'].iloc[-1]
            prior_return = ((current_price - price_60days_ago) / price_60days_ago) * 100

            if prior_return < self.config.PRIOR_TREND_MIN:
                return {
                    'passed': False,
                    'reason': f"사전 상승 부족 ({prior_return:.1f}% < {self.config.PRIOR_TREND_MIN}%)",
                    'prior_return': prior_return
                }

        # 2. 거래량 패턴 분석
        volume_result = self.volume_analyzer.analyze_volume_pattern(df)

        return {
            'passed': True,
            'reason': "구조적 필터 통과",
            'volume_result': volume_result,
            'prior_return': prior_return if 'prior_return' in locals() else 0.0
        }

    def _trigger_filter(self, ticker: str, df: pd.DataFrame, structural_result: Dict) -> Dict:
        """Stage 3: 미시적 트리거 (거래량 + 돌파 조건)"""

        volume_result = structural_result['volume_result']

        # 1. 거래량 급감(VDU) 후 폭증 패턴 확인
        if not volume_result.is_spike:
            return {
                'passed': False,
                'reason': f"거래량 폭증 부족 ({volume_result.spike_ratio:.1f}x < {self.config.VOLUME_SPIKE_THRESHOLD}x)",
                'volume_result': volume_result
            }

        # 2. 피벗 포인트 돌파 확인 (간단한 버전)
        # Phase 2에서 정교한 패턴 기반 피벗 포인트 계산 예정
        latest = df.iloc[-1]
        ma200 = latest['ma200']
        current_price = latest['close']

        if pd.isna(ma200) or current_price <= ma200:
            return {
                'passed': False,
                'reason': "MA200 돌파 부족",
                'volume_result': volume_result
            }

        # 3. 돌파 강도 확인
        breakout_pct = ((current_price - ma200) / ma200) * 100
        if breakout_pct < 2.0:  # 최소 2% 돌파
            return {
                'passed': False,
                'reason': f"돌파 강도 부족 ({breakout_pct:.1f}% < 2.0%)",
                'volume_result': volume_result
            }

        return {
            'passed': True,
            'reason': "미시적 트리거 통과",
            'volume_result': volume_result,
            'breakout_pct': breakout_pct
        }

    def _generate_trading_signal(self, ticker: str, df: pd.DataFrame,
                                macro_result: Dict, structural_result: Dict,
                                trigger_result: Dict) -> TradingSignal:
        """최종 거래 신호 생성"""

        latest = df.iloc[-1]
        current_price = latest['close']
        stage_result = macro_result['stage_result']
        rs_result = macro_result['rs_result']
        volume_result = trigger_result['volume_result']

        # 신뢰도 계산
        confidence = 0.7  # 기본 신뢰도
        confidence += stage_result.confidence * 0.2
        confidence += (rs_result.rs_rating / 100) * 0.1
        confidence = min(confidence, 1.0)

        # 품질 점수 계산 (0-25점)
        quality_score = self._calculate_enhanced_quality_score(
            stage_result, rs_result, volume_result, df
        )

        # 손절가 계산 (Phase 3에서 정교화 예정)
        stop_loss = current_price * (1 - self.config.ONEIL_STOP_LOSS)

        # 리스크 리워드 비율 (단순화)
        risk_reward_ratio = 3.0  # 기본 3:1

        # 매수 추천 여부 결정
        if trigger_result['passed'] and quality_score >= 15.0:
            action = "BUY"
            pattern_type = "BREAKOUT"  # Phase 2에서 정교한 패턴 분류 예정
        else:
            action = "HOLD"
            pattern_type = "INCOMPLETE"

        reasons = [
            f"Stage {stage_result.stage}",
            f"RS Rating {rs_result.rs_rating}",
            f"거래량 {volume_result.pattern}",
            macro_result['reason'],
            structural_result['reason'],
            trigger_result['reason']
        ]

        return TradingSignal(
            ticker=ticker,
            action=action,
            confidence=confidence,
            stage=stage_result.stage,
            pattern_type=pattern_type,
            rs_rating=rs_result.rs_rating,
            volume_pattern=volume_result.pattern,
            entry_price=current_price,
            stop_loss=stop_loss,
            risk_reward_ratio=risk_reward_ratio,
            reasons=reasons,
            quality_score=quality_score
        )

    def _calculate_enhanced_quality_score(self, stage_result: StageResult,
                                         rs_result: RSResult, volume_result: VolumeResult,
                                         df: pd.DataFrame) -> float:
        """향상된 품질 점수 계산 (0-25점)"""

        score = 0.0

        # 1. Stage 신뢰도 (0-5점)
        score += stage_result.confidence * 5

        # 2. 이평선 정배열 보너스 (0-3점)
        if stage_result.ma_alignment:
            score += 3.0

        # 3. RS Rating (0-5점)
        score += min(5.0, (rs_result.rs_rating / 100) * 5)

        # 4. 거래량 패턴 (0-4점)
        if volume_result.is_spike:
            score += 2.0
        if volume_result.is_vdu:
            score += 2.0

        # 5. 추세 강도 (0-3점)
        score += min(3.0, stage_result.trend_strength * 3)

        # 6. 52주 고점 근접도 (0-3점)
        score += stage_result.high_52w_proximity * 3

        # 7. 기술적 지표 보너스 (0-2점)
        if not df.empty:
            latest = df.iloc[-1]
            if 'rsi' in df.columns and pd.notna(latest['rsi']):
                rsi = latest['rsi']
                if 40 <= rsi <= 70:
                    score += 2.0
                elif 30 <= rsi <= 80:
                    score += 1.0

        return round(score, 1)

    def analyze_all_tickers(self) -> List[TradingSignal]:
        """전체 종목 분석"""
        tickers = self._get_active_tickers()
        signals = []

        logger.info(f"🚀 {len(tickers)}개 종목 고도화 분석 시작")

        for ticker in tickers:
            signal = self.analyze_ticker(ticker)
            signals.append(signal)

            if signal.action == "BUY":
                logger.info(f"✅ {ticker}: BUY 신호 (품질: {signal.quality_score:.1f}점)")

        return signals

    def _get_active_tickers(self) -> List[str]:
        """활성 종목 목록 조회"""
        try:
            conn = sqlite3.connect(self.db_path)

            query = """
            SELECT DISTINCT ticker
            FROM ohlcv_data
            WHERE date >= date('now', '-30 days')
            ORDER BY ticker
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            return df['ticker'].tolist()

        except Exception as e:
            logger.error(f"❌ 활성 종목 조회 실패: {e}")
            return []

# ============================================================================
# Phase 1 컴포넌트 클래스들
# ============================================================================

class StageAnalyzer:
    """정교한 4-Stage 국면 분석 + 이평선 정배열"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.config = TrendConfig()

    def analyze_stage(self, df: pd.DataFrame) -> StageResult:
        """다차원 Stage 분석"""

        # 1. 이평선 정배열 확인
        ma_alignment = self._check_ma_alignment(df)

        # 2. SMA 기울기 정량화 (20일간)
        ma_slopes = self._calculate_ma_slopes(df)

        # 3. 52주 고점 근접성
        high_52w_proximity = self._check_52w_proximity(df)

        # 4. 추세 강도 계산
        trend_strength = self._calculate_trend_strength(df, ma_slopes)

        # 5. 종합 Stage 판정
        stage, confidence = self._determine_stage(df, ma_alignment, ma_slopes, high_52w_proximity)

        return StageResult(
            stage=stage,
            confidence=confidence,
            ma_alignment=ma_alignment,
            ma_slopes=ma_slopes,
            high_52w_proximity=high_52w_proximity,
            trend_strength=trend_strength
        )

    def _check_ma_alignment(self, df: pd.DataFrame) -> bool:
        """이평선 정배열 확인: 50일 > 150일 > 200일"""
        if df.empty:
            return False

        latest = df.iloc[-1]

        # ma20를 ma50으로, ma120을 ma150으로 근사 사용
        # Phase 2에서 정확한 MA50, MA150 추가 예정
        ma50 = latest.get('ma20', np.nan)  # 임시로 ma20 사용
        ma150 = latest.get('ma120', np.nan)  # 임시로 ma120 사용
        ma200 = latest.get('ma200', np.nan)

        if any(pd.isna([ma50, ma150, ma200])):
            return False

        # 정배열: 단기 > 중기 > 장기
        return ma50 > ma150 > ma200

    def _calculate_ma_slopes(self, df: pd.DataFrame, period: int = 20) -> Dict[str, float]:
        """이동평균선 기울기 정량화"""
        slopes = {}

        if len(df) < period:
            return {ma: 0.0 for ma in ['ma20', 'ma120', 'ma200']}

        for ma in ['ma20', 'ma120', 'ma200']:
            if ma in df.columns:
                current_ma = df[ma].iloc[-1]
                past_ma = df[ma].iloc[-period]

                if pd.notna(current_ma) and pd.notna(past_ma) and past_ma > 0:
                    slopes[ma] = ((current_ma / past_ma - 1) * 100)
                else:
                    slopes[ma] = 0.0
            else:
                slopes[ma] = 0.0

        return slopes

    def _check_52w_proximity(self, df: pd.DataFrame, threshold: float = 0.75) -> float:
        """52주 고점 근접도 계산 (0-1)"""
        if len(df) < 252:
            weeks_available = len(df) // 5  # 주 단위로 근사
            if weeks_available < 26:  # 최소 6개월
                return 0.0
            high_period = weeks_available * 5
        else:
            high_period = 252

        high_52w = df['high'].tail(high_period).max()
        current_price = df['close'].iloc[-1]

        if pd.isna(high_52w) or high_52w <= 0:
            return 0.0

        proximity = current_price / high_52w
        return min(1.0, proximity)

    def _calculate_trend_strength(self, df: pd.DataFrame, ma_slopes: Dict[str, float]) -> float:
        """추세 강도 계산 (0-1)"""

        # 1. MA 기울기 점수
        slope_score = 0.0
        for ma, slope in ma_slopes.items():
            if slope > self.config.MA_SLOPE_THRESHOLD:
                slope_score += 0.33

        # 2. 가격 모멘텀 점수
        momentum_score = 0.0
        if len(df) >= 20:
            price_20days_ago = df['close'].iloc[-20]
            current_price = df['close'].iloc[-1]

            if pd.notna(price_20days_ago) and price_20days_ago > 0:
                momentum = ((current_price / price_20days_ago - 1) * 100)
                if momentum > 5.0:  # 5% 이상 상승
                    momentum_score = min(1.0, momentum / 20.0)  # 20% 상승 시 만점

        return min(1.0, (slope_score + momentum_score) / 2)

    def _determine_stage(self, df: pd.DataFrame, ma_alignment: bool,
                        ma_slopes: Dict[str, float], high_52w_proximity: float) -> Tuple[int, float]:
        """종합 Stage 판정"""

        if df.empty:
            return 1, 0.0

        latest = df.iloc[-1]
        current_price = latest['close']
        ma200 = latest.get('ma200', np.nan)

        # Stage 2 조건들
        stage2_conditions = []

        # 1. MA200 위에 위치
        if pd.notna(ma200) and current_price > ma200:
            stage2_conditions.append(True)
        else:
            stage2_conditions.append(False)

        # 2. 이평선 정배열
        stage2_conditions.append(ma_alignment)

        # 3. MA200 상승 추세
        ma200_slope = ma_slopes.get('ma200', 0.0)
        stage2_conditions.append(ma200_slope > self.config.MA_SLOPE_THRESHOLD)

        # 4. 52주 고점 근접성
        stage2_conditions.append(high_52w_proximity >= self.config.HIGH_52W_THRESHOLD)

        # Stage 2 신뢰도 계산
        stage2_score = sum(stage2_conditions) / len(stage2_conditions)

        if stage2_score >= 0.75:  # 75% 이상 조건 만족
            return 2, stage2_score
        elif pd.notna(ma200) and current_price < ma200 and ma200_slope < -self.config.MA_SLOPE_THRESHOLD:
            return 4, 0.7  # 하락 추세
        elif pd.notna(ma200) and abs(current_price - ma200) / ma200 < 0.05 and abs(ma200_slope) < 0.5:
            return 3, 0.5  # 횡보
        else:
            return 1, 0.4  # 기본값


class RelativeStrengthCalculator:
    """시장 대비 상대 강도 계산"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.config = TrendConfig()

    def calculate_rs_rating(self, ticker: str, df: pd.DataFrame) -> RSResult:
        """IBD 스타일 RS Rating 계산"""

        # 1. 1년간 수익률 계산
        year_return = self._calculate_return(df, period=252)

        # 2. 전체 시장 데이터와 비교하여 percentile 계산
        market_percentile = self._get_market_percentile(ticker, year_return)

        # 3. RS Rating = percentile을 100점 만점으로 변환
        rs_rating = int(market_percentile)

        # 4. 52주 고점 근접성 확인
        high_52w_proximity = self._check_52w_proximity(df, threshold=self.config.HIGH_52W_THRESHOLD)

        return RSResult(
            rs_rating=rs_rating,
            year_return=year_return,
            market_percentile=market_percentile,
            high_52w_proximity=high_52w_proximity
        )

    def _calculate_return(self, df: pd.DataFrame, period: int = 252) -> float:
        """수익률 계산"""
        if len(df) < period:
            period = len(df) - 1

        if period <= 0:
            return 0.0

        start_price = df['close'].iloc[-period]
        end_price = df['close'].iloc[-1]

        if pd.isna(start_price) or pd.isna(end_price) or start_price <= 0:
            return 0.0

        return ((end_price / start_price - 1) * 100)

    def _get_market_percentile(self, ticker: str, return_value: float) -> float:
        """전체 시장 대비 percentile 계산"""
        try:
            # SQLite에서 모든 ticker의 1년 수익률 조회
            all_returns = self._query_all_ticker_returns()

            if not all_returns or len(all_returns) < 10:
                return 50.0  # 데이터 부족 시 중간값

            # percentile 계산
            percentile = (np.sum(all_returns <= return_value) / len(all_returns)) * 100
            return min(100.0, max(0.0, percentile))

        except Exception as e:
            logger.warning(f"⚠️ {ticker} 시장 percentile 계산 실패: {e}")
            return 50.0

    def _query_all_ticker_returns(self) -> List[float]:
        """모든 ticker의 1년 수익률 조회"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 1년 전 날짜와 최근 날짜의 가격 비교
            query = """
            WITH price_comparison AS (
                SELECT
                    ticker,
                    FIRST_VALUE(close) OVER (PARTITION BY ticker ORDER BY date ASC) as start_price,
                    LAST_VALUE(close) OVER (PARTITION BY ticker ORDER BY date ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_price
                FROM ohlcv_data
                WHERE date >= date('now', '-400 days')  -- 여유있게 400일
                    AND date <= date('now')
                    AND close IS NOT NULL
                    AND close > 0
            )
            SELECT DISTINCT
                ticker,
                ((end_price / start_price - 1) * 100) as year_return
            FROM price_comparison
            WHERE start_price > 0 AND end_price > 0
                AND year_return BETWEEN -95 AND 1000  -- 극단값 제거
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            return df['year_return'].tolist()

        except Exception as e:
            logger.error(f"❌ 전체 ticker 수익률 조회 실패: {e}")
            return []

    def _check_52w_proximity(self, df: pd.DataFrame, threshold: float = 0.75) -> bool:
        """52주 고점 대비 75% 이내 위치 확인"""
        if len(df) < 200:  # 최소 200일 데이터
            return False

        period = min(252, len(df))
        high_52w = df['high'].tail(period).max()
        current_price = df['close'].iloc[-1]

        if pd.isna(high_52w) or high_52w <= 0:
            return False

        return current_price >= (high_52w * threshold)


class VolumeAnalyzer:
    """정교한 거래량 패턴 분석"""

    def __init__(self):
        self.config = TrendConfig()

    def analyze_volume_pattern(self, df: pd.DataFrame) -> VolumeResult:
        """VDU + 돌파 거래량 종합 분석"""

        if df.empty or len(df) < 50:
            return VolumeResult(
                is_vdu=False, is_spike=False,
                vdu_ratio=1.0, spike_ratio=1.0,
                pattern="INSUFFICIENT_DATA"
            )

        # 1. 거래량 급감(VDU) 감지
        is_vdu, vdu_ratio = self._detect_volume_dry_up(df)

        # 2. 돌파 거래량 폭증 감지
        is_spike, spike_ratio = self._detect_volume_spike(df)

        # 3. 패턴 분류
        if is_spike and spike_ratio >= 2.0:
            pattern = "SPIKE"
        elif is_vdu and vdu_ratio <= 0.6:
            pattern = "VDU"
        elif is_spike:
            pattern = "MODERATE_SPIKE"
        else:
            pattern = "NORMAL"

        return VolumeResult(
            is_vdu=is_vdu,
            is_spike=is_spike,
            vdu_ratio=vdu_ratio,
            spike_ratio=spike_ratio,
            pattern=pattern
        )

    def _detect_volume_dry_up(self, df: pd.DataFrame, window: int = 5) -> Tuple[bool, float]:
        """거래량 급감 감지 (최근 5일 평균 < 50일 평균 * 0.8)"""

        if len(df) < 50:
            return False, 1.0

        avg_50 = df['volume'].tail(50).mean()
        recent_avg = df['volume'].tail(window).mean()

        if pd.isna(avg_50) or pd.isna(recent_avg) or avg_50 <= 0:
            return False, 1.0

        vdu_ratio = recent_avg / avg_50
        is_vdu = vdu_ratio < self.config.VDU_THRESHOLD

        return is_vdu, vdu_ratio

    def _detect_volume_spike(self, df: pd.DataFrame, threshold: float = None) -> Tuple[bool, float]:
        """돌파 거래량 폭증 감지 (현재 > 50일 평균 * 1.5)"""

        if threshold is None:
            threshold = self.config.VOLUME_SPIKE_THRESHOLD

        if len(df) < 50:
            return False, 1.0

        avg_50 = df['volume'].tail(50).mean()
        current_volume = df['volume'].iloc[-1]

        if pd.isna(avg_50) or pd.isna(current_volume) or avg_50 <= 0:
            return False, 1.0

        spike_ratio = current_volume / avg_50
        is_spike = spike_ratio >= threshold

        return is_spike, spike_ratio


# ============================================================================
# 테스트 및 실행 함수
# ============================================================================

def test_advanced_analyzer():
    """고도화 분석기 테스트"""

    print("🧪 Advanced Trend Analyzer 테스트")
    print("=" * 60)

    # 분석기 초기화
    analyzer = AdvancedTrendAnalyzer()

    # 몇 개 종목 테스트
    test_tickers = ['KRW-BTC', 'KRW-ETH', 'KRW-SOL', 'KRW-DOT', 'KRW-DOGE']

    for ticker in test_tickers:
        print(f"\n🔍 {ticker} 고도화 분석:")
        signal = analyzer.analyze_ticker(ticker)

        print(f"   Action: {signal.action}")
        print(f"   Confidence: {signal.confidence:.2f}")
        print(f"   Stage: {signal.stage}")
        print(f"   RS Rating: {signal.rs_rating}")
        print(f"   Quality Score: {signal.quality_score:.1f}")
        print(f"   Volume Pattern: {signal.volume_pattern}")
        if signal.action == "BUY":
            print(f"   Entry: {signal.entry_price:.0f}")
            print(f"   Stop Loss: {signal.stop_loss:.0f}")
        print(f"   Reasons: {signal.reasons[:2]}")  # 처음 2개만


if __name__ == "__main__":
    test_advanced_analyzer()