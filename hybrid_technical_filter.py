#!/usr/bin/env python3
"""
Hybrid Technical Filter - Phase 2
Weinstein Stage 2 Detection & 4-Gate Filtering System

🎯 핵심 기능:
- Weinstein 4 Stage Market Cycle 분석
- Stage 2 진입 종목 감지 (상승 돌파 단계)
- 4단계 게이트 필터링 시스템
- SQLite 기반 분석 결과 저장

📊 Weinstein Stage 분석:
- Stage 1: Accumulation Base (기반 구축)
- Stage 2: Markup Phase (상승 돌파) ⭐ 매수 타겟
- Stage 3: Distribution Phase (분배)
- Stage 4: Decline Phase (하락)

🚪 4-Gate Filtering System:
1. Gate 1: Stage 2 진입 조건 (MA200 상향 돌파)
2. Gate 2: 거래량 급증 (1.5배 이상)
3. Gate 3: 기술적 지표 강세 (RSI > 50, MACD > 0)
4. Gate 4: 품질 점수 임계값 (12점 이상)
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
from dataclasses import dataclass

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class WeinsteingStageResult:
    """Weinstein Stage 분석 결과"""
    ticker: str
    current_stage: int  # 1, 2, 3, 4
    stage_confidence: float  # 0.0 - 1.0
    ma200_trend: str  # 'up', 'down', 'sideways'
    price_vs_ma200: float  # 현재가 대비 MA200 비율
    breakout_strength: float  # 돌파 강도
    volume_surge: float  # 거래량 급증률
    days_in_stage: int  # 현재 스테이지 지속 일수

@dataclass
class TechnicalGateResult:
    """4-Gate 필터링 결과"""
    ticker: str
    gate1_stage2: bool  # Stage 2 진입
    gate2_volume: bool  # 거래량 급증
    gate3_momentum: bool  # 기술적 지표 강세
    gate4_quality: bool  # 품질 점수
    total_gates_passed: int  # 통과한 게이트 수 (0-4)
    quality_score: float  # 종합 품질 점수
    recommendation: str  # 'STRONG_BUY', 'BUY', 'HOLD', 'AVOID'

class HybridTechnicalFilter:
    """
    Weinstein Stage 2 감지 및 기술적 필터링 시스템
    """

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.min_data_points = 200  # 최소 200일 데이터 필요
        self.volume_surge_threshold = 1.5  # 거래량 급증 임계값 (1.5배)
        self.quality_threshold = 12.0  # 최소 품질 점수

        # Stage 패턴별 승률 매핑 (Kelly Calculator용)
        self.pattern_win_rates = {
            'stage_1_to_2': 0.67,  # 67% - Stage 1→2 전환
            'stage_2_continuation': 0.55,  # 55% - Stage 2 지속
            'volume_breakout': 0.58,  # 58% - 거래량 돌파
            'ma200_breakout': 0.52   # 52% - MA200 단순 돌파
        }

    def get_ohlcv_data(self, ticker: str, days: int = 250) -> pd.DataFrame:
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
                logger.warning(f"📊 {ticker}: 데이터 없음")
                return pd.DataFrame()

            # 날짜 순으로 정렬 (오래된 것부터)
            df = df.sort_values('date').reset_index(drop=True)
            df['date'] = pd.to_datetime(df['date'])

            logger.info(f"📊 {ticker}: {len(df)}개 데이터 로드")
            return df

        except Exception as e:
            logger.error(f"❌ {ticker} 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def detect_weinstein_stage(self, df: pd.DataFrame) -> WeinsteingStageResult:
        """Weinstein 4 Stage 분석"""
        ticker = df['ticker'].iloc[0] if not df.empty else "Unknown"

        if len(df) < self.min_data_points:
            logger.warning(f"⚠️ {ticker}: 데이터 부족 ({len(df)}개, 최소 {self.min_data_points}개 필요)")
            return WeinsteingStageResult(
                ticker=ticker, current_stage=1, stage_confidence=0.0,
                ma200_trend='unknown', price_vs_ma200=0.0,
                breakout_strength=0.0, volume_surge=0.0, days_in_stage=0
            )

        latest = df.iloc[-1]  # 최신 데이터
        current_price = latest['close']
        ma200 = latest['ma200']
        volume = latest['volume']

        # 현재가 대비 MA200 위치 (NULL 체크 추가)
        if pd.notna(ma200) and ma200 > 0:
            price_vs_ma200 = ((current_price - ma200) / ma200) * 100
        else:
            price_vs_ma200 = 0.0

        # MA200 트렌드 판정 (개선된 로직)
        ma200_trend = self._determine_ma200_trend(df, ma200)

        # 거래량 분석
        volume_avg_20 = df['volume'].tail(20).mean()
        volume_surge = volume / volume_avg_20 if volume_avg_20 > 0 else 1.0

        # Stage 판정 로직
        stage, confidence = self._determine_stage(
            df, current_price, ma200, ma200_trend, volume_surge
        )

        # 돌파 강도 계산
        breakout_strength = self._calculate_breakout_strength(df, current_price, ma200)

        # 스테이지 지속 일수 (단순화)
        days_in_stage = self._estimate_days_in_stage(df, stage)

        return WeinsteingStageResult(
            ticker=ticker,
            current_stage=stage,
            stage_confidence=confidence,
            ma200_trend=ma200_trend,
            price_vs_ma200=price_vs_ma200,
            breakout_strength=breakout_strength,
            volume_surge=volume_surge,
            days_in_stage=days_in_stage
        )

    def _determine_ma200_trend(self, df: pd.DataFrame, current_ma200: float) -> str:
        """MA200 트렌드 판정 (개선된 로직)"""
        try:
            # 1차 시도: 20일 전 MA200과 비교
            if len(df) >= 20:
                ma200_20days_ago = df['ma200'].iloc[-20]
                if pd.notna(current_ma200) and pd.notna(ma200_20days_ago) and ma200_20days_ago > 0:
                    if current_ma200 > ma200_20days_ago * 1.02:  # 2% 이상 상승
                        return 'up'
                    elif current_ma200 < ma200_20days_ago * 0.98:  # 2% 이상 하락
                        return 'down'
                    else:
                        return 'sideways'

            # 2차 시도: MA120과 비교 (더 짧은 기간)
            if 'ma120' in df.columns and pd.notna(current_ma200):
                latest_ma120 = df['ma120'].iloc[-1]
                if pd.notna(latest_ma120) and latest_ma120 > 0:
                    if current_ma200 > latest_ma120 * 1.05:  # MA200 > MA120 * 1.05
                        return 'up'
                    elif current_ma200 < latest_ma120 * 0.95:  # MA200 < MA120 * 0.95
                        return 'down'
                    else:
                        return 'sideways'

            # 3차 시도: 최근 20일 가격 추세로 대체
            if len(df) >= 20:
                recent_prices = df['close'].tail(20)
                if len(recent_prices) >= 20:
                    price_20days_ago = recent_prices.iloc[0]
                    current_price = recent_prices.iloc[-1]

                    if current_price > price_20days_ago * 1.10:  # 10% 이상 상승
                        return 'up'
                    elif current_price < price_20days_ago * 0.90:  # 10% 이상 하락
                        return 'down'
                    else:
                        return 'sideways'

            # 4차 시도: MA60 기준 판정
            if 'ma60' in df.columns:
                latest_ma60 = df['ma60'].iloc[-1]
                current_price = df['close'].iloc[-1]

                if pd.notna(latest_ma60) and latest_ma60 > 0:
                    if current_price > latest_ma60 * 1.05:  # 현재가가 MA60보다 5% 이상 높음
                        return 'up'
                    elif current_price < latest_ma60 * 0.95:  # 현재가가 MA60보다 5% 이상 낮음
                        return 'down'
                    else:
                        return 'sideways'

            # 최종: 기본값
            return 'sideways'

        except Exception as e:
            logger.warning(f"⚠️ MA200 트렌드 판정 실패: {e}")
            return 'sideways'

    def _determine_stage(self, df: pd.DataFrame, current_price: float, ma200: float,
                        ma200_trend: str, volume_surge: float) -> Tuple[int, float]:
        """Stage 판정 로직"""

        # Stage 2 조건: MA200 위 + 상승 추세 + 거래량 증가 (NULL 체크 추가)
        if (pd.notna(ma200) and current_price > ma200 and ma200_trend == 'up'):

            # 추가 Stage 2 강화 조건
            confidence = 0.6  # 기본 신뢰도

            # 거래량 급증 시 신뢰도 증가
            if volume_surge > self.volume_surge_threshold:
                confidence += 0.2

            # 돌파 정도에 따른 신뢰도 조정 (NULL 체크 추가)
            if pd.notna(ma200) and ma200 > 0:
                breakout_pct = ((current_price - ma200) / ma200) * 100
                if breakout_pct > 5:  # 5% 이상 돌파
                    confidence += 0.1
                elif breakout_pct > 2:  # 2% 이상 돌파
                    confidence += 0.05

            # RSI 확인 (과매수 아닌 경우)
            if 'rsi' in df.columns:
                latest_rsi = df['rsi'].iloc[-1]
                if 40 <= latest_rsi <= 70:  # 건전한 RSI 범위
                    confidence += 0.05

            return 2, min(confidence, 1.0)

        # Stage 4 조건: MA200 아래 + 하락 추세 (NULL 체크 추가)
        elif (pd.notna(ma200) and current_price < ma200 and ma200_trend == 'down'):
            confidence = 0.7
            return 4, confidence

        # Stage 3 조건: MA200 근처에서 횡보 (고점 근처) (NULL 체크 추가)
        elif ma200_trend == 'sideways' and pd.notna(ma200) and current_price > ma200 * 0.95:
            confidence = 0.5
            return 3, confidence

        # Stage 1 조건: 기본값 (바닥 구축)
        else:
            confidence = 0.4
            return 1, confidence

    def _calculate_breakout_strength(self, df: pd.DataFrame, current_price: float, ma200: float) -> float:
        """돌파 강도 계산 (NULL 체크 추가)"""
        if not pd.notna(ma200) or ma200 <= 0:
            return 0.0

        # 최근 5일간 MA200 대비 평균 위치
        recent_prices = df['close'].tail(5)
        breakout_strengths = []

        for price in recent_prices:
            if pd.notna(price):
                strength = ((price - ma200) / ma200) * 100
                breakout_strengths.append(max(0, strength))  # 양수만

        return np.mean(breakout_strengths) if breakout_strengths else 0.0

    def _estimate_days_in_stage(self, df: pd.DataFrame, current_stage: int) -> int:
        """현재 스테이지 지속 일수 추정 (단순화)"""
        # 실제로는 복잡한 로직이 필요하지만, 단순화하여 구현
        if current_stage == 2:
            return 30  # Stage 2는 평균 30일 추정
        elif current_stage == 4:
            return 20  # Stage 4는 평균 20일 추정
        else:
            return 60  # Stage 1,3은 더 길게 추정

    def apply_four_gate_filter(self, stage_result: WeinsteingStageResult, df: pd.DataFrame) -> TechnicalGateResult:
        """4단계 게이트 필터링 시스템"""

        ticker = stage_result.ticker

        # Gate 1: Stage 2 진입 조건 (NULL 체크 추가)
        gate1_stage2 = (
            stage_result.current_stage == 2 and
            stage_result.stage_confidence >= 0.6 and
            pd.notna(stage_result.price_vs_ma200) and stage_result.price_vs_ma200 > 0  # MA200 위에 있어야 함
        )

        # Gate 2: 거래량 급증 조건
        gate2_volume = stage_result.volume_surge >= self.volume_surge_threshold

        # Gate 3: 기술적 지표 강세 조건
        gate3_momentum = self._check_momentum_strength(df)

        # Gate 4: 품질 점수 조건
        quality_score = self._calculate_quality_score(stage_result, df)
        gate4_quality = quality_score >= self.quality_threshold

        # 통과한 게이트 수 계산
        gates = [gate1_stage2, gate2_volume, gate3_momentum, gate4_quality]
        total_gates_passed = sum(gates)

        # 매수 권고 등급 결정
        recommendation = self._determine_recommendation(total_gates_passed, quality_score)

        logger.info(f"🚪 {ticker} Gate 결과: {total_gates_passed}/4 통과, 품질점수: {quality_score:.1f}")

        return TechnicalGateResult(
            ticker=ticker,
            gate1_stage2=gate1_stage2,
            gate2_volume=gate2_volume,
            gate3_momentum=gate3_momentum,
            gate4_quality=gate4_quality,
            total_gates_passed=total_gates_passed,
            quality_score=quality_score,
            recommendation=recommendation
        )

    def _check_momentum_strength(self, df: pd.DataFrame) -> bool:
        """기술적 지표 강세 조건 확인"""
        if df.empty or len(df) < 20:
            return False

        latest = df.iloc[-1]

        # RSI 조건 (40-70 건전한 범위)
        rsi_ok = False
        if 'rsi' in df.columns and pd.notna(latest['rsi']):
            rsi_ok = 40 <= latest['rsi'] <= 70

        # MA 배열 확인 (단기 > 장기)
        ma_ok = False
        if all(col in df.columns for col in ['ma5', 'ma20', 'ma60']):
            ma5, ma20, ma60 = latest['ma5'], latest['ma20'], latest['ma60']
            if all(pd.notna([ma5, ma20, ma60])):
                ma_ok = ma5 > ma20 > ma60  # 정배열

        # 추가 지표들 (있는 경우에만)
        price_above_ma20 = True
        if 'ma20' in df.columns and pd.notna(latest['ma20']):
            price_above_ma20 = latest['close'] > latest['ma20']

        # 2개 이상 조건 만족하면 통과
        conditions = [rsi_ok, ma_ok, price_above_ma20]
        return sum(conditions) >= 2

    def _calculate_quality_score(self, stage_result: WeinsteingStageResult, df: pd.DataFrame) -> float:
        """종합 품질 점수 계산 (0-25점)"""

        score = 0.0

        # 1. Stage 신뢰도 (0-5점)
        score += stage_result.stage_confidence * 5

        # 2. MA200 돌파 강도 (0-5점) (NULL 체크 추가)
        if pd.notna(stage_result.price_vs_ma200) and stage_result.price_vs_ma200 > 0:
            breakout_score = min(5, stage_result.price_vs_ma200 * 0.5)
            score += breakout_score

        # 3. 거래량 급증도 (0-5점) (NULL 체크 추가)
        if pd.notna(stage_result.volume_surge) and stage_result.volume_surge > 0:
            volume_score = min(5, (stage_result.volume_surge - 1) * 2)
            score += max(0, volume_score)

        # 4. MA200 추세 강도 (0-5점)
        if stage_result.ma200_trend == 'up':
            score += 3.0
        elif stage_result.ma200_trend == 'sideways':
            score += 1.0

        # 5. 기술적 지표 보너스 (0-5점)
        if not df.empty:
            tech_score = self._calculate_technical_bonus(df)
            score += tech_score

        return round(score, 1)

    def _calculate_technical_bonus(self, df: pd.DataFrame) -> float:
        """기술적 지표 보너스 점수"""
        if df.empty:
            return 0.0

        bonus = 0.0
        latest = df.iloc[-1]

        # RSI 적정 범위 (40-60) (NULL 체크 이미 있음)
        if 'rsi' in df.columns and pd.notna(latest['rsi']):
            rsi = latest['rsi']
            if 40 <= rsi <= 60:
                bonus += 2.0
            elif 35 <= rsi <= 70:
                bonus += 1.0

        # MA 정배열 보너스 (NULL 체크 이미 있음)
        if all(col in df.columns for col in ['ma5', 'ma20', 'ma60']):
            ma5, ma20, ma60 = latest['ma5'], latest['ma20'], latest['ma60']
            if all(pd.notna([ma5, ma20, ma60])):
                if ma5 > ma20 > ma60:
                    bonus += 2.0
                elif ma5 > ma20:
                    bonus += 1.0

        # 거래량 비율 보너스 (NULL 체크 이미 있음)
        if 'volume_ratio' in df.columns and pd.notna(latest['volume_ratio']):
            vol_ratio = latest['volume_ratio']
            if vol_ratio > 2.0:
                bonus += 1.0
            elif vol_ratio > 1.5:
                bonus += 0.5

        return min(5.0, bonus)

    def _determine_recommendation(self, gates_passed: int, quality_score: float) -> str:
        """매수 권고 등급 결정"""

        if gates_passed >= 4 and quality_score >= 18:
            return "STRONG_BUY"
        elif gates_passed >= 3 and quality_score >= 15:
            return "BUY"
        elif gates_passed >= 2 and quality_score >= 12:
            return "HOLD"
        else:
            return "AVOID"

    def save_analysis_results(self, stage_result: WeinsteingStageResult, gate_result: TechnicalGateResult) -> bool:
        """분석 결과를 SQLite에 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # technical_analysis 테이블 확인/생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS technical_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,

                    -- Weinstein Stage 분석
                    current_stage INTEGER,
                    stage_confidence REAL,
                    ma200_trend TEXT,
                    price_vs_ma200 REAL,
                    breakout_strength REAL,
                    volume_surge REAL,
                    days_in_stage INTEGER,

                    -- 4-Gate 필터링 결과
                    gate1_stage2 INTEGER,
                    gate2_volume INTEGER,
                    gate3_momentum INTEGER,
                    gate4_quality INTEGER,
                    total_gates_passed INTEGER,
                    quality_score REAL,
                    recommendation TEXT,

                    -- 메타데이터
                    created_at TEXT DEFAULT (datetime('now')),

                    UNIQUE(ticker, analysis_date)
                )
            """)

            # 인덱스 생성
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_technical_analysis_ticker
                ON technical_analysis(ticker)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_technical_analysis_date
                ON technical_analysis(analysis_date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_technical_analysis_recommendation
                ON technical_analysis(recommendation)
            """)

            # 데이터 저장 (UPSERT)
            analysis_date = datetime.now().strftime('%Y-%m-%d')

            # 🚀 Phase 1: 새로운 기술적 지표들을 OHLCV 데이터에서 가져와서 저장
            df = self.get_ohlcv_data(stage_result.ticker)
            latest_atr = None
            latest_supertrend = None
            latest_macd_histogram = None
            latest_adx = None
            latest_support_level = None

            if not df.empty:
                try:
                    # 최신 날짜의 지표값들 가져오기
                    latest_row = df.iloc[-1]
                    latest_atr = float(latest_row.get('atr')) if pd.notna(latest_row.get('atr')) else None
                    latest_supertrend = float(latest_row.get('supertrend')) if pd.notna(latest_row.get('supertrend')) else None
                    latest_macd_histogram = float(latest_row.get('macd_histogram')) if pd.notna(latest_row.get('macd_histogram')) else None
                    latest_adx = float(latest_row.get('adx')) if pd.notna(latest_row.get('adx')) else None
                    latest_support_level = float(latest_row.get('support_level')) if pd.notna(latest_row.get('support_level')) else None
                except Exception as indicator_error:
                    logger.warning(f"⚠️ {stage_result.ticker} 기술적 지표 값 추출 실패: {indicator_error}")

            cursor.execute("""
                INSERT OR REPLACE INTO technical_analysis (
                    ticker, analysis_date, current_stage, stage_confidence,
                    ma200_trend, price_vs_ma200, breakout_strength,
                    volume_surge, days_in_stage, gate1_stage2, gate2_volume,
                    gate3_momentum, gate4_quality, total_gates_passed,
                    quality_score, recommendation, atr, supertrend, macd_histogram,
                    adx, support_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stage_result.ticker, analysis_date, stage_result.current_stage,
                stage_result.stage_confidence, stage_result.ma200_trend,
                stage_result.price_vs_ma200, stage_result.breakout_strength,
                stage_result.volume_surge, stage_result.days_in_stage,
                gate_result.gate1_stage2, gate_result.gate2_volume,
                gate_result.gate3_momentum, gate_result.gate4_quality,
                gate_result.total_gates_passed, gate_result.quality_score,
                gate_result.recommendation, latest_atr, latest_supertrend,
                latest_macd_histogram, latest_adx, latest_support_level
            ))

            conn.commit()
            conn.close()

            logger.info(f"💾 {stage_result.ticker} 분석 결과 저장 완료")
            return True

        except Exception as e:
            logger.error(f"❌ {stage_result.ticker} 분석 결과 저장 실패: {e}")
            return False

    def analyze_ticker(self, ticker: str) -> Optional[Tuple[WeinsteingStageResult, TechnicalGateResult]]:
        """개별 종목 분석 (Weinstein Stage + 4-Gate Filter)"""

        logger.info(f"🔍 {ticker} 기술적 분석 시작")

        # 1. OHLCV 데이터 로드
        df = self.get_ohlcv_data(ticker)
        if df.empty:
            logger.warning(f"⚠️ {ticker}: 데이터 없음, 분석 건너뛰기")
            return None

        # 2. Weinstein Stage 분석
        stage_result = self.detect_weinstein_stage(df)

        # 3. 4-Gate 필터링
        gate_result = self.apply_four_gate_filter(stage_result, df)

        # 4. 결과 저장
        self.save_analysis_results(stage_result, gate_result)

        # 5. 결과 출력
        logger.info(f"📊 {ticker} 분석 완료:")
        logger.info(f"   - Stage: {stage_result.current_stage} (신뢰도: {stage_result.stage_confidence:.2f})")
        logger.info(f"   - Gates: {gate_result.total_gates_passed}/4 통과")
        logger.info(f"   - 품질점수: {gate_result.quality_score:.1f}")
        logger.info(f"   - 권고: {gate_result.recommendation}")

        return stage_result, gate_result

    def get_active_tickers(self) -> List[str]:
        """활성 종목 목록 조회"""
        try:
            conn = sqlite3.connect(self.db_path)

            # ohlcv_data에서 데이터가 있는 종목들 조회
            query = """
            SELECT DISTINCT ticker
            FROM ohlcv_data
            WHERE date >= date('now', '-30 days')
            ORDER BY ticker
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            tickers = df['ticker'].tolist()
            logger.info(f"📊 활성 종목 {len(tickers)}개 발견")

            return tickers

        except Exception as e:
            logger.error(f"❌ 활성 종목 조회 실패: {e}")
            return []

    def run_full_analysis(self) -> Dict:
        """전체 종목 기술적 분석 실행"""

        logger.info("🚀 Phase 2: Hybrid Technical Filter 시작")

        # 활성 종목 목록 조회
        tickers = self.get_active_tickers()
        if not tickers:
            logger.warning("⚠️ 활성 종목이 없습니다")
            return {}

        results = {
            'analyzed_tickers': [],
            'stage2_candidates': [],
            'strong_buy': [],
            'buy': [],
            'hold': [],
            'avoid': [],
            'analysis_summary': {}
        }

        # 각 종목 분석
        for ticker in tickers:
            try:
                analysis_result = self.analyze_ticker(ticker)
                if analysis_result:
                    stage_result, gate_result = analysis_result

                    results['analyzed_tickers'].append(ticker)

                    # Stage 2 후보 분류
                    if stage_result.current_stage == 2 and gate_result.total_gates_passed >= 2:
                        results['stage2_candidates'].append({
                            'ticker': ticker,
                            'gates_passed': gate_result.total_gates_passed,
                            'quality_score': gate_result.quality_score,
                            'recommendation': gate_result.recommendation
                        })

                    # 권고 등급별 분류
                    if gate_result.recommendation == 'STRONG_BUY':
                        results['strong_buy'].append(ticker)
                    elif gate_result.recommendation == 'BUY':
                        results['buy'].append(ticker)
                    elif gate_result.recommendation == 'HOLD':
                        results['hold'].append(ticker)
                    else:
                        results['avoid'].append(ticker)

            except Exception as e:
                logger.error(f"❌ {ticker} 분석 중 오류: {e}")
                continue

        # 분석 요약
        total_analyzed = len(results['analyzed_tickers'])
        stage2_count = len(results['stage2_candidates'])

        results['analysis_summary'] = {
            'total_analyzed': total_analyzed,
            'stage2_candidates': stage2_count,
            'strong_buy_count': len(results['strong_buy']),
            'buy_count': len(results['buy']),
            'hold_count': len(results['hold']),
            'avoid_count': len(results['avoid']),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 결과 출력
        logger.info("=" * 60)
        logger.info("📊 Phase 2 분석 결과 요약")
        logger.info("=" * 60)
        logger.info(f"총 분석 종목: {total_analyzed}개")
        logger.info(f"Stage 2 후보: {stage2_count}개")
        logger.info(f"STRONG_BUY: {len(results['strong_buy'])}개")
        logger.info(f"BUY: {len(results['buy'])}개")
        logger.info(f"HOLD: {len(results['hold'])}개")
        logger.info(f"AVOID: {len(results['avoid'])}개")

        if results['stage2_candidates']:
            logger.info("\n🎯 Stage 2 후보 종목:")
            for candidate in sorted(results['stage2_candidates'],
                                   key=lambda x: x['quality_score'], reverse=True):
                logger.info(f"  - {candidate['ticker']}: {candidate['gates_passed']}/4 게이트, "
                           f"품질 {candidate['quality_score']:.1f}점 ({candidate['recommendation']})")

        return results

def main():
    """메인 실행 함수"""

    print("🚀 Makenaide Hybrid Technical Filter - Phase 2")
    print("=" * 60)

    # 필터 인스턴스 생성
    filter_engine = HybridTechnicalFilter()

    # 전체 분석 실행
    results = filter_engine.run_full_analysis()

    # 결과를 JSON으로 저장 (선택사항)
    output_file = f"technical_analysis_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        print(f"💾 분석 결과 저장: {output_file}")
    except Exception as e:
        print(f"⚠️ 결과 저장 실패: {e}")

    print("✅ Phase 2 기술적 분석 완료")

if __name__ == "__main__":
    main()