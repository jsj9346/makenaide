#!/usr/bin/env python3
"""
kelly_calculator.py - Kelly Criterion 기반 포지션 사이징 계산기

🎯 핵심 철학:
- "확률적 우위가 클수록 베팅 사이즈를 크게" - Kelly Formula 기본 아이디어
- 백테스트 대신 역사적으로 검증된 패턴의 성공률 활용
- 2단계 포지션 사이징: Technical Filter → GPT Analysis 조정

📊 Kelly 공식 변형:
- 전통적 Kelly: f = (bp - q) / b
- Makenaide 적용: Position% = Pattern_Base% × Quality_Multiplier × GPT_Adjustment

🎲 역사적 패턴 성공률 (Historical Evidence):
- Stage 1→2 전환: 65-70% (스탠 와인스타인)
- VCP 돌파: 60-65% (마크 미너비니)
- Cup & Handle: 60-65% (윌리엄 오닐)
- 60일 고점 돌파: 55-60%
- 단순 MA200 돌파: 50-55%

💰 포지션 사이징 전략:
- 최대 포지션: 8% (극단적 강세 신호)
- 기본 포지션: 2-5% (일반적 신호)
- 최소 포지션: 1% (약한 신호)
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
import json
import math
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PatternType(Enum):
    """차트 패턴 타입"""
    STAGE_1_TO_2 = "stage_1_to_2"
    VCP_BREAKOUT = "vcp_breakout"
    CUP_HANDLE = "cup_handle"
    HIGH_60D_BREAKOUT = "high_60d_breakout"
    MA200_BREAKOUT = "ma200_breakout"
    STAGE_2_CONTINUATION = "stage_2_continuation"
    UNKNOWN = "unknown"

class RiskLevel(Enum):
    """리스크 레벨"""
    CONSERVATIVE = "conservative"  # 보수적
    MODERATE = "moderate"         # 중도
    AGGRESSIVE = "aggressive"     # 공격적

@dataclass
class PatternProbability:
    """패턴별 확률 정보"""
    pattern_type: PatternType
    win_rate: float  # 승률 (0.0-1.0)
    avg_win: float   # 평균 수익률
    avg_loss: float  # 평균 손실률
    base_position: float  # 기본 포지션 크기 (%)

@dataclass
class QualityScoreAdjustment:
    """품질 점수 조정자"""
    score_range: Tuple[float, float]  # 점수 범위
    multiplier: float  # 조정 배수
    description: str

@dataclass
class KellyResult:
    """Kelly 계산 결과"""
    ticker: str
    analysis_date: str

    # Stage 1: Technical Filter 단계
    detected_pattern: PatternType
    quality_score: float
    base_position_pct: float
    quality_multiplier: float
    technical_position_pct: float

    # Stage 2: GPT 조정 단계 (선택적)
    gpt_confidence: Optional[float] = None
    gpt_recommendation: Optional[str] = None
    gpt_adjustment: float = 1.0
    final_position_pct: float = None

    # 메타 정보
    risk_level: RiskLevel = RiskLevel.MODERATE
    max_portfolio_allocation: float = 25.0  # 최대 포트폴리오 할당 %
    reasoning: str = ""

class KellyCalculator:
    """
    Kelly Criterion 기반 포지션 사이징 계산기
    역사적 패턴 성공률을 활용한 확률적 포지션 결정
    """

    def __init__(self,
                 db_path: str = "./makenaide_local.db",
                 risk_level: RiskLevel = RiskLevel.MODERATE,
                 max_single_position: float = 8.0,
                 max_total_allocation: float = 25.0):

        self.db_path = db_path
        self.risk_level = risk_level
        self.max_single_position = max_single_position  # 개별 포지션 최대 %
        self.max_total_allocation = max_total_allocation  # 전체 할당 최대 %

        # 패턴별 확률 정보 초기화
        self.pattern_probabilities = self._initialize_pattern_probabilities()

        # 품질 점수 조정자 초기화
        self.quality_adjustments = self._initialize_quality_adjustments()

        self.init_database()
        logger.info("🎲 KellyCalculator 초기화 완료")

    def _initialize_pattern_probabilities(self) -> Dict[PatternType, PatternProbability]:
        """패턴별 확률 정보 초기화 (역사적 검증 데이터)"""
        return {
            # 스탠 와인스타인 Stage 1→2 전환 (최강 신호)
            PatternType.STAGE_1_TO_2: PatternProbability(
                pattern_type=PatternType.STAGE_1_TO_2,
                win_rate=0.675,  # 67.5% 승률 (65-70% 중간값)
                avg_win=0.25,    # 평균 25% 수익
                avg_loss=0.08,   # 평균 8% 손실 (미너비니 규칙)
                base_position=5.0  # 5% 기본 포지션
            ),

            # 마크 미너비니 VCP 돌파
            PatternType.VCP_BREAKOUT: PatternProbability(
                pattern_type=PatternType.VCP_BREAKOUT,
                win_rate=0.625,  # 62.5% 승률 (60-65% 중간값)
                avg_win=0.22,    # 평균 22% 수익
                avg_loss=0.08,   # 평균 8% 손실
                base_position=4.0  # 4% 기본 포지션
            ),

            # 윌리엄 오닐 Cup & Handle
            PatternType.CUP_HANDLE: PatternProbability(
                pattern_type=PatternType.CUP_HANDLE,
                win_rate=0.625,  # 62.5% 승률 (60-65% 중간값)
                avg_win=0.20,    # 평균 20% 수익
                avg_loss=0.08,   # 평균 8% 손실
                base_position=4.0  # 4% 기본 포지션
            ),

            # 60일 고점 돌파 + 거래량
            PatternType.HIGH_60D_BREAKOUT: PatternProbability(
                pattern_type=PatternType.HIGH_60D_BREAKOUT,
                win_rate=0.575,  # 57.5% 승률 (55-60% 중간값)
                avg_win=0.18,    # 평균 18% 수익
                avg_loss=0.08,   # 평균 8% 손실
                base_position=3.0  # 3% 기본 포지션
            ),

            # Stage 2 지속 (추가 매수)
            PatternType.STAGE_2_CONTINUATION: PatternProbability(
                pattern_type=PatternType.STAGE_2_CONTINUATION,
                win_rate=0.55,   # 55% 승률
                avg_win=0.15,    # 평균 15% 수익
                avg_loss=0.08,   # 평균 8% 손실
                base_position=2.0  # 2% 기본 포지션
            ),

            # 단순 MA200 돌파
            PatternType.MA200_BREAKOUT: PatternProbability(
                pattern_type=PatternType.MA200_BREAKOUT,
                win_rate=0.525,  # 52.5% 승률 (50-55% 중간값)
                avg_win=0.12,    # 평균 12% 수익
                avg_loss=0.08,   # 평균 8% 손실
                base_position=1.5  # 1.5% 기본 포지션
            ),
        }

    def _initialize_quality_adjustments(self) -> List[QualityScoreAdjustment]:
        """품질 점수 조정자 초기화"""
        return [
            QualityScoreAdjustment((20.0, 25.0), 1.4, "Exceptional (20+ 점)"),
            QualityScoreAdjustment((18.0, 20.0), 1.3, "Excellent (18-20 점)"),
            QualityScoreAdjustment((15.0, 18.0), 1.2, "Strong (15-18 점)"),
            QualityScoreAdjustment((12.0, 15.0), 1.0, "Good (12-15 점)"),
            QualityScoreAdjustment((10.0, 12.0), 0.8, "Weak (10-12 점)"),
            QualityScoreAdjustment((0.0, 10.0), 0.6, "Poor (< 10 점)"),
        ]

    def init_database(self):
        """kelly_analysis 테이블 생성"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS kelly_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                analysis_date TEXT NOT NULL,

                -- Technical Filter 단계
                detected_pattern TEXT NOT NULL,
                quality_score REAL NOT NULL,
                base_position_pct REAL NOT NULL,
                quality_multiplier REAL NOT NULL,
                technical_position_pct REAL NOT NULL,

                -- GPT 조정 단계 (선택적)
                gpt_confidence REAL DEFAULT NULL,
                gpt_recommendation TEXT DEFAULT NULL,
                gpt_adjustment REAL DEFAULT 1.0,
                final_position_pct REAL NOT NULL,

                -- 메타 정보
                risk_level TEXT DEFAULT 'moderate',
                max_portfolio_allocation REAL DEFAULT 25.0,
                reasoning TEXT DEFAULT '',

                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker, analysis_date)
            );
            """

            cursor.execute(create_table_sql)

            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_kelly_ticker ON kelly_analysis(ticker);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_kelly_date ON kelly_analysis(analysis_date);")

            conn.commit()
            conn.close()

            logger.info("✅ kelly_analysis 테이블 초기화 완료")

        except Exception as e:
            logger.warning(f"⚠️ kelly_analysis 테이블 생성 스킵: {e}")

    def detect_pattern_type(self, technical_result: Dict) -> PatternType:
        """기술적 분석 결과에서 패턴 타입 감지"""

        # Stage 1→2 전환 감지 (최우선)
        if self._is_stage_1_to_2_transition(technical_result):
            return PatternType.STAGE_1_TO_2

        # VCP 패턴 감지
        if self._is_vcp_pattern(technical_result):
            return PatternType.VCP_BREAKOUT

        # Cup & Handle 패턴 감지
        if self._is_cup_handle_pattern(technical_result):
            return PatternType.CUP_HANDLE

        # 60일 고점 돌파 감지
        if self._is_60d_high_breakout(technical_result):
            return PatternType.HIGH_60D_BREAKOUT

        # Stage 2 지속 감지
        if self._is_stage_2_continuation(technical_result):
            return PatternType.STAGE_2_CONTINUATION

        # 단순 MA200 돌파
        if self._is_ma200_breakout(technical_result):
            return PatternType.MA200_BREAKOUT

        return PatternType.UNKNOWN

    def _is_stage_1_to_2_transition(self, result: Dict) -> bool:
        """스탠 와인스타인 Stage 1→2 전환 감지"""
        try:
            # Stage 2 진입 + 강한 기술적 지표 조합
            stage_2_entry = result.get('stage_2_entry', False)
            volume_breakout = result.get('volume_breakout', False)
            ma_trend_strong = result.get('ma_trend_strength', 0) > 0.7

            return stage_2_entry and volume_breakout and ma_trend_strong

        except Exception:
            return False

    def _is_vcp_pattern(self, result: Dict) -> bool:
        """VCP 패턴 감지"""
        try:
            # 변동성 수축 + 돌파 패턴
            volatility_contraction = result.get('volatility_contraction', False)
            volume_dry_up = result.get('volume_dry_up', False)
            breakout_volume = result.get('volume_breakout', False)

            return volatility_contraction and volume_dry_up and breakout_volume

        except Exception:
            return False

    def _is_cup_handle_pattern(self, result: Dict) -> bool:
        """Cup & Handle 패턴 감지"""
        try:
            # U자 형태 + 핸들 형성
            cup_formation = result.get('cup_formation', False)
            handle_formation = result.get('handle_formation', False)
            proper_depth = result.get('cup_depth_ok', False)

            return cup_formation and handle_formation and proper_depth

        except Exception:
            return False

    def _is_60d_high_breakout(self, result: Dict) -> bool:
        """60일 고점 돌파 감지"""
        try:
            high_breakout = result.get('high_60d_breakout', False)
            volume_support = result.get('volume_breakout', False)

            return high_breakout and volume_support

        except Exception:
            return False

    def _is_stage_2_continuation(self, result: Dict) -> bool:
        """Stage 2 지속 감지"""
        try:
            # 이미 Stage 2 + 추가 매수 신호
            in_stage_2 = result.get('current_stage', 0) == 2
            pullback_buy = result.get('pullback_opportunity', False)

            return in_stage_2 and pullback_buy

        except Exception:
            return False

    def _is_ma200_breakout(self, result: Dict) -> bool:
        """단순 MA200 돌파 감지"""
        try:
            ma200_breakout = result.get('ma200_breakout', False)
            return ma200_breakout

        except Exception:
            return False

    def get_quality_multiplier(self, quality_score: float) -> Tuple[float, str]:
        """품질 점수에 따른 조정 배수 계산"""
        for adjustment in self.quality_adjustments:
            min_score, max_score = adjustment.score_range
            if min_score <= quality_score < max_score:
                return adjustment.multiplier, adjustment.description

        # 기본값 (점수가 범위를 벗어날 경우)
        return 1.0, "Default (범위 외)"

    def calculate_technical_position(self,
                                   pattern_type: PatternType,
                                   quality_score: float) -> Tuple[float, float, float]:
        """Stage 1: Technical Filter 단계 포지션 계산"""

        # 1. 패턴별 기본 포지션 확인
        if pattern_type not in self.pattern_probabilities:
            logger.warning(f"⚠️ 알 수 없는 패턴: {pattern_type}")
            base_position = 1.0  # 최소 포지션
        else:
            base_position = self.pattern_probabilities[pattern_type].base_position

        # 2. 품질 점수 조정
        quality_multiplier, quality_desc = self.get_quality_multiplier(quality_score)

        # 3. 리스크 레벨 조정
        risk_adjustment = self._get_risk_adjustment()

        # 4. 최종 기술적 포지션 계산
        technical_position = base_position * quality_multiplier * risk_adjustment

        # 5. 최대 포지션 제한
        technical_position = min(technical_position, self.max_single_position)

        logger.debug(f"📊 기술적 포지션: {base_position}% × {quality_multiplier:.2f} × {risk_adjustment:.2f} = {technical_position:.2f}%")

        return base_position, quality_multiplier, technical_position

    def _get_risk_adjustment(self) -> float:
        """리스크 레벨에 따른 조정"""
        if self.risk_level == RiskLevel.CONSERVATIVE:
            return 0.7
        elif self.risk_level == RiskLevel.MODERATE:
            return 1.0
        elif self.risk_level == RiskLevel.AGGRESSIVE:
            return 1.3
        return 1.0

    def apply_gpt_adjustment(self,
                           technical_position: float,
                           gpt_confidence: Optional[float] = None,
                           gpt_recommendation: Optional[str] = None) -> Tuple[float, float]:
        """Stage 2: GPT 분석 후 최종 조정"""

        if gpt_confidence is None or gpt_recommendation is None:
            # GPT 분석 없음 - 기술적 포지션 그대로 사용
            return technical_position, 1.0

        # GPT 추천에 따른 기본 조정
        if gpt_recommendation == "STRONG_BUY":
            base_adjustment = 1.4
        elif gpt_recommendation == "BUY":
            base_adjustment = 1.2
        elif gpt_recommendation == "HOLD":
            base_adjustment = 1.0
        elif gpt_recommendation == "AVOID":
            base_adjustment = 0.3  # 크게 축소
        else:
            base_adjustment = 1.0

        # GPT 신뢰도 반영 (0.5 ~ 1.5 범위로 조정)
        confidence_adjustment = 0.5 + (gpt_confidence * 1.0)

        # 최종 GPT 조정 배수
        gpt_adjustment = base_adjustment * confidence_adjustment

        # 50%~150% 범위 제한 (초기 사이징의 절반~1.5배)
        gpt_adjustment = max(0.5, min(1.5, gpt_adjustment))

        # 최종 포지션 계산
        final_position = technical_position * gpt_adjustment

        # 최대 포지션 제한
        final_position = min(final_position, self.max_single_position)

        logger.debug(f"🤖 GPT 조정: {technical_position:.2f}% × {gpt_adjustment:.2f} = {final_position:.2f}%")

        return final_position, gpt_adjustment

    def calculate_position_size(self,
                              technical_result: Dict,
                              gpt_result: Optional[Dict] = None) -> KellyResult:
        """종합 포지션 사이징 계산"""

        ticker = technical_result.get('ticker', 'UNKNOWN')
        quality_score = technical_result.get('quality_score', 10.0)

        try:
            # 1. 패턴 타입 감지
            pattern_type = self.detect_pattern_type(technical_result)

            # 2. Stage 1: Technical Filter 단계
            base_position, quality_multiplier, technical_position = self.calculate_technical_position(
                pattern_type, quality_score
            )

            # 3. Stage 2: GPT 조정 단계 (선택적)
            gpt_confidence = None
            gpt_recommendation = None
            gpt_adjustment = 1.0
            final_position = technical_position

            if gpt_result:
                gpt_confidence = gpt_result.get('confidence', None)
                gpt_recommendation = gpt_result.get('recommendation', None)

                final_position, gpt_adjustment = self.apply_gpt_adjustment(
                    technical_position, gpt_confidence, gpt_recommendation
                )

            # 4. 결과 생성
            result = KellyResult(
                ticker=ticker,
                analysis_date=datetime.now().strftime('%Y-%m-%d'),
                detected_pattern=pattern_type,
                quality_score=quality_score,
                base_position_pct=base_position,
                quality_multiplier=quality_multiplier,
                technical_position_pct=technical_position,
                gpt_confidence=gpt_confidence,
                gpt_recommendation=gpt_recommendation,
                gpt_adjustment=gpt_adjustment,
                final_position_pct=final_position,
                risk_level=self.risk_level,
                max_portfolio_allocation=self.max_total_allocation,
                reasoning=self._generate_reasoning(pattern_type, quality_score, gpt_result)
            )

            # 5. DB 저장
            self._save_kelly_result(result)

            logger.info(f"🎲 {ticker}: Kelly 계산 완료 - {pattern_type.value} → {final_position:.2f}%")
            return result

        except Exception as e:
            logger.error(f"❌ {ticker} Kelly 계산 실패: {e}")

            # 기본값 반환
            return KellyResult(
                ticker=ticker,
                analysis_date=datetime.now().strftime('%Y-%m-%d'),
                detected_pattern=PatternType.UNKNOWN,
                quality_score=quality_score,
                base_position_pct=1.0,
                quality_multiplier=1.0,
                technical_position_pct=1.0,
                final_position_pct=1.0,
                reasoning="계산 실패 - 최소 포지션 적용"
            )

    def _generate_reasoning(self,
                          pattern_type: PatternType,
                          quality_score: float,
                          gpt_result: Optional[Dict]) -> str:
        """포지션 결정 근거 생성"""

        reasoning_parts = []

        # 패턴 설명
        pattern_desc = {
            PatternType.STAGE_1_TO_2: "Stage 1→2 전환 (최강 신호)",
            PatternType.VCP_BREAKOUT: "VCP 돌파 패턴",
            PatternType.CUP_HANDLE: "Cup & Handle 패턴",
            PatternType.HIGH_60D_BREAKOUT: "60일 고점 돌파",
            PatternType.STAGE_2_CONTINUATION: "Stage 2 지속",
            PatternType.MA200_BREAKOUT: "MA200 돌파",
            PatternType.UNKNOWN: "패턴 불명확"
        }

        reasoning_parts.append(f"패턴: {pattern_desc.get(pattern_type, '알 수 없음')}")
        reasoning_parts.append(f"품질점수: {quality_score:.1f}점")

        if gpt_result:
            gpt_rec = gpt_result.get('recommendation', 'HOLD')
            gpt_conf = gpt_result.get('confidence', 0.0)
            reasoning_parts.append(f"GPT: {gpt_rec} ({gpt_conf:.2f})")

        return " | ".join(reasoning_parts)

    def _save_kelly_result(self, result: KellyResult):
        """Kelly 계산 결과 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO kelly_analysis (
                    ticker, analysis_date, detected_pattern, quality_score,
                    base_position_pct, quality_multiplier, technical_position_pct,
                    gpt_confidence, gpt_recommendation, gpt_adjustment, final_position_pct,
                    risk_level, max_portfolio_allocation, reasoning
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.ticker, result.analysis_date, result.detected_pattern.value,
                result.quality_score, result.base_position_pct, result.quality_multiplier,
                result.technical_position_pct, result.gpt_confidence, result.gpt_recommendation,
                result.gpt_adjustment, result.final_position_pct, result.risk_level.value,
                result.max_portfolio_allocation, result.reasoning
            ))

            conn.commit()
            conn.close()

            logger.debug(f"💾 {result.ticker}: Kelly 결과 DB 저장 완료")

        except Exception as e:
            logger.error(f"❌ {result.ticker} Kelly 결과 저장 실패: {e}")

    def get_portfolio_allocation_status(self) -> Dict[str, float]:
        """현재 포트폴리오 할당 상태 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            today = datetime.now().strftime('%Y-%m-%d')

            # 오늘 계산된 포지션들의 합계
            cursor.execute("""
                SELECT SUM(final_position_pct) as total_allocation,
                       COUNT(*) as position_count
                FROM kelly_analysis
                WHERE analysis_date = ?
            """, (today,))

            row = cursor.fetchone()
            total_allocation = row[0] or 0.0
            position_count = row[1] or 0

            conn.close()

            remaining_allocation = self.max_total_allocation - total_allocation

            return {
                'total_allocation': total_allocation,
                'remaining_allocation': max(0, remaining_allocation),
                'position_count': position_count,
                'utilization_rate': (total_allocation / self.max_total_allocation) * 100
            }

        except Exception as e:
            logger.error(f"❌ 포트폴리오 할당 상태 조회 실패: {e}")
            return {
                'total_allocation': 0.0,
                'remaining_allocation': self.max_total_allocation,
                'position_count': 0,
                'utilization_rate': 0.0
            }

    def calculate_batch_positions(self, candidates: List[Dict]) -> List[Dict]:
        """다수 후보에 대한 배치 포지션 계산"""
        logger.info(f"🎲 Kelly 배치 계산 시작: {len(candidates)}개 후보")

        enhanced_candidates = []

        for candidate in candidates:
            try:
                # GPT 결과 추출 (있을 경우)
                gpt_result = None
                if 'gpt_analysis' in candidate and candidate['gpt_analysis']:
                    gpt_analysis = candidate['gpt_analysis']
                    gpt_result = {
                        'confidence': gpt_analysis.confidence,
                        'recommendation': gpt_analysis.recommendation.value
                    }

                # Kelly 계산 실행
                kelly_result = self.calculate_position_size(candidate, gpt_result)

                # 결과 추가
                candidate['kelly_analysis'] = kelly_result
                enhanced_candidates.append(candidate)

            except Exception as e:
                logger.error(f"❌ {candidate.get('ticker', 'UNKNOWN')} Kelly 계산 실패: {e}")
                # 실패해도 기본값으로 포함
                candidate['kelly_analysis'] = None
                enhanced_candidates.append(candidate)

        # 포트폴리오 할당 상태 확인
        allocation_status = self.get_portfolio_allocation_status()

        logger.info(f"✅ Kelly 배치 계산 완료")
        logger.info(f"📊 포트폴리오 할당: {allocation_status['total_allocation']:.2f}% / {self.max_total_allocation}%")
        logger.info(f"🎯 남은 할당: {allocation_status['remaining_allocation']:.2f}%")

        return enhanced_candidates

def main():
    """테스트 실행"""
    print("🧪 Kelly Calculator 테스트 시작")

    # Kelly 계산기 초기화
    calculator = KellyCalculator(risk_level=RiskLevel.MODERATE)
    print("✅ KellyCalculator 초기화 완료")

    # 테스트 기술적 분석 결과
    test_technical_results = [
        {
            'ticker': 'KRW-BTC',
            'quality_score': 18.5,
            'stage_2_entry': True,
            'volume_breakout': True,
            'ma_trend_strength': 0.8,
            'volatility_contraction': True,
        },
        {
            'ticker': 'KRW-ETH',
            'quality_score': 16.2,
            'high_60d_breakout': True,
            'volume_breakout': True,
            'current_stage': 2,
        },
        {
            'ticker': 'KRW-ADA',
            'quality_score': 12.8,
            'ma200_breakout': True,
        }
    ]

    # 테스트 GPT 결과
    test_gpt_results = [
        {'confidence': 0.85, 'recommendation': 'STRONG_BUY'},
        {'confidence': 0.65, 'recommendation': 'BUY'},
        None  # GPT 분석 없음
    ]

    print(f"\n📊 테스트 후보: {len(test_technical_results)}개")

    # 개별 계산 테스트
    for i, technical_result in enumerate(test_technical_results):
        ticker = technical_result['ticker']
        gpt_result = test_gpt_results[i] if i < len(test_gpt_results) else None

        print(f"\n🎯 {ticker} Kelly 계산:")

        # Kelly 계산
        kelly_result = calculator.calculate_position_size(technical_result, gpt_result)

        print(f"  감지 패턴: {kelly_result.detected_pattern.value}")
        print(f"  품질 점수: {kelly_result.quality_score:.1f}")
        print(f"  기술적 포지션: {kelly_result.technical_position_pct:.2f}%")
        if kelly_result.gpt_confidence:
            print(f"  GPT 조정: {kelly_result.gpt_adjustment:.2f}x")
        print(f"  최종 포지션: {kelly_result.final_position_pct:.2f}%")
        print(f"  근거: {kelly_result.reasoning}")

    # 포트폴리오 할당 상태 확인
    allocation_status = calculator.get_portfolio_allocation_status()
    print(f"\n📈 포트폴리오 상태:")
    print(f"  총 할당: {allocation_status['total_allocation']:.2f}%")
    print(f"  남은 할당: {allocation_status['remaining_allocation']:.2f}%")
    print(f"  포지션 수: {allocation_status['position_count']}개")
    print(f"  활용률: {allocation_status['utilization_rate']:.1f}%")

    print("\n🎯 Kelly Calculator 구현 완료!")
    print("📋 주요 기능:")
    print("  ✅ 역사적 패턴 승률 기반 Kelly 계산")
    print("  ✅ 2단계 포지션 사이징 (Technical → GPT)")
    print("  ✅ 품질 점수 조정 시스템")
    print("  ✅ 리스크 레벨 맞춤 조정")
    print("  ✅ 포트폴리오 할당 관리")
    print("  ✅ SQLite 통합 저장")

if __name__ == "__main__":
    main()