#!/usr/bin/env python3
"""
통합 추세 필터링 시스템
기존 hybrid_technical_filter.py와 새로운 advanced_trend_analyzer.py를 통합하는 인터페이스

🎯 목적:
- 기존 makenaide 파이프라인과의 호환성 유지
- 새로운 고도화 필터링 시스템 점진적 도입
- 성능 비교 및 검증 기능 제공
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from hybrid_technical_filter import HybridTechnicalFilter
from advanced_trend_analyzer import AdvancedTrendAnalyzer
import logging
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import json

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class IntegratedResult:
    """통합 분석 결과"""
    ticker: str

    # 기존 시스템 결과
    legacy_stage: int
    legacy_quality: float
    legacy_recommendation: str
    legacy_confidence: float

    # 새 시스템 결과
    new_stage: int
    new_quality: float
    new_recommendation: str
    new_confidence: float
    new_rs_rating: int
    new_volume_pattern: str

    # 통합 결정
    final_recommendation: str
    final_confidence: float
    decision_rationale: str

    # 메타 정보
    data_quality_score: float  # 데이터 품질 점수 (0-1)
    analysis_mode: str  # LEGACY_ONLY, NEW_ONLY, INTEGRATED


class IntegratedTrendFilter:
    """통합 추세 필터링 시스템"""

    def __init__(self,
                 mode: str = "auto",  # auto, legacy, new, integrated
                 db_path: str = "./makenaide_local.db"):
        """
        Args:
            mode: 필터링 모드
                - auto: 자동 선택 (데이터 품질에 따라)
                - legacy: 기존 시스템만 사용
                - new: 새 시스템만 사용
                - integrated: 두 시스템 결과 통합
        """
        self.mode = mode
        self.db_path = db_path

        # 시스템 초기화
        self.legacy_filter = HybridTechnicalFilter()
        self.advanced_analyzer = AdvancedTrendAnalyzer(db_path)

        # 성능 통계
        self.stats = {
            'total_analyzed': 0,
            'legacy_used': 0,
            'new_used': 0,
            'integrated_used': 0,
            'data_quality_issues': 0
        }

        logger.info(f"🚀 IntegratedTrendFilter 초기화 완료 (모드: {mode})")

    def analyze_ticker(self, ticker: str) -> IntegratedResult:
        """ticker 통합 분석"""
        logger.info(f"🔍 {ticker} 통합 분석 시작")

        self.stats['total_analyzed'] += 1

        # 1. 데이터 품질 평가
        data_quality = self._assess_data_quality(ticker)

        # 2. 기존 시스템 분석
        legacy_result = self._analyze_with_legacy(ticker)

        # 3. 새 시스템 분석 (데이터 품질에 따라)
        new_result = self._analyze_with_new_system(ticker, data_quality)

        # 4. 모드에 따른 최종 결정
        integrated_result = self._make_integrated_decision(
            ticker, legacy_result, new_result, data_quality
        )

        logger.info(f"📊 {ticker} 통합 분석 완료: {integrated_result.final_recommendation}")
        return integrated_result

    def _assess_data_quality(self, ticker: str) -> float:
        """데이터 품질 평가 (0.0 ~ 1.0)"""
        try:
            # AdvancedTrendAnalyzer의 데이터 로드 시도
            df = self.advanced_analyzer._get_ohlcv_data(ticker)

            if df.empty:
                return 0.0

            # 기술적 지표 완성도 확인
            ma200_completeness = df['ma200'].notna().sum() / len(df)
            rsi_completeness = df['rsi'].notna().sum() / len(df)
            volume_completeness = df['volume'].notna().sum() / len(df)

            # 데이터 기간 적절성 확인
            data_period_score = min(1.0, len(df) / 200)  # 200일 기준

            # 종합 품질 점수
            quality_score = (
                ma200_completeness * 0.4 +  # MA200이 가장 중요
                rsi_completeness * 0.3 +
                volume_completeness * 0.2 +
                data_period_score * 0.1
            )

            logger.debug(f"📊 {ticker} 데이터 품질: {quality_score:.2f}")
            return quality_score

        except Exception as e:
            logger.warning(f"⚠️ {ticker} 데이터 품질 평가 실패: {e}")
            return 0.0

    def _analyze_with_legacy(self, ticker: str) -> Dict:
        """기존 시스템으로 분석"""
        try:
            result = self.legacy_filter.analyze_ticker(ticker)
            if result:
                stage_result, gate_result = result
                return {
                    'success': True,
                    'stage': stage_result.current_stage,
                    'quality': gate_result.quality_score,
                    'recommendation': gate_result.recommendation,
                    'confidence': stage_result.stage_confidence,
                    'raw_result': result
                }
            else:
                return {'success': False, 'reason': 'No result'}

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Legacy 분석 실패: {e}")
            return {'success': False, 'reason': str(e)}

    def _analyze_with_new_system(self, ticker: str, data_quality: float) -> Dict:
        """새 시스템으로 분석"""
        # 데이터 품질이 너무 낮으면 스킵
        if data_quality < 0.3:  # 30% 이하면 신뢰도 부족
            return {
                'success': False,
                'reason': f'데이터 품질 부족 ({data_quality:.2f})'
            }

        try:
            result = self.advanced_analyzer.analyze_ticker(ticker)
            return {
                'success': True,
                'stage': result.stage,
                'quality': result.quality_score,
                'recommendation': result.action,
                'confidence': result.confidence,
                'rs_rating': result.rs_rating,
                'volume_pattern': result.volume_pattern,
                'raw_result': result
            }

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Advanced 분석 실패: {e}")
            return {'success': False, 'reason': str(e)}

    def _make_integrated_decision(self,
                                  ticker: str,
                                  legacy_result: Dict,
                                  new_result: Dict,
                                  data_quality: float) -> IntegratedResult:
        """통합 의사결정"""

        # 기본값 설정
        legacy_data = {
            'stage': legacy_result.get('stage', 1),
            'quality': legacy_result.get('quality', 0.0),
            'recommendation': legacy_result.get('recommendation', 'AVOID'),
            'confidence': legacy_result.get('confidence', 0.0)
        }

        new_data = {
            'stage': new_result.get('stage', 1),
            'quality': new_result.get('quality', 0.0),
            'recommendation': new_result.get('recommendation', 'AVOID'),
            'confidence': new_result.get('confidence', 0.0),
            'rs_rating': new_result.get('rs_rating', 0),
            'volume_pattern': new_result.get('volume_pattern', 'NORMAL')
        }

        # 모드별 결정 로직
        if self.mode == "legacy" or not new_result['success']:
            # Legacy 시스템만 사용
            final_recommendation = legacy_data['recommendation']
            final_confidence = legacy_data['confidence']
            decision_rationale = "Legacy 시스템 사용"
            analysis_mode = "LEGACY_ONLY"
            self.stats['legacy_used'] += 1

        elif self.mode == "new" and new_result['success']:
            # 새 시스템만 사용
            final_recommendation = new_data['recommendation']
            final_confidence = new_data['confidence']
            decision_rationale = "고도화 시스템 사용"
            analysis_mode = "NEW_ONLY"
            self.stats['new_used'] += 1

        elif self.mode == "integrated" and new_result['success']:
            # 두 시스템 결과 통합
            final_recommendation, final_confidence, decision_rationale = \
                self._integrate_recommendations(legacy_data, new_data, data_quality)
            analysis_mode = "INTEGRATED"
            self.stats['integrated_used'] += 1

        else:  # auto 모드
            # 데이터 품질에 따른 자동 선택
            if data_quality >= 0.7 and new_result['success']:
                # 고품질 데이터 -> 새 시스템 우선
                final_recommendation = new_data['recommendation']
                final_confidence = new_data['confidence']
                decision_rationale = f"고품질 데이터 (품질: {data_quality:.2f}) -> 고도화 시스템"
                analysis_mode = "NEW_ONLY"
                self.stats['new_used'] += 1

            elif data_quality >= 0.5 and new_result['success']:
                # 중간 품질 -> 통합 결정
                final_recommendation, final_confidence, decision_rationale = \
                    self._integrate_recommendations(legacy_data, new_data, data_quality)
                analysis_mode = "INTEGRATED"
                self.stats['integrated_used'] += 1

            else:
                # 저품질 데이터 -> Legacy 시스템
                final_recommendation = legacy_data['recommendation']
                final_confidence = legacy_data['confidence']
                decision_rationale = f"저품질 데이터 (품질: {data_quality:.2f}) -> Legacy 시스템"
                analysis_mode = "LEGACY_ONLY"
                self.stats['legacy_used'] += 1
                self.stats['data_quality_issues'] += 1

        return IntegratedResult(
            ticker=ticker,
            legacy_stage=legacy_data['stage'],
            legacy_quality=legacy_data['quality'],
            legacy_recommendation=legacy_data['recommendation'],
            legacy_confidence=legacy_data['confidence'],
            new_stage=new_data['stage'],
            new_quality=new_data['quality'],
            new_recommendation=new_data['recommendation'],
            new_confidence=new_data['confidence'],
            new_rs_rating=new_data['rs_rating'],
            new_volume_pattern=new_data['volume_pattern'],
            final_recommendation=final_recommendation,
            final_confidence=final_confidence,
            decision_rationale=decision_rationale,
            data_quality_score=data_quality,
            analysis_mode=analysis_mode
        )

    def _integrate_recommendations(self,
                                   legacy_data: Dict,
                                   new_data: Dict,
                                   data_quality: float) -> Tuple[str, float, str]:
        """두 시스템의 추천사항 통합"""

        legacy_rec = legacy_data['recommendation']
        new_rec = new_data['recommendation']
        legacy_conf = legacy_data['confidence']
        new_conf = new_data['confidence']

        # 데이터 품질에 따른 가중치
        new_weight = data_quality  # 0.5 ~ 1.0
        legacy_weight = 1.0 - new_weight

        # 추천사항 통합 로직
        if legacy_rec == new_rec:
            # 두 시스템이 일치 -> 신뢰도 높음
            final_recommendation = legacy_rec
            final_confidence = (legacy_conf * legacy_weight + new_conf * new_weight)
            rationale = f"두 시스템 일치 ({legacy_rec})"

        elif legacy_rec == "BUY" or new_rec == "BUY":
            # 한쪽이 BUY -> 보수적으로 HOLD
            final_recommendation = "HOLD"
            final_confidence = 0.6  # 중간 신뢰도
            rationale = f"시스템 불일치 (L:{legacy_rec}, N:{new_rec}) -> 보수적 HOLD"

        else:
            # 둘 다 AVOID/HOLD -> 더 보수적인 것 선택
            final_recommendation = "AVOID"
            final_confidence = (legacy_conf * legacy_weight + new_conf * new_weight)
            rationale = f"보수적 선택 (L:{legacy_rec}, N:{new_rec})"

        return final_recommendation, final_confidence, rationale

    def get_statistics(self) -> Dict:
        """성능 통계 반환"""
        total = self.stats['total_analyzed']
        if total == 0:
            return self.stats

        return {
            **self.stats,
            'legacy_ratio': self.stats['legacy_used'] / total,
            'new_ratio': self.stats['new_used'] / total,
            'integrated_ratio': self.stats['integrated_used'] / total,
            'data_quality_issue_ratio': self.stats['data_quality_issues'] / total
        }

    def analyze_multiple_tickers(self, tickers: List[str]) -> List[IntegratedResult]:
        """여러 ticker 일괄 분석"""
        logger.info(f"🚀 {len(tickers)}개 ticker 일괄 분석 시작")

        results = []
        for i, ticker in enumerate(tickers, 1):
            try:
                result = self.analyze_ticker(ticker)
                results.append(result)
                logger.info(f"✅ [{i}/{len(tickers)}] {ticker}: {result.final_recommendation}")

            except Exception as e:
                logger.error(f"❌ [{i}/{len(tickers)}] {ticker} 분석 실패: {e}")

        # 통계 요약
        stats = self.get_statistics()
        logger.info(f"📊 분석 완료: Legacy {stats['legacy_used']}개, "
                   f"New {stats['new_used']}개, Integrated {stats['integrated_used']}개")

        return results


def test_integrated_filter():
    """통합 필터 테스트"""
    print("🧪 IntegratedTrendFilter 테스트")
    print("=" * 50)

    # 1. 다양한 모드 테스트
    modes = ["auto", "legacy", "integrated"]
    test_tickers = ['KRW-1INCH', 'KRW-ADA', 'KRW-AAVE']

    for mode in modes:
        print(f"\n📊 {mode.upper()} 모드 테스트:")
        filter_system = IntegratedTrendFilter(mode=mode)

        for ticker in test_tickers:
            try:
                result = filter_system.analyze_ticker(ticker)
                print(f"   {ticker}: {result.final_recommendation} "
                      f"(신뢰도: {result.final_confidence:.2f}, "
                      f"모드: {result.analysis_mode})")
                print(f"     이유: {result.decision_rationale}")

            except Exception as e:
                print(f"   {ticker}: 분석 실패 - {e}")

        # 통계 출력
        stats = filter_system.get_statistics()
        print(f"   📈 통계: Legacy {stats['legacy_used']}개, "
              f"New {stats['new_used']}개, Integrated {stats['integrated_used']}개")

    print("\n✅ 통합 필터 테스트 완료!")


if __name__ == "__main__":
    test_integrated_filter()