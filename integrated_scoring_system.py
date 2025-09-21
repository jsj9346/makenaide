#!/usr/bin/env python3
"""
Makenaide Integrated Scoring System
LayeredScoringEngine을 makenaide_local.py에 통합하기 위한 통합 모듈

🎯 핵심 목표:
- Phase 2 기술적 분석을 LayeredScoringEngine으로 완전 교체
- hybrid_technical_filter.py와 호환성 유지
- SQLite 데이터베이스 구조 유지
- 기존 makenaide_local.py 파이프라인에 최소 변경으로 통합
"""

import sys
import os
import asyncio
import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass

# 로컬 모듈 import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from layered_scoring_engine import LayeredScoringEngine, LayerType
from basic_scoring_modules import *
from adaptive_scoring_config import AdaptiveScoringManager, MarketRegime, InvestorProfile

@dataclass
class IntegratedFilterResult:
    """통합 필터링 결과 - 기존 hybrid_technical_filter와 호환"""
    ticker: str
    stage: int
    total_score: float
    quality_score: float  # 기존 시스템 호환용
    recommendation: str
    confidence: float

    # LayeredScoringEngine 결과
    macro_score: float
    structural_score: float
    micro_score: float
    quality_gates_passed: bool

    # 상세 분석 정보
    details: Dict
    analysis_timestamp: datetime

class IntegratedScoringSystem:
    """
    LayeredScoringEngine을 기존 makenaide 시스템에 통합하는 메인 클래스

    🔄 기존 hybrid_technical_filter.py 완전 교체
    📊 SQLite 기반 데이터 처리
    ⚡ 비동기 병렬 처리 지원
    🎯 적응형 임계값 동적 조정
    """

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.scoring_engine = LayeredScoringEngine(db_path)
        self.adaptive_manager = AdaptiveScoringManager()
        self.current_market_regime = MarketRegime.SIDEWAYS
        self.investor_profile = InvestorProfile.MODERATE

        # LayeredScoringEngine 모듈 등록
        self._setup_scoring_modules()

    def _setup_scoring_modules(self):
        """점수 모듈 등록 및 설정"""
        print("📦 LayeredScoringEngine 모듈 등록 중...")

        # Macro Layer 모듈들 (25점)
        self.scoring_engine.register_module(MarketRegimeModule())     # 5점
        self.scoring_engine.register_module(VolumeProfileModule())    # 10점
        self.scoring_engine.register_module(PriceActionModule())      # 10점

        # Structural Layer 모듈들 (45점)
        self.scoring_engine.register_module(StageAnalysisModule())    # 15점
        self.scoring_engine.register_module(MovingAverageModule())    # 15점
        self.scoring_engine.register_module(RelativeStrengthModule()) # 15점

        # Micro Layer 모듈들 (30점)
        self.scoring_engine.register_module(PatternRecognitionModule()) # 10점
        self.scoring_engine.register_module(VolumeSpikeModule())         # 10점
        self.scoring_engine.register_module(MomentumModule())            # 10점

        stats = self.scoring_engine.get_statistics()
        print(f"✅ 등록 완료: {stats}")

    def update_market_conditions(self, market_regime: MarketRegime = None,
                                investor_profile: InvestorProfile = None):
        """시장 상황 및 투자 성향 업데이트"""
        if market_regime:
            self.current_market_regime = market_regime
            print(f"🌡️ 시장 상황 업데이트: {market_regime.value}")

        if investor_profile:
            self.investor_profile = investor_profile
            print(f"👤 투자 성향 업데이트: {investor_profile.value}")

    def get_adaptive_thresholds(self) -> Dict[str, float]:
        """현재 시장 상황에 맞는 적응형 임계값 조회"""
        return self.adaptive_manager.get_adaptive_thresholds(
            self.current_market_regime,
            self.investor_profile
        )

    async def analyze_ticker(self, ticker: str) -> Optional[IntegratedFilterResult]:
        """
        단일 ticker 분석 - hybrid_technical_filter.analyze_ticker 교체

        Args:
            ticker: 분석할 ticker 코드

        Returns:
            IntegratedFilterResult 또는 None (분석 실패 시)
        """
        try:
            # LayeredScoringEngine 분석 실행
            scoring_result = await self.scoring_engine.analyze_ticker(ticker)

            if not scoring_result:
                return None

            # 적응형 임계값 적용
            thresholds = self.get_adaptive_thresholds()
            pass_threshold = thresholds['pass_threshold']
            buy_threshold = thresholds['buy_threshold']

            # 추천사항 결정
            if scoring_result.total_score >= buy_threshold:
                recommendation = "BUY"
            elif scoring_result.total_score >= pass_threshold:
                recommendation = "WATCH"
            else:
                recommendation = "AVOID"

            # Stage 정보 추출
            stage_module_result = None
            for layer_result in scoring_result.layer_results.values():
                for module_result in layer_result.module_results:
                    if module_result.module_name == "StageAnalysisModule":
                        stage_module_result = module_result
                        break

            current_stage = stage_module_result.details.get('current_stage', 1) if stage_module_result else 1

            # Layer별 점수 추출
            macro_score = scoring_result.layer_results.get(LayerType.MACRO, type('obj', (object,), {'score': 0})).score
            structural_score = scoring_result.layer_results.get(LayerType.STRUCTURAL, type('obj', (object,), {'score': 0})).score
            micro_score = scoring_result.layer_results.get(LayerType.MICRO, type('obj', (object,), {'score': 0})).score

            # 통합 결과 생성
            result = IntegratedFilterResult(
                ticker=ticker,
                stage=current_stage,
                total_score=scoring_result.total_score,
                quality_score=scoring_result.total_score,  # 호환성을 위해 동일 값 사용
                recommendation=recommendation,
                confidence=scoring_result.confidence,
                macro_score=macro_score,
                structural_score=structural_score,
                micro_score=micro_score,
                quality_gates_passed=scoring_result.quality_gates_passed,
                details=self._extract_details(scoring_result),
                analysis_timestamp=datetime.now()
            )

            return result

        except Exception as e:
            print(f"❌ {ticker} 분석 실패: {e}")
            return None

    def _extract_details(self, scoring_result) -> Dict:
        """LayeredScoringEngine 결과에서 상세 정보 추출"""
        details = {
            'total_score': scoring_result.total_score,
            'confidence': scoring_result.confidence,
            'quality_gates_passed': scoring_result.quality_gates_passed,
            'layers': {}
        }

        for layer_type, layer_result in scoring_result.layer_results.items():
            details['layers'][layer_type.value] = {
                'score': layer_result.score,
                'max_score': layer_result.max_score,
                'score_percentage': layer_result.score_percentage,
                'modules': {}
            }

            for module_result in layer_result.module_results:
                details['layers'][layer_type.value]['modules'][module_result.module_name] = {
                    'score': module_result.score,
                    'confidence': module_result.confidence,
                    'execution_time': module_result.execution_time,
                    'details': module_result.details
                }

        return details

    async def analyze_multiple_tickers(self, tickers: List[str],
                                     max_concurrent: int = 10) -> List[IntegratedFilterResult]:
        """
        다중 ticker 병렬 분석 - hybrid_technical_filter 대체

        Args:
            tickers: 분석할 ticker 리스트
            max_concurrent: 최대 동시 실행 개수

        Returns:
            성공한 분석 결과 리스트
        """
        print(f"📊 {len(tickers)}개 ticker 병렬 분석 시작...")

        # 세마포어로 동시 실행 제한
        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_with_semaphore(ticker):
            async with semaphore:
                return await self.analyze_ticker(ticker)

        # 병렬 실행
        results = await asyncio.gather(
            *[analyze_with_semaphore(ticker) for ticker in tickers],
            return_exceptions=True
        )

        # 성공한 결과만 필터링
        successful_results = []
        for result in results:
            if isinstance(result, IntegratedFilterResult):
                successful_results.append(result)
            elif isinstance(result, Exception):
                print(f"❌ 분석 예외: {result}")

        print(f"✅ 분석 완료: {len(successful_results)}/{len(tickers)} 성공")
        return successful_results

    async def run_full_analysis(self) -> List[IntegratedFilterResult]:
        """
        전체 분석 실행 - hybrid_technical_filter.run_full_analysis() 대체

        모든 활성 ticker에 대해 점수제 분석을 실행하고
        결과를 DB에 저장한 후 반환
        """
        try:
            # 활성 ticker 목록 조회
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT DISTINCT ticker
                FROM ohlcv_data
                WHERE date >= date('now', '-7 days')
                AND close IS NOT NULL
                AND volume IS NOT NULL
                ORDER BY ticker
            """
            cursor = conn.execute(query)
            tickers = [row[0] for row in cursor.fetchall()]
            conn.close()

            if not tickers:
                print("⚠️ 분석할 ticker가 없습니다")
                return []

            print(f"🔍 전체 분석 시작: {len(tickers)}개 ticker")

            # 병렬 분석 실행
            results = await self.analyze_multiple_tickers(tickers)

            # 결과 DB 저장
            if results:
                self.save_results_to_db(results)

            return results

        except Exception as e:
            print(f"❌ 전체 분석 실패: {e}")
            return []

    def save_results_to_db(self, results: List[IntegratedFilterResult]):
        """
        분석 결과를 SQLite DB에 저장
        기존 makenaide_technical_analysis 테이블과 호환
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 통합 technical_analysis 테이블 사용 (Phase 3 통합 완료)
            # HybridTechnicalFilter 데이터 보존하며 LayeredScoring 컬럼만 업데이트

            # LayeredScoring 결과를 통합 technical_analysis 테이블에 UPSERT
            for result in results:
                # 1. 기존 레코드 확인
                cursor.execute("""
                    SELECT ticker FROM technical_analysis
                    WHERE ticker = ? AND analysis_date = DATE('now', '+9 hours')
                """, (result.ticker,))

                existing_record = cursor.fetchone()

                if existing_record:
                    # 2-A. 기존 레코드가 있으면 LayeredScoring 컬럼만 UPDATE
                    cursor.execute("""
                        UPDATE technical_analysis SET
                            macro_score = ?,
                            structural_score = ?,
                            micro_score = ?,
                            total_score = ?,
                            quality_gates_passed = ?,
                            analysis_details = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE ticker = ? AND analysis_date = DATE('now', '+9 hours')
                    """, (
                        result.macro_score,
                        result.structural_score,
                        result.micro_score,
                        result.total_score,
                        result.quality_gates_passed,
                        str(result.details),
                        result.ticker
                    ))
                else:
                    # 2-B. 새 레코드면 INSERT (Weinstein Stage 데이터 없이 LayeredScoring만)
                    cursor.execute("""
                        INSERT INTO technical_analysis (
                            ticker, analysis_date,
                            macro_score, structural_score, micro_score, total_score,
                            quality_gates_passed, analysis_details,
                            source_table, created_at, updated_at
                        ) VALUES (?, DATE('now', '+9 hours'), ?, ?, ?, ?, ?, ?, 'integrated_scoring_system', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (
                        result.ticker,
                        result.macro_score,
                        result.structural_score,
                        result.micro_score,
                        result.total_score,
                        result.quality_gates_passed,
                        str(result.details)
                    ))

            conn.commit()
            print(f"💾 {len(results)}개 LayeredScoring 결과 통합 테이블 저장 완료")

        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
        finally:
            conn.close()

    def get_filtered_candidates(self, min_score: float = None) -> List[Dict]:
        """
        DB에서 필터링된 매수 후보 조회
        makenaide_local.py에서 거래 대상 선별용
        """
        try:
            conn = sqlite3.connect(self.db_path)

            # 적응형 임계값 사용
            if min_score is None:
                thresholds = self.get_adaptive_thresholds()
                min_score = thresholds['pass_threshold']

            query = """
                SELECT ticker, total_score, recommendation, stage_confidence as confidence,
                       macro_score, structural_score, micro_score,
                       quality_gates_passed, updated_at as analysis_timestamp
                FROM technical_analysis
                WHERE total_score >= ? AND quality_gates_passed = 1
                  AND total_score IS NOT NULL
                ORDER BY total_score DESC
            """

            df = pd.read_sql_query(query, conn, params=(min_score,))
            conn.close()

            candidates = df.to_dict('records')
            print(f"🎯 매수 후보 {len(candidates)}개 조회 (최소 점수: {min_score:.1f})")

            return candidates

        except Exception as e:
            print(f"❌ 후보 조회 실패: {e}")
            return []

    def get_statistics(self) -> Dict:
        """시스템 통계 정보 조회"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 기본 통계
            query = """
                SELECT
                    COUNT(*) as total_analyzed,
                    COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy_count,
                    COUNT(CASE WHEN recommendation = 'WATCH' THEN 1 END) as watch_count,
                    COUNT(CASE WHEN recommendation = 'AVOID' THEN 1 END) as avoid_count,
                    COUNT(CASE WHEN quality_gates_passed = 1 THEN 1 END) as quality_passed,
                    AVG(total_score) as avg_score,
                    MAX(total_score) as max_score,
                    MIN(total_score) as min_score
                FROM technical_analysis
                WHERE DATE(updated_at) = DATE('now', '+9 hours')
                  AND total_score IS NOT NULL
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            if len(df) > 0:
                stats = df.iloc[0].to_dict()

                # 적응형 설정 정보 추가
                thresholds = self.get_adaptive_thresholds()
                stats.update({
                    'market_regime': self.current_market_regime.value,
                    'investor_profile': self.investor_profile.value,
                    'current_thresholds': thresholds
                })

                return stats
            else:
                return {'error': '오늘 분석 데이터 없음'}

        except Exception as e:
            return {'error': f'통계 조회 실패: {e}'}


async def main_integration_test():
    """통합 시스템 테스트"""
    print("🚀 Makenaide Integrated Scoring System 테스트")
    print("=" * 80)

    # 시스템 초기화
    system = IntegratedScoringSystem()

    # 테스트 ticker 조회
    try:
        conn = sqlite3.connect("./makenaide_local.db")
        query = """
            SELECT DISTINCT ticker
            FROM ohlcv_data
            WHERE date >= date('now', '-7 days')
            AND close IS NOT NULL
            AND volume IS NOT NULL
            ORDER BY ticker
            LIMIT 20
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        test_tickers = df['ticker'].tolist()
    except Exception as e:
        print(f"⚠️ SQLite 데이터 조회 실패: {e}")
        test_tickers = ["KRW-BTC", "KRW-ETH", "KRW-ADA"]

    if not test_tickers:
        print("❌ 테스트할 ticker가 없습니다.")
        return

    print(f"🎯 테스트 대상: {test_tickers[:5]} ... (총 {len(test_tickers)}개)")

    # 시장 상황 설정
    system.update_market_conditions(MarketRegime.SIDEWAYS, InvestorProfile.MODERATE)

    # 병렬 분석 실행
    start_time = datetime.now()
    results = await system.analyze_multiple_tickers(test_tickers)
    end_time = datetime.now()

    processing_time = (end_time - start_time).total_seconds()
    avg_time = processing_time / len(test_tickers) if test_tickers else 0

    print(f"\n📊 분석 결과:")
    print(f"   ⚡ 처리 시간: {processing_time:.2f}초 ({avg_time*1000:.1f}ms/ticker)")
    print(f"   ✅ 성공률: {len(results)}/{len(test_tickers)} ({len(results)/len(test_tickers)*100:.1f}%)")

    if results:
        # 결과 저장
        system.save_results_to_db(results)

        # 상위 결과 표시
        results.sort(key=lambda x: x.total_score, reverse=True)
        print(f"\n🏆 상위 5개 결과:")
        print("-" * 80)
        print(f"{'Ticker':<12} {'총점':<8} {'추천':<8} {'Macro':<8} {'Struct':<8} {'Micro':<8} {'Gate'}")
        print("-" * 80)

        for result in results[:5]:
            gate_status = "PASS" if result.quality_gates_passed else "FAIL"
            print(f"{result.ticker:<12} {result.total_score:<8.1f} {result.recommendation:<8} "
                  f"{result.macro_score:<8.1f} {result.structural_score:<8.1f} {result.micro_score:<8.1f} {gate_status}")

        # 매수 후보 조회
        candidates = system.get_filtered_candidates()
        print(f"\n🎯 Quality Gate 통과 매수 후보: {len(candidates)}개")

        # 통계 정보
        stats = system.get_statistics()
        if 'error' not in stats:
            print(f"\n📈 오늘 분석 통계:")
            print(f"   총 분석: {stats.get('total_analyzed', 0)}개")
            print(f"   BUY: {stats.get('buy_count', 0)}개")
            print(f"   WATCH: {stats.get('watch_count', 0)}개")
            print(f"   Quality Gate 통과: {stats.get('quality_passed', 0)}개")
            print(f"   평균 점수: {stats.get('avg_score', 0):.1f}")
            print(f"   현재 임계값: 통과 {stats['current_thresholds']['pass_threshold']:.1f}, "
                  f"매수 {stats['current_thresholds']['buy_threshold']:.1f}")

    print("\n✅ 통합 시스템 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main_integration_test())