#!/usr/bin/env python3
"""
기존 시스템 vs 새로운 점수제 시스템 비교 분석
성능, 정확도, 통과율, 처리 속도 등 종합 비교

🎯 비교 목표:
1. 통과율 및 추천사항 분포 비교
2. 처리 성능 및 속도 비교
3. 분석 품질 및 상세도 비교
4. 실제 거래 적합성 평가
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from layered_scoring_engine import LayeredScoringEngine
from basic_scoring_modules import *
from hybrid_technical_filter import HybridTechnicalFilter
import asyncio
import sqlite3
import pandas as pd
from datetime import datetime
import time
from typing import Dict, List, Any

async def compare_scoring_systems():
    """기존 vs 새로운 시스템 종합 비교"""
    print("🔬 기존 시스템 vs 새로운 점수제 시스템 종합 비교")
    print("=" * 80)

    # 1. 시스템 초기화
    print("📊 1. 시스템 초기화...")

    # 기존 시스템
    legacy_filter = HybridTechnicalFilter()

    # 새로운 시스템
    scoring_engine = LayeredScoringEngine()

    # 모듈 등록
    scoring_engine.register_module(MarketRegimeModule())
    scoring_engine.register_module(VolumeProfileModule())
    scoring_engine.register_module(PriceActionModule())
    scoring_engine.register_module(StageAnalysisModule())
    scoring_engine.register_module(MovingAverageModule())
    scoring_engine.register_module(RelativeStrengthModule())
    scoring_engine.register_module(PatternRecognitionModule())
    scoring_engine.register_module(VolumeSpikeModule())
    scoring_engine.register_module(MomentumModule())

    # 2. 테스트 데이터 준비
    print("📊 2. 테스트 데이터 준비...")
    test_tickers = get_test_tickers(limit=30)  # 30개 ticker로 테스트

    if len(test_tickers) < 10:
        print("⚠️ 충분한 테스트 데이터가 없습니다.")
        return

    print(f"🎯 테스트 대상: {len(test_tickers)}개 ticker")

    # 3. 기존 시스템 성능 테스트
    print(f"\n📊 3. 기존 시스템 (HybridTechnicalFilter) 테스트...")
    legacy_results = []
    legacy_start_time = time.time()

    for ticker in test_tickers:
        try:
            result = legacy_filter.analyze_ticker(ticker)
            if result:
                stage_result, gate_result = result
                legacy_results.append({
                    'ticker': ticker,
                    'stage': stage_result.current_stage,
                    'quality': gate_result.quality_score,
                    'recommendation': gate_result.recommendation,
                    'confidence': stage_result.stage_confidence,
                    'success': True
                })
            else:
                legacy_results.append({
                    'ticker': ticker,
                    'success': False,
                    'recommendation': 'AVOID'
                })
        except Exception as e:
            legacy_results.append({
                'ticker': ticker,
                'success': False,
                'error': str(e),
                'recommendation': 'AVOID'
            })

    legacy_end_time = time.time()
    legacy_duration = legacy_end_time - legacy_start_time

    # 4. 새로운 시스템 성능 테스트
    print(f"📊 4. 새로운 시스템 (LayeredScoringEngine) 테스트...")
    scoring_start_time = time.time()

    scoring_results_dict = await scoring_engine.analyze_multiple_tickers(test_tickers)
    scoring_results = list(scoring_results_dict.values())

    scoring_end_time = time.time()
    scoring_duration = scoring_end_time - scoring_start_time

    # 5. 결과 비교 분석
    print(f"\n📈 5. 결과 비교 분석:")
    print("=" * 80)

    # 5.1 처리 성능 비교
    print(f"⚡ 처리 성능 비교:")
    print(f"   기존 시스템: {legacy_duration:.2f}초 ({legacy_duration/len(test_tickers)*1000:.1f}ms/ticker)")
    print(f"   새 시스템:  {scoring_duration:.2f}초 ({scoring_duration/len(test_tickers)*1000:.1f}ms/ticker)")

    speed_improvement = legacy_duration / scoring_duration if scoring_duration > 0 else float('inf')
    print(f"   속도 개선:  {speed_improvement:.1f}배 빠름")

    # 5.2 성공률 비교
    legacy_success = sum(1 for r in legacy_results if r.get('success', False))
    scoring_success = sum(1 for r in scoring_results if r.total_score > 0)

    print(f"\n✅ 처리 성공률:")
    print(f"   기존 시스템: {legacy_success}/{len(test_tickers)} ({legacy_success/len(test_tickers)*100:.1f}%)")
    print(f"   새 시스템:  {scoring_success}/{len(test_tickers)} ({scoring_success/len(test_tickers)*100:.1f}%)")

    # 5.3 추천사항 분포 비교
    legacy_recommendations = {}
    scoring_recommendations = {}

    for result in legacy_results:
        rec = result.get('recommendation', 'AVOID')
        legacy_recommendations[rec] = legacy_recommendations.get(rec, 0) + 1

    for result in scoring_results:
        rec = result.recommendation
        scoring_recommendations[rec] = scoring_recommendations.get(rec, 0) + 1

    print(f"\n🎯 추천사항 분포:")
    print(f"   기존 시스템: {legacy_recommendations}")
    print(f"   새 시스템:  {scoring_recommendations}")

    # 5.4 품질 점수 비교
    legacy_qualities = [r.get('quality', 0) for r in legacy_results if r.get('success', False)]
    scoring_qualities = [r.total_score for r in scoring_results if r.total_score > 0]

    if legacy_qualities and scoring_qualities:
        print(f"\n📊 품질 점수 비교:")
        print(f"   기존 시스템 평균: {sum(legacy_qualities)/len(legacy_qualities):.1f}")
        print(f"   새 시스템 평균:  {sum(scoring_qualities)/len(scoring_qualities):.1f}")

    # 6. 상세 비교 테이블
    print(f"\n📋 6. 상세 비교 테이블:")
    print("-" * 120)
    print(f"{'Ticker':<12} {'Legacy':<20} {'Scoring':<20} {'Legacy점수':<12} {'Scoring점수':<12} {'개선도'}")
    print("-" * 120)

    for i, ticker in enumerate(test_tickers[:15]):  # 처음 15개만 표시
        legacy_result = legacy_results[i] if i < len(legacy_results) else {}
        scoring_result = scoring_results[i] if i < len(scoring_results) else None

        legacy_rec = legacy_result.get('recommendation', 'ERROR')
        legacy_quality = legacy_result.get('quality', 0)

        if scoring_result:
            scoring_rec = scoring_result.recommendation
            scoring_quality = scoring_result.total_score

            # 개선도 계산 (새 시스템이 더 세부적인 분석 제공)
            improvement = "✅" if scoring_quality > legacy_quality else "➖"
        else:
            scoring_rec = 'ERROR'
            scoring_quality = 0
            improvement = "❌"

        print(f"{ticker:<12} {legacy_rec:<20} {scoring_rec:<20} {legacy_quality:<12.1f} {scoring_quality:<12.1f} {improvement}")

    # 7. 종합 평가
    print("-" * 120)
    print(f"\n📊 7. 종합 평가:")

    print(f"\n🚀 새로운 점수제 시스템의 장점:")
    print(f"   ✅ 처리 속도: {speed_improvement:.1f}배 향상")
    print(f"   ✅ 성공률: {scoring_success/len(test_tickers)*100:.1f}% (vs {legacy_success/len(test_tickers)*100:.1f}%)")
    print(f"   ✅ 상세 분석: Layer별 세부 점수 제공")
    print(f"   ✅ 확장성: 모듈 기반 플러그인 구조")
    print(f"   ✅ 유연성: 동적 가중치 조정 가능")

    # Quality Gate 기준으로 실제 거래 가능한 종목 찾기
    quality_passed_scoring = [r for r in scoring_results if r.quality_gates_passed]

    print(f"\n🎯 거래 적합성 분석:")
    print(f"   Quality Gate 통과 (새 시스템): {len(quality_passed_scoring)}개")

    if quality_passed_scoring:
        print(f"   통과 종목 예시:")
        for result in quality_passed_scoring[:3]:
            print(f"     {result.ticker}: {result.total_score:.1f}점, {result.recommendation}")

    # 8. 기준 조정 제안
    suggest_threshold_adjustment(scoring_results)


def suggest_threshold_adjustment(scoring_results: List):
    """Quality Gate 기준 조정 제안"""
    print(f"\n🔧 8. Quality Gate 기준 조정 제안:")

    scores = [r.total_score for r in scoring_results]
    scores.sort(reverse=True)

    # 상위 20% 기준으로 새로운 임계값 제안
    if len(scores) >= 5:
        top_20_percent_index = max(1, len(scores) // 5)
        suggested_threshold = scores[top_20_percent_index]

        print(f"   📊 현재 점수 분포: 최고 {max(scores):.1f}, 평균 {sum(scores)/len(scores):.1f}, 최저 {min(scores):.1f}")
        print(f"   🎯 상위 20% 기준: {suggested_threshold:.1f}점")
        print(f"   💡 제안: Quality Gate 총점 기준을 60점 → {suggested_threshold:.0f}점으로 조정")

        # 조정된 기준으로 통과율 계산
        adjusted_pass = sum(1 for score in scores if score >= suggested_threshold)
        print(f"   📈 조정 시 통과율: {adjusted_pass}/{len(scores)} ({adjusted_pass/len(scores)*100:.1f}%)")


def get_test_tickers(limit: int = 50) -> List[str]:
    """테스트용 ticker 목록 조회"""
    try:
        conn = sqlite3.connect("./makenaide_local.db")

        query = """
        SELECT DISTINCT ticker
        FROM ohlcv_data
        WHERE date >= date('now', '-7 days')
        AND close IS NOT NULL
        AND volume IS NOT NULL
        AND ma20 IS NOT NULL
        ORDER BY ticker
        LIMIT ?
        """

        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()

        return df['ticker'].tolist()

    except Exception as e:
        print(f"⚠️ 테스트 데이터 조회 실패: {e}")
        return []


async def analyze_top_scoring_tickers():
    """상위 점수 ticker 상세 분석"""
    print(f"\n🏆 상위 점수 ticker 상세 분석")
    print("=" * 60)

    # 시스템 초기화
    scoring_engine = LayeredScoringEngine()

    # 모듈 등록
    scoring_engine.register_module(MarketRegimeModule())
    scoring_engine.register_module(VolumeProfileModule())
    scoring_engine.register_module(PriceActionModule())
    scoring_engine.register_module(StageAnalysisModule())
    scoring_engine.register_module(MovingAverageModule())
    scoring_engine.register_module(RelativeStrengthModule())
    scoring_engine.register_module(PatternRecognitionModule())
    scoring_engine.register_module(VolumeSpikeModule())
    scoring_engine.register_module(MomentumModule())

    # 전체 ticker 분석
    test_tickers = get_test_tickers(100)
    results_dict = await scoring_engine.analyze_multiple_tickers(test_tickers)
    results = list(results_dict.values())

    # 점수순 정렬
    results.sort(key=lambda x: x.total_score, reverse=True)

    print(f"📊 상위 10개 ticker 상세 분석:")
    print("-" * 100)
    print(f"{'순위':<4} {'Ticker':<12} {'총점':<8} {'Macro':<8} {'Struct':<8} {'Micro':<8} {'Gate':<6} {'추천'}")
    print("-" * 100)

    for i, result in enumerate(results[:10], 1):
        macro_score = result.layer_results.get(LayerType.MACRO, type('obj', (object,), {'score': 0})).score
        structural_score = result.layer_results.get(LayerType.STRUCTURAL, type('obj', (object,), {'score': 0})).score
        micro_score = result.layer_results.get(LayerType.MICRO, type('obj', (object,), {'score': 0})).score

        gate_status = "PASS" if result.quality_gates_passed else "FAIL"

        print(f"{i:<4} {result.ticker:<12} {result.total_score:<8.1f} {macro_score:<8.1f} "
              f"{structural_score:<8.1f} {micro_score:<8.1f} {gate_status:<6} {result.recommendation}")

    # 최고 점수 ticker 상세 분석
    if results:
        top_result = results[0]
        print(f"\n🔍 최고 점수 ticker ({top_result.ticker}) 상세 분석:")

        for layer_type, layer_result in top_result.layer_results.items():
            print(f"\n   📊 {layer_type.value.upper()} Layer ({layer_result.score:.1f}/{layer_result.max_score}):")
            for module_result in layer_result.module_results:
                print(f"     - {module_result.module_name}: {module_result.score:.1f}점")
                if module_result.details:
                    key_details = list(module_result.details.items())[:2]
                    for key, value in key_details:
                        print(f"       └ {key}: {value}")


async def main():
    """메인 실행 함수"""
    await compare_scoring_systems()
    await analyze_top_scoring_tickers()


if __name__ == "__main__":
    asyncio.run(main())