#!/usr/bin/env python3
"""
LayeredScoringEngine - 점수제 기반 추세 필터링 시스템 Phase 1
기존 이진 필터링의 한계를 극복하는 3단계 계층형 점수 시스템

🎯 핵심 설계 철학:
- Binary Filtering (AND Gates) → Weighted Scoring System
- 0% 통과율 → 20% 목표 통과율
- 경직된 임계값 → 적응형 동적 기준
- 단일 모노리스 → 플러그인 기반 모듈러 구조

🏛️ 3-Layer Architecture:
- Layer 1: Macro Analysis (25점) - 거시적 시장 환경
- Layer 2: Structural Analysis (45점) - 구조적 기술 분석
- Layer 3: Micro Triggers (30점) - 미시적 트리거 신호

🔌 Plugin-based Design:
- ScoringModule 추상 인터페이스
- ModuleRegistry 동적 로딩 시스템
- Quality Gates 검증 메커니즘
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, Any, Tuple
from enum import Enum
import pandas as pd
import numpy as np
import sqlite3
import logging
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LayerType(Enum):
    """Layer 타입 정의"""
    MACRO = "macro"           # 거시적 분석 (25점)
    STRUCTURAL = "structural" # 구조적 분석 (45점)
    MICRO = "micro"          # 미시적 분석 (30점)


@dataclass
class ModuleScore:
    """개별 모듈 점수 결과"""
    score: float            # 0-100 점수
    confidence: float       # 0-1 신뢰도
    details: Dict[str, Any] # 세부 분석 정보
    module_name: str        # 모듈명
    execution_time: float = 0.0  # 실행 시간 (ms)

    def __post_init__(self):
        # 점수 범위 검증
        self.score = max(0.0, min(100.0, self.score))
        self.confidence = max(0.0, min(1.0, self.confidence))


@dataclass
class LayerResult:
    """Layer별 점수 결과"""
    layer_type: LayerType
    score: float            # 0-해당층 만점
    max_score: float        # 해당 층 만점
    confidence: float       # 0-1 신뢰도
    module_results: List[ModuleScore]  # 개별 모듈 결과
    quality_gate_passed: bool = False  # Quality Gate 통과 여부
    execution_time: float = 0.0

    @property
    def score_percentage(self) -> float:
        """백분율 점수 (0-100)"""
        return (self.score / self.max_score) * 100 if self.max_score > 0 else 0.0


@dataclass
class ScoringResult:
    """최종 점수 결과"""
    ticker: str
    total_score: float      # 0-100 종합 점수
    layer_results: Dict[LayerType, LayerResult]  # Layer별 결과

    # Quality Gates 결과
    quality_gates_passed: bool = False
    quality_gate_details: Dict[str, bool] = field(default_factory=dict)

    # 메타 정보
    recommendation: str = "AVOID"  # AVOID, HOLD, BUY, STRONG_BUY
    confidence: float = 0.0       # 0-1 종합 신뢰도
    analysis_timestamp: datetime = field(default_factory=datetime.now)
    execution_time: float = 0.0   # 전체 실행 시간 (ms)

    # 세부 정보
    reasons: List[str] = field(default_factory=list)  # 점수 근거
    warnings: List[str] = field(default_factory=list) # 경고사항

    @classmethod
    def create_invalid(cls, ticker: str, error_msg: str) -> 'ScoringResult':
        """오류 발생 시 기본 결과 생성"""
        return cls(
            ticker=ticker,
            total_score=0.0,
            layer_results={},
            recommendation="AVOID",
            confidence=0.0,
            warnings=[f"Analysis failed: {error_msg}"]
        )


class ScoringModule(ABC):
    """점수 모듈 추상 기본 클래스"""

    def __init__(self, name: str, layer_type: LayerType, weight: float):
        self.name = name
        self.layer_type = layer_type
        self.weight = weight  # 해당 Layer 내에서의 가중치

    @abstractmethod
    def calculate_score(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """점수 계산 (동기 버전)"""
        pass

    async def calculate_score_async(self, data: pd.DataFrame, config: Dict[str, Any]) -> ModuleScore:
        """점수 계산 (비동기 버전) - 기본적으로 동기 버전 호출"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, self.calculate_score, data, config)

    def validate_data(self, data: pd.DataFrame) -> Tuple[bool, str]:
        """데이터 유효성 검증"""
        if data.empty:
            return False, "Empty DataFrame"

        required_columns = self.get_required_columns()
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            return False, f"Missing columns: {missing_columns}"

        return True, "Valid"

    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """필수 데이터 컬럼 반환"""
        pass

    def get_config_template(self) -> Dict[str, Any]:
        """모듈 설정 템플릿 반환"""
        return {
            "enabled": True,
            "weight": self.weight,
            "min_data_points": 50
        }


class ModuleRegistry:
    """모듈 등록 및 관리 시스템"""

    def __init__(self):
        self.modules: Dict[LayerType, List[ScoringModule]] = {
            LayerType.MACRO: [],
            LayerType.STRUCTURAL: [],
            LayerType.MICRO: []
        }
        logger.info("🔌 ModuleRegistry 초기화 완료")

    def register_module(self, module: ScoringModule):
        """모듈 등록"""
        self.modules[module.layer_type].append(module)
        logger.info(f"📦 모듈 등록: {module.name} ({module.layer_type.value})")

    def get_modules(self, layer_type: LayerType) -> List[ScoringModule]:
        """특정 Layer의 모듈들 반환"""
        return self.modules[layer_type]

    def get_all_modules(self) -> Dict[LayerType, List[ScoringModule]]:
        """모든 모듈 반환"""
        return self.modules.copy()

    def remove_module(self, layer_type: LayerType, module_name: str):
        """모듈 제거"""
        self.modules[layer_type] = [
            m for m in self.modules[layer_type]
            if m.name != module_name
        ]
        logger.info(f"🗑️ 모듈 제거: {module_name} ({layer_type.value})")


class QualityGateValidator:
    """Quality Gate 검증 시스템"""

    def __init__(self):
        # Layer별 최소 점수 요구사항 (백분율)
        self.min_score_requirements = {
            LayerType.MACRO: 40.0,        # 25점 중 10점 (40%)
            LayerType.STRUCTURAL: 44.4,   # 45점 중 20점 (44.4%)
            LayerType.MICRO: 33.3         # 30점 중 10점 (33.3%)
        }

        # 전체 최소 점수
        self.min_total_score = 60.0  # 100점 중 60점

        logger.info("🚪 QualityGateValidator 초기화 완료")

    def validate_layer(self, layer_result: LayerResult) -> bool:
        """개별 Layer Quality Gate 검증"""
        min_required = self.min_score_requirements[layer_result.layer_type]
        actual_percentage = layer_result.score_percentage

        passed = actual_percentage >= min_required

        logger.debug(f"🚪 {layer_result.layer_type.value} Quality Gate: "
                    f"{actual_percentage:.1f}% >= {min_required:.1f}% = {passed}")

        return passed

    def validate_total(self, total_score: float) -> bool:
        """전체 점수 Quality Gate 검증"""
        passed = total_score >= self.min_total_score

        logger.debug(f"🚪 Total Quality Gate: "
                    f"{total_score:.1f} >= {self.min_total_score:.1f} = {passed}")

        return passed

    def validate_all(self, layer_results: Dict[LayerType, LayerResult],
                    total_score: float) -> Tuple[bool, Dict[str, bool]]:
        """전체 Quality Gate 검증"""
        details = {}

        # Layer별 검증
        for layer_type, layer_result in layer_results.items():
            details[f"{layer_type.value}_gate"] = self.validate_layer(layer_result)

        # 전체 점수 검증
        details["total_score_gate"] = self.validate_total(total_score)

        # 모든 Gate 통과 여부
        all_passed = all(details.values())

        return all_passed, details


class LayerProcessor:
    """Layer별 점수 처리기"""

    def __init__(self, layer_type: LayerType, max_score: float):
        self.layer_type = layer_type
        self.max_score = max_score
        self.modules: List[ScoringModule] = []

    def add_module(self, module: ScoringModule):
        """모듈 추가"""
        if module.layer_type != self.layer_type:
            raise ValueError(f"Layer mismatch: expected {self.layer_type}, got {module.layer_type}")
        self.modules.append(module)

    async def process(self, ticker: str, data: pd.DataFrame,
                     config: Dict[str, Any]) -> LayerResult:
        """Layer 점수 계산"""
        start_time = datetime.now()

        if not self.modules:
            logger.warning(f"⚠️ {self.layer_type.value} Layer에 모듈이 없습니다")
            return LayerResult(
                layer_type=self.layer_type,
                score=0.0,
                max_score=self.max_score,
                confidence=0.0,
                module_results=[]
            )

        # 병렬 모듈 실행
        module_tasks = []
        for module in self.modules:
            if config.get(f"{module.name}_enabled", True):
                task = module.calculate_score_async(data, config)
                module_tasks.append(task)

        try:
            module_results = await asyncio.gather(*module_tasks)
        except Exception as e:
            logger.error(f"❌ {self.layer_type.value} Layer 처리 실패: {e}")
            return LayerResult(
                layer_type=self.layer_type,
                score=0.0,
                max_score=self.max_score,
                confidence=0.0,
                module_results=[]
            )

        # 가중 평균 점수 계산
        total_weight = sum(module.weight for module in self.modules)
        if total_weight == 0:
            weighted_score = 0.0
            avg_confidence = 0.0
        else:
            weighted_score = sum(
                result.score * module.weight
                for result, module in zip(module_results, self.modules)
            ) / total_weight

            avg_confidence = sum(
                result.confidence * module.weight
                for result, module in zip(module_results, self.modules)
            ) / total_weight

        # Layer 점수는 max_score 비율로 변환
        layer_score = (weighted_score / 100.0) * self.max_score

        execution_time = (datetime.now() - start_time).total_seconds() * 1000

        return LayerResult(
            layer_type=self.layer_type,
            score=layer_score,
            max_score=self.max_score,
            confidence=avg_confidence,
            module_results=module_results,
            execution_time=execution_time
        )


class LayeredScoringEngine:
    """메인 점수제 엔진"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path

        # Layer 프로세서 초기화 (점수 배분: 25 + 45 + 30 = 100)
        self.layer_processors = {
            LayerType.MACRO: LayerProcessor(LayerType.MACRO, 25.0),
            LayerType.STRUCTURAL: LayerProcessor(LayerType.STRUCTURAL, 45.0),
            LayerType.MICRO: LayerProcessor(LayerType.MICRO, 30.0)
        }

        # 핵심 컴포넌트
        self.module_registry = ModuleRegistry()
        self.quality_gate_validator = QualityGateValidator()

        # 설정
        self.config = self._load_default_config()

        logger.info("🚀 LayeredScoringEngine 초기화 완료")
        logger.info(f"📊 Layer 점수 배분: Macro(25) + Structural(45) + Micro(30) = 100")

    def _load_default_config(self) -> Dict[str, Any]:
        """기본 설정 로드"""
        return {
            # Layer 가중치
            "macro_weight": 0.25,
            "structural_weight": 0.45,
            "micro_weight": 0.30,

            # Quality Gate 설정
            "quality_gates_enabled": True,
            "min_total_score": 60.0,

            # 성능 설정
            "parallel_processing": True,
            "cache_enabled": True,
            "max_concurrent_tasks": 10
        }

    def register_module(self, module: ScoringModule):
        """모듈 등록"""
        self.module_registry.register_module(module)
        self.layer_processors[module.layer_type].add_module(module)

    def _get_ohlcv_data(self, ticker: str) -> pd.DataFrame:
        """SQLite에서 OHLCV 데이터 로드"""
        try:
            conn = sqlite3.connect(self.db_path)

            query = """
            SELECT date, open, high, low, close, volume,
                   ma5, ma20, ma60, ma120, ma200, rsi
            FROM ohlcv_data
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT 300
            """

            df = pd.read_sql_query(query, conn, params=(ticker,))
            conn.close()

            if df.empty:
                logger.warning(f"⚠️ {ticker}: OHLCV 데이터 없음")
                return pd.DataFrame()

            # 날짜순 정렬
            df = df.sort_values('date').reset_index(drop=True)

            # MACD 계산 (간단 버전)
            if len(df) >= 26:
                exp1 = df['close'].ewm(span=12).mean()
                exp2 = df['close'].ewm(span=26).mean()
                df['macd'] = exp1 - exp2
                df['macd_signal'] = df['macd'].ewm(span=9).mean()
            else:
                df['macd'] = 0.0
                df['macd_signal'] = 0.0

            logger.debug(f"📊 {ticker}: {len(df)}일 데이터 로드")
            return df

        except Exception as e:
            logger.error(f"❌ {ticker} 데이터 로드 실패: {e}")
            return pd.DataFrame()

    async def analyze_ticker(self, ticker: str) -> ScoringResult:
        """ticker 점수 분석"""
        start_time = datetime.now()

        logger.info(f"🔍 {ticker} 점수 분석 시작")

        # 1. 데이터 로드
        data = self._get_ohlcv_data(ticker)
        if data.empty:
            return ScoringResult.create_invalid(ticker, "데이터 없음")

        # 2. 3개 Layer 병렬 처리
        layer_tasks = [
            self.layer_processors[LayerType.MACRO].process(ticker, data, self.config),
            self.layer_processors[LayerType.STRUCTURAL].process(ticker, data, self.config),
            self.layer_processors[LayerType.MICRO].process(ticker, data, self.config)
        ]

        try:
            layer_results_list = await asyncio.gather(*layer_tasks)
        except Exception as e:
            logger.error(f"❌ {ticker} Layer 처리 실패: {e}")
            return ScoringResult.create_invalid(ticker, str(e))

        # 3. 결과 정리
        layer_results = {
            result.layer_type: result
            for result in layer_results_list
        }

        # 4. 총점 계산
        total_score = sum(result.score for result in layer_results.values())

        # 5. Quality Gate 검증
        quality_gates_passed = False
        quality_gate_details = {}

        if self.config.get("quality_gates_enabled", True):
            quality_gates_passed, quality_gate_details = \
                self.quality_gate_validator.validate_all(layer_results, total_score)

        # 6. 추천사항 결정
        recommendation = self._determine_recommendation(
            total_score, quality_gates_passed, layer_results
        )

        # 7. 신뢰도 계산
        confidence = self._calculate_confidence(layer_results)

        execution_time = (datetime.now() - start_time).total_seconds() * 1000

        result = ScoringResult(
            ticker=ticker,
            total_score=total_score,
            layer_results=layer_results,
            quality_gates_passed=quality_gates_passed,
            quality_gate_details=quality_gate_details,
            recommendation=recommendation,
            confidence=confidence,
            execution_time=execution_time
        )

        logger.info(f"✅ {ticker} 분석 완료: {total_score:.1f}점, {recommendation}")

        return result

    def _determine_recommendation(self, total_score: float, quality_gates_passed: bool,
                                 layer_results: Dict[LayerType, LayerResult]) -> str:
        """추천사항 결정"""

        # Quality Gate 실패시 AVOID
        if not quality_gates_passed:
            return "AVOID"

        # 점수 기반 추천
        if total_score >= 80:
            return "STRONG_BUY"
        elif total_score >= 70:
            return "BUY"
        elif total_score >= 60:
            return "HOLD"
        else:
            return "AVOID"

    def _calculate_confidence(self, layer_results: Dict[LayerType, LayerResult]) -> float:
        """신뢰도 계산"""
        if not layer_results:
            return 0.0

        # Layer별 신뢰도의 가중 평균
        total_weight = 0.0
        weighted_confidence = 0.0

        layer_weights = {
            LayerType.MACRO: 0.25,
            LayerType.STRUCTURAL: 0.45,
            LayerType.MICRO: 0.30
        }

        for layer_type, result in layer_results.items():
            weight = layer_weights[layer_type]
            weighted_confidence += result.confidence * weight
            total_weight += weight

        return weighted_confidence / total_weight if total_weight > 0 else 0.0

    async def analyze_multiple_tickers(self, tickers: List[str]) -> Dict[str, ScoringResult]:
        """여러 ticker 일괄 분석"""
        logger.info(f"🚀 {len(tickers)}개 ticker 일괄 분석 시작")

        # 배치 처리 (20개씩)
        batch_size = 20
        batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]

        results = {}
        for i, batch in enumerate(batches, 1):
            logger.info(f"📦 배치 {i}/{len(batches)} 처리 중...")

            batch_tasks = [self.analyze_ticker(ticker) for ticker in batch]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            for ticker, result in zip(batch, batch_results):
                if not isinstance(result, Exception):
                    results[ticker] = result
                else:
                    logger.error(f"❌ {ticker} 분석 실패: {result}")
                    results[ticker] = ScoringResult.create_invalid(ticker, str(result))

        logger.info(f"✅ 일괄 분석 완료: {len(results)}개 결과")
        return results

    def get_statistics(self) -> Dict[str, Any]:
        """엔진 통계 정보"""
        stats = {
            "registered_modules": {
                layer_type.value: len(modules)
                for layer_type, modules in self.module_registry.get_all_modules().items()
            },
            "layer_max_scores": {
                layer_type.value: processor.max_score
                for layer_type, processor in self.layer_processors.items()
            },
            "quality_gate_thresholds": self.quality_gate_validator.min_score_requirements,
            "config": self.config
        }
        return stats


def test_layered_scoring_engine():
    """LayeredScoringEngine 기본 테스트"""
    print("🧪 LayeredScoringEngine 기본 테스트")
    print("=" * 60)

    # 엔진 초기화
    engine = LayeredScoringEngine()

    # 통계 출력
    stats = engine.get_statistics()
    print(f"📊 등록된 모듈 수: {stats['registered_modules']}")
    print(f"📊 Layer 점수 배분: {stats['layer_max_scores']}")

    # 주의: 실제 모듈이 등록되지 않았으므로 분석은 아직 불가능
    print("\n⚠️ 주의: 아직 Scoring Module이 등록되지 않았습니다.")
    print("📦 다음 단계에서 기본 모듈들을 구현하고 등록할 예정입니다.")

    print("\n✅ LayeredScoringEngine 핵심 구조 테스트 완료!")


if __name__ == "__main__":
    test_layered_scoring_engine()