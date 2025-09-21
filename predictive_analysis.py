#!/usr/bin/env python3
"""
Makenaide Phase 4 예측적 분석 시스템 (Predictive Analysis System)

Phase 4 핵심 구성요소:
- 실시간 시스템 건강도 모니터링
- 장애 패턴 기반 예측 알고리즘
- 조기 경고 시스템 (Early Warning System)
- 자동 위험도 스코어링
- 예방적 조치 제안

고급 예측 기능:
- 시계열 분석 기반 트렌드 예측
- 연쇄 장애 예측 (Cascading Failure Prediction)
- 시스템 부하 예측
- 계절성 및 주기성 분석
"""

import os
import sys
import json
import sqlite3
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, NamedTuple
from dataclasses import dataclass
from enum import Enum
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from failure_tracker import FailureTracker, FailureRecord, FailurePattern, SystemHealthMetrics
from sns_notification_system import (
    FailureType, FailureSubType, FailureSeverity,
    NotificationLevel, NotificationCategory
)

class PredictionConfidence(Enum):
    """예측 신뢰도 레벨"""
    VERY_LOW = "VERY_LOW"      # 0-20%
    LOW = "LOW"                # 20-40%
    MEDIUM = "MEDIUM"          # 40-60%
    HIGH = "HIGH"              # 60-80%
    VERY_HIGH = "VERY_HIGH"    # 80-100%

class RiskLevel(Enum):
    """위험도 레벨"""
    MINIMAL = "MINIMAL"        # 0-10%
    LOW = "LOW"               # 10-30%
    MODERATE = "MODERATE"     # 30-50%
    HIGH = "HIGH"             # 50-70%
    CRITICAL = "CRITICAL"     # 70-100%

@dataclass
class PredictionResult:
    """예측 결과 데이터 클래스"""
    prediction_id: str
    prediction_type: str
    risk_level: RiskLevel
    confidence: PredictionConfidence
    predicted_failure_time: Optional[datetime]
    failure_probability: float
    affected_components: List[str]
    recommended_actions: List[str]
    evidence: List[str]
    metadata: Dict
    created_at: datetime

@dataclass
class TrendAnalysis:
    """트렌드 분석 결과"""
    metric_name: str
    trend_direction: str  # "increasing", "decreasing", "stable", "volatile"
    trend_strength: float  # 0.0-1.0
    slope: float
    r_squared: float
    prediction_7d: float
    prediction_30d: float
    anomaly_score: float

class PredictiveAnalyzer:
    """Phase 4 예측적 분석 엔진"""

    def __init__(self, db_path: str = "makenaide_local.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.failure_tracker = FailureTracker(db_path)

        # 예측 모델 설정
        self.prediction_thresholds = {
            'failure_rate_threshold': 0.15,  # 15% 이상 실패율
            'error_spike_threshold': 3.0,    # 평균 대비 3배 이상 에러
            'response_time_threshold': 2.0,  # 평균 대비 2배 이상 응답시간
            'memory_threshold': 0.85,        # 85% 이상 메모리 사용률
            'cascade_risk_threshold': 0.6    # 60% 이상 연쇄 장애 위험
        }

        # 예측 알고리즘 가중치
        self.algorithm_weights = {
            'historical_pattern': 0.3,
            'trend_analysis': 0.25,
            'seasonal_pattern': 0.2,
            'system_metrics': 0.15,
            'external_factors': 0.1
        }

        self._init_prediction_tables()

    def _init_prediction_tables(self):
        """예측 분석 테이블 초기화"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 예측 결과 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prediction_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    prediction_id TEXT UNIQUE NOT NULL,
                    prediction_type TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    predicted_failure_time TEXT,
                    failure_probability REAL NOT NULL,
                    affected_components TEXT,
                    recommended_actions TEXT,
                    evidence TEXT,
                    metadata TEXT,
                    created_at TEXT NOT NULL,
                    validated_at TEXT,
                    actual_outcome TEXT,
                    accuracy_score REAL
                )
            """)

            # 트렌드 분석 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trend_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    trend_direction TEXT NOT NULL,
                    trend_strength REAL NOT NULL,
                    slope REAL NOT NULL,
                    r_squared REAL NOT NULL,
                    prediction_7d REAL,
                    prediction_30d REAL,
                    anomaly_score REAL NOT NULL,
                    metadata TEXT,
                    created_at TEXT NOT NULL
                )
            """)

            # 예측 정확도 추적 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prediction_accuracy (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    prediction_id TEXT NOT NULL,
                    prediction_type TEXT NOT NULL,
                    predicted_time TEXT,
                    actual_time TEXT,
                    accuracy_score REAL NOT NULL,
                    confidence_level TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (prediction_id) REFERENCES prediction_results (prediction_id)
                )
            """)

            conn.commit()

    def analyze_system_health_trends(self, days: int = 30) -> List[TrendAnalysis]:
        """시스템 건강도 트렌드 분석"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # 시스템 메트릭 데이터 수집
        metrics_data = self.failure_tracker.get_system_health_history(
            start_date.isoformat(),
            end_date.isoformat()
        )

        trends = []

        if not metrics_data:
            self.logger.warning("시스템 건강도 데이터가 충분하지 않습니다.")
            return trends

        # 분석할 메트릭들
        metric_names = [
            'failure_rate', 'avg_response_time', 'memory_usage',
            'cpu_usage', 'error_count', 'success_rate'
        ]

        for metric_name in metric_names:
            try:
                trend = self._analyze_metric_trend(metrics_data, metric_name, days)
                if trend:
                    trends.append(trend)
                    self._save_trend_analysis(trend)
            except Exception as e:
                self.logger.error(f"메트릭 {metric_name} 트렌드 분석 오류: {e}")

        return trends

    def _analyze_metric_trend(self, metrics_data: List[SystemHealthMetrics],
                             metric_name: str, days: int) -> Optional[TrendAnalysis]:
        """개별 메트릭의 트렌드 분석"""
        try:
            # 메트릭 값 추출
            values = []
            timestamps = []

            for metric in metrics_data:
                if hasattr(metric, metric_name):
                    value = getattr(metric, metric_name)
                    if value is not None:
                        values.append(float(value))
                        timestamps.append(datetime.fromisoformat(metric.timestamp))

            if len(values) < 5:  # 최소 5개 데이터 포인트 필요
                return None

            # 선형 회귀 분석 (간단한 구현)
            n = len(values)
            x_values = list(range(n))

            # 기울기 계산
            x_mean = statistics.mean(x_values)
            y_mean = statistics.mean(values)

            numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values))
            denominator = sum((x - x_mean) ** 2 for x in x_values)

            if denominator == 0:
                slope = 0
            else:
                slope = numerator / denominator

            # R² 계산
            y_pred = [slope * x + (y_mean - slope * x_mean) for x in x_values]
            ss_res = sum((y - yp) ** 2 for y, yp in zip(values, y_pred))
            ss_tot = sum((y - y_mean) ** 2 for y in values)

            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

            # 트렌드 방향 결정
            if abs(slope) < 0.01:
                trend_direction = "stable"
            elif slope > 0:
                trend_direction = "increasing"
            else:
                trend_direction = "decreasing"

            # 변동성 확인
            std_dev = statistics.stdev(values) if len(values) > 1 else 0
            mean_val = statistics.mean(values)
            coefficient_of_variation = std_dev / mean_val if mean_val != 0 else 0

            if coefficient_of_variation > 0.3:
                trend_direction = "volatile"

            # 트렌드 강도 (0.0-1.0)
            trend_strength = min(abs(slope) * 10, 1.0)

            # 미래 예측
            prediction_7d = slope * (n + 7) + (y_mean - slope * x_mean)
            prediction_30d = slope * (n + 30) + (y_mean - slope * x_mean)

            # 이상치 스코어 계산
            recent_values = values[-7:] if len(values) >= 7 else values
            recent_mean = statistics.mean(recent_values)
            historical_mean = statistics.mean(values[:-7]) if len(values) > 7 else recent_mean

            anomaly_score = abs(recent_mean - historical_mean) / historical_mean if historical_mean != 0 else 0
            anomaly_score = min(anomaly_score, 1.0)

            return TrendAnalysis(
                metric_name=metric_name,
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                slope=slope,
                r_squared=r_squared,
                prediction_7d=prediction_7d,
                prediction_30d=prediction_30d,
                anomaly_score=anomaly_score
            )

        except Exception as e:
            self.logger.error(f"메트릭 {metric_name} 트렌드 분석 오류: {e}")
            return None

    def predict_failure_probability(self, time_window_hours: int = 24) -> PredictionResult:
        """지정된 시간 내 장애 발생 확률 예측"""
        try:
            prediction_id = f"failure_prob_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # 과거 30일 데이터 분석
            analysis_period = datetime.now() - timedelta(days=30)
            recent_patterns = self.failure_tracker.get_recent_patterns(analysis_period.isoformat())

            # 다양한 예측 알고리즘 결과 수집
            predictions = {
                'historical_pattern': self._predict_from_historical_patterns(recent_patterns, time_window_hours),
                'trend_analysis': self._predict_from_trends(time_window_hours),
                'seasonal_pattern': self._predict_from_seasonal_patterns(time_window_hours),
                'system_metrics': self._predict_from_system_metrics(),
                'external_factors': self._predict_from_external_factors()
            }

            # 가중 평균으로 최종 확률 계산
            weighted_probability = sum(
                predictions[algo] * self.algorithm_weights[algo]
                for algo in predictions
            )

            # 위험도 레벨 결정
            risk_level = self._calculate_risk_level(weighted_probability)

            # 신뢰도 계산
            confidence = self._calculate_prediction_confidence(predictions, recent_patterns)

            # 영향받을 수 있는 컴포넌트 식별
            affected_components = self._identify_affected_components(recent_patterns)

            # 권장 조치 생성
            recommended_actions = self._generate_recommended_actions(
                risk_level, affected_components, predictions
            )

            # 증거 수집
            evidence = self._collect_prediction_evidence(predictions, recent_patterns)

            # 예측된 장애 시간 계산
            predicted_failure_time = None
            if weighted_probability > 0.5:
                predicted_failure_time = datetime.now() + timedelta(
                    hours=time_window_hours * (1 - weighted_probability)
                )

            result = PredictionResult(
                prediction_id=prediction_id,
                prediction_type="failure_probability",
                risk_level=risk_level,
                confidence=confidence,
                predicted_failure_time=predicted_failure_time,
                failure_probability=weighted_probability,
                affected_components=affected_components,
                recommended_actions=recommended_actions,
                evidence=evidence,
                metadata={
                    'time_window_hours': time_window_hours,
                    'algorithm_scores': predictions,
                    'analysis_period_days': 30
                },
                created_at=datetime.now()
            )

            # 결과 저장
            self._save_prediction_result(result)

            return result

        except Exception as e:
            self.logger.error(f"장애 확률 예측 오류: {e}")
            # 기본 예측 결과 반환
            return PredictionResult(
                prediction_id=f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                prediction_type="failure_probability",
                risk_level=RiskLevel.LOW,
                confidence=PredictionConfidence.VERY_LOW,
                predicted_failure_time=None,
                failure_probability=0.1,
                affected_components=[],
                recommended_actions=["시스템 모니터링 강화"],
                evidence=["예측 분석 오류로 인한 기본값"],
                metadata={'error': str(e)},
                created_at=datetime.now()
            )

    def _predict_from_historical_patterns(self, patterns: List[FailurePattern],
                                        time_window_hours: int) -> float:
        """과거 패턴 기반 예측"""
        if not patterns:
            return 0.1  # 기본 낮은 확률

        # 최근 패턴들의 빈도 분석
        pattern_frequencies = {}
        for pattern in patterns:
            key = f"{pattern.failure_type}_{pattern.sub_type}"
            pattern_frequencies[key] = pattern_frequencies.get(key, 0) + pattern.occurrence_count

        # 시간 가중 빈도 계산
        total_frequency = sum(pattern_frequencies.values())
        if total_frequency == 0:
            return 0.1

        # 시간 윈도우 내 예상 장애 수
        daily_failure_rate = total_frequency / 30  # 30일 평균
        window_failure_rate = daily_failure_rate * (time_window_hours / 24)

        # 확률로 변환 (0-1 범위)
        probability = min(window_failure_rate / 5, 0.9)  # 최대 90%

        return probability

    def _predict_from_trends(self, time_window_hours: int) -> float:
        """트렌드 분석 기반 예측"""
        try:
            trends = self.analyze_system_health_trends(days=14)

            if not trends:
                return 0.1

            risk_score = 0.0
            weight_sum = 0.0

            for trend in trends:
                # 메트릭별 가중치
                metric_weights = {
                    'failure_rate': 0.3,
                    'error_count': 0.25,
                    'avg_response_time': 0.2,
                    'memory_usage': 0.15,
                    'cpu_usage': 0.1
                }

                weight = metric_weights.get(trend.metric_name, 0.1)

                # 트렌드가 악화되는 경우 위험도 증가
                if trend.trend_direction == "increasing" and trend.metric_name in ['failure_rate', 'error_count', 'avg_response_time']:
                    risk_score += trend.trend_strength * weight * trend.anomaly_score
                elif trend.trend_direction == "decreasing" and trend.metric_name in ['success_rate']:
                    risk_score += trend.trend_strength * weight * trend.anomaly_score
                elif trend.trend_direction == "volatile":
                    risk_score += 0.5 * weight * trend.anomaly_score

                weight_sum += weight

            if weight_sum > 0:
                normalized_risk = risk_score / weight_sum
                return min(normalized_risk, 0.8)

            return 0.1

        except Exception as e:
            self.logger.error(f"트렌드 예측 오류: {e}")
            return 0.1

    def _predict_from_seasonal_patterns(self, time_window_hours: int) -> float:
        """계절성/주기성 패턴 기반 예측"""
        try:
            now = datetime.now()

            # 시간대별 장애 패턴 분석
            hour_risk = self._get_hourly_failure_risk(now.hour)

            # 요일별 장애 패턴 분석
            weekday_risk = self._get_weekday_failure_risk(now.weekday())

            # 월별 장애 패턴 분석
            monthly_risk = self._get_monthly_failure_risk(now.month)

            # 가중 평균
            seasonal_probability = (hour_risk * 0.5 + weekday_risk * 0.3 + monthly_risk * 0.2)

            return min(seasonal_probability, 0.7)

        except Exception as e:
            self.logger.error(f"계절성 예측 오류: {e}")
            return 0.1

    def _predict_from_system_metrics(self) -> float:
        """현재 시스템 메트릭 기반 예측"""
        try:
            # 최근 시스템 건강도 조회
            recent_health = self.failure_tracker.get_current_system_health()

            if not recent_health:
                return 0.2

            risk_factors = []

            # 실패율 체크
            if recent_health.failure_rate > self.prediction_thresholds['failure_rate_threshold']:
                risk_factors.append(recent_health.failure_rate * 0.3)

            # 응답시간 체크
            if recent_health.avg_response_time > 5.0:  # 5초 이상
                risk_factors.append(0.2)

            # 메모리 사용률 체크
            if recent_health.memory_usage > self.prediction_thresholds['memory_threshold']:
                risk_factors.append((recent_health.memory_usage - 0.8) * 0.5)

            # 에러 카운트 체크
            if recent_health.error_count > 10:
                risk_factors.append(min(recent_health.error_count / 50, 0.3))

            total_risk = sum(risk_factors)
            return min(total_risk, 0.8)

        except Exception as e:
            self.logger.error(f"시스템 메트릭 예측 오류: {e}")
            return 0.1

    def _predict_from_external_factors(self) -> float:
        """외부 요인 기반 예측"""
        # 시간대, 시장 상황 등 외부 요인 고려
        now = datetime.now()

        # 거래 활성 시간대 (한국 시간 기준)
        if 9 <= now.hour <= 18:
            return 0.2  # 거래 시간대는 부하가 높아 위험도 증가
        elif 0 <= now.hour <= 6:
            return 0.1  # 새벽 시간대는 상대적으로 안전
        else:
            return 0.15  # 기본 위험도

    def _calculate_risk_level(self, probability: float) -> RiskLevel:
        """확률 기반 위험도 레벨 계산"""
        if probability < 0.1:
            return RiskLevel.MINIMAL
        elif probability < 0.3:
            return RiskLevel.LOW
        elif probability < 0.5:
            return RiskLevel.MODERATE
        elif probability < 0.7:
            return RiskLevel.HIGH
        else:
            return RiskLevel.CRITICAL

    def _calculate_prediction_confidence(self, predictions: Dict[str, float],
                                       patterns: List[FailurePattern]) -> PredictionConfidence:
        """예측 신뢰도 계산"""
        # 예측 알고리즘들의 일치도 확인
        prediction_values = list(predictions.values())
        if len(prediction_values) < 2:
            return PredictionConfidence.LOW

        std_dev = statistics.stdev(prediction_values)
        mean_val = statistics.mean(prediction_values)

        # 변동계수 계산
        cv = std_dev / mean_val if mean_val != 0 else 1.0

        # 데이터 품질 평가
        data_quality = len(patterns) / 10  # 10개 패턴당 1점
        data_quality = min(data_quality, 1.0)

        # 신뢰도 점수 계산 (0-1)
        consistency_score = 1 - min(cv, 1.0)
        confidence_score = (consistency_score * 0.7 + data_quality * 0.3)

        if confidence_score < 0.2:
            return PredictionConfidence.VERY_LOW
        elif confidence_score < 0.4:
            return PredictionConfidence.LOW
        elif confidence_score < 0.6:
            return PredictionConfidence.MEDIUM
        elif confidence_score < 0.8:
            return PredictionConfidence.HIGH
        else:
            return PredictionConfidence.VERY_HIGH

    def _identify_affected_components(self, patterns: List[FailurePattern]) -> List[str]:
        """영향받을 수 있는 컴포넌트 식별"""
        components = set()

        for pattern in patterns:
            if pattern.metadata:
                try:
                    metadata = json.loads(pattern.metadata)
                    if 'phase' in metadata:
                        components.add(f"Phase {metadata['phase']}")
                    if 'component' in metadata:
                        components.add(metadata['component'])
                except:
                    pass

            # 실패 유형별 컴포넌트 매핑
            failure_component_map = {
                'API_ERROR': ['업비트 API', '외부 연동'],
                'DATABASE_ERROR': ['SQLite DB', '데이터 관리'],
                'NETWORK_ERROR': ['네트워크', '외부 연동'],
                'TRADING_ERROR': ['거래 엔진', '포트폴리오 관리'],
                'DATA_ERROR': ['데이터 수집', '기술적 분석']
            }

            if pattern.failure_type in failure_component_map:
                components.update(failure_component_map[pattern.failure_type])

        return list(components)

    def _generate_recommended_actions(self, risk_level: RiskLevel,
                                    affected_components: List[str],
                                    predictions: Dict[str, float]) -> List[str]:
        """위험도에 따른 권장 조치 생성"""
        actions = []

        # 위험도별 기본 조치
        if risk_level == RiskLevel.CRITICAL:
            actions.extend([
                "🚨 즉시 시스템 점검 실시",
                "📞 운영팀 긴급 소집",
                "🔄 백업 시스템 준비",
                "📊 실시간 모니터링 강화"
            ])
        elif risk_level == RiskLevel.HIGH:
            actions.extend([
                "⚠️ 시스템 상태 집중 모니터링",
                "🔧 예방적 유지보수 실시",
                "📋 장애 대응 계획 점검"
            ])
        elif risk_level == RiskLevel.MODERATE:
            actions.extend([
                "📈 성능 메트릭 모니터링 증가",
                "🔍 로그 분석 강화"
            ])

        # 컴포넌트별 맞춤 조치
        for component in affected_components:
            if 'API' in component:
                actions.append("🔗 API 연결 상태 및 응답시간 점검")
            elif 'DB' in component:
                actions.append("💾 데이터베이스 성능 및 무결성 점검")
            elif '거래' in component:
                actions.append("💰 거래 시스템 및 포트폴리오 상태 점검")

        # 예측 알고리즘별 조치
        if predictions.get('trend_analysis', 0) > 0.5:
            actions.append("📊 성능 트렌드 개선 조치 실시")

        if predictions.get('system_metrics', 0) > 0.5:
            actions.append("🖥️ 시스템 리소스 최적화")

        return list(set(actions))  # 중복 제거

    def _collect_prediction_evidence(self, predictions: Dict[str, float],
                                   patterns: List[FailurePattern]) -> List[str]:
        """예측 근거 수집"""
        evidence = []

        # 알고리즘별 근거
        for algo, score in predictions.items():
            if score > 0.3:
                if algo == 'historical_pattern':
                    evidence.append(f"📈 과거 패턴 분석: {score:.1%} 위험도")
                elif algo == 'trend_analysis':
                    evidence.append(f"📊 트렌드 분석: {score:.1%} 악화 경향")
                elif algo == 'system_metrics':
                    evidence.append(f"🖥️ 시스템 메트릭: {score:.1%} 임계치 근접")

        # 패턴 근거
        if patterns:
            recent_failures = sum(1 for p in patterns if p.last_occurrence)
            evidence.append(f"🔍 최근 30일 {recent_failures}개 유사 패턴 감지")

        return evidence

    def _save_prediction_result(self, result: PredictionResult):
        """예측 결과 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO prediction_results (
                        prediction_id, prediction_type, risk_level, confidence,
                        predicted_failure_time, failure_probability, affected_components,
                        recommended_actions, evidence, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result.prediction_id,
                    result.prediction_type,
                    result.risk_level.value,
                    result.confidence.value,
                    result.predicted_failure_time.isoformat() if result.predicted_failure_time else None,
                    result.failure_probability,
                    json.dumps(result.affected_components),
                    json.dumps(result.recommended_actions),
                    json.dumps(result.evidence),
                    json.dumps(result.metadata),
                    result.created_at.isoformat()
                ))
                conn.commit()

        except Exception as e:
            self.logger.error(f"예측 결과 저장 오류: {e}")

    def _save_trend_analysis(self, trend: TrendAnalysis):
        """트렌드 분석 결과 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO trend_analysis (
                        metric_name, analysis_date, trend_direction, trend_strength,
                        slope, r_squared, prediction_7d, prediction_30d,
                        anomaly_score, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trend.metric_name,
                    datetime.now().date().isoformat(),
                    trend.trend_direction,
                    trend.trend_strength,
                    trend.slope,
                    trend.r_squared,
                    trend.prediction_7d,
                    trend.prediction_30d,
                    trend.anomaly_score,
                    datetime.now().isoformat()
                ))
                conn.commit()

        except Exception as e:
            self.logger.error(f"트렌드 분석 저장 오류: {e}")

    def _get_hourly_failure_risk(self, hour: int) -> float:
        """시간대별 장애 위험도 조회"""
        # 실제로는 DB에서 시간대별 통계를 조회해야 함
        # 여기서는 일반적인 패턴으로 근사
        high_risk_hours = [9, 10, 15, 16, 21, 22]  # 거래 활성 시간
        if hour in high_risk_hours:
            return 0.3
        elif 0 <= hour <= 6:
            return 0.1  # 새벽은 상대적으로 안전
        else:
            return 0.2

    def _get_weekday_failure_risk(self, weekday: int) -> float:
        """요일별 장애 위험도 조회"""
        # 0=월요일, 6=일요일
        if weekday < 5:  # 평일
            return 0.25
        else:  # 주말
            return 0.15

    def _get_monthly_failure_risk(self, month: int) -> float:
        """월별 장애 위험도 조회"""
        # 실제로는 계절성 데이터 분석 필요
        return 0.2  # 기본값

    def generate_daily_prediction_report(self) -> Dict:
        """일일 예측 보고서 생성"""
        try:
            # 24시간 예측
            prediction_24h = self.predict_failure_probability(24)

            # 7일 예측
            prediction_7d = self.predict_failure_probability(24 * 7)

            # 트렌드 분석
            trends = self.analyze_system_health_trends(14)

            # 위험 요소 식별
            risk_factors = self._identify_current_risk_factors()

            report = {
                'report_date': datetime.now().isoformat(),
                'predictions': {
                    '24_hours': {
                        'risk_level': prediction_24h.risk_level.value,
                        'probability': prediction_24h.failure_probability,
                        'confidence': prediction_24h.confidence.value,
                        'affected_components': prediction_24h.affected_components,
                        'recommended_actions': prediction_24h.recommended_actions
                    },
                    '7_days': {
                        'risk_level': prediction_7d.risk_level.value,
                        'probability': prediction_7d.failure_probability,
                        'confidence': prediction_7d.confidence.value
                    }
                },
                'trends': [
                    {
                        'metric': trend.metric_name,
                        'direction': trend.trend_direction,
                        'strength': trend.trend_strength,
                        'anomaly_score': trend.anomaly_score
                    }
                    for trend in trends
                ],
                'risk_factors': risk_factors,
                'summary': self._generate_prediction_summary(prediction_24h, trends)
            }

            return report

        except Exception as e:
            self.logger.error(f"일일 예측 보고서 생성 오류: {e}")
            return {
                'error': str(e),
                'report_date': datetime.now().isoformat()
            }

    def _identify_current_risk_factors(self) -> List[Dict]:
        """현재 위험 요소 식별"""
        risk_factors = []

        try:
            # 최근 시스템 건강도 확인
            health = self.failure_tracker.get_current_system_health()

            if health:
                if health.failure_rate > 0.1:
                    risk_factors.append({
                        'type': 'high_failure_rate',
                        'value': health.failure_rate,
                        'description': f'높은 실패율: {health.failure_rate:.1%}'
                    })

                if health.avg_response_time > 3.0:
                    risk_factors.append({
                        'type': 'slow_response',
                        'value': health.avg_response_time,
                        'description': f'느린 응답시간: {health.avg_response_time:.1f}초'
                    })

                if health.error_count > 5:
                    risk_factors.append({
                        'type': 'high_error_count',
                        'value': health.error_count,
                        'description': f'높은 에러 수: {health.error_count}개'
                    })

        except Exception as e:
            self.logger.error(f"위험 요소 식별 오류: {e}")

        return risk_factors

    def _generate_prediction_summary(self, prediction: PredictionResult,
                                   trends: List[TrendAnalysis]) -> str:
        """예측 요약 생성"""
        summary_parts = []

        # 위험도 요약
        if prediction.risk_level == RiskLevel.CRITICAL:
            summary_parts.append("🚨 긴급: 높은 장애 위험도 감지")
        elif prediction.risk_level == RiskLevel.HIGH:
            summary_parts.append("⚠️ 주의: 중간 장애 위험도")
        elif prediction.risk_level == RiskLevel.MODERATE:
            summary_parts.append("📊 모니터링: 보통 위험도")
        else:
            summary_parts.append("✅ 안정: 낮은 위험도")

        # 트렌드 요약
        deteriorating_trends = [t for t in trends if t.trend_direction == "increasing" and t.anomaly_score > 0.3]
        if deteriorating_trends:
            summary_parts.append(f"📈 {len(deteriorating_trends)}개 메트릭 악화 중")

        # 신뢰도 요약
        summary_parts.append(f"신뢰도: {prediction.confidence.value}")

        return " | ".join(summary_parts)

def main():
    """메인 함수 - 예측 분석 실행"""
    analyzer = PredictiveAnalyzer()

    print("🔮 Makenaide Phase 4 예측적 분석 시스템")
    print("=" * 50)

    # 24시간 장애 확률 예측
    prediction = analyzer.predict_failure_probability(24)

    print(f"\n📊 24시간 장애 예측 결과:")
    print(f"   위험도: {prediction.risk_level.value}")
    print(f"   확률: {prediction.failure_probability:.1%}")
    print(f"   신뢰도: {prediction.confidence.value}")

    if prediction.affected_components:
        print(f"   영향 컴포넌트: {', '.join(prediction.affected_components)}")

    if prediction.recommended_actions:
        print(f"\n🔧 권장 조치:")
        for action in prediction.recommended_actions:
            print(f"   - {action}")

    # 트렌드 분석
    trends = analyzer.analyze_system_health_trends(14)
    if trends:
        print(f"\n📈 시스템 트렌드 분석 (14일):")
        for trend in trends:
            print(f"   {trend.metric_name}: {trend.trend_direction} (강도: {trend.trend_strength:.2f})")

    # 일일 보고서 생성
    report = analyzer.generate_daily_prediction_report()
    print(f"\n📋 일일 예측 보고서 생성 완료")
    print(f"   요약: {report.get('summary', 'N/A')}")

if __name__ == "__main__":
    main()