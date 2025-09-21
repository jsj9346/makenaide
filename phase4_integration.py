#!/usr/bin/env python3
"""
Makenaide Phase 4 통합 모듈: SNS 시스템과 실패 추적기 연동

Phase 1-3의 SNS 알림 시스템과 Phase 4의 실패 추적/분석 시스템을
완벽하게 통합하여 지능형 실패 관리 플랫폼을 제공합니다.

🎯 통합 기능:
- Phase 1-3 SNS 알림과 Phase 4 실패 추적 자동 연동
- 실시간 패턴 분석 및 예측 알림
- 지능형 실패 방지 권고사항 생성
- 시스템 건강도 기반 사전 경고
- 자동 복구 시스템 트리거
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

# Phase 1-3 SNS 시스템 연동
from sns_notification_system import (
    MakenaideSNSNotifier,
    NotificationMessage,
    NotificationLevel,
    NotificationCategory,
    FailureType,
    FailureSubType,
    FailureSeverity,
    notify_detailed_failure,
    send_secure_notification
)

# Phase 4 실패 추적 시스템 연동
from failure_tracker import (
    FailureTracker,
    FailureRecord,
    FailurePattern,
    SystemHealthMetrics
)

logger = logging.getLogger(__name__)

@dataclass
class Phase4AlertConfig:
    """Phase 4 알림 설정"""
    enable_pattern_alerts: bool = True
    enable_health_monitoring: bool = True
    enable_predictive_warnings: bool = True
    min_pattern_frequency: int = 3  # 패턴 알림 최소 빈도
    health_check_interval: int = 60  # 건강도 체크 간격(분)
    critical_health_threshold: float = 30.0  # 치명적 건강도 임계값
    warning_health_threshold: float = 70.0  # 경고 건강도 임계값

class IntegratedFailureManager:
    """Phase 1-4 통합 실패 관리자"""

    def __init__(self,
                 db_path: str = "./makenaide_failures.db",
                 config: Optional[Phase4AlertConfig] = None):
        """통합 실패 관리자 초기화"""
        self.failure_tracker = FailureTracker(db_path)
        self.sns_notifier = MakenaideSNSNotifier()
        self.config = config or Phase4AlertConfig()

        logger.info("🔗 Phase 1-4 통합 실패 관리 시스템 초기화 완료")

    def handle_failure_with_tracking(self,
                                   failure_type: str,
                                   error_message: str,
                                   execution_id: str,
                                   sub_type: Optional[str] = None,
                                   severity: str = "MEDIUM",
                                   phase: Optional[str] = None,
                                   metadata: Optional[Dict] = None) -> Tuple[bool, int]:
        """
        실패 처리: Phase 1-3 알림 + Phase 4 추적 통합

        Args:
            failure_type: 실패 유형
            error_message: 에러 메시지
            execution_id: 실행 ID
            sub_type: 상세 실패 유형
            severity: 심각도
            phase: 실패 발생 단계
            metadata: 추가 메타데이터

        Returns:
            Tuple[bool, int]: (SNS 알림 성공 여부, 실패 기록 ID)
        """
        try:
            # 1. Phase 4: 실패 기록 저장 및 패턴 분석
            failure_id = self.failure_tracker.record_failure(
                failure_type=failure_type,
                error_message=error_message,
                execution_id=execution_id,
                sub_type=sub_type,
                severity=severity,
                phase=phase,
                metadata=metadata
            )

            # 2. Phase 2-3: 상세 실패 알림 (보안 처리 포함)
            notification_success = notify_detailed_failure(
                failure_type=failure_type,
                sub_type=sub_type,
                error_message=error_message,
                phase=phase,
                execution_id=execution_id,
                metadata=metadata
            )

            # 3. Phase 4: 패턴 기반 추가 분석 및 알림
            self._analyze_and_alert_patterns(failure_type, sub_type, failure_id)

            # 4. Phase 4: 시스템 건강도 체크 및 필요시 경고
            self._check_system_health_and_alert()

            logger.info(f"✅ 통합 실패 처리 완료: 알림={notification_success}, 추적ID={failure_id}")
            return notification_success, failure_id

        except Exception as e:
            logger.error(f"❌ 통합 실패 처리 오류: {e}")
            return False, -1

    def _analyze_and_alert_patterns(self, failure_type: str, sub_type: Optional[str], failure_id: int):
        """실패 패턴 분석 및 필요시 알림 발송"""
        try:
            # 최근 패턴 분석 (지난 24시간)
            patterns = self.failure_tracker.get_failure_patterns(hours=24)

            # 현재 실패와 관련된 패턴 찾기
            relevant_patterns = [
                p for p in patterns
                if p.failure_type == failure_type and
                (sub_type is None or p.sub_type == sub_type)
            ]

            for pattern in relevant_patterns:
                if self._should_send_pattern_alert(pattern):
                    self._send_pattern_alert(pattern, failure_id)

        except Exception as e:
            logger.error(f"❌ 패턴 분석 알림 오류: {e}")

    def _should_send_pattern_alert(self, pattern: FailurePattern) -> bool:
        """패턴 알림 발송 필요 여부 판단"""
        if not self.config.enable_pattern_alerts:
            return False

        # 빈도 기준
        if pattern.frequency < self.config.min_pattern_frequency:
            return False

        # 위험도 기준 (50점 이상)
        if pattern.risk_score < 50:
            return False

        # 증가 추세 또는 높은 빈도
        if pattern.trend == "increasing" or pattern.frequency >= 5:
            return True

        return False

    def _send_pattern_alert(self, pattern: FailurePattern, related_failure_id: int):
        """패턴 기반 알림 발송"""
        try:
            # 패턴 분석 메시지 생성
            message_parts = [
                f"🔍 실패 패턴 감지: {pattern.failure_type}",
                f"상세 유형: {pattern.sub_type}",
                f"빈도: {pattern.frequency}회 (위험도: {pattern.risk_score:.1f}/100)",
                f"트렌드: {pattern.trend}",
                f"마지막 발생: {pattern.last_occurrence}",
                ""
            ]

            # 권고사항 추가
            recommendations = self.failure_tracker.generate_recommendations(pattern)
            if recommendations:
                message_parts.append("💡 권고사항:")
                for i, rec in enumerate(recommendations, 1):
                    message_parts.append(f"  {i}. {rec}")
                message_parts.append("")

            # 관련 실패 기록 정보
            message_parts.extend([
                f"🔗 관련 실패 기록 ID: {related_failure_id}",
                "📊 자세한 패턴 분석은 대시보드에서 확인하세요."
            ])

            # 심각도 결정
            if pattern.risk_score >= 80:
                level = NotificationLevel.CRITICAL
            elif pattern.risk_score >= 60:
                level = NotificationLevel.WARNING
            else:
                level = NotificationLevel.INFO

            # 패턴 알림 전송
            pattern_notification = NotificationMessage(
                level=level,
                category=NotificationCategory.SYSTEM,
                title=f"🔍 실패 패턴 감지: {pattern.failure_type}",
                message="\n".join(message_parts),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=f"pattern_{pattern.pattern_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            # Phase 3 보안 알림 시스템 사용
            success = send_secure_notification(pattern_notification)

            if success:
                logger.info(f"📊 패턴 알림 발송 완료: {pattern.pattern_id}")
            else:
                logger.warning(f"⚠️ 패턴 알림 발송 실패: {pattern.pattern_id}")

        except Exception as e:
            logger.error(f"❌ 패턴 알림 발송 오류: {e}")

    def _check_system_health_and_alert(self):
        """시스템 건강도 체크 및 필요시 경고 알림"""
        if not self.config.enable_health_monitoring:
            return

        try:
            health = self.failure_tracker.get_system_health()

            # 건강도 기반 알림 판단
            if health.health_score <= self.config.critical_health_threshold:
                self._send_health_alert(health, "CRITICAL")
            elif health.health_score <= self.config.warning_health_threshold:
                self._send_health_alert(health, "WARNING")

        except Exception as e:
            logger.error(f"❌ 시스템 건강도 체크 오류: {e}")

    def _send_health_alert(self, health: SystemHealthMetrics, alert_type: str):
        """시스템 건강도 경고 알림 발송"""
        try:
            # 건강도 상태 메시지 생성
            message_parts = [
                f"🏥 시스템 건강도 경고",
                f"현재 건강도: {health.health_score:.1f}/100 ({health.risk_level})",
                f"24시간 내 실패: {health.total_failures_24h}회",
                f"치명적 실패: {health.critical_failures_24h}회",
                f"평균 해결 시간: {health.avg_resolution_time:.1f}분",
                f"실패율 트렌드: {health.failure_rate_trend}",
                f"주요 실패 유형: {health.most_common_failure}",
                ""
            ]

            # 권고사항 추가
            recommendations = self._generate_health_recommendations(health)
            if recommendations:
                message_parts.append("💡 권고사항:")
                for i, rec in enumerate(recommendations, 1):
                    message_parts.append(f"  {i}. {rec}")
                message_parts.append("")

            message_parts.append("📊 자세한 분석은 시스템 대시보드에서 확인하세요.")

            # 알림 레벨 결정
            if alert_type == "CRITICAL":
                level = NotificationLevel.CRITICAL
                title_emoji = "🚨"
            else:
                level = NotificationLevel.WARNING
                title_emoji = "⚠️"

            # 건강도 알림 전송
            health_notification = NotificationMessage(
                level=level,
                category=NotificationCategory.SYSTEM,
                title=f"{title_emoji} 시스템 건강도 경고 ({health.health_score:.1f}/100)",
                message="\n".join(message_parts),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=f"health_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            # Phase 3 보안 알림 시스템 사용
            success = send_secure_notification(health_notification)

            if success:
                logger.info(f"🏥 건강도 알림 발송 완료: {alert_type}")
            else:
                logger.warning(f"⚠️ 건강도 알림 발송 실패: {alert_type}")

        except Exception as e:
            logger.error(f"❌ 건강도 알림 발송 오류: {e}")

    def _generate_health_recommendations(self, health: SystemHealthMetrics) -> List[str]:
        """건강도 기반 권고사항 생성"""
        recommendations = []

        # 건강도 점수 기반
        if health.health_score < 30:
            recommendations.append("🚨 즉시 시스템 점검 및 근본 원인 분석 필요")
            recommendations.append("🔧 주요 구성 요소 재시작 고려")
        elif health.health_score < 50:
            recommendations.append("⚡ 시스템 모니터링 강화 및 예방적 조치 수행")

        # 실패 횟수 기반
        if health.total_failures_24h >= 10:
            recommendations.append("📊 실패 패턴 분석 및 자동화된 복구 절차 도입")

        if health.critical_failures_24h >= 3:
            recommendations.append("🔥 치명적 실패 대응 절차 점검 및 개선")

        # 해결 시간 기반
        if health.avg_resolution_time > 60:
            recommendations.append("⏰ 평균 해결 시간 단축을 위한 자동화 도구 도입")

        # 트렌드 기반
        if health.failure_rate_trend == "increasing":
            recommendations.append("📈 실패율 증가 추세 - 시스템 용량 및 성능 점검")

        # 주요 실패 유형 기반
        failure_specific_recommendations = {
            "API_KEY_MISSING": "🔑 API 키 관리 시스템 및 자동 갱신 도입",
            "MEMORY_INSUFFICIENT": "💾 메모리 사용량 최적화 및 인스턴스 업그레이드",
            "RATE_LIMIT_EXCEEDED": "🚦 API 호출 최적화 및 캐싱 전략 개선"
        }

        if health.most_common_failure in failure_specific_recommendations:
            recommendations.append(failure_specific_recommendations[health.most_common_failure])

        return recommendations[:5]  # 최대 5개

    def generate_daily_report(self) -> str:
        """일일 시스템 상태 리포트 생성"""
        try:
            health = self.failure_tracker.get_system_health()
            patterns = self.failure_tracker.get_failure_patterns(hours=24)

            # 리포트 헤더
            report_lines = [
                "📊 Makenaide 일일 시스템 상태 리포트",
                "=" * 50,
                f"📅 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ""
            ]

            # 시스템 건강도 요약
            report_lines.extend([
                "🏥 시스템 건강도",
                "-" * 20,
                f"건강도 점수: {health.health_score:.1f}/100",
                f"위험 레벨: {health.risk_level}",
                f"24시간 내 총 실패: {health.total_failures_24h}회",
                f"치명적 실패: {health.critical_failures_24h}회",
                f"평균 해결 시간: {health.avg_resolution_time:.1f}분",
                f"실패율 트렌드: {health.failure_rate_trend}",
                f"주요 실패 유형: {health.most_common_failure}",
                ""
            ])

            # 주요 실패 패턴
            if patterns:
                report_lines.extend([
                    "🔍 주요 실패 패턴 (위험도 순)",
                    "-" * 30
                ])

                for i, pattern in enumerate(patterns[:5], 1):
                    risk_emoji = "🔥" if pattern.risk_score >= 80 else "⚡" if pattern.risk_score >= 60 else "💡"
                    report_lines.append(
                        f"{i}. {risk_emoji} {pattern.failure_type}:{pattern.sub_type} "
                        f"(빈도: {pattern.frequency}, 위험도: {pattern.risk_score:.1f})"
                    )
                report_lines.append("")

            # 권고사항
            recommendations = self._generate_health_recommendations(health)
            if recommendations:
                report_lines.extend([
                    "💡 권고사항",
                    "-" * 15
                ])
                for i, rec in enumerate(recommendations, 1):
                    report_lines.append(f"{i}. {rec}")
                report_lines.append("")

            # 푸터
            report_lines.extend([
                "=" * 50,
                "🤖 Makenaide Phase 4 통합 분석 시스템"
            ])

            return "\n".join(report_lines)

        except Exception as e:
            logger.error(f"❌ 일일 리포트 생성 오류: {e}")
            return f"❌ 리포트 생성 실패: {e}"

    def get_failure_analytics(self, days: int = 7) -> Dict[str, Any]:
        """실패 분석 데이터 반환 (API 또는 대시보드용)"""
        try:
            patterns = self.failure_tracker.get_failure_patterns(hours=days * 24)
            health = self.failure_tracker.get_system_health()

            analytics = {
                "system_health": {
                    "score": health.health_score,
                    "risk_level": health.risk_level,
                    "total_failures_24h": health.total_failures_24h,
                    "critical_failures_24h": health.critical_failures_24h,
                    "avg_resolution_time": health.avg_resolution_time,
                    "trend": health.failure_rate_trend,
                    "most_common_failure": health.most_common_failure
                },
                "failure_patterns": [
                    {
                        "type": p.failure_type,
                        "sub_type": p.sub_type,
                        "frequency": p.frequency,
                        "risk_score": p.risk_score,
                        "trend": p.trend,
                        "last_occurrence": p.last_occurrence
                    }
                    for p in patterns[:10]
                ],
                "summary": {
                    "total_patterns": len(patterns),
                    "high_risk_patterns": len([p for p in patterns if p.risk_score >= 70]),
                    "increasing_patterns": len([p for p in patterns if p.trend == "increasing"]),
                    "analysis_period_days": days,
                    "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            }

            return analytics

        except Exception as e:
            logger.error(f"❌ 실패 분석 데이터 생성 오류: {e}")
            return {}

# 전역 인스턴스 (싱글톤 패턴)
_global_failure_manager: Optional[IntegratedFailureManager] = None

def get_failure_manager() -> IntegratedFailureManager:
    """전역 실패 관리자 인스턴스 반환"""
    global _global_failure_manager
    if _global_failure_manager is None:
        _global_failure_manager = IntegratedFailureManager()
    return _global_failure_manager

# 편의 함수들 (기존 코드와의 호환성)
def handle_failure_with_phase4(failure_type: str,
                              error_message: str,
                              execution_id: str,
                              sub_type: Optional[str] = None,
                              severity: str = "MEDIUM",
                              phase: Optional[str] = None,
                              metadata: Optional[Dict] = None) -> Tuple[bool, int]:
    """Phase 4 통합 실패 처리 (편의 함수)"""
    manager = get_failure_manager()
    return manager.handle_failure_with_tracking(
        failure_type=failure_type,
        error_message=error_message,
        execution_id=execution_id,
        sub_type=sub_type,
        severity=severity,
        phase=phase,
        metadata=metadata
    )

def generate_daily_failure_report() -> str:
    """일일 실패 리포트 생성 (편의 함수)"""
    manager = get_failure_manager()
    return manager.generate_daily_report()

def get_system_analytics(days: int = 7) -> Dict[str, Any]:
    """시스템 분석 데이터 반환 (편의 함수)"""
    manager = get_failure_manager()
    return manager.get_failure_analytics(days)

if __name__ == "__main__":
    # 테스트 코드
    manager = IntegratedFailureManager()

    # 테스트 실패 처리
    success, failure_id = manager.handle_failure_with_tracking(
        failure_type="API_KEY_MISSING",
        sub_type="API_ACCESS_KEY_MISSING",
        error_message="UPBIT_ACCESS_KEY 환경변수가 설정되지 않음",
        execution_id="phase4_test_001",
        severity="CRITICAL",
        phase="초기화",
        metadata={"config_file": ".env", "test_mode": True}
    )

    print(f"✅ Phase 4 통합 테스트: 알림={success}, 추적ID={failure_id}")

    # 일일 리포트 생성
    report = manager.generate_daily_report()
    print("\n" + report)