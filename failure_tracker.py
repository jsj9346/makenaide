#!/usr/bin/env python3
"""
Makenaide Phase 4: 실패 패턴 분석 및 예방 시스템

SQLite 기반 실패 이력 추적, 패턴 분석, 예측적 분석을 제공하는
지능형 실패 관리 시스템입니다.

🎯 주요 기능:
- 실시간 실패 이력 추적 및 저장
- 패턴 분석을 통한 반복 실패 감지
- 시간대별/유형별 실패 트렌드 분석
- 예측적 위험도 평가
- 자동 복구 제안 시스템
- 실패 방지 권고사항 생성
"""

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from collections import defaultdict, Counter
from pathlib import Path
import statistics
import logging

# Phase 1-3 연동을 위한 import
from sns_notification_system import (
    FailureType, FailureSubType, FailureSeverity,
    NotificationMessage, NotificationLevel, NotificationCategory
)

logger = logging.getLogger(__name__)

@dataclass
class FailureRecord:
    """실패 기록 데이터 클래스"""
    timestamp: str
    execution_id: str
    failure_type: str
    sub_type: Optional[str]
    severity: str
    phase: Optional[str]
    error_message: str
    metadata: str = "{}"  # JSON 문자열로 변경
    id: int = 0  # 테스트용 ID 필드 추가
    recovery_attempted: bool = False
    recovery_successful: bool = False
    resolution_time: Optional[int] = None  # 해결까지 걸린 시간(분)
    similar_failure_count: int = 0
    failure_hash: Optional[str] = None

@dataclass
class FailurePattern:
    """실패 패턴 분석 결과"""
    pattern_id: str
    failure_type: str
    sub_type: str
    occurrence_count: int  # frequency 대신 occurrence_count 사용
    first_occurrence: str  # 추가
    last_occurrence: str
    avg_resolution_time: float
    success_rate: float
    risk_score: float
    trend: str  # "increasing", "stable", "decreasing"
    recommendations: str  # JSON 문자열로 변경
    metadata: str = "{}"  # 추가

@dataclass
class SystemHealthMetrics:
    """시스템 건강도 메트릭"""
    timestamp: str
    total_failures_24h: int = 0
    critical_failures_24h: int = 0
    avg_resolution_time: float = 0.0
    failure_rate_trend: str = "stable"
    most_common_failure: str = ""
    risk_level: str = "LOW"  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    health_score: float = 100.0  # 0-100
    metrics_json: str = "{}"
    # 테스트용 추가 필드들
    failure_rate: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    error_count: int = 0
    success_rate: float = 1.0
    active_connections: int = 0

class FailureTracker:
    """Phase 4: 실패 패턴 분석 및 예방 시스템"""

    def __init__(self, db_path: str = "./makenaide_failures.db"):
        """
        실패 추적기 초기화

        Args:
            db_path: SQLite 데이터베이스 파일 경로
        """
        self.db_path = db_path
        self.init_database()
        logger.info(f"🔍 실패 추적 시스템 초기화 완료: {db_path}")

    def init_database(self):
        """SQLite 데이터베이스 및 테이블 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 실패 기록 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS failure_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        execution_id TEXT NOT NULL,
                        failure_type TEXT NOT NULL,
                        sub_type TEXT,
                        severity TEXT NOT NULL,
                        phase TEXT,
                        error_message TEXT NOT NULL,
                        metadata TEXT,
                        recovery_attempted INTEGER DEFAULT 0,
                        recovery_successful INTEGER DEFAULT 0,
                        resolution_time INTEGER,
                        similar_failure_count INTEGER DEFAULT 0,
                        failure_hash TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # 인덱스 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_records_timestamp ON failure_records(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_records_type ON failure_records(failure_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_records_subtype ON failure_records(sub_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_records_severity ON failure_records(severity)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_records_hash ON failure_records(failure_hash)")

                # 패턴 분석 결과 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS failure_patterns (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pattern_id TEXT UNIQUE NOT NULL,
                        failure_type TEXT NOT NULL,
                        sub_type TEXT,
                        frequency INTEGER DEFAULT 1,
                        first_occurrence TEXT NOT NULL,
                        last_occurrence TEXT NOT NULL,
                        avg_resolution_time REAL DEFAULT 0,
                        success_rate REAL DEFAULT 0,
                        risk_score REAL DEFAULT 0,
                        trend TEXT DEFAULT 'stable',
                        recommendations TEXT,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # 패턴 테이블 인덱스 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_patterns_id ON failure_patterns(pattern_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_patterns_type ON failure_patterns(failure_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_failure_patterns_risk ON failure_patterns(risk_score)")

                # 시스템 건강도 메트릭 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_health_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_failures_24h INTEGER DEFAULT 0,
                        critical_failures_24h INTEGER DEFAULT 0,
                        avg_resolution_time REAL DEFAULT 0,
                        failure_rate_trend TEXT DEFAULT 'stable',
                        most_common_failure TEXT,
                        risk_level TEXT DEFAULT 'LOW',
                        health_score REAL DEFAULT 100,
                        metrics_json TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # 시스템 건강도 테이블 인덱스 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_system_health_timestamp ON system_health_metrics(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_system_health_risk ON system_health_metrics(risk_level)")

                # 복구 시도 이력 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS recovery_attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        failure_record_id INTEGER,
                        recovery_method TEXT NOT NULL,
                        attempted_at TEXT NOT NULL,
                        successful INTEGER DEFAULT 0,
                        execution_time INTEGER,  -- 실행 시간(초)
                        error_message TEXT,
                        metadata TEXT,  -- JSON
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (failure_record_id) REFERENCES failure_records (id)
                    )
                """)

                conn.commit()
                logger.info("✅ 실패 추적 데이터베이스 테이블 초기화 완료")

        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
            raise

    def generate_failure_hash(self, failure_type: str, sub_type: str, error_message: str) -> str:
        """실패 패턴 식별을 위한 해시 생성"""
        # 에러 메시지에서 동적 부분 제거 (타임스탬프, ID 등)
        cleaned_message = error_message

        # 일반적인 동적 패턴 제거
        import re
        patterns_to_remove = [
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # 타임스탬프
            r'\d{8}_\d{6}',  # execution_id 패턴
            r'ID: \w+',  # ID 패턴
            r'\d+\.\d+\.\d+\.\d+',  # IP 주소
            r'[0-9a-f]{8,}',  # 긴 16진수 해시
        ]

        for pattern in patterns_to_remove:
            cleaned_message = re.sub(pattern, '[DYNAMIC]', cleaned_message)

        # 해시 생성
        hash_input = f"{failure_type}:{sub_type}:{cleaned_message[:200]}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:12]

    def record_failure(self,
                      failure_type: str,
                      error_message: str,
                      execution_id: str,
                      sub_type: Optional[str] = None,
                      severity: str = "MEDIUM",
                      phase: Optional[str] = None,
                      metadata: Optional[Dict] = None) -> int:
        """
        실패 기록을 데이터베이스에 저장

        Args:
            failure_type: 실패 유형 (FailureType enum value)
            error_message: 에러 메시지
            execution_id: 실행 ID
            sub_type: 상세 실패 유형 (FailureSubType enum value)
            severity: 심각도 (FailureSeverity enum value)
            phase: 실패 발생 단계
            metadata: 추가 메타데이터

        Returns:
            int: 생성된 실패 기록 ID
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            metadata = metadata or {}
            failure_hash = self.generate_failure_hash(failure_type, sub_type or "", error_message)

            # 유사한 실패 개수 계산
            similar_count = self._count_similar_failures(failure_hash, hours=24)

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO failure_records (
                        timestamp, execution_id, failure_type, sub_type, severity,
                        phase, error_message, metadata, similar_failure_count, failure_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    timestamp, execution_id, failure_type, sub_type, severity,
                    phase, error_message, json.dumps(metadata), similar_count, failure_hash
                ))

                failure_id = cursor.lastrowid
                conn.commit()

                logger.info(f"📝 실패 기록 저장 완료: ID={failure_id}, Type={failure_type}, Hash={failure_hash}")

                # 패턴 분석 업데이트
                self._update_failure_patterns(failure_type, sub_type, failure_hash, timestamp)

                return failure_id

        except Exception as e:
            logger.error(f"❌ 실패 기록 저장 오류: {e}")
            raise

    def _count_similar_failures(self, failure_hash: str, hours: int = 24) -> int:
        """지정된 시간 내 유사한 실패 개수 계산"""
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM failure_records
                    WHERE failure_hash = ? AND timestamp >= ?
                """, (failure_hash, cutoff_time))

                count = cursor.fetchone()[0]
                return count

        except Exception as e:
            logger.error(f"❌ 유사 실패 개수 계산 오류: {e}")
            return 0

    def _update_failure_patterns(self, failure_type: str, sub_type: str, failure_hash: str, timestamp: str):
        """실패 패턴 분석 업데이트"""
        try:
            pattern_id = f"{failure_type}:{sub_type or 'NONE'}:{failure_hash}"

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 기존 패턴 확인
                cursor.execute("""
                    SELECT frequency, first_occurrence FROM failure_patterns
                    WHERE pattern_id = ?
                """, (pattern_id,))

                result = cursor.fetchone()

                if result:
                    # 기존 패턴 업데이트
                    frequency, first_occurrence = result
                    new_frequency = frequency + 1

                    cursor.execute("""
                        UPDATE failure_patterns
                        SET frequency = ?, last_occurrence = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE pattern_id = ?
                    """, (new_frequency, timestamp, pattern_id))

                else:
                    # 새 패턴 생성
                    cursor.execute("""
                        INSERT INTO failure_patterns (
                            pattern_id, failure_type, sub_type, frequency,
                            first_occurrence, last_occurrence
                        ) VALUES (?, ?, ?, 1, ?, ?)
                    """, (pattern_id, failure_type, sub_type, timestamp, timestamp))

                conn.commit()
                logger.debug(f"📊 패턴 업데이트 완료: {pattern_id}")

        except Exception as e:
            logger.error(f"❌ 패턴 업데이트 오류: {e}")

    def get_failure_patterns(self, hours: int = 168) -> List[FailurePattern]:
        """
        지정된 시간 내 실패 패턴 분석 결과 반환

        Args:
            hours: 분석 기간 (기본: 168시간 = 7일)

        Returns:
            List[FailurePattern]: 실패 패턴 리스트
        """
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        p.pattern_id, p.failure_type, p.sub_type, p.frequency,
                        p.last_occurrence, p.avg_resolution_time, p.success_rate,
                        p.risk_score, p.trend, p.recommendations
                    FROM failure_patterns p
                    WHERE p.last_occurrence >= ?
                    ORDER BY p.frequency DESC, p.risk_score DESC
                """, (cutoff_time,))

                patterns = []
                for row in cursor.fetchall():
                    pattern_id, failure_type, sub_type, frequency, last_occurrence, \
                    avg_resolution_time, success_rate, risk_score, trend, recommendations = row

                    # 위험도 점수 계산
                    calculated_risk_score = self._calculate_risk_score(
                        frequency, avg_resolution_time, success_rate, last_occurrence
                    )

                    # 추천사항 파싱
                    try:
                        recommendations_list = json.loads(recommendations) if recommendations else []
                    except:
                        recommendations_list = []

                    pattern = FailurePattern(
                        pattern_id=pattern_id,
                        failure_type=failure_type,
                        sub_type=sub_type or "UNKNOWN",
                        frequency=frequency,
                        last_occurrence=last_occurrence,
                        avg_resolution_time=avg_resolution_time or 0,
                        success_rate=success_rate or 0,
                        risk_score=calculated_risk_score,
                        trend=trend or "stable",
                        recommendations=recommendations_list
                    )

                    patterns.append(pattern)

                logger.info(f"📊 {len(patterns)}개 실패 패턴 분석 완료")
                return patterns

        except Exception as e:
            logger.error(f"❌ 실패 패턴 조회 오류: {e}")
            return []

    def _calculate_risk_score(self, frequency: int, avg_resolution_time: float,
                            success_rate: float, last_occurrence: str) -> float:
        """위험도 점수 계산 (0-100)"""
        try:
            # 빈도 점수 (0-40)
            frequency_score = min(frequency * 2, 40)

            # 해결 시간 점수 (0-30)
            resolution_score = min(avg_resolution_time / 10, 30)

            # 실패율 점수 (0-20)
            failure_rate_score = (100 - success_rate) * 0.2

            # 최근성 점수 (0-10)
            try:
                last_time = datetime.strptime(last_occurrence, '%Y-%m-%d %H:%M:%S')
                hours_ago = (datetime.now() - last_time).total_seconds() / 3600
                recency_score = max(10 - hours_ago / 24, 0)
            except:
                recency_score = 0

            total_score = frequency_score + resolution_score + failure_rate_score + recency_score
            return min(total_score, 100)

        except Exception as e:
            logger.error(f"❌ 위험도 점수 계산 오류: {e}")
            return 0

    def get_system_health(self) -> SystemHealthMetrics:
        """현재 시스템 건강도 메트릭 반환"""
        try:
            now = datetime.now()
            yesterday = now - timedelta(days=1)
            yesterday_str = yesterday.strftime('%Y-%m-%d %H:%M:%S')

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 24시간 내 실패 통계
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_failures,
                        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_failures,
                        AVG(CASE WHEN resolution_time IS NOT NULL THEN resolution_time END) as avg_resolution
                    FROM failure_records
                    WHERE timestamp >= ?
                """, (yesterday_str,))

                stats = cursor.fetchone()
                total_failures, critical_failures, avg_resolution_time = stats
                avg_resolution_time = avg_resolution_time or 0

                # 가장 빈번한 실패 유형
                cursor.execute("""
                    SELECT failure_type, COUNT(*) as count
                    FROM failure_records
                    WHERE timestamp >= ?
                    GROUP BY failure_type
                    ORDER BY count DESC
                    LIMIT 1
                """, (yesterday_str,))

                most_common = cursor.fetchone()
                most_common_failure = most_common[0] if most_common else "NONE"

                # 건강도 점수 계산
                health_score = self._calculate_health_score(total_failures, critical_failures, avg_resolution_time)

                # 위험 레벨 결정
                risk_level = self._determine_risk_level(health_score, critical_failures, total_failures)

                # 트렌드 분석 (간단한 7일 vs 1일 비교)
                week_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("""
                    SELECT COUNT(*) FROM failure_records
                    WHERE timestamp >= ?
                """, (week_ago,))

                week_failures = cursor.fetchone()[0]
                daily_avg = week_failures / 7 if week_failures > 0 else 0

                if total_failures > daily_avg * 1.5:
                    failure_rate_trend = "increasing"
                elif total_failures < daily_avg * 0.5:
                    failure_rate_trend = "decreasing"
                else:
                    failure_rate_trend = "stable"

                metrics = SystemHealthMetrics(
                    timestamp=now.strftime('%Y-%m-%d %H:%M:%S'),
                    total_failures_24h=total_failures,
                    critical_failures_24h=critical_failures,
                    avg_resolution_time=avg_resolution_time,
                    failure_rate_trend=failure_rate_trend,
                    most_common_failure=most_common_failure,
                    risk_level=risk_level,
                    health_score=health_score
                )

                logger.info(f"🏥 시스템 건강도 분석 완료: Score={health_score:.1f}, Risk={risk_level}")
                return metrics

        except Exception as e:
            logger.error(f"❌ 시스템 건강도 분석 오류: {e}")
            return SystemHealthMetrics(
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                total_failures_24h=0,
                critical_failures_24h=0,
                avg_resolution_time=0,
                failure_rate_trend="unknown",
                most_common_failure="NONE",
                risk_level="UNKNOWN",
                health_score=0
            )

    def _calculate_health_score(self, total_failures: int, critical_failures: int, avg_resolution_time: float) -> float:
        """시스템 건강도 점수 계산 (0-100)"""
        base_score = 100

        # 총 실패 횟수에 따른 감점
        failure_penalty = min(total_failures * 5, 50)

        # 치명적 실패에 따른 추가 감점
        critical_penalty = critical_failures * 15

        # 평균 해결 시간에 따른 감점
        resolution_penalty = min(avg_resolution_time / 10, 20)

        health_score = base_score - failure_penalty - critical_penalty - resolution_penalty
        return max(health_score, 0)

    def _determine_risk_level(self, health_score: float, critical_failures: int, total_failures: int) -> str:
        """위험 레벨 결정"""
        if critical_failures >= 3 or health_score < 30:
            return "CRITICAL"
        elif critical_failures >= 1 or health_score < 50 or total_failures >= 10:
            return "HIGH"
        elif health_score < 70 or total_failures >= 5:
            return "MEDIUM"
        else:
            return "LOW"

    def generate_recommendations(self, pattern: FailurePattern) -> List[str]:
        """실패 패턴에 대한 권고사항 생성"""
        recommendations = []

        # 빈도 기반 권고
        if pattern.frequency >= 5:
            recommendations.append(f"🔄 {pattern.failure_type} 유형의 반복적 실패 감지 - 근본 원인 분석 필요")

        # 심각도 기반 권고
        if pattern.sub_type and "CRITICAL" in pattern.sub_type:
            recommendations.append("🚨 치명적 실패 패턴 - 즉시 시스템 점검 및 모니터링 강화 권장")

        # 해결 시간 기반 권고
        if pattern.avg_resolution_time > 60:
            recommendations.append("⏰ 평균 해결 시간이 길음 - 자동화된 복구 절차 도입 고려")

        # 성공률 기반 권고
        if pattern.success_rate < 50:
            recommendations.append("📈 복구 성공률이 낮음 - 복구 절차 개선 및 문서화 필요")

        # 트렌드 기반 권고
        if pattern.trend == "increasing":
            recommendations.append("📊 실패 증가 추세 - 예방적 조치 및 시스템 업그레이드 검토")

        # 특정 실패 유형별 권고
        failure_specific_recommendations = {
            "API_KEY_MISSING": [
                "🔑 API 키 관리 자동화 및 만료 알림 설정",
                "🔒 보안 자격 증명 로테이션 정책 수립"
            ],
            "MEMORY_INSUFFICIENT": [
                "💾 메모리 사용량 모니터링 강화",
                "⚡ EC2 인스턴스 업그레이드 고려"
            ],
            "RATE_LIMIT_EXCEEDED": [
                "🚦 API 호출 속도 제한 및 재시도 로직 개선",
                "📊 API 사용량 패턴 분석 및 최적화"
            ]
        }

        if pattern.failure_type in failure_specific_recommendations:
            recommendations.extend(failure_specific_recommendations[pattern.failure_type])

        return recommendations[:5]  # 최대 5개 권고사항

    def record_recovery_attempt(self, failure_record_id: int, recovery_method: str,
                              successful: bool, execution_time: int = 0,
                              error_message: str = "", metadata: Dict = None) -> int:
        """복구 시도 기록"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO recovery_attempts (
                        failure_record_id, recovery_method, attempted_at, successful,
                        execution_time, error_message, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    failure_record_id, recovery_method,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    1 if successful else 0, execution_time, error_message,
                    json.dumps(metadata or {})
                ))

                attempt_id = cursor.lastrowid

                # 원본 실패 기록 업데이트
                cursor.execute("""
                    UPDATE failure_records
                    SET recovery_attempted = 1, recovery_successful = ?
                    WHERE id = ?
                """, (1 if successful else 0, failure_record_id))

                conn.commit()

                logger.info(f"🔧 복구 시도 기록: ID={attempt_id}, 성공={successful}")
                return attempt_id

        except Exception as e:
            logger.error(f"❌ 복구 시도 기록 오류: {e}")
            raise

    def cleanup_old_records(self, days: int = 30):
        """오래된 실패 기록 정리"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 오래된 기록 삭제
                cursor.execute("DELETE FROM failure_records WHERE timestamp < ?", (cutoff_date,))
                deleted_failures = cursor.rowcount

                cursor.execute("DELETE FROM recovery_attempts WHERE attempted_at < ?", (cutoff_date,))
                deleted_recoveries = cursor.rowcount

                cursor.execute("DELETE FROM system_health_metrics WHERE timestamp < ?", (cutoff_date,))
                deleted_metrics = cursor.rowcount

                conn.commit()

                logger.info(f"🧹 오래된 기록 정리 완료: 실패={deleted_failures}, 복구={deleted_recoveries}, 메트릭={deleted_metrics}")

        except Exception as e:
            logger.error(f"❌ 기록 정리 오류: {e}")

    def get_recent_patterns(self, since_date: str) -> List[FailurePattern]:
        """최근 패턴 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pattern_id, failure_type, sub_type, frequency,
                           first_occurrence, last_occurrence, avg_resolution_time,
                           success_rate, risk_score, trend, recommendations
                    FROM failure_patterns
                    WHERE last_occurrence >= ?
                    ORDER BY last_occurrence DESC
                """, (since_date,))

                patterns = []
                for row in cursor.fetchall():
                    pattern = FailurePattern(
                        pattern_id=row[0],
                        failure_type=row[1],
                        sub_type=row[2],
                        occurrence_count=row[3],
                        first_occurrence=row[4] if len(row) > 4 else row[5],  # fallback
                        last_occurrence=row[5],
                        avg_resolution_time=row[6],
                        success_rate=row[7],
                        risk_score=row[8],
                        trend=row[9],
                        recommendations=row[10],
                        metadata="{}"
                    )
                    patterns.append(pattern)

                return patterns

        except Exception as e:
            logger.error(f"최근 패턴 조회 오류: {e}")
            return []

    def get_recent_failures(self, hours: int = 24) -> List[FailureRecord]:
        """최근 실패 기록 조회"""
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, timestamp, execution_id, failure_type, sub_type,
                           severity, phase, error_message, metadata
                    FROM failure_records
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                """, (cutoff_time,))

                failures = []
                for row in cursor.fetchall():
                    failure = FailureRecord(
                        id=row[0],
                        timestamp=row[1],
                        execution_id=row[2],
                        failure_type=row[3],
                        sub_type=row[4],
                        severity=row[5],
                        phase=row[6],
                        error_message=row[7],
                        metadata=row[8] or "{}"
                    )
                    failures.append(failure)

                return failures

        except Exception as e:
            logger.error(f"최근 실패 기록 조회 오류: {e}")
            return []

    def get_failure_statistics(self, days: int = 7) -> Dict:
        """실패 통계 조회"""
        try:
            cutoff_time = (datetime.now() - timedelta(days=days)).isoformat()

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 전체 실패 수
                cursor.execute("""
                    SELECT COUNT(*) FROM failure_records
                    WHERE timestamp >= ?
                """, (cutoff_time,))
                total_failures = cursor.fetchone()[0]

                # 심각도별 통계
                cursor.execute("""
                    SELECT severity, COUNT(*) FROM failure_records
                    WHERE timestamp >= ?
                    GROUP BY severity
                """, (cutoff_time,))
                severity_stats = dict(cursor.fetchall())

                # 타입별 통계
                cursor.execute("""
                    SELECT failure_type, COUNT(*) FROM failure_records
                    WHERE timestamp >= ?
                    GROUP BY failure_type
                """, (cutoff_time,))
                type_stats = dict(cursor.fetchall())

                return {
                    'total_failures': total_failures,
                    'severity_stats': severity_stats,
                    'type_stats': type_stats,
                    'period_days': days
                }

        except Exception as e:
            logger.error(f"실패 통계 조회 오류: {e}")
            return {'total_failures': 0, 'severity_stats': {}, 'type_stats': {}, 'period_days': days}

    def get_current_system_health(self) -> Optional[SystemHealthMetrics]:
        """현재 시스템 건강도 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp, total_failures_24h, critical_failures_24h,
                           avg_resolution_time, failure_rate_trend, most_common_failure,
                           risk_level, health_score, metrics_json
                    FROM system_health_metrics
                    ORDER BY timestamp DESC
                    LIMIT 1
                """)

                row = cursor.fetchone()
                if row:
                    return SystemHealthMetrics(
                        timestamp=row[0],
                        total_failures_24h=row[1],
                        critical_failures_24h=row[2],
                        avg_resolution_time=row[3],
                        failure_rate_trend=row[4],
                        most_common_failure=row[5],
                        risk_level=row[6],
                        health_score=row[7],
                        metrics_json=row[8]
                    )

                return None

        except Exception as e:
            logger.error(f"현재 시스템 건강도 조회 오류: {e}")
            return None

    def get_system_health_history(self, start_date: str, end_date: str) -> List[SystemHealthMetrics]:
        """시스템 건강도 이력 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp, total_failures_24h, critical_failures_24h,
                           avg_resolution_time, failure_rate_trend, most_common_failure,
                           risk_level, health_score, metrics_json
                    FROM system_health_metrics
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp ASC
                """, (start_date, end_date))

                history = []
                for row in cursor.fetchall():
                    metrics = SystemHealthMetrics(
                        timestamp=row[0],
                        total_failures_24h=row[1],
                        critical_failures_24h=row[2],
                        avg_resolution_time=row[3],
                        failure_rate_trend=row[4],
                        most_common_failure=row[5],
                        risk_level=row[6],
                        health_score=row[7],
                        metrics_json=row[8]
                    )
                    history.append(metrics)

                return history

        except Exception as e:
            logger.error(f"시스템 건강도 이력 조회 오류: {e}")
            return []

    def record_system_health(self, metrics: SystemHealthMetrics) -> int:
        """시스템 건강도 메트릭 기록"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO system_health_metrics (
                        timestamp, total_failures_24h, critical_failures_24h,
                        avg_resolution_time, failure_rate_trend, most_common_failure,
                        risk_level, health_score, metrics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp,
                    metrics.total_failures_24h,
                    metrics.critical_failures_24h,
                    metrics.avg_resolution_time,
                    metrics.failure_rate_trend,
                    metrics.most_common_failure,
                    metrics.risk_level,
                    metrics.health_score,
                    metrics.metrics_json
                ))

                health_id = cursor.lastrowid
                conn.commit()
                return health_id

        except Exception as e:
            logger.error(f"시스템 건강도 기록 오류: {e}")
            return 0

if __name__ == "__main__":
    # 테스트 코드
    tracker = FailureTracker()

    # 샘플 실패 기록
    failure_id = tracker.record_failure(
        failure_type="API_KEY_MISSING",
        sub_type="API_ACCESS_KEY_MISSING",
        error_message="UPBIT_ACCESS_KEY 환경변수가 설정되지 않음",
        execution_id="20250918_test",
        severity="CRITICAL",
        phase="초기화",
        metadata={"config_file": ".env"}
    )

    print(f"✅ 실패 기록 생성: ID={failure_id}")

    # 시스템 건강도 확인
    health = tracker.get_system_health()
    print(f"🏥 시스템 건강도: {health.health_score:.1f}/100, 위험도: {health.risk_level}")

    # 패턴 분석
    patterns = tracker.get_failure_patterns()
    print(f"📊 실패 패턴 {len(patterns)}개 감지")