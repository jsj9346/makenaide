#!/usr/bin/env python3
"""
Makenaide Phase 4 자동 복구 제안 시스템 (Automatic Recovery Suggestion System)

Phase 4 자동 복구 핵심 기능:
- 장애 유형별 맞춤 복구 시나리오
- 자동 진단 및 근본 원인 분석
- 단계별 복구 액션 플랜
- 복구 성공률 추적 및 학습
- 안전한 자동 실행 (승인 기반)

복구 전략:
- 즉시 복구 (Immediate Recovery): 안전한 자동 실행
- 감독 복구 (Supervised Recovery): 승인 후 실행
- 수동 복구 (Manual Recovery): 가이드만 제공
"""

import os
import sys
import json
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, NamedTuple, Callable
from dataclasses import dataclass
from enum import Enum
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from failure_tracker import FailureTracker, FailureRecord, FailurePattern
from predictive_analysis import PredictiveAnalyzer, PredictionResult, RiskLevel
from sns_notification_system import (
    FailureType, FailureSubType, FailureSeverity,
    NotificationLevel, NotificationCategory, NotificationMessage
)

class RecoveryActionType(Enum):
    """복구 액션 유형"""
    RESTART_SERVICE = "RESTART_SERVICE"           # 서비스 재시작
    CLEAR_CACHE = "CLEAR_CACHE"                   # 캐시 정리
    RESET_CONNECTION = "RESET_CONNECTION"         # 연결 재설정
    CLEANUP_LOGS = "CLEANUP_LOGS"                 # 로그 정리
    RESTART_PROCESS = "RESTART_PROCESS"           # 프로세스 재시작
    BACKUP_DATA = "BACKUP_DATA"                   # 데이터 백업
    VALIDATE_CONFIG = "VALIDATE_CONFIG"           # 설정 검증
    REPAIR_DATABASE = "REPAIR_DATABASE"           # 데이터베이스 복구
    NETWORK_RESET = "NETWORK_RESET"               # 네트워크 재설정
    RESOURCE_CLEANUP = "RESOURCE_CLEANUP"         # 리소스 정리
    API_RECONNECT = "API_RECONNECT"               # API 재연결
    SCHEDULE_MAINTENANCE = "SCHEDULE_MAINTENANCE" # 유지보수 스케줄링

class RecoveryExecutionMode(Enum):
    """복구 실행 모드"""
    AUTOMATIC = "AUTOMATIC"     # 자동 실행 (안전한 액션만)
    SUPERVISED = "SUPERVISED"   # 승인 후 실행
    MANUAL = "MANUAL"          # 수동 실행 (가이드만 제공)
    SIMULATION = "SIMULATION"   # 시뮬레이션 모드

class RecoveryStatus(Enum):
    """복구 상태"""
    PENDING = "PENDING"           # 대기 중
    APPROVED = "APPROVED"         # 승인됨
    EXECUTING = "EXECUTING"       # 실행 중
    COMPLETED = "COMPLETED"       # 완료
    FAILED = "FAILED"            # 실패
    CANCELLED = "CANCELLED"       # 취소됨
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"  # 승인 필요

@dataclass
class RecoveryAction:
    """개별 복구 액션"""
    action_id: str
    action_type: RecoveryActionType
    description: str
    command: Optional[str]
    parameters: Dict
    execution_mode: RecoveryExecutionMode
    estimated_duration: int  # seconds
    risk_level: RiskLevel
    prerequisites: List[str]
    success_criteria: List[str]
    rollback_command: Optional[str]

@dataclass
class RecoveryPlan:
    """복구 계획"""
    plan_id: str
    failure_type: str
    failure_sub_type: Optional[str]
    severity: FailureSeverity
    actions: List[RecoveryAction]
    execution_order: List[str]  # action_id 순서
    estimated_total_duration: int
    success_probability: float
    created_at: datetime
    approved_by: Optional[str]
    approval_required: bool

@dataclass
class RecoveryExecution:
    """복구 실행 기록"""
    execution_id: str
    plan_id: str
    failure_record_id: int
    status: RecoveryStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    executed_actions: List[str]
    success_rate: float
    error_message: Optional[str]
    metadata: Dict

class AutoRecoverySystem:
    """Phase 4 자동 복구 시스템"""

    def __init__(self, db_path: str = "makenaide_local.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.failure_tracker = FailureTracker(db_path)
        self.predictive_analyzer = PredictiveAnalyzer(db_path)

        # 복구 설정
        self.config = {
            'enable_automatic_recovery': True,
            'require_approval_for_high_risk': True,
            'max_concurrent_recoveries': 3,
            'recovery_timeout_minutes': 30,
            'simulation_mode': False
        }

        # 안전한 자동 실행이 가능한 액션들
        self.safe_automatic_actions = {
            RecoveryActionType.CLEAR_CACHE,
            RecoveryActionType.CLEANUP_LOGS,
            RecoveryActionType.VALIDATE_CONFIG,
            RecoveryActionType.API_RECONNECT
        }

        # 복구 액션 라이브러리
        self.recovery_library = self._initialize_recovery_library()

        self._init_recovery_tables()

    def _init_recovery_tables(self):
        """복구 시스템 테이블 초기화"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 복구 계획 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id TEXT UNIQUE NOT NULL,
                    failure_type TEXT NOT NULL,
                    failure_sub_type TEXT,
                    severity TEXT NOT NULL,
                    actions TEXT NOT NULL,
                    execution_order TEXT NOT NULL,
                    estimated_total_duration INTEGER NOT NULL,
                    success_probability REAL NOT NULL,
                    approval_required BOOLEAN NOT NULL,
                    approved_by TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT
                )
            """)

            # 복구 실행 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT UNIQUE NOT NULL,
                    plan_id TEXT NOT NULL,
                    failure_record_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    executed_actions TEXT,
                    success_rate REAL,
                    error_message TEXT,
                    metadata TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (failure_record_id) REFERENCES failure_records (id),
                    FOREIGN KEY (plan_id) REFERENCES recovery_plans (plan_id)
                )
            """)

            # 복구 액션 성공률 추적 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_action_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_type TEXT NOT NULL,
                    failure_type TEXT NOT NULL,
                    execution_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    avg_duration REAL DEFAULT 0,
                    last_success_rate REAL DEFAULT 0,
                    last_updated TEXT NOT NULL
                )
            """)

            conn.commit()

    def _initialize_recovery_library(self) -> Dict[str, List[RecoveryAction]]:
        """복구 액션 라이브러리 초기화"""
        library = {}

        # API 오류 복구 액션들
        library['API_ERROR'] = [
            RecoveryAction(
                action_id="api_reconnect_01",
                action_type=RecoveryActionType.API_RECONNECT,
                description="업비트 API 연결 재설정",
                command=None,  # Python 함수로 처리
                parameters={'retry_count': 3, 'backoff_seconds': 5},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=30,
                risk_level=RiskLevel.LOW,
                prerequisites=["인터넷 연결 확인"],
                success_criteria=["API 응답 200 확인", "잔고 조회 성공"],
                rollback_command=None
            ),
            RecoveryAction(
                action_id="api_cache_clear_01",
                action_type=RecoveryActionType.CLEAR_CACHE,
                description="API 응답 캐시 정리",
                command="rm -rf /tmp/api_cache/*",
                parameters={},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=10,
                risk_level=RiskLevel.MINIMAL,
                prerequisites=[],
                success_criteria=["캐시 디렉토리 정리 확인"],
                rollback_command=None
            )
        ]

        # 데이터베이스 오류 복구 액션들
        library['DATABASE_ERROR'] = [
            RecoveryAction(
                action_id="db_backup_01",
                action_type=RecoveryActionType.BACKUP_DATA,
                description="SQLite 데이터베이스 백업",
                command="cp makenaide_local.db makenaide_local_backup_{timestamp}.db",
                parameters={'backup_path': './backups/'},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=60,
                risk_level=RiskLevel.MINIMAL,
                prerequisites=["디스크 공간 확인"],
                success_criteria=["백업 파일 생성 확인"],
                rollback_command=None
            ),
            RecoveryAction(
                action_id="db_repair_01",
                action_type=RecoveryActionType.REPAIR_DATABASE,
                description="SQLite VACUUM 및 무결성 검사",
                command=None,  # Python 함수로 처리
                parameters={'check_integrity': True, 'vacuum': True},
                execution_mode=RecoveryExecutionMode.SUPERVISED,
                estimated_duration=300,
                risk_level=RiskLevel.MODERATE,
                prerequisites=["데이터베이스 백업 완료"],
                success_criteria=["PRAGMA integrity_check OK"],
                rollback_command="cp makenaide_local_backup_{timestamp}.db makenaide_local.db"
            )
        ]

        # 네트워크 오류 복구 액션들
        library['NETWORK_ERROR'] = [
            RecoveryAction(
                action_id="network_reset_01",
                action_type=RecoveryActionType.NETWORK_RESET,
                description="네트워크 연결 재설정",
                command=None,  # Python 함수로 처리
                parameters={'test_endpoints': ['https://api.upbit.com/v1/market/all']},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=45,
                risk_level=RiskLevel.LOW,
                prerequisites=[],
                success_criteria=["외부 API 연결 성공"],
                rollback_command=None
            )
        ]

        # 거래 시스템 오류 복구 액션들
        library['TRADING_ERROR'] = [
            RecoveryAction(
                action_id="trading_restart_01",
                action_type=RecoveryActionType.RESTART_PROCESS,
                description="거래 엔진 재시작",
                command=None,  # Python 함수로 처리
                parameters={'graceful_shutdown': True, 'save_state': True},
                execution_mode=RecoveryExecutionMode.SUPERVISED,
                estimated_duration=120,
                risk_level=RiskLevel.HIGH,
                prerequisites=["포지션 상태 저장"],
                success_criteria=["거래 엔진 정상 초기화"],
                rollback_command=None
            ),
            RecoveryAction(
                action_id="trading_validate_01",
                action_type=RecoveryActionType.VALIDATE_CONFIG,
                description="거래 설정 및 API 키 검증",
                command=None,  # Python 함수로 처리
                parameters={'check_api_keys': True, 'validate_balance': True},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=30,
                risk_level=RiskLevel.LOW,
                prerequisites=[],
                success_criteria=["API 키 유효성 확인", "잔고 조회 성공"],
                rollback_command=None
            )
        ]

        # 시스템 리소스 오류 복구 액션들
        library['SYSTEM_ERROR'] = [
            RecoveryAction(
                action_id="resource_cleanup_01",
                action_type=RecoveryActionType.RESOURCE_CLEANUP,
                description="시스템 리소스 정리",
                command=None,  # Python 함수로 처리
                parameters={'clear_temp': True, 'cleanup_logs': True},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=60,
                risk_level=RiskLevel.LOW,
                prerequisites=[],
                success_criteria=["디스크 공간 확보", "메모리 사용률 개선"],
                rollback_command=None
            ),
            RecoveryAction(
                action_id="log_cleanup_01",
                action_type=RecoveryActionType.CLEANUP_LOGS,
                description="오래된 로그 파일 정리",
                command="find ./logs -name '*.log' -mtime +7 -delete",
                parameters={'retention_days': 7},
                execution_mode=RecoveryExecutionMode.AUTOMATIC,
                estimated_duration=30,
                risk_level=RiskLevel.MINIMAL,
                prerequisites=[],
                success_criteria=["로그 파일 정리 완료"],
                rollback_command=None
            )
        ]

        return library

    def generate_recovery_plan(self, failure_record: FailureRecord) -> Optional[RecoveryPlan]:
        """장애 기록을 기반으로 복구 계획 생성"""
        try:
            failure_type = failure_record.failure_type
            failure_sub_type = failure_record.sub_type
            severity = FailureSeverity(failure_record.severity)

            # 해당 장애 유형의 복구 액션들 조회
            available_actions = self.recovery_library.get(failure_type, [])

            if not available_actions:
                self.logger.warning(f"장애 유형 {failure_type}에 대한 복구 액션이 없습니다.")
                return None

            # 장애 심각도와 상황에 따라 적절한 액션들 선택
            selected_actions = self._select_recovery_actions(
                available_actions, failure_record, severity
            )

            if not selected_actions:
                return None

            # 실행 순서 결정
            execution_order = self._determine_execution_order(selected_actions, failure_record)

            # 총 예상 시간 계산
            total_duration = sum(action.estimated_duration for action in selected_actions)

            # 성공 확률 예측
            success_probability = self._calculate_success_probability(
                selected_actions, failure_type, failure_sub_type
            )

            # 승인 필요 여부 결정
            approval_required = self._requires_approval(selected_actions, severity)

            plan_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{failure_record.id}"

            plan = RecoveryPlan(
                plan_id=plan_id,
                failure_type=failure_type,
                failure_sub_type=failure_sub_type,
                severity=severity,
                actions=selected_actions,
                execution_order=execution_order,
                estimated_total_duration=total_duration,
                success_probability=success_probability,
                created_at=datetime.now(),
                approved_by=None,
                approval_required=approval_required
            )

            # 계획을 데이터베이스에 저장
            self._save_recovery_plan(plan)

            return plan

        except Exception as e:
            self.logger.error(f"복구 계획 생성 오류: {e}")
            return None

    def _select_recovery_actions(self, available_actions: List[RecoveryAction],
                               failure_record: FailureRecord,
                               severity: FailureSeverity) -> List[RecoveryAction]:
        """상황에 맞는 복구 액션들 선택"""
        selected = []

        # 심각도별 액션 선택 전략
        if severity == FailureSeverity.CRITICAL:
            # 중요한 장애는 모든 가능한 복구 액션 포함
            selected = available_actions.copy()
        elif severity == FailureSeverity.HIGH:
            # 높은 심각도는 위험도가 높지 않은 액션들 선택
            selected = [action for action in available_actions
                       if action.risk_level != RiskLevel.CRITICAL]
        else:
            # 일반적인 경우는 안전한 액션들만 선택
            selected = [action for action in available_actions
                       if action.risk_level in [RiskLevel.MINIMAL, RiskLevel.LOW]]

        # 과거 성공률을 고려하여 액션 필터링
        filtered_actions = []
        for action in selected:
            success_rate = self._get_action_success_rate(action.action_type.value, failure_record.failure_type)
            if success_rate > 0.3:  # 30% 이상 성공률인 액션만 선택
                filtered_actions.append(action)

        return filtered_actions

    def _determine_execution_order(self, actions: List[RecoveryAction],
                                 failure_record: FailureRecord) -> List[str]:
        """복구 액션 실행 순서 결정"""
        # 위험도와 의존성을 고려한 순서 결정
        action_priority = {}

        for action in actions:
            priority = 0

            # 위험도가 낮을수록 먼저 실행
            if action.risk_level == RiskLevel.MINIMAL:
                priority += 100
            elif action.risk_level == RiskLevel.LOW:
                priority += 80
            elif action.risk_level == RiskLevel.MODERATE:
                priority += 60
            else:
                priority += 40

            # 액션 유형별 우선순위
            type_priority = {
                RecoveryActionType.BACKUP_DATA: 200,
                RecoveryActionType.VALIDATE_CONFIG: 150,
                RecoveryActionType.CLEAR_CACHE: 120,
                RecoveryActionType.API_RECONNECT: 100,
                RecoveryActionType.RESTART_SERVICE: 80,
                RecoveryActionType.REPAIR_DATABASE: 60,
                RecoveryActionType.RESTART_PROCESS: 40
            }

            priority += type_priority.get(action.action_type, 50)

            action_priority[action.action_id] = priority

        # 우선순위에 따라 정렬
        sorted_actions = sorted(actions, key=lambda a: action_priority[a.action_id], reverse=True)
        return [action.action_id for action in sorted_actions]

    def _calculate_success_probability(self, actions: List[RecoveryAction],
                                     failure_type: str, failure_sub_type: Optional[str]) -> float:
        """복구 성공 확률 계산"""
        if not actions:
            return 0.0

        individual_probabilities = []

        for action in actions:
            # 과거 성공률 조회
            success_rate = self._get_action_success_rate(action.action_type.value, failure_type)
            individual_probabilities.append(success_rate)

        # 액션들의 조합 성공 확률 계산 (독립 사건 가정)
        combined_failure_prob = 1.0
        for prob in individual_probabilities:
            combined_failure_prob *= (1 - prob)

        combined_success_prob = 1 - combined_failure_prob

        return min(combined_success_prob, 0.95)  # 최대 95%

    def _requires_approval(self, actions: List[RecoveryAction], severity: FailureSeverity) -> bool:
        """승인 필요 여부 결정"""
        # 고위험 액션이 포함된 경우 승인 필요
        for action in actions:
            if action.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                return True
            if action.execution_mode == RecoveryExecutionMode.SUPERVISED:
                return True

        # 중요한 장애의 경우 승인 필요
        if severity == FailureSeverity.CRITICAL:
            return True

        return False

    def execute_recovery_plan(self, plan: RecoveryPlan,
                            failure_record_id: int,
                            auto_approve: bool = False) -> RecoveryExecution:
        """복구 계획 실행"""
        execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{plan.plan_id}"

        execution = RecoveryExecution(
            execution_id=execution_id,
            plan_id=plan.plan_id,
            failure_record_id=failure_record_id,
            status=RecoveryStatus.PENDING,
            started_at=None,
            completed_at=None,
            executed_actions=[],
            success_rate=0.0,
            error_message=None,
            metadata={}
        )

        try:
            # 승인 확인
            if plan.approval_required and not auto_approve:
                execution.status = RecoveryStatus.REQUIRES_APPROVAL
                self._save_recovery_execution(execution)
                self.logger.info(f"복구 계획 {plan.plan_id}는 승인이 필요합니다.")
                return execution

            # 실행 시작
            execution.status = RecoveryStatus.EXECUTING
            execution.started_at = datetime.now()
            self._save_recovery_execution(execution)

            self.logger.info(f"복구 계획 {plan.plan_id} 실행 시작")

            # 액션들을 순서대로 실행
            total_actions = len(plan.execution_order)
            successful_actions = 0

            for action_id in plan.execution_order:
                action = next((a for a in plan.actions if a.action_id == action_id), None)
                if not action:
                    continue

                try:
                    self.logger.info(f"복구 액션 실행: {action.description}")

                    # 전제 조건 확인
                    if not self._check_prerequisites(action):
                        self.logger.warning(f"액션 {action_id} 전제 조건 불충족")
                        continue

                    # 액션 실행
                    success = self._execute_single_action(action)

                    if success:
                        successful_actions += 1
                        execution.executed_actions.append(action_id)
                        self.logger.info(f"액션 {action_id} 실행 성공")

                        # 성공 기준 확인
                        if self._validate_success_criteria(action):
                            self._update_action_stats(action.action_type.value, plan.failure_type, True)
                        else:
                            self.logger.warning(f"액션 {action_id} 성공 기준 미충족")
                    else:
                        self.logger.error(f"액션 {action_id} 실행 실패")
                        self._update_action_stats(action.action_type.value, plan.failure_type, False)

                except Exception as e:
                    self.logger.error(f"액션 {action_id} 실행 중 오류: {e}")
                    execution.error_message = str(e)

            # 실행 완료
            execution.success_rate = successful_actions / total_actions if total_actions > 0 else 0
            execution.completed_at = datetime.now()

            if execution.success_rate >= 0.8:
                execution.status = RecoveryStatus.COMPLETED
                self.logger.info(f"복구 계획 {plan.plan_id} 실행 완료 (성공률: {execution.success_rate:.1%})")
            else:
                execution.status = RecoveryStatus.FAILED
                self.logger.error(f"복구 계획 {plan.plan_id} 실행 실패 (성공률: {execution.success_rate:.1%})")

            self._save_recovery_execution(execution)
            return execution

        except Exception as e:
            execution.status = RecoveryStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.now()
            self._save_recovery_execution(execution)
            self.logger.error(f"복구 계획 실행 오류: {e}")
            return execution

    def _execute_single_action(self, action: RecoveryAction) -> bool:
        """개별 복구 액션 실행"""
        try:
            if self.config.get('simulation_mode', False):
                self.logger.info(f"[시뮬레이션] 액션 실행: {action.description}")
                time.sleep(1)  # 시뮬레이션 지연
                return True

            action_type = action.action_type

            if action_type == RecoveryActionType.API_RECONNECT:
                return self._execute_api_reconnect(action)
            elif action_type == RecoveryActionType.CLEAR_CACHE:
                return self._execute_clear_cache(action)
            elif action_type == RecoveryActionType.BACKUP_DATA:
                return self._execute_backup_data(action)
            elif action_type == RecoveryActionType.REPAIR_DATABASE:
                return self._execute_repair_database(action)
            elif action_type == RecoveryActionType.VALIDATE_CONFIG:
                return self._execute_validate_config(action)
            elif action_type == RecoveryActionType.RESOURCE_CLEANUP:
                return self._execute_resource_cleanup(action)
            elif action_type == RecoveryActionType.CLEANUP_LOGS:
                return self._execute_cleanup_logs(action)
            elif action_type == RecoveryActionType.NETWORK_RESET:
                return self._execute_network_reset(action)
            else:
                # 기본 명령어 실행
                if action.command:
                    return self._execute_shell_command(action.command, action.parameters)

            return False

        except Exception as e:
            self.logger.error(f"액션 실행 오류 ({action.action_type}): {e}")
            return False

    def _execute_api_reconnect(self, action: RecoveryAction) -> bool:
        """API 재연결 실행"""
        try:
            import requests
            import pyupbit

            # 업비트 API 연결 테스트
            retry_count = action.parameters.get('retry_count', 3)
            backoff_seconds = action.parameters.get('backoff_seconds', 5)

            for attempt in range(retry_count):
                try:
                    # 마켓 정보 조회로 연결 테스트
                    markets = pyupbit.get_tickers()
                    if markets and len(markets) > 0:
                        self.logger.info("API 재연결 성공")
                        return True
                except Exception as e:
                    self.logger.warning(f"API 연결 시도 {attempt + 1} 실패: {e}")
                    if attempt < retry_count - 1:
                        time.sleep(backoff_seconds)

            return False

        except Exception as e:
            self.logger.error(f"API 재연결 오류: {e}")
            return False

    def _execute_clear_cache(self, action: RecoveryAction) -> bool:
        """캐시 정리 실행"""
        try:
            if action.command:
                return self._execute_shell_command(action.command, action.parameters)

            # 기본 캐시 정리
            import tempfile
            import shutil

            temp_dir = tempfile.gettempdir()
            cache_patterns = ['*_cache*', '*temp*', '*.tmp']

            for pattern in cache_patterns:
                try:
                    import glob
                    files = glob.glob(os.path.join(temp_dir, pattern))
                    for file in files:
                        if os.path.isfile(file):
                            os.remove(file)
                        elif os.path.isdir(file):
                            shutil.rmtree(file)
                except Exception as e:
                    self.logger.warning(f"캐시 정리 중 오류: {e}")

            return True

        except Exception as e:
            self.logger.error(f"캐시 정리 오류: {e}")
            return False

    def _execute_backup_data(self, action: RecoveryAction) -> bool:
        """데이터 백업 실행"""
        try:
            import shutil

            backup_path = action.parameters.get('backup_path', './backups/')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # 백업 디렉토리 생성
            os.makedirs(backup_path, exist_ok=True)

            # SQLite 데이터베이스 백업
            if os.path.exists(self.db_path):
                backup_file = f"{backup_path}/makenaide_local_backup_{timestamp}.db"
                shutil.copy2(self.db_path, backup_file)
                self.logger.info(f"데이터베이스 백업 완료: {backup_file}")
                return True

            return False

        except Exception as e:
            self.logger.error(f"데이터 백업 오류: {e}")
            return False

    def _execute_repair_database(self, action: RecoveryAction) -> bool:
        """데이터베이스 복구 실행"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 무결성 검사
                if action.parameters.get('check_integrity', True):
                    cursor.execute("PRAGMA integrity_check")
                    result = cursor.fetchone()
                    if result[0] != 'ok':
                        self.logger.error(f"데이터베이스 무결성 오류: {result[0]}")
                        return False

                # VACUUM 실행
                if action.parameters.get('vacuum', True):
                    cursor.execute("VACUUM")
                    self.logger.info("데이터베이스 VACUUM 완료")

                return True

        except Exception as e:
            self.logger.error(f"데이터베이스 복구 오류: {e}")
            return False

    def _execute_validate_config(self, action: RecoveryAction) -> bool:
        """설정 검증 실행"""
        try:
            # API 키 검증
            if action.parameters.get('check_api_keys', True):
                # 실제로는 환경변수나 설정 파일에서 API 키 확인
                api_keys = ['UPBIT_ACCESS_KEY', 'UPBIT_SECRET_KEY']
                for key in api_keys:
                    if not os.getenv(key):
                        self.logger.error(f"API 키 {key}가 설정되지 않음")
                        return False

            # 잔고 조회 테스트
            if action.parameters.get('validate_balance', True):
                try:
                    import pyupbit
                    # 실제 잔고 조회는 API 키가 있을 때만 가능
                    # 여기서는 마켓 정보로 대체
                    markets = pyupbit.get_tickers()
                    if not markets:
                        return False
                except Exception:
                    return False

            return True

        except Exception as e:
            self.logger.error(f"설정 검증 오류: {e}")
            return False

    def _execute_resource_cleanup(self, action: RecoveryAction) -> bool:
        """리소스 정리 실행"""
        try:
            # 임시 파일 정리
            if action.parameters.get('clear_temp', True):
                import tempfile
                import shutil
                temp_dir = tempfile.gettempdir()

                for item in os.listdir(temp_dir):
                    if item.startswith('tmp') or item.startswith('temp'):
                        try:
                            item_path = os.path.join(temp_dir, item)
                            if os.path.isfile(item_path):
                                os.remove(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                        except Exception:
                            pass

            # 로그 정리
            if action.parameters.get('cleanup_logs', True):
                self._execute_cleanup_logs(action)

            return True

        except Exception as e:
            self.logger.error(f"리소스 정리 오류: {e}")
            return False

    def _execute_cleanup_logs(self, action: RecoveryAction) -> bool:
        """로그 정리 실행"""
        try:
            log_dir = './logs'
            retention_days = action.parameters.get('retention_days', 7)
            cutoff_time = datetime.now() - timedelta(days=retention_days)

            if os.path.exists(log_dir):
                for filename in os.listdir(log_dir):
                    if filename.endswith('.log'):
                        file_path = os.path.join(log_dir, filename)
                        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))

                        if file_time < cutoff_time:
                            os.remove(file_path)
                            self.logger.info(f"오래된 로그 파일 삭제: {filename}")

            return True

        except Exception as e:
            self.logger.error(f"로그 정리 오류: {e}")
            return False

    def _execute_network_reset(self, action: RecoveryAction) -> bool:
        """네트워크 재설정 실행"""
        try:
            import requests

            test_endpoints = action.parameters.get('test_endpoints', ['https://api.upbit.com/v1/market/all'])

            for endpoint in test_endpoints:
                try:
                    response = requests.get(endpoint, timeout=10)
                    if response.status_code == 200:
                        self.logger.info(f"네트워크 연결 확인: {endpoint}")
                        return True
                except Exception as e:
                    self.logger.warning(f"네트워크 테스트 실패 ({endpoint}): {e}")

            return False

        except Exception as e:
            self.logger.error(f"네트워크 재설정 오류: {e}")
            return False

    def _execute_shell_command(self, command: str, parameters: Dict) -> bool:
        """셸 명령어 실행"""
        try:
            # 파라미터로 명령어 포맷팅
            formatted_command = command.format(**parameters, timestamp=datetime.now().strftime('%Y%m%d_%H%M%S'))

            result = subprocess.run(
                formatted_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300  # 5분 타임아웃
            )

            if result.returncode == 0:
                self.logger.info(f"명령어 실행 성공: {formatted_command}")
                return True
            else:
                self.logger.error(f"명령어 실행 실패: {formatted_command}, 오류: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error(f"명령어 실행 타임아웃: {command}")
            return False
        except Exception as e:
            self.logger.error(f"명령어 실행 오류: {e}")
            return False

    def _check_prerequisites(self, action: RecoveryAction) -> bool:
        """전제 조건 확인"""
        for prerequisite in action.prerequisites:
            if "디스크 공간" in prerequisite:
                if not self._check_disk_space():
                    return False
            elif "인터넷 연결" in prerequisite:
                if not self._check_internet_connection():
                    return False
            # 추가 전제 조건 확인 로직...

        return True

    def _validate_success_criteria(self, action: RecoveryAction) -> bool:
        """성공 기준 확인"""
        for criteria in action.success_criteria:
            if "API 응답 200" in criteria:
                if not self._check_api_response():
                    return False
            elif "데이터베이스" in criteria and "무결성" in criteria:
                if not self._check_database_integrity():
                    return False
            # 추가 성공 기준 확인 로직...

        return True

    def _check_disk_space(self, min_free_gb: float = 1.0) -> bool:
        """디스크 공간 확인"""
        try:
            import shutil
            free_bytes = shutil.disk_usage('.').free
            free_gb = free_bytes / (1024**3)
            return free_gb >= min_free_gb
        except Exception:
            return True  # 확인 불가 시 통과

    def _check_internet_connection(self) -> bool:
        """인터넷 연결 확인"""
        try:
            import requests
            response = requests.get('https://api.upbit.com/v1/market/all', timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    def _check_api_response(self) -> bool:
        """API 응답 확인"""
        try:
            import pyupbit
            markets = pyupbit.get_tickers()
            return markets is not None and len(markets) > 0
        except Exception:
            return False

    def _check_database_integrity(self) -> bool:
        """데이터베이스 무결성 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                return result[0] == 'ok'
        except Exception:
            return False

    def _get_action_success_rate(self, action_type: str, failure_type: str) -> float:
        """액션 성공률 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT last_success_rate FROM recovery_action_stats
                    WHERE action_type = ? AND failure_type = ?
                """, (action_type, failure_type))

                result = cursor.fetchone()
                if result:
                    return result[0]

                # 기본 성공률 반환
                return 0.7

        except Exception:
            return 0.7  # 기본 성공률

    def _update_action_stats(self, action_type: str, failure_type: str, success: bool):
        """액션 통계 업데이트"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 기존 통계 조회
                cursor.execute("""
                    SELECT execution_count, success_count FROM recovery_action_stats
                    WHERE action_type = ? AND failure_type = ?
                """, (action_type, failure_type))

                result = cursor.fetchone()

                if result:
                    exec_count, success_count = result
                    new_exec_count = exec_count + 1
                    new_success_count = success_count + (1 if success else 0)
                    new_success_rate = new_success_count / new_exec_count

                    cursor.execute("""
                        UPDATE recovery_action_stats
                        SET execution_count = ?, success_count = ?,
                            last_success_rate = ?, last_updated = ?
                        WHERE action_type = ? AND failure_type = ?
                    """, (new_exec_count, new_success_count, new_success_rate,
                          datetime.now().isoformat(), action_type, failure_type))
                else:
                    # 새 통계 생성
                    new_success_rate = 1.0 if success else 0.0
                    cursor.execute("""
                        INSERT INTO recovery_action_stats
                        (action_type, failure_type, execution_count, success_count,
                         last_success_rate, last_updated)
                        VALUES (?, ?, 1, ?, ?, ?)
                    """, (action_type, failure_type, 1 if success else 0,
                          new_success_rate, datetime.now().isoformat()))

                conn.commit()

        except Exception as e:
            self.logger.error(f"액션 통계 업데이트 오류: {e}")

    def _save_recovery_plan(self, plan: RecoveryPlan):
        """복구 계획 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO recovery_plans (
                        plan_id, failure_type, failure_sub_type, severity,
                        actions, execution_order, estimated_total_duration,
                        success_probability, approval_required, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    plan.plan_id,
                    plan.failure_type,
                    plan.failure_sub_type,
                    plan.severity.value,
                    json.dumps([{
                        'action_id': action.action_id,
                        'action_type': action.action_type.value,
                        'description': action.description,
                        'execution_mode': action.execution_mode.value,
                        'risk_level': action.risk_level.value
                    } for action in plan.actions]),
                    json.dumps(plan.execution_order),
                    plan.estimated_total_duration,
                    plan.success_probability,
                    plan.approval_required,
                    plan.created_at.isoformat()
                ))
                conn.commit()

        except Exception as e:
            self.logger.error(f"복구 계획 저장 오류: {e}")

    def _save_recovery_execution(self, execution: RecoveryExecution):
        """복구 실행 기록 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 기존 기록 확인
                cursor.execute("SELECT id FROM recovery_executions WHERE execution_id = ?",
                             (execution.execution_id,))
                existing = cursor.fetchone()

                if existing:
                    # 업데이트
                    cursor.execute("""
                        UPDATE recovery_executions
                        SET status = ?, started_at = ?, completed_at = ?,
                            executed_actions = ?, success_rate = ?, error_message = ?,
                            metadata = ?
                        WHERE execution_id = ?
                    """, (
                        execution.status.value,
                        execution.started_at.isoformat() if execution.started_at else None,
                        execution.completed_at.isoformat() if execution.completed_at else None,
                        json.dumps(execution.executed_actions),
                        execution.success_rate,
                        execution.error_message,
                        json.dumps(execution.metadata),
                        execution.execution_id
                    ))
                else:
                    # 새로 삽입
                    cursor.execute("""
                        INSERT INTO recovery_executions (
                            execution_id, plan_id, failure_record_id, status,
                            started_at, completed_at, executed_actions, success_rate,
                            error_message, metadata, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        execution.execution_id,
                        execution.plan_id,
                        execution.failure_record_id,
                        execution.status.value,
                        execution.started_at.isoformat() if execution.started_at else None,
                        execution.completed_at.isoformat() if execution.completed_at else None,
                        json.dumps(execution.executed_actions),
                        execution.success_rate,
                        execution.error_message,
                        json.dumps(execution.metadata),
                        datetime.now().isoformat()
                    ))

                conn.commit()

        except Exception as e:
            self.logger.error(f"복구 실행 기록 저장 오류: {e}")

    def get_recovery_suggestions(self, failure_record: FailureRecord) -> Dict:
        """장애에 대한 복구 제안 생성"""
        try:
            # 복구 계획 생성
            plan = self.generate_recovery_plan(failure_record)

            if not plan:
                return {
                    'status': 'no_recovery_plan',
                    'message': '해당 장애 유형에 대한 복구 계획을 찾을 수 없습니다.',
                    'suggestions': [
                        '수동으로 시스템 상태를 확인하세요.',
                        '관련 로그를 검토하세요.',
                        '필요시 전문가에게 문의하세요.'
                    ]
                }

            suggestions = {
                'plan_id': plan.plan_id,
                'failure_analysis': {
                    'type': plan.failure_type,
                    'sub_type': plan.failure_sub_type,
                    'severity': plan.severity.value
                },
                'recovery_actions': [
                    {
                        'action_id': action.action_id,
                        'description': action.description,
                        'execution_mode': action.execution_mode.value,
                        'risk_level': action.risk_level.value,
                        'estimated_duration': action.estimated_duration
                    }
                    for action in plan.actions
                ],
                'execution_summary': {
                    'total_actions': len(plan.actions),
                    'estimated_duration': plan.estimated_total_duration,
                    'success_probability': plan.success_probability,
                    'approval_required': plan.approval_required
                },
                'recommendations': self._generate_recovery_recommendations(plan),
                'next_steps': self._generate_next_steps(plan)
            }

            return suggestions

        except Exception as e:
            self.logger.error(f"복구 제안 생성 오류: {e}")
            return {
                'status': 'error',
                'message': f'복구 제안 생성 중 오류가 발생했습니다: {str(e)}'
            }

    def _generate_recovery_recommendations(self, plan: RecoveryPlan) -> List[str]:
        """복구 권장사항 생성"""
        recommendations = []

        if plan.approval_required:
            recommendations.append("🔍 고위험 복구 액션이 포함되어 있어 승인이 필요합니다.")

        if plan.success_probability < 0.5:
            recommendations.append("⚠️ 복구 성공 확률이 낮습니다. 수동 개입을 고려하세요.")

        if plan.estimated_total_duration > 300:  # 5분 이상
            recommendations.append("⏰ 복구 시간이 오래 걸릴 수 있습니다. 사용자에게 미리 알리세요.")

        # 액션별 권장사항
        for action in plan.actions:
            if action.risk_level == RiskLevel.HIGH:
                recommendations.append(f"🚨 '{action.description}' 액션은 고위험입니다. 신중히 검토하세요.")

        if not recommendations:
            recommendations.append("✅ 안전한 복구 계획입니다. 실행을 진행할 수 있습니다.")

        return recommendations

    def _generate_next_steps(self, plan: RecoveryPlan) -> List[str]:
        """다음 단계 안내 생성"""
        next_steps = []

        if plan.approval_required:
            next_steps.extend([
                "1. 복구 계획을 검토하고 승인하세요.",
                "2. 필요시 백업을 먼저 수행하세요.",
                "3. 복구 실행을 시작하세요."
            ])
        else:
            next_steps.extend([
                "1. 복구 계획이 자동 승인되었습니다.",
                "2. 즉시 복구를 실행할 수 있습니다.",
                "3. 실행 중 모니터링을 지속하세요."
            ])

        next_steps.append("4. 복구 완료 후 결과를 확인하세요.")

        return next_steps

def main():
    """메인 함수 - 자동 복구 시스템 테스트"""
    recovery_system = AutoRecoverySystem()

    print("🔧 Makenaide Phase 4 자동 복구 시스템")
    print("=" * 50)

    # 테스트 장애 기록 생성
    test_failure = FailureRecord(
        id=1,
        failure_type="API_ERROR",
        sub_type="CONNECTION_TIMEOUT",
        severity="HIGH",
        error_message="업비트 API 연결 타임아웃",
        execution_id="test_exec_001",
        phase="Phase 1",
        timestamp=datetime.now().isoformat(),
        metadata=json.dumps({"endpoint": "https://api.upbit.com"})
    )

    print(f"\n📋 테스트 장애: {test_failure.failure_type} - {test_failure.error_message}")

    # 복구 제안 생성
    suggestions = recovery_system.get_recovery_suggestions(test_failure)

    if 'plan_id' in suggestions:
        print(f"\n🔧 복구 계획 ID: {suggestions['plan_id']}")
        print(f"📊 복구 액션 수: {suggestions['execution_summary']['total_actions']}")
        print(f"⏱️ 예상 소요시간: {suggestions['execution_summary']['estimated_duration']}초")
        print(f"📈 성공 확률: {suggestions['execution_summary']['success_probability']:.1%}")

        print(f"\n🔧 복구 액션들:")
        for action in suggestions['recovery_actions']:
            print(f"   - {action['description']} ({action['execution_mode']}, 위험도: {action['risk_level']})")

        print(f"\n💡 권장사항:")
        for rec in suggestions['recommendations']:
            print(f"   {rec}")

        print(f"\n📝 다음 단계:")
        for step in suggestions['next_steps']:
            print(f"   {step}")

        # 시뮬레이션 모드로 복구 실행 테스트
        recovery_system.config['simulation_mode'] = True

        # 복구 계획 조회
        plan_id = suggestions['plan_id']
        # 실제로는 데이터베이스에서 계획을 조회해야 함

    else:
        print(f"\n❌ 복구 제안 실패: {suggestions.get('message', '알 수 없는 오류')}")

if __name__ == "__main__":
    main()