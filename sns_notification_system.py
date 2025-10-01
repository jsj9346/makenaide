"""
Makenaide SNS 통합 알림 시스템

🎯 설계 원칙:
- 파이프라인 단계별 맞춤 알림
- 중요도 기반 알림 필터링
- 비용 효율적 메시지 전송
- 실시간 거래 상황 모니터링

🔔 알림 카테고리:
1. 🚨 CRITICAL: 시스템 오류, 거래 실패
2. ⚠️ WARNING: BEAR 시장, 조건 미충족
3. ✅ SUCCESS: 거래 성공, 목표 달성
4. ℹ️ INFO: 파이프라인 진행 상황
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from enum import Enum
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

class NotificationLevel(Enum):
    """알림 중요도 레벨"""
    CRITICAL = "CRITICAL"    # 즉시 알림 필요
    WARNING = "WARNING"      # 주의 필요
    SUCCESS = "SUCCESS"      # 성공 알림
    INFO = "INFO"           # 정보성 알림

class NotificationCategory(Enum):
    """알림 카테고리"""
    SYSTEM = "SYSTEM"           # 시스템 상태
    PIPELINE = "PIPELINE"       # 파이프라인 진행
    TRADING = "TRADING"         # 거래 관련
    PORTFOLIO = "PORTFOLIO"     # 포트폴리오 관리
    MARKET = "MARKET"          # 시장 상황

class FailureType(Enum):
    """파이프라인 실패 유형"""
    API_KEY_MISSING = "API_KEY_MISSING"           # API 키 미설정
    INIT_FAILURE = "INIT_FAILURE"                 # 시스템 초기화 실패
    PHASE0_FAILURE = "PHASE0_FAILURE"             # Phase 0: 종목 스캔 실패
    PHASE1_FAILURE = "PHASE1_FAILURE"             # Phase 1: 데이터 수집 실패
    CRITICAL_ERROR = "CRITICAL_ERROR"             # 치명적 예외 발생

class FailureSubType(Enum):
    """상세 실패 유형 분류 (Phase 2)"""
    # API 키 관련 상세 분류
    API_ACCESS_KEY_MISSING = "API_ACCESS_KEY_MISSING"
    API_SECRET_KEY_MISSING = "API_SECRET_KEY_MISSING"
    API_BOTH_KEYS_MISSING = "API_BOTH_KEYS_MISSING"  # 추가: 양쪽 키 모두 누락
    API_INVALID_CREDENTIALS = "API_INVALID_CREDENTIALS"
    API_PERMISSION_DENIED = "API_PERMISSION_DENIED"
    API_AUTHENTICATION_FAILED = "API_AUTHENTICATION_FAILED"  # 추가: 인증 실패

    # 시스템 초기화 상세 분류
    DATABASE_CONNECTION_FAILED = "DATABASE_CONNECTION_FAILED"
    SQLITE_INIT_FAILED = "SQLITE_INIT_FAILED"
    MEMORY_INSUFFICIENT = "MEMORY_INSUFFICIENT"
    DISK_SPACE_INSUFFICIENT = "DISK_SPACE_INSUFFICIENT"
    COMPONENT_INIT_FAILED = "COMPONENT_INIT_FAILED"
    SYSTEM_INITIALIZATION_FAILED = "SYSTEM_INITIALIZATION_FAILED"  # 추가: 시스템 초기화 실패
    NETWORK_CONNECTION_FAILED = "NETWORK_CONNECTION_FAILED"  # 추가: 네트워크 연결 실패

    # Phase 0 상세 분류
    UPBIT_API_UNAVAILABLE = "UPBIT_API_UNAVAILABLE"
    NETWORK_TIMEOUT = "NETWORK_TIMEOUT"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    TICKER_PARSING_FAILED = "TICKER_PARSING_FAILED"
    TICKER_SCAN_FAILED = "TICKER_SCAN_FAILED"  # 추가: 종목 스캔 실패

    # Phase 1 상세 분류
    OHLCV_FETCH_FAILED = "OHLCV_FETCH_FAILED"
    DATA_VALIDATION_FAILED = "DATA_VALIDATION_FAILED"
    SQLITE_WRITE_FAILED = "SQLITE_WRITE_FAILED"
    TECHNICAL_INDICATOR_FAILED = "TECHNICAL_INDICATOR_FAILED"
    DATA_COLLECTION_FAILED = "DATA_COLLECTION_FAILED"  # 추가: 데이터 수집 실패

    # 치명적 오류 상세 분류
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION"
    MEMORY_ERROR = "MEMORY_ERROR"
    SYSTEM_EXIT = "SYSTEM_EXIT"
    KEYBOARD_INTERRUPT = "KEYBOARD_INTERRUPT"
    UNEXPECTED_EXCEPTION = "UNEXPECTED_EXCEPTION"  # 추가: 예상치 못한 예외
    SYSTEM_PERMISSION_DENIED = "SYSTEM_PERMISSION_DENIED"  # 추가: 시스템 권한 거부

class FailureSeverity(Enum):
    """실패 심각도"""
    CRITICAL = "CRITICAL"      # 즉시 조치 필요, 파이프라인 완전 중단
    HIGH = "HIGH"              # 24시간 내 조치 필요
    MEDIUM = "MEDIUM"          # 일주일 내 조치 필요
    LOW = "LOW"                # 모니터링 필요

class SpamPreventionLevel(Enum):
    """Phase 3: 스팸 방지 레벨"""
    DISABLED = "DISABLED"      # 스팸 방지 비활성화
    LOW = "LOW"                # 기본 중복 제거만
    MEDIUM = "MEDIUM"          # 시간창 기반 제한
    HIGH = "HIGH"              # 엄격한 제한 + 패턴 분석
    AGGRESSIVE = "AGGRESSIVE"  # 최대 제한 + 지능형 필터링

class SecurityMode(Enum):
    """Phase 3: 보안 모드"""
    DEVELOPMENT = "DEVELOPMENT"    # 개발 환경 (모든 메시지 허용)
    STAGING = "STAGING"           # 스테이징 환경 (제한적 필터링)
    PRODUCTION = "PRODUCTION"     # 운영 환경 (엄격한 보안)

@dataclass
class NotificationMessage:
    """알림 메시지 구조"""
    level: NotificationLevel
    category: NotificationCategory
    title: str
    message: str
    timestamp: str
    execution_id: Optional[str] = None
    ticker: Optional[str] = None
    amount: Optional[float] = None
    metadata: Optional[Dict] = None

@dataclass
class SpamPreventionConfig:
    """Phase 3: 스팸 방지 설정"""
    level: SpamPreventionLevel
    time_window_minutes: int = 5
    max_messages_per_window: int = 3
    duplicate_detection: bool = True
    pattern_analysis: bool = False
    cooldown_multiplier: float = 2.0

@dataclass
class SecurityConfig:
    """Phase 3: 보안 설정"""
    mode: SecurityMode
    sensitive_data_masking: bool = True
    message_encryption: bool = False
    audit_logging: bool = True
    ip_filtering: bool = False
    allowed_sources: List[str] = None

class MakenaideSNSNotifier:
    """Makenaide SNS 통합 알림 시스템"""

    def __init__(self):
        self.sns_client = boto3.client('sns', region_name='ap-northeast-2')

        # SNS Topic ARNs
        self.topics = {
            'trading': os.getenv('SNS_MAKENAIDE_TRADING_ALERTS_ARN',
                               'arn:aws:sns:ap-northeast-2:901361833359:makenaide-trading-alerts'),
            'system': os.getenv('SNS_MAKENAIDE_SYSTEM_ALERTS_ARN',
                              'arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts')
        }

        # 알림 설정
        self.notification_config = {
            'enabled': os.getenv('SNS_NOTIFICATIONS_ENABLED', 'true').lower() == 'true',
            'critical_only': os.getenv('SNS_CRITICAL_ONLY', 'false').lower() == 'true',
            'max_daily_messages': int(os.getenv('SNS_MAX_DAILY_MESSAGES', '50')),
            'quiet_hours': {
                'start': int(os.getenv('SNS_QUIET_START', '1')),    # 01:00 KST
                'end': int(os.getenv('SNS_QUIET_END', '7'))         # 07:00 KST
            }
        }

        # 메시지 카운터 (일일 제한)
        self.daily_message_count = 0
        self.last_reset_date = datetime.now().date()

        # Phase 3: 스팸 방지 설정
        self.spam_prevention = SpamPreventionConfig(
            level=SpamPreventionLevel(os.getenv('SNS_SPAM_PREVENTION_LEVEL', 'MEDIUM')),
            time_window_minutes=int(os.getenv('SNS_TIME_WINDOW_MINUTES', '5')),
            max_messages_per_window=int(os.getenv('SNS_MAX_MESSAGES_PER_WINDOW', '3')),
            duplicate_detection=os.getenv('SNS_DUPLICATE_DETECTION', 'true').lower() == 'true',
            pattern_analysis=os.getenv('SNS_PATTERN_ANALYSIS', 'false').lower() == 'true',
            cooldown_multiplier=float(os.getenv('SNS_COOLDOWN_MULTIPLIER', '2.0'))
        )

        # Phase 3: 보안 설정
        self.security = SecurityConfig(
            mode=SecurityMode(os.getenv('SNS_SECURITY_MODE', 'PRODUCTION')),
            sensitive_data_masking=os.getenv('SNS_SENSITIVE_DATA_MASKING', 'true').lower() == 'true',
            message_encryption=os.getenv('SNS_MESSAGE_ENCRYPTION', 'false').lower() == 'true',
            audit_logging=os.getenv('SNS_AUDIT_LOGGING', 'true').lower() == 'true',
            ip_filtering=os.getenv('SNS_IP_FILTERING', 'false').lower() == 'true',
            allowed_sources=os.getenv('SNS_ALLOWED_SOURCES', '').split(',') if os.getenv('SNS_ALLOWED_SOURCES') else []
        )

        # Phase 3: 스팸 방지 및 보안 상태 추적
        self.message_history = []  # 최근 메시지 기록
        self.blocked_messages = []  # 차단된 메시지 기록
        self.security_events = []  # 보안 이벤트 기록
        self.duplicate_hashes = set()  # 중복 메시지 해시
        self.cooldown_until = {}  # 카테고리별 쿨다운 시간

    def _should_send_notification(self, level: NotificationLevel) -> bool:
        """알림 발송 여부 판단"""
        # 알림 비활성화 확인
        if not self.notification_config['enabled']:
            return False

        # CRITICAL만 알림 모드 확인
        if self.notification_config['critical_only'] and level != NotificationLevel.CRITICAL:
            return False

        # 일일 메시지 제한 확인
        current_date = datetime.now().date()
        if current_date != self.last_reset_date:
            self.daily_message_count = 0
            self.last_reset_date = current_date

        if self.daily_message_count >= self.notification_config['max_daily_messages']:
            if level == NotificationLevel.CRITICAL:
                # CRITICAL은 제한 무시
                pass
            else:
                logger.warning(f"일일 알림 한도 도달: {self.daily_message_count}")
                return False

        # 조용한 시간 확인 (CRITICAL 제외)
        if level != NotificationLevel.CRITICAL:
            current_hour = datetime.now().hour
            quiet_start = self.notification_config['quiet_hours']['start']
            quiet_end = self.notification_config['quiet_hours']['end']

            if quiet_start <= current_hour < quiet_end:
                logger.info(f"조용한 시간대: {current_hour}시")
                return False

        return True

    def _format_message(self, notification: NotificationMessage) -> tuple:
        """메시지 포맷팅"""
        # 이모지 매핑
        level_emojis = {
            NotificationLevel.CRITICAL: "🚨",
            NotificationLevel.WARNING: "⚠️",
            NotificationLevel.SUCCESS: "✅",
            NotificationLevel.INFO: "ℹ️"
        }

        category_emojis = {
            NotificationCategory.SYSTEM: "🖥️",
            NotificationCategory.PIPELINE: "⚙️",
            NotificationCategory.TRADING: "💸",
            NotificationCategory.PORTFOLIO: "📊",
            NotificationCategory.MARKET: "🌡️"
        }

        emoji = level_emojis.get(notification.level, "📢")
        cat_emoji = category_emojis.get(notification.category, "📋")

        # 제목 포맷팅
        subject = f"{emoji} Makenaide {notification.category.value} - {notification.title}"

        # 메시지 본문 포맷팅
        message_lines = [
            f"{cat_emoji} {notification.title}",
            "",
            notification.message,
            "",
            f"📅 시간: {notification.timestamp}",
        ]

        # 선택적 정보 추가
        if notification.execution_id:
            message_lines.append(f"🔍 실행 ID: {notification.execution_id}")

        if notification.ticker:
            message_lines.append(f"🏷️ 종목: {notification.ticker}")

        if notification.amount:
            message_lines.append(f"💰 금액: {notification.amount:,.0f}원")

        if notification.metadata:
            message_lines.append("")
            message_lines.append("📋 추가 정보:")
            for key, value in notification.metadata.items():
                message_lines.append(f"  • {key}: {value}")

        message_lines.extend([
            "",
            "---",
            "🤖 Makenaide 자동매매 시스템"
        ])

        return subject, "\n".join(message_lines)

    def _send_to_sns(self, topic_arn: str, subject: str, message: str) -> bool:
        """SNS로 메시지 전송"""
        try:
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=message
            )

            message_id = response.get('MessageId')
            logger.info(f"SNS 알림 전송 성공: {message_id}")
            self.daily_message_count += 1
            return True

        except Exception as e:
            logger.error(f"SNS 알림 전송 실패: {e}")
            return False

    def send_notification(self, notification: NotificationMessage) -> bool:
        """통합 알림 전송"""
        if not self._should_send_notification(notification.level):
            logger.debug(f"알림 스킵: {notification.title}")
            return False

        try:
            subject, message = self._format_message(notification)

            # 카테고리별 토픽 선택
            if notification.category in [NotificationCategory.TRADING, NotificationCategory.PORTFOLIO]:
                topic_arn = self.topics['trading']
            else:
                topic_arn = self.topics['system']

            return self._send_to_sns(topic_arn, subject, message)

        except Exception as e:
            logger.error(f"알림 전송 중 오류: {e}")
            return False

    # =================================================================
    # 파이프라인 단계별 알림 메서드
    # =================================================================

    def notify_pipeline_start(self, execution_id: str):
        """파이프라인 시작 알림"""
        notification = NotificationMessage(
            level=NotificationLevel.INFO,
            category=NotificationCategory.PIPELINE,
            title="파이프라인 시작",
            message="Makenaide 자동매매 파이프라인이 시작되었습니다.",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id
        )
        return self.send_notification(notification)

    def notify_phase_complete(self, phase: str, results: Dict, execution_id: str):
        """Phase 완료 알림"""
        if phase == "Phase 0":
            self._notify_scanner_complete(results, execution_id)
        elif phase == "Phase 1":
            self._notify_data_collection_complete(results, execution_id)
        elif phase == "Phase 2":
            self._notify_technical_filter_complete(results, execution_id)
        elif phase == "Phase 3":
            self._notify_gpt_analysis_complete(results, execution_id)

    def _notify_scanner_complete(self, results: Dict, execution_id: str):
        """종목 스캔 완료 알림"""
        ticker_count = results.get('ticker_count', 0)

        notification = NotificationMessage(
            level=NotificationLevel.INFO,
            category=NotificationCategory.PIPELINE,
            title="종목 스캔 완료",
            message=f"업비트 전체 종목 스캔이 완료되었습니다.\n총 {ticker_count}개 종목을 확인했습니다.",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id,
            metadata={'ticker_count': ticker_count}
        )
        return self.send_notification(notification)

    def _notify_data_collection_complete(self, results: Dict, execution_id: str):
        """데이터 수집 완료 알림"""
        gap_days = results.get('gap_days', 0)
        collected_count = results.get('collected_count', 0)

        if gap_days > 0:
            notification = NotificationMessage(
                level=NotificationLevel.INFO,
                category=NotificationCategory.PIPELINE,
                title="데이터 수집 완료",
                message=f"증분 데이터 수집이 완료되었습니다.\n{gap_days}일 갭 데이터를 {collected_count}개 종목에 대해 수집했습니다.",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'gap_days': gap_days, 'collected_count': collected_count}
            )
            return self.send_notification(notification)

    def _notify_technical_filter_complete(self, results: Dict, execution_id: str):
        """기술적 필터링 완료 알림"""
        candidates = results.get('stage2_candidates', [])
        candidate_count = len(candidates)

        if candidate_count > 0:
            top_candidates = candidates[:3]  # 상위 3개만
            candidate_list = "\n".join([f"• {c.get('ticker', 'Unknown')}: {c.get('quality_score', 0):.1f}점"
                                      for c in top_candidates])

            notification = NotificationMessage(
                level=NotificationLevel.SUCCESS,
                category=NotificationCategory.PIPELINE,
                title=f"Stage 2 후보 발견",
                message=f"Weinstein Stage 2 진입 후보 {candidate_count}개를 발견했습니다.\n\n상위 후보:\n{candidate_list}",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'candidate_count': candidate_count}
            )
            return self.send_notification(notification)

    def _notify_gpt_analysis_complete(self, results: Dict, execution_id: str):
        """GPT 분석 완료 알림"""
        approved_count = results.get('gpt_approved', 0)
        total_cost = results.get('total_cost', 0.0)

        if approved_count > 0:
            notification = NotificationMessage(
                level=NotificationLevel.SUCCESS,
                category=NotificationCategory.PIPELINE,
                title="GPT 분석 완료",
                message=f"GPT 패턴 분석이 완료되었습니다.\n{approved_count}개 종목이 매수 추천을 받았습니다.",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'approved_count': approved_count, 'cost_usd': total_cost}
            )
            return self.send_notification(notification)

    def notify_market_sentiment(self, sentiment: str, trading_allowed: bool, execution_id: str):
        """시장 감정 분석 결과 알림"""
        if sentiment == "BEAR":
            notification = NotificationMessage(
                level=NotificationLevel.WARNING,
                category=NotificationCategory.MARKET,
                title="BEAR 시장 감지",
                message="시장 감정 분석 결과 약세장이 감지되었습니다.\n모든 매수 거래가 중단됩니다.",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'sentiment': sentiment, 'trading_allowed': trading_allowed}
            )
            return self.send_notification(notification)

    def notify_trade_execution(self, trade_result: Dict, execution_id: str):
        """거래 실행 결과 알림"""
        ticker = trade_result.get('ticker')
        action = trade_result.get('action')  # 'BUY' or 'SELL'
        amount = trade_result.get('amount', 0)
        price = trade_result.get('price', 0)
        success = trade_result.get('success', False)
        reason = trade_result.get('reason', '')

        if success:
            if action == 'BUY':
                notification = NotificationMessage(
                    level=NotificationLevel.SUCCESS,
                    category=NotificationCategory.TRADING,
                    title=f"매수 성공 - {ticker}",
                    message=f"{ticker} 매수가 성공적으로 완료되었습니다.\n매수가: {price:,.0f}원\n투자금액: {amount:,.0f}원",
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    execution_id=execution_id,
                    ticker=ticker,
                    amount=amount,
                    metadata={'action': action, 'price': price}
                )
            else:  # SELL
                notification = NotificationMessage(
                    level=NotificationLevel.SUCCESS,
                    category=NotificationCategory.TRADING,
                    title=f"매도 성공 - {ticker}",
                    message=f"{ticker} 매도가 성공적으로 완료되었습니다.\n매도가: {price:,.0f}원\n매도 사유: {reason}",
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    execution_id=execution_id,
                    ticker=ticker,
                    amount=amount,
                    metadata={'action': action, 'price': price, 'reason': reason}
                )
        else:
            notification = NotificationMessage(
                level=NotificationLevel.WARNING,
                category=NotificationCategory.TRADING,
                title=f"거래 실패 - {ticker}",
                message=f"{ticker} {action} 거래가 실패했습니다.\n실패 사유: {reason}",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                ticker=ticker,
                metadata={'action': action, 'reason': reason}
            )

        return self.send_notification(notification)

    def notify_portfolio_update(self, portfolio_summary: Dict, execution_id: str):
        """포트폴리오 업데이트 알림"""
        total_value = portfolio_summary.get('total_value', 0)
        total_pnl = portfolio_summary.get('total_pnl', 0)
        pnl_ratio = portfolio_summary.get('pnl_ratio', 0)
        position_count = portfolio_summary.get('position_count', 0)

        if position_count > 0:
            notification = NotificationMessage(
                level=NotificationLevel.INFO,
                category=NotificationCategory.PORTFOLIO,
                title="포트폴리오 현황",
                message=f"현재 포트폴리오 상태입니다.\n\n총 평가액: {total_value:,.0f}원\n손익: {total_pnl:+,.0f}원 ({pnl_ratio:+.1f}%)\n보유 종목: {position_count}개",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={
                    'total_value': total_value,
                    'total_pnl': total_pnl,
                    'pnl_ratio': pnl_ratio,
                    'position_count': position_count
                }
            )
            return self.send_notification(notification)

    def notify_direct_purchase_detected(self, tickers: List[str], execution_id: str):
        """직접 매수 종목 감지 알림"""
        ticker_count = len(tickers)
        ticker_list = ', '.join(tickers)

        notification = NotificationMessage(
            level=NotificationLevel.WARNING,
            category=NotificationCategory.PORTFOLIO,
            title=f"직접 매수 종목 {ticker_count}개 감지",
            message=f"시스템 외부에서 직접 매수한 종목이 감지되어 자동으로 포트폴리오에 등록되었습니다.\n\n감지된 종목: {ticker_list}\n\n이제 해당 종목들도 자동 포트폴리오 관리 대상에 포함됩니다.",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id,
            metadata={
                'detected_tickers': tickers,
                'ticker_count': ticker_count,
                'detection_time': datetime.now().isoformat()
            }
        )
        return self.send_notification(notification)

    def notify_pipeline_complete(self, execution_summary: Dict, execution_id: str):
        """파이프라인 완료 알림"""
        success = execution_summary.get('success', False)
        duration = execution_summary.get('duration_seconds', 0)
        trades_executed = execution_summary.get('trades_executed', 0)
        errors = execution_summary.get('errors', [])

        if success:
            notification = NotificationMessage(
                level=NotificationLevel.SUCCESS,
                category=NotificationCategory.PIPELINE,
                title="파이프라인 완료",
                message=f"Makenaide 파이프라인이 성공적으로 완료되었습니다.\n\n실행 시간: {duration:.1f}초\n실행된 거래: {trades_executed}건",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'duration': duration, 'trades_executed': trades_executed}
            )
        else:
            error_count = len(errors)
            error_summary = errors[0] if errors else "Unknown error"

            notification = NotificationMessage(
                level=NotificationLevel.CRITICAL,
                category=NotificationCategory.PIPELINE,
                title="파이프라인 실패",
                message=f"Makenaide 파이프라인 실행이 실패했습니다.\n\n오류 수: {error_count}건\n주요 오류: {error_summary}",
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={'error_count': error_count, 'errors': errors}
            )

        return self.send_notification(notification)

    def notify_system_error(self, error_message: str, execution_id: str = None):
        """시스템 오류 알림"""
        notification = NotificationMessage(
            level=NotificationLevel.CRITICAL,
            category=NotificationCategory.SYSTEM,
            title="시스템 오류 발생",
            message=f"시스템에서 치명적 오류가 발생했습니다.\n\n오류 내용: {error_message}",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id,
            metadata={'error': error_message}
        )
        return self.send_notification(notification)

    def notify_discovered_stocks(self, technical_candidates: List[Dict], gpt_candidates: List[Dict], execution_id: str):
        """발굴 종목 리스트 알림"""
        try:
            # 기술적 분석 통과 종목이 있는 경우에만 알림
            if not technical_candidates and not gpt_candidates:
                return False

            message_parts = []

            # 🎯 기술적 분석 통과 종목
            if technical_candidates:
                # 중복 종목 제거 (종목별 최고 점수만 유지)
                seen_tickers = set()
                unique_technical = []
                for candidate in technical_candidates:
                    ticker = candidate.get('ticker', 'Unknown')
                    if ticker not in seen_tickers:
                        seen_tickers.add(ticker)
                        unique_technical.append(candidate)

                message_parts.append("🎯 기술적 분석 통과 종목:")

                # 상위 10개만 표시 (너무 길어지지 않도록)
                top_technical = unique_technical[:10]

                for i, candidate in enumerate(top_technical, 1):
                    ticker = candidate.get('ticker', 'Unknown')
                    quality_score = candidate.get('quality_score', 0)
                    gates_passed = candidate.get('gates_passed', 0)
                    recommendation = candidate.get('recommendation', 'Unknown')

                    # Stage 2 진입 단계 표시
                    stage_emoji = "🟢" if gates_passed >= 3 else "🟡" if gates_passed >= 2 else "🔵"

                    message_parts.append(
                        f"  {i}. {stage_emoji} {ticker}: {quality_score:.1f}점 ({gates_passed}/4 게이트) - {recommendation}"
                    )

                if len(unique_technical) > 10:
                    message_parts.append(f"  ... 외 {len(unique_technical) - 10}개 종목")

                message_parts.append("")

            # 🤖 GPT 분석 승인 종목
            if gpt_candidates:
                # 중복 종목 제거 (종목별 최고 신뢰도만 유지)
                seen_gpt_tickers = set()
                unique_gpt = []
                for candidate in gpt_candidates:
                    ticker = candidate.get('ticker', 'Unknown')
                    if ticker not in seen_gpt_tickers:
                        seen_gpt_tickers.add(ticker)
                        unique_gpt.append(candidate)

                message_parts.append("🤖 GPT 분석 승인 종목:")

                for i, candidate in enumerate(unique_gpt, 1):
                    ticker = candidate.get('ticker', 'Unknown')
                    confidence = candidate.get('confidence', 0)
                    pattern = candidate.get('pattern', 'Unknown')
                    gpt_recommendation = candidate.get('recommendation', 'Unknown')

                    # 신뢰도에 따른 이모지
                    confidence_emoji = "🔥" if confidence >= 80 else "⭐" if confidence >= 70 else "💡"

                    message_parts.append(
                        f"  {i}. {confidence_emoji} {ticker}: {confidence:.0f}% 신뢰도 - {pattern} ({gpt_recommendation})"
                    )

                message_parts.append("")

            # 투자 가이드 추가
            if technical_candidates or gpt_candidates:
                message_parts.extend([
                    "📋 투자 가이드:",
                    "• 🟢 3-4 게이트 통과: 강력한 Stage 2 후보",
                    "• 🟡 2 게이트 통과: 주의깊게 관찰",
                    "• 🔥 GPT 80%+: 강한 패턴 신호",
                    "• ⭐ GPT 70%+: 중간 강도 신호",
                    "",
                    "⚠️ 시장 감정 분석 후 최종 거래 결정"
                ])

            # 종합 요약 (중복 제거 후 실제 고유 종목 수)
            total_technical = len(unique_technical) if technical_candidates else 0
            total_gpt = len(unique_gpt) if gpt_candidates else 0

            title = f"발굴 종목 리스트 (기술적: {total_technical}개, GPT: {total_gpt}개)"

            notification = NotificationMessage(
                level=NotificationLevel.CRITICAL,  # 발굴 종목은 중요한 거래 기회이므로 조용한 시간대에도 알림
                category=NotificationCategory.TRADING,
                title=title,
                message="\n".join(message_parts),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={
                    'technical_count': total_technical,
                    'gpt_count': total_gpt,
                    'total_discovered': total_technical + total_gpt
                }
            )

            return self.send_notification(notification)

        except Exception as e:
            logger.error(f"발굴 종목 알림 전송 실패: {e}")
            return False

    def notify_kelly_position_sizing(self, position_sizes: Dict[str, float], execution_id: str):
        """Kelly 포지션 사이징 결과 알림"""
        try:
            if not position_sizes:
                return False

            message_parts = [
                "🧮 Kelly 공식 포지션 사이징 결과:",
                ""
            ]

            # 포지션 사이즈 정렬 (큰 순서대로)
            sorted_positions = sorted(position_sizes.items(), key=lambda x: x[1], reverse=True)

            total_allocation = sum(position_sizes.values())

            for i, (ticker, position_size) in enumerate(sorted_positions, 1):
                # 포지션 크기에 따른 이모지
                size_emoji = "🔥" if position_size >= 6.0 else "⭐" if position_size >= 4.0 else "💡"

                message_parts.append(
                    f"  {i}. {size_emoji} {ticker}: {position_size:.1f}% 할당"
                )

            message_parts.extend([
                "",
                f"📊 총 할당 비율: {total_allocation:.1f}%",
                f"💰 현금 보유: {100 - total_allocation:.1f}%",
                "",
                "📋 Kelly 공식 가이드:",
                "• 🔥 6%+: 고신뢰도 패턴",
                "• ⭐ 4-6%: 중간 신뢰도",
                "• 💡 2-4%: 낮은 신뢰도",
                "",
                "⚠️ 시장 감정에 따라 최종 조정됩니다"
            ])

            notification = NotificationMessage(
                level=NotificationLevel.INFO,
                category=NotificationCategory.TRADING,
                title=f"Kelly 포지션 사이징 ({len(position_sizes)}개 종목)",
                message="\n".join(message_parts),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={
                    'position_count': len(position_sizes),
                    'total_allocation': total_allocation,
                    'cash_ratio': 100 - total_allocation
                }
            )

            return self.send_notification(notification)

        except Exception as e:
            logger.error(f"Kelly 포지션 사이징 알림 전송 실패: {e}")
            return False

    def notify_market_analysis_summary(self, market_data: Dict, execution_id: str):
        """시장 분석 종합 요약 알림"""
        try:
            fear_greed = market_data.get('fear_greed_index', 50)
            btc_change_24h = market_data.get('btc_change_24h', 0)
            btc_trend = market_data.get('btc_trend', 'SIDEWAYS')
            sentiment = market_data.get('final_sentiment', 'NEUTRAL')
            trading_allowed = market_data.get('trading_allowed', True)
            position_adjustment = market_data.get('position_adjustment', 1.0)

            # Fear & Greed 지수 해석
            if fear_greed <= 25:
                fg_status = "😱 극도의 공포"
                fg_emoji = "🔴"
            elif fear_greed <= 45:
                fg_status = "😰 공포"
                fg_emoji = "🟠"
            elif fear_greed <= 55:
                fg_status = "😐 중립"
                fg_emoji = "🟡"
            elif fear_greed <= 75:
                fg_status = "😏 탐욕"
                fg_emoji = "🟢"
            else:
                fg_status = "🤑 극도의 탐욕"
                fg_emoji = "🔥"

            # BTC 트렌드 해석
            btc_emoji = "📈" if btc_change_24h > 5 else "📉" if btc_change_24h < -5 else "➡️"

            # 최종 시장 감정
            sentiment_emoji = "🐻" if sentiment == "BEAR" else "🐂" if sentiment == "BULL" else "🐨"

            message_parts = [
                "🌡️ 종합 시장 분석 결과:",
                "",
                f"{fg_emoji} Fear & Greed Index: {fear_greed}/100 ({fg_status})",
                f"{btc_emoji} BTC 24시간: {btc_change_24h:+.1f}% ({btc_trend})",
                f"{sentiment_emoji} 최종 시장 감정: {sentiment}",
                "",
                f"🚦 거래 허용: {'✅ 예' if trading_allowed else '❌ 아니오'}",
                f"⚖️ 포지션 조정: {position_adjustment:.2f}x",
                ""
            ]

            # 거래 가이드
            if sentiment == "BEAR":
                message_parts.extend([
                    "🚫 거래 중단 권고:",
                    "• 약세장 진입으로 모든 매수 신호 무시",
                    "• 기존 포지션 점검 필요",
                    "• 손절 기준 엄격 적용"
                ])
            elif sentiment == "BULL":
                message_parts.extend([
                    "🚀 적극적 거래 환경:",
                    "• 강세장 신호로 포지션 확대 고려",
                    f"• 기본 포지션 대비 {position_adjustment:.0%} 조정",
                    "• 수익 실현 기준 상향 조정 가능"
                ])
            else:
                message_parts.extend([
                    "⚖️ 중립적 시장 환경:",
                    "• 선별적 거래 권장",
                    "• 보수적 포지션 사이징",
                    "• 시장 변화 지속 모니터링"
                ])

            notification = NotificationMessage(
                level=NotificationLevel.INFO,
                category=NotificationCategory.MARKET,
                title=f"시장 분석 요약 - {sentiment}",
                message="\n".join(message_parts),
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                execution_id=execution_id,
                metadata={
                    'fear_greed_index': fear_greed,
                    'btc_change_24h': btc_change_24h,
                    'sentiment': sentiment,
                    'trading_allowed': trading_allowed,
                    'position_adjustment': position_adjustment
                }
            )

            return self.send_notification(notification)

        except Exception as e:
            logger.error(f"시장 분석 요약 알림 전송 실패: {e}")
            return False

    def notify_ec2_shutdown(self, execution_id: str, reason: str = "파이프라인 완료"):
        """EC2 종료 알림"""
        notification = NotificationMessage(
            level=NotificationLevel.INFO,
            category=NotificationCategory.SYSTEM,
            title="EC2 자동 종료",
            message=f"EC2 인스턴스가 자동 종료됩니다.\n\n종료 사유: {reason}\n1분 후 shutdown 명령이 실행됩니다.",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id,
            metadata={'reason': reason}
        )
        return self.send_notification(notification)

    def notify_pipeline_failure(self, failure_type: str, error_message: str,
                               phase: str = None, execution_id: str = None,
                               metadata: Dict = None) -> bool:
        """파이프라인 실패 시 통합 알림 전송"""

        # 실패 유형별 제목 매핑
        failure_titles = {
            FailureType.API_KEY_MISSING.value: "🔑 API 키 설정 오류",
            FailureType.INIT_FAILURE.value: "⚙️ 시스템 초기화 실패",
            FailureType.PHASE0_FAILURE.value: "📡 종목 스캔 실패",
            FailureType.PHASE1_FAILURE.value: "📊 데이터 수집 실패",
            FailureType.CRITICAL_ERROR.value: "💥 치명적 시스템 오류"
        }

        title = failure_titles.get(failure_type, "❌ 파이프라인 실패")

        # EC2 자동 종료 예고 메시지 포함
        auto_shutdown = os.getenv('EC2_AUTO_SHUTDOWN', 'false').lower() == 'true'
        shutdown_notice = "\n\n🔌 EC2 자동 종료: 30초 후 인스턴스가 종료됩니다." if auto_shutdown else ""

        # 상세 메타데이터 구성
        failure_metadata = {
            'failure_type': failure_type,
            'phase': phase,
            'auto_shutdown': auto_shutdown,
            'severity': 'CRITICAL',
            'requires_immediate_attention': True
        }

        if metadata:
            failure_metadata.update(metadata)

        notification = NotificationMessage(
            level=NotificationLevel.CRITICAL,
            category=NotificationCategory.PIPELINE,
            title=title,
            message=f"{error_message}{shutdown_notice}",
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id or datetime.now().strftime('%Y%m%d_%H%M%S'),
            metadata=failure_metadata
        )

        return self.send_notification(notification)

    def _get_failure_template(self, failure_type: str, sub_type: str = None) -> Dict:
        """Phase 2: 실패 유형별 상세 템플릿 반환"""

        # 기본 템플릿 구조
        base_templates = {
            FailureType.API_KEY_MISSING.value: {
                'icon': '🔑',
                'title': 'API 키 설정 오류',
                'severity': FailureSeverity.CRITICAL.value,
                'immediate_action': True,
                'recovery_time': '5분',
                'categories': ['설정', '인증']
            },
            FailureType.INIT_FAILURE.value: {
                'icon': '⚙️',
                'title': '시스템 초기화 실패',
                'severity': FailureSeverity.CRITICAL.value,
                'immediate_action': True,
                'recovery_time': '10분',
                'categories': ['시스템', '인프라']
            },
            FailureType.PHASE0_FAILURE.value: {
                'icon': '📡',
                'title': '종목 스캔 실패',
                'severity': FailureSeverity.HIGH.value,
                'immediate_action': True,
                'recovery_time': '15분',
                'categories': ['네트워크', 'API']
            },
            FailureType.PHASE1_FAILURE.value: {
                'icon': '📊',
                'title': '데이터 수집 실패',
                'severity': FailureSeverity.HIGH.value,
                'immediate_action': True,
                'recovery_time': '20분',
                'categories': ['데이터', '저장소']
            },
            FailureType.CRITICAL_ERROR.value: {
                'icon': '💥',
                'title': '치명적 시스템 오류',
                'severity': FailureSeverity.CRITICAL.value,
                'immediate_action': True,
                'recovery_time': '30분',
                'categories': ['시스템', '예외']
            }
        }

        # 상세 서브타입별 템플릿
        detailed_templates = {
            # API 키 관련
            FailureSubType.API_ACCESS_KEY_MISSING.value: {
                'title': 'Access Key 누락',
                'description': 'UPBIT_ACCESS_KEY 환경변수가 설정되지 않았습니다.',
                'solution': '.env 파일에서 UPBIT_ACCESS_KEY를 확인하고 설정해주세요.',
                'documentation': 'https://docs.upbit.com/docs/user-request-guide'
            },
            FailureSubType.API_SECRET_KEY_MISSING.value: {
                'title': 'Secret Key 누락',
                'description': 'UPBIT_SECRET_KEY 환경변수가 설정되지 않았습니다.',
                'solution': '.env 파일에서 UPBIT_SECRET_KEY를 확인하고 설정해주세요.',
                'documentation': 'https://docs.upbit.com/docs/user-request-guide'
            },
            FailureSubType.API_INVALID_CREDENTIALS.value: {
                'title': 'API 인증 실패',
                'description': 'API 키가 유효하지 않거나 만료되었습니다.',
                'solution': '업비트에서 새로운 API 키를 발급받아 교체해주세요.',
                'documentation': 'https://docs.upbit.com/docs/create-authorization-request'
            },

            # 시스템 초기화 관련
            FailureSubType.DATABASE_CONNECTION_FAILED.value: {
                'title': '데이터베이스 연결 실패',
                'description': 'SQLite 데이터베이스에 연결할 수 없습니다.',
                'solution': '데이터베이스 파일 권한과 디스크 공간을 확인해주세요.',
                'documentation': 'SQLite 파일 접근 권한 확인 필요'
            },
            FailureSubType.MEMORY_INSUFFICIENT.value: {
                'title': '메모리 부족',
                'description': '시스템 메모리가 부족하여 초기화에 실패했습니다.',
                'solution': 'EC2 인스턴스를 재시작하거나 메모리 사용량을 확인해주세요.',
                'documentation': 'EC2 t3.medium 메모리 4GB 한계'
            },

            # Phase 0 관련
            FailureSubType.UPBIT_API_UNAVAILABLE.value: {
                'title': '업비트 API 서비스 중단',
                'description': '업비트 API 서버에 접속할 수 없습니다.',
                'solution': '업비트 서비스 상태를 확인하고 잠시 후 재시도해주세요.',
                'documentation': 'https://upbit.com/service_center/notice'
            },
            FailureSubType.RATE_LIMIT_EXCEEDED.value: {
                'title': 'API 호출 한도 초과',
                'description': '업비트 API 호출 한도를 초과했습니다.',
                'solution': '1분 후 재시도하거나 호출 빈도를 조정해주세요.',
                'documentation': 'https://docs.upbit.com/docs/market-info-trade-price-detail'
            },

            # Phase 1 관련
            FailureSubType.SQLITE_WRITE_FAILED.value: {
                'title': 'SQLite 쓰기 실패',
                'description': 'SQLite 데이터베이스에 데이터를 저장할 수 없습니다.',
                'solution': '디스크 공간과 파일 권한을 확인해주세요.',
                'documentation': 'SQLite VACUUM으로 데이터베이스 최적화 필요'
            },
            FailureSubType.TECHNICAL_INDICATOR_FAILED.value: {
                'title': '기술적 지표 계산 실패',
                'description': '기술적 지표 계산 중 오류가 발생했습니다.',
                'solution': 'OHLCV 데이터의 완정성을 확인하고 재계산해주세요.',
                'documentation': 'MA, RSI, ADX 계산 알고리즘 확인 필요'
            },

            # 새로 추가된 실패 유형들
            FailureSubType.API_BOTH_KEYS_MISSING.value: {
                'title': '모든 API 키 누락',
                'description': 'Access Key와 Secret Key가 모두 설정되지 않았습니다.',
                'solution': '.env 파일에 UPBIT_ACCESS_KEY와 UPBIT_SECRET_KEY를 모두 설정해주세요.',
                'documentation': 'https://docs.upbit.com/docs/user-request-guide'
            },
            FailureSubType.API_AUTHENTICATION_FAILED.value: {
                'title': 'API 인증 실패',
                'description': 'API 키 인증에 실패했습니다.',
                'solution': 'API 키 유효성을 확인하고 업비트에서 새로 발급받아 주세요.',
                'documentation': 'https://docs.upbit.com/docs/create-authorization-request'
            },
            FailureSubType.SYSTEM_INITIALIZATION_FAILED.value: {
                'title': '시스템 초기화 실패',
                'description': '시스템 구성 요소 초기화에 실패했습니다.',
                'solution': 'EC2 인스턴스를 재시작하고 로그를 확인해주세요.',
                'documentation': 'EC2 시스템 리소스 및 구성 확인 필요'
            },
            FailureSubType.NETWORK_CONNECTION_FAILED.value: {
                'title': '네트워크 연결 실패',
                'description': '외부 API 서버에 연결할 수 없습니다.',
                'solution': '네트워크 연결 상태와 보안 그룹 설정을 확인해주세요.',
                'documentation': 'EC2 보안 그룹 및 네트워크 ACL 확인'
            },
            FailureSubType.TICKER_SCAN_FAILED.value: {
                'title': '종목 스캔 실패',
                'description': '업비트 종목 리스트 조회에 실패했습니다.',
                'solution': 'API 연결 상태를 확인하고 재시도해주세요.',
                'documentation': 'https://docs.upbit.com/docs/market-info-trade-price-detail'
            },
            FailureSubType.DATA_COLLECTION_FAILED.value: {
                'title': '데이터 수집 실패',
                'description': 'OHLCV 데이터 수집 과정에서 오류가 발생했습니다.',
                'solution': 'API 호출 한도와 디스크 공간을 확인해주세요.',
                'documentation': '데이터 수집 파이프라인 점검 필요'
            },
            FailureSubType.UNEXPECTED_EXCEPTION.value: {
                'title': '예상치 못한 예외',
                'description': '처리되지 않은 예외가 발생했습니다.',
                'solution': '로그를 확인하고 시스템을 재시작해주세요.',
                'documentation': '스택 트레이스 분석 및 버그 리포트 필요'
            },
            FailureSubType.SYSTEM_PERMISSION_DENIED.value: {
                'title': '시스템 권한 거부',
                'description': '시스템 리소스에 접근할 권한이 없습니다.',
                'solution': '파일 권한과 EC2 사용자 권한을 확인해주세요.',
                'documentation': 'EC2 인스턴스 파일 시스템 권한 점검'
            }
        }

        # 기본 템플릿 가져오기
        template = base_templates.get(failure_type, {
            'icon': '❌',
            'title': '알 수 없는 오류',
            'severity': FailureSeverity.MEDIUM.value,
            'immediate_action': False,
            'recovery_time': '확인 필요',
            'categories': ['기타']
        })

        # 상세 정보 추가
        if sub_type and sub_type in detailed_templates:
            template.update(detailed_templates[sub_type])

        return template

    def notify_detailed_failure(self, failure_type: str, sub_type: str = None, error_message: str = "",
                              phase: str = None, execution_id: str = None, metadata: Dict = None) -> bool:
        """Phase 2: 상세 실패 분류 기반 알림 전송"""

        # 템플릿 가져오기
        template = self._get_failure_template(failure_type, sub_type)

        # 상세 제목 구성
        detailed_title = f"{template['icon']} {template['title']}"
        if sub_type and 'title' in template:
            detailed_title += f" - {template.get('title', '')}"

        # EC2 자동 종료 예고 메시지 포함
        auto_shutdown = os.getenv('EC2_AUTO_SHUTDOWN', 'false').lower() == 'true'
        shutdown_notice = "\n\n🔌 EC2 자동 종료: 30초 후 인스턴스가 종료됩니다." if auto_shutdown else ""

        # 상세 메시지 구성
        detailed_message = error_message if error_message else template.get('description', '')

        # 해결 방안 추가
        if 'solution' in template:
            detailed_message += f"\n\n💡 해결 방안:\n{template['solution']}"

        # 문서 링크 추가
        if 'documentation' in template:
            detailed_message += f"\n\n📚 참고: {template['documentation']}"

        detailed_message += shutdown_notice

        # 상세 메타데이터 구성
        failure_metadata = {
            'failure_type': failure_type,
            'sub_type': sub_type,
            'phase': phase,
            'severity': template['severity'],
            'immediate_action': template['immediate_action'],
            'recovery_time': template['recovery_time'],
            'categories': template['categories'],
            'auto_shutdown': auto_shutdown,
            'template_version': '2.0'
        }

        if metadata:
            failure_metadata.update(metadata)

        notification = NotificationMessage(
            level=NotificationLevel.CRITICAL if template['severity'] in ['CRITICAL', 'HIGH'] else NotificationLevel.WARNING,
            category=NotificationCategory.PIPELINE,
            title=detailed_title,
            message=detailed_message,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            execution_id=execution_id or datetime.now().strftime('%Y%m%d_%H%M%S'),
            metadata=failure_metadata
        )

        # 추가 로깅
        logger.error(f"상세 실패 알림: {failure_type} - {sub_type} | {template['severity']}")

        return self.send_notification(notification)

    def get_failure_analytics(self) -> Dict:
        """Phase 2: 실패 패턴 분석 데이터 반환"""
        return {
            'total_failure_types': len(FailureType),
            'total_sub_types': len(FailureSubType),
            'severity_levels': [s.value for s in FailureSeverity],
            'template_coverage': {
                'api_related': 4,
                'system_related': 3,
                'phase0_related': 4,
                'phase1_related': 4,
                'critical_related': 4
            },
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def _generate_message_hash(self, notification: NotificationMessage) -> str:
        """Phase 3: 메시지 중복 감지용 해시 생성"""
        import hashlib

        # 메시지 핵심 내용으로 해시 생성
        hash_content = f"{notification.level.value}|{notification.category.value}|{notification.title}|{notification.message[:100]}"
        return hashlib.md5(hash_content.encode('utf-8')).hexdigest()

    def _mask_sensitive_data(self, message: str) -> str:
        """Phase 3: 민감한 데이터 마스킹"""
        import re

        if not self.security.sensitive_data_masking:
            return message

        # API 키 패턴 마스킹
        message = re.sub(r'[A-Za-z0-9]{32,}', lambda m: m.group()[:4] + '*' * (len(m.group()) - 8) + m.group()[-4:], message)

        # 이메일 주소 마스킹
        message = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        lambda m: m.group().split('@')[0][:2] + '***@' + m.group().split('@')[1], message)

        # IP 주소 마스킹
        message = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
                        lambda m: '.'.join(m.group().split('.')[:2]) + '.***.***.', message)

        return message

    def _is_spam_blocked(self, notification: NotificationMessage) -> tuple:
        """Phase 3: 스팸 방지 검사"""
        current_time = datetime.now()

        if self.spam_prevention.level == SpamPreventionLevel.DISABLED:
            return False, "스팸 방지 비활성화"

        # 1. 중복 메시지 검사
        if self.spam_prevention.duplicate_detection:
            message_hash = self._generate_message_hash(notification)
            if message_hash in self.duplicate_hashes:
                return True, f"중복 메시지 차단 (hash: {message_hash[:8]})"
            self.duplicate_hashes.add(message_hash)

        # 2. 시간창 기반 제한 검사
        time_window = self.spam_prevention.time_window_minutes
        from datetime import timedelta
        cutoff_time = current_time - timedelta(minutes=time_window)

        # 최근 메시지 기록 정리
        self.message_history = [
            msg for msg in self.message_history
            if datetime.fromisoformat(msg['timestamp']) > cutoff_time
        ]

        # 시간창 내 메시지 수 확인
        category_key = notification.category.value
        recent_category_messages = [
            msg for msg in self.message_history
            if msg['category'] == category_key
        ]

        if len(recent_category_messages) >= self.spam_prevention.max_messages_per_window:
            # 쿨다운 적용
            cooldown_minutes = time_window * self.spam_prevention.cooldown_multiplier
            self.cooldown_until[category_key] = current_time + timedelta(minutes=cooldown_minutes)
            return True, f"시간창 제한 초과 ({len(recent_category_messages)}/{self.spam_prevention.max_messages_per_window})"

        # 3. 쿨다운 상태 검사
        if category_key in self.cooldown_until:
            if current_time < self.cooldown_until[category_key]:
                remaining = (self.cooldown_until[category_key] - current_time).total_seconds() / 60
                return True, f"쿨다운 중 (남은 시간: {remaining:.1f}분)"
            else:
                del self.cooldown_until[category_key]

        # 4. 패턴 분석 (HIGH/AGGRESSIVE 레벨에서만)
        if self.spam_prevention.level in [SpamPreventionLevel.HIGH, SpamPreventionLevel.AGGRESSIVE]:
            if self.spam_prevention.pattern_analysis:
                # 동일한 실행 ID로 너무 많은 메시지가 오는지 검사
                if notification.execution_id:
                    execution_messages = [
                        msg for msg in self.message_history
                        if msg.get('execution_id') == notification.execution_id
                    ]
                    if len(execution_messages) >= 5:  # 같은 실행 ID로 5개 이상
                        return True, f"실행 ID 스팸 패턴 감지 ({notification.execution_id})"

        # 메시지 기록 추가
        self.message_history.append({
            'timestamp': current_time.isoformat(),
            'category': category_key,
            'level': notification.level.value,
            'execution_id': notification.execution_id,
            'hash': self._generate_message_hash(notification)
        })

        return False, "스팸 검사 통과"

    def _security_check(self, notification: NotificationMessage) -> tuple:
        """Phase 3: 보안 검사"""
        current_time = datetime.now()

        # 개발 모드에서는 모든 보안 검사 통과
        if self.security.mode == SecurityMode.DEVELOPMENT:
            return True, "개발 모드 - 보안 검사 생략"

        # 1. 민감한 데이터 검사
        if self.security.sensitive_data_masking:
            notification.message = self._mask_sensitive_data(notification.message)

        # 2. 메시지 크기 검사 (운영 환경에서 엄격)
        max_message_size = 1000 if self.security.mode == SecurityMode.PRODUCTION else 2000
        if len(notification.message) > max_message_size:
            return False, f"메시지 크기 초과 ({len(notification.message)}/{max_message_size})"

        # 3. 보안 이벤트 로깅
        if self.security.audit_logging:
            security_event = {
                'timestamp': current_time.isoformat(),
                'event_type': 'notification_security_check',
                'level': notification.level.value,
                'category': notification.category.value,
                'execution_id': notification.execution_id,
                'message_size': len(notification.message),
                'masked': self.security.sensitive_data_masking
            }
            self.security_events.append(security_event)

            # 보안 이벤트 기록 제한 (최대 100개)
            if len(self.security_events) > 100:
                self.security_events = self.security_events[-50:]  # 최근 50개만 유지

        return True, "보안 검사 통과"

    def send_notification_with_security(self, notification: NotificationMessage) -> bool:
        """Phase 3: 보안 및 스팸 방지가 적용된 알림 전송"""

        # 1. 기본 알림 발송 조건 검사
        if not self._should_send_notification(notification.level):
            logger.info(f"알림 발송 조건 미충족: {notification.level}")
            return False

        # 2. 스팸 방지 검사
        is_spam, spam_reason = self._is_spam_blocked(notification)
        if is_spam:
            logger.warning(f"스팸 차단: {spam_reason}")
            self.blocked_messages.append({
                'timestamp': datetime.now().isoformat(),
                'reason': f"SPAM: {spam_reason}",
                'title': notification.title,
                'level': notification.level.value,
                'category': notification.category.value
            })

            # CRITICAL 메시지는 스팸 차단되어도 최소 정보는 전송
            if notification.level == NotificationLevel.CRITICAL and self.security.mode == SecurityMode.PRODUCTION:
                emergency_notification = NotificationMessage(
                    level=NotificationLevel.CRITICAL,
                    category=NotificationCategory.SYSTEM,
                    title="🚨 중요 알림 차단됨",
                    message=f"중요한 알림이 스팸 방지로 차단되었습니다: {spam_reason}",
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    execution_id=notification.execution_id
                )
                return self.send_notification(emergency_notification)

            return False

        # 3. 보안 검사
        security_passed, security_reason = self._security_check(notification)
        if not security_passed:
            logger.error(f"보안 검사 실패: {security_reason}")
            self.blocked_messages.append({
                'timestamp': datetime.now().isoformat(),
                'reason': f"SECURITY: {security_reason}",
                'title': notification.title,
                'level': notification.level.value,
                'category': notification.category.value
            })
            return False

        # 4. 최종 전송
        logger.info(f"Phase 3 보안 검사 통과: {notification.title}")
        return self.send_notification(notification)

    def get_security_analytics(self) -> Dict:
        """Phase 3: 보안 및 스팸 방지 분석 데이터"""
        from datetime import timedelta
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=24)

        # 최근 24시간 통계
        recent_blocked = [
            msg for msg in self.blocked_messages
            if datetime.fromisoformat(msg['timestamp']) > cutoff_time
        ]

        recent_security_events = [
            event for event in self.security_events
            if datetime.fromisoformat(event['timestamp']) > cutoff_time
        ]

        spam_blocks = [msg for msg in recent_blocked if msg['reason'].startswith('SPAM:')]
        security_blocks = [msg for msg in recent_blocked if msg['reason'].startswith('SECURITY:')]

        return {
            'spam_prevention': {
                'level': self.spam_prevention.level.value,
                'total_blocked_24h': len(spam_blocks),
                'duplicate_count': len(self.duplicate_hashes),
                'active_cooldowns': len(self.cooldown_until),
                'message_history_size': len(self.message_history)
            },
            'security': {
                'mode': self.security.mode.value,
                'total_blocked_24h': len(security_blocks),
                'audit_events_24h': len(recent_security_events),
                'sensitive_data_masking': self.security.sensitive_data_masking,
                'audit_logging': self.security.audit_logging
            },
            'performance': {
                'total_messages_processed': len(self.message_history) + len(self.blocked_messages),
                'block_rate_24h': f"{len(recent_blocked) / max(1, len(self.message_history)) * 100:.1f}%",
                'last_updated': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }
        }

# 전역 알림기 인스턴스
_notifier_instance = None

def get_notifier() -> MakenaideSNSNotifier:
    """싱글톤 알림기 인스턴스 반환"""
    global _notifier_instance
    if _notifier_instance is None:
        _notifier_instance = MakenaideSNSNotifier()
    return _notifier_instance

# 편의 함수들
def notify_pipeline_start(execution_id: str):
    return get_notifier().notify_pipeline_start(execution_id)

def notify_phase_complete(phase: str, results: Dict, execution_id: str):
    return get_notifier().notify_phase_complete(phase, results, execution_id)

def notify_market_sentiment(sentiment: str, trading_allowed: bool, execution_id: str):
    return get_notifier().notify_market_sentiment(sentiment, trading_allowed, execution_id)

def notify_trade_execution(trade_result: Dict, execution_id: str):
    return get_notifier().notify_trade_execution(trade_result, execution_id)

def notify_portfolio_update(portfolio_summary: Dict, execution_id: str):
    return get_notifier().notify_portfolio_update(portfolio_summary, execution_id)

def notify_pipeline_complete(execution_summary: Dict, execution_id: str):
    return get_notifier().notify_pipeline_complete(execution_summary, execution_id)

def notify_system_error(error_message: str, execution_id: str = None):
    return get_notifier().notify_system_error(error_message, execution_id)

def notify_ec2_shutdown(execution_id: str, reason: str = "파이프라인 완료"):
    return get_notifier().notify_ec2_shutdown(execution_id, reason)

def notify_pipeline_failure(failure_type: str, error_message: str, phase: str = None, execution_id: str = None, metadata: Dict = None):
    """파이프라인 실패 알림 편의 함수"""
    return get_notifier().notify_pipeline_failure(failure_type, error_message, phase, execution_id, metadata)

def notify_detailed_failure(failure_type: str, sub_type: str = None, error_message: str = "",
                          phase: str = None, execution_id: str = None, metadata: Dict = None):
    """Phase 2: 상세 실패 분류 알림 편의 함수"""
    return get_notifier().notify_detailed_failure(failure_type, sub_type, error_message, phase, execution_id, metadata)

def send_secure_notification(notification: NotificationMessage):
    """Phase 3: 보안 및 스팸 방지가 적용된 알림 전송 편의 함수"""
    return get_notifier().send_notification_with_security(notification)

def get_security_analytics():
    """Phase 3: 보안 및 스팸 방지 분석 데이터 편의 함수"""
    return get_notifier().get_security_analytics()

# ✨ NEW: 발굴 종목 리스트 관련 편의 함수들
def notify_discovered_stocks(technical_candidates: List[Dict], gpt_candidates: List[Dict], execution_id: str):
    return get_notifier().notify_discovered_stocks(technical_candidates, gpt_candidates, execution_id)

def notify_kelly_position_sizing(position_sizes: Dict[str, float], execution_id: str):
    return get_notifier().notify_kelly_position_sizing(position_sizes, execution_id)

def notify_market_analysis_summary(market_data: Dict, execution_id: str):
    return get_notifier().notify_market_analysis_summary(market_data, execution_id)

if __name__ == "__main__":
    # 테스트 코드
    notifier = MakenaideSNSNotifier()

    # 테스트 알림 전송
    test_notification = NotificationMessage(
        level=NotificationLevel.INFO,
        category=NotificationCategory.SYSTEM,
        title="시스템 테스트",
        message="SNS 알림 시스템 테스트입니다.",
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        execution_id="test_001"
    )

    result = notifier.send_notification(test_notification)
    print(f"테스트 알림 전송 결과: {result}")