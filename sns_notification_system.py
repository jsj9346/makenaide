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
                message_parts.append("🎯 기술적 분석 통과 종목:")

                # 상위 10개만 표시 (너무 길어지지 않도록)
                top_technical = technical_candidates[:10]

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

                if len(technical_candidates) > 10:
                    message_parts.append(f"  ... 외 {len(technical_candidates) - 10}개 종목")

                message_parts.append("")

            # 🤖 GPT 분석 승인 종목
            if gpt_candidates:
                message_parts.append("🤖 GPT 분석 승인 종목:")

                for i, candidate in enumerate(gpt_candidates, 1):
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

            # 종합 요약
            total_technical = len(technical_candidates)
            total_gpt = len(gpt_candidates)

            title = f"발굴 종목 리스트 (기술적: {total_technical}개, GPT: {total_gpt}개)"

            notification = NotificationMessage(
                level=NotificationLevel.SUCCESS,
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