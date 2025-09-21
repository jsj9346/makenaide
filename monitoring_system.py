#!/usr/bin/env python3
"""
Makenaide 모니터링 및 안전장치 시스템
EC2 자동 시작/종료 시스템의 안전성과 신뢰성 확보

🎯 기능:
1. 실시간 시스템 상태 모니터링
2. 비용 폭탄 방지 안전장치
3. 무한 루프 방지 메커니즘
4. 응급 상황 대응 시스템
5. 자동 복구 기능
"""

import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"

@dataclass
class SystemStatus:
    ec2_running: bool
    lambda_errors: int
    cost_today: float
    last_successful_run: Optional[datetime]
    failed_attempts: int
    emergency_shutdown_triggered: bool

class MakenaideMonitor:
    """Makenaide 시스템 모니터링 및 안전장치"""

    def __init__(self):
        self.ec2_client = boto3.client('ec2')
        self.lambda_client = boto3.client('lambda')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.sns_client = boto3.client('sns')
        self.ce_client = boto3.client('ce')  # Cost Explorer

        self.instance_id = 'i-082bf343089af62d3'
        self.lambda_function_name = 'makenaide-ec2-starter'
        self.sns_alerts_arn = 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts'

        # 안전 임계값
        self.max_daily_cost = 20.0  # $20/일
        self.max_runtime_hours = 3.0  # 3시간
        self.max_failed_attempts = 5  # 5회 연속 실패
        self.emergency_cost_threshold = 50.0  # $50 비상 임계값

    def get_system_status(self) -> SystemStatus:
        """현재 시스템 상태 조회"""
        try:
            # EC2 상태 확인
            ec2_response = self.ec2_client.describe_instances(
                InstanceIds=[self.instance_id]
            )
            ec2_running = ec2_response['Reservations'][0]['Instances'][0]['State']['Name'] == 'running'

            # Lambda 오류 수 확인 (지난 24시간)
            lambda_errors = self._get_lambda_error_count()

            # 오늘 비용 확인
            cost_today = self._get_daily_cost()

            # 마지막 성공 실행 시간
            last_successful_run = self._get_last_successful_run()

            # 연속 실패 횟수
            failed_attempts = self._get_failed_attempts()

            # 비상 종료 상태
            emergency_shutdown = cost_today > self.emergency_cost_threshold

            return SystemStatus(
                ec2_running=ec2_running,
                lambda_errors=lambda_errors,
                cost_today=cost_today,
                last_successful_run=last_successful_run,
                failed_attempts=failed_attempts,
                emergency_shutdown_triggered=emergency_shutdown
            )

        except Exception as e:
            logger.error(f"❌ 시스템 상태 조회 실패: {e}")
            return SystemStatus(
                ec2_running=False,
                lambda_errors=999,
                cost_today=999.0,
                last_successful_run=None,
                failed_attempts=999,
                emergency_shutdown_triggered=True
            )

    def _get_lambda_error_count(self) -> int:
        """Lambda 함수 오류 수 조회 (지난 24시간)"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)

            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Errors',
                Dimensions=[
                    {
                        'Name': 'FunctionName',
                        'Value': self.lambda_function_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1시간 단위
                Statistics=['Sum']
            )

            total_errors = sum(point['Sum'] for point in response['Datapoints'])
            return int(total_errors)

        except Exception as e:
            logger.warning(f"Lambda 오류 수 조회 실패: {e}")
            return 0

    def _get_daily_cost(self) -> float:
        """오늘 AWS 비용 조회"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

            response = self.ce_client.get_cost_and_usage(
                TimePeriod={
                    'Start': today,
                    'End': tomorrow
                },
                Granularity='DAILY',
                Metrics=['BlendedCost'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ]
            )

            total_cost = 0.0
            for day in response['ResultsByTime']:
                for group in day['Groups']:
                    cost = float(group['Metrics']['BlendedCost']['Amount'])
                    total_cost += cost

            return total_cost

        except Exception as e:
            logger.warning(f"일일 비용 조회 실패: {e}")
            return 0.0

    def _get_last_successful_run(self) -> Optional[datetime]:
        """마지막 성공 실행 시간 조회"""
        try:
            # CloudWatch Logs에서 성공 로그 검색
            logs_client = boto3.client('logs')
            log_group = f'/aws/lambda/{self.lambda_function_name}'

            response = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=int((datetime.now() - timedelta(days=7)).timestamp() * 1000),
                filterPattern='[timestamp, requestId, level="✅", message*="successfully"]'
            )

            if response['events']:
                # 가장 최근 성공 로그의 타임스탬프
                latest_event = max(response['events'], key=lambda x: x['timestamp'])
                return datetime.fromtimestamp(latest_event['timestamp'] / 1000)

            return None

        except Exception as e:
            logger.warning(f"마지막 성공 실행 시간 조회 실패: {e}")
            return None

    def _get_failed_attempts(self) -> int:
        """연속 실패 횟수 조회"""
        try:
            # CloudWatch Logs에서 실패 로그 검색
            logs_client = boto3.client('logs')
            log_group = f'/aws/lambda/{self.lambda_function_name}'

            response = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=int((datetime.now() - timedelta(hours=24)).timestamp() * 1000),
                filterPattern='[timestamp, requestId, level="❌", message*]'
            )

            return len(response['events'])

        except Exception as e:
            logger.warning(f"실패 횟수 조회 실패: {e}")
            return 0

    def check_safety_violations(self, status: SystemStatus) -> List[Tuple[AlertLevel, str]]:
        """안전 규칙 위반 사항 확인"""
        violations = []

        # 1. 비용 폭탄 확인
        if status.cost_today > self.emergency_cost_threshold:
            violations.append((AlertLevel.EMERGENCY, f"비상 비용 임계값 초과: ${status.cost_today:.2f}"))
        elif status.cost_today > self.max_daily_cost:
            violations.append((AlertLevel.CRITICAL, f"일일 비용 한도 초과: ${status.cost_today:.2f}"))

        # 2. EC2 장시간 실행 확인
        if status.ec2_running:
            running_time = self._get_ec2_running_time()
            if running_time > self.max_runtime_hours:
                violations.append((AlertLevel.CRITICAL, f"EC2 장시간 실행: {running_time:.1f}시간"))

        # 3. 연속 실패 확인
        if status.failed_attempts >= self.max_failed_attempts:
            violations.append((AlertLevel.CRITICAL, f"연속 실패 {status.failed_attempts}회"))

        # 4. Lambda 오류 급증
        if status.lambda_errors > 10:
            violations.append((AlertLevel.WARNING, f"Lambda 오류 급증: {status.lambda_errors}회"))

        # 5. 장기간 성공하지 못한 경우
        if status.last_successful_run:
            hours_since_success = (datetime.now() - status.last_successful_run).total_seconds() / 3600
            if hours_since_success > 48:  # 48시간
                violations.append((AlertLevel.WARNING, f"48시간 이상 성공하지 못함"))

        return violations

    def _get_ec2_running_time(self) -> float:
        """EC2 실행 시간 조회 (시간 단위)"""
        try:
            response = self.ec2_client.describe_instances(
                InstanceIds=[self.instance_id]
            )

            instance = response['Reservations'][0]['Instances'][0]
            if instance['State']['Name'] != 'running':
                return 0.0

            launch_time = instance['LaunchTime']
            running_time = (datetime.now(launch_time.tzinfo) - launch_time).total_seconds() / 3600
            return running_time

        except Exception as e:
            logger.warning(f"EC2 실행 시간 조회 실패: {e}")
            return 0.0

    def emergency_shutdown(self, reason: str) -> bool:
        """비상 종료 실행"""
        try:
            logger.critical(f"🚨 비상 종료 실행: {reason}")

            # 1. EC2 강제 종료
            if self._is_ec2_running():
                self.ec2_client.stop_instances(
                    InstanceIds=[self.instance_id],
                    Force=True
                )
                logger.info("🔌 EC2 강제 종료 완료")

            # 2. EventBridge 규칙 비활성화
            self._disable_eventbridge_rules()

            # 3. 비상 알림 발송
            self.send_alert(
                AlertLevel.EMERGENCY,
                f"🚨 Makenaide 비상 종료",
                f"""
비상 상황으로 인해 Makenaide 시스템을 긴급 종료했습니다.

❌ 종료 이유: {reason}

🔧 조치 사항:
1. 시스템 로그 확인
2. 문제 원인 분석
3. 안전 확인 후 수동 재시작

⚠️ 모든 자동 실행이 중단되었습니다.
수동으로 재시작하기 전까지 거래가 실행되지 않습니다.
                """.strip()
            )

            return True

        except Exception as e:
            logger.error(f"❌ 비상 종료 실패: {e}")
            return False

    def _is_ec2_running(self) -> bool:
        """EC2 실행 상태 확인"""
        try:
            response = self.ec2_client.describe_instances(
                InstanceIds=[self.instance_id]
            )
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            return state == 'running'
        except:
            return False

    def _disable_eventbridge_rules(self) -> bool:
        """EventBridge 규칙 비활성화"""
        try:
            events_client = boto3.client('events')

            # Makenaide 관련 규칙 조회
            response = events_client.list_rules()
            makenaide_rules = [
                rule['Name'] for rule in response['Rules']
                if 'makenaide' in rule['Name'].lower()
            ]

            # 규칙 비활성화
            for rule_name in makenaide_rules:
                events_client.disable_rule(Name=rule_name)
                logger.info(f"⏸️ EventBridge 규칙 비활성화: {rule_name}")

            return True

        except Exception as e:
            logger.error(f"❌ EventBridge 규칙 비활성화 실패: {e}")
            return False

    def send_alert(self, level: AlertLevel, subject: str, message: str) -> bool:
        """알림 발송"""
        try:
            # 알림 레벨에 따른 이모지
            level_emojis = {
                AlertLevel.INFO: "ℹ️",
                AlertLevel.WARNING: "⚠️",
                AlertLevel.CRITICAL: "🚨",
                AlertLevel.EMERGENCY: "🆘"
            }

            emoji = level_emojis.get(level, "📢")
            full_subject = f"{emoji} {subject}"

            # 메시지에 시간 정보 추가
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S KST')
            full_message = f"""
{message}

📊 시스템 정보:
- 발생 시간: {timestamp}
- 알림 레벨: {level.value}
- 인스턴스: {self.instance_id}

🔗 확인 방법:
- CloudWatch: AWS 콘솔에서 로그 확인
- EC2 SSH: ssh -i /Users/13ruce/aws/makenaide-key.pem ec2-user@52.78.186.226
            """.strip()

            # SNS 발송
            self.sns_client.publish(
                TopicArn=self.sns_alerts_arn,
                Subject=full_subject,
                Message=full_message,
                MessageAttributes={
                    'level': {
                        'DataType': 'String',
                        'StringValue': level.value
                    }
                }
            )

            logger.info(f"📧 알림 발송 완료: {subject}")
            return True

        except Exception as e:
            logger.error(f"❌ 알림 발송 실패: {e}")
            return False

    def run_safety_check(self) -> bool:
        """안전 점검 실행"""
        try:
            logger.info("🛡️ 안전 점검 시작")

            # 시스템 상태 조회
            status = self.get_system_status()

            # 상태 정보 로그
            logger.info(f"📊 시스템 상태:")
            logger.info(f"   - EC2 실행: {status.ec2_running}")
            logger.info(f"   - Lambda 오류: {status.lambda_errors}회")
            logger.info(f"   - 오늘 비용: ${status.cost_today:.2f}")
            logger.info(f"   - 연속 실패: {status.failed_attempts}회")

            # 안전 규칙 위반 확인
            violations = self.check_safety_violations(status)

            if not violations:
                logger.info("✅ 모든 안전 규칙 통과")
                return True

            # 위반 사항 처리
            emergency_required = False
            for level, message in violations:
                logger.warning(f"{level.value}: {message}")

                if level == AlertLevel.EMERGENCY:
                    emergency_required = True
                elif level == AlertLevel.CRITICAL:
                    # 중요 알림 발송
                    self.send_alert(level, "Makenaide 중요 알림", message)

            # 비상 상황 처리
            if emergency_required:
                self.emergency_shutdown("안전 규칙 위반으로 인한 비상 종료")
                return False

            return True

        except Exception as e:
            logger.error(f"❌ 안전 점검 실패: {e}")
            self.send_alert(
                AlertLevel.CRITICAL,
                "Makenaide 안전 점검 실패",
                f"안전 점검 시스템에서 오류가 발생했습니다: {e}"
            )
            return False

    def generate_status_report(self) -> str:
        """시스템 상태 보고서 생성"""
        try:
            status = self.get_system_status()
            violations = self.check_safety_violations(status)

            report = f"""
📊 Makenaide 시스템 상태 보고서
생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S KST')}

🖥️ EC2 상태:
- 실행 상태: {'실행 중' if status.ec2_running else '중지됨'}
- 실행 시간: {self._get_ec2_running_time():.1f}시간

⚡ Lambda 상태:
- 24시간 오류: {status.lambda_errors}회
- 연속 실패: {status.failed_attempts}회

💰 비용 상태:
- 오늘 비용: ${status.cost_today:.2f}
- 일일 한도: ${self.max_daily_cost}

📈 마지막 성공:
- {status.last_successful_run.strftime('%Y-%m-%d %H:%M:%S') if status.last_successful_run else '알 수 없음'}

🛡️ 안전 상태:
            """

            if violations:
                report += f"❌ {len(violations)}개 위반 사항 발견\n"
                for level, message in violations:
                    report += f"   - {level.value}: {message}\n"
            else:
                report += "✅ 모든 안전 규칙 통과\n"

            return report.strip()

        except Exception as e:
            return f"❌ 상태 보고서 생성 실패: {e}"

def main():
    """메인 실행 함수"""
    try:
        monitor = MakenaideMonitor()

        # 안전 점검 실행
        safety_ok = monitor.run_safety_check()

        # 상태 보고서 생성
        report = monitor.generate_status_report()
        print(report)

        if not safety_ok:
            print("\n❌ 안전 점검 실패 - 시스템 점검 필요")
            return False

        print("\n✅ 안전 점검 완료")
        return True

    except Exception as e:
        print(f"❌ 모니터링 시스템 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)