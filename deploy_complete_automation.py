#!/usr/bin/env python3
"""
Makenaide 완전 자동화 시스템 배포
EventBridge → Lambda → EC2 → Makenaide → Auto Shutdown

🎯 전체 시스템 구성:
1. EventBridge 스케줄 (6개 거래 시간대)
2. Lambda 함수 (EC2 자동 시작)
3. EC2 자동 실행 스크립트
4. Makenaide 자동 종료 로직
5. IAM 권한 설정
"""

import subprocess
import time
import logging
from typing import Dict, List, Tuple

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_python_script(script_name: str) -> Tuple[bool, str]:
    """Python 스크립트 실행"""
    try:
        logger.info(f"🔧 실행 중: {script_name}")
        result = subprocess.run(
            ['python3', script_name],
            capture_output=True,
            text=True,
            check=True
        )

        if result.stdout:
            logger.info(f"✅ 출력:\n{result.stdout}")

        return True, result.stdout

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ 스크립트 실행 실패: {script_name}")
        if e.stderr:
            logger.error(f"오류: {e.stderr}")
        return False, e.stderr

def check_aws_permissions() -> bool:
    """AWS 권한 확인"""
    try:
        logger.info("🔍 AWS 권한 확인 중...")

        # AWS CLI 설정 확인
        result = subprocess.run(['aws', 'sts', 'get-caller-identity'],
                               capture_output=True, text=True, check=True)

        import json
        identity = json.loads(result.stdout)

        logger.info(f"✅ AWS 계정: {identity.get('Account')}")
        logger.info(f"✅ 사용자: {identity.get('Arn')}")

        return True

    except Exception as e:
        logger.error(f"❌ AWS 권한 확인 실패: {e}")
        return False

def deploy_system() -> bool:
    """전체 시스템 배포"""
    try:
        logger.info("🚀 Makenaide 완전 자동화 시스템 배포 시작")
        logger.info("=" * 70)

        # 0. AWS 권한 확인
        logger.info("0️⃣ AWS 권한 확인")
        if not check_aws_permissions():
            return False

        # 1. Lambda 함수 배포
        logger.info("\n1️⃣ Lambda 함수 배포")
        success, output = run_python_script('deploy_lambda_ec2_starter.py')
        if not success:
            logger.error("Lambda 함수 배포 실패")
            return False

        # Lambda 배포 후 잠시 대기 (IAM 권한 전파)
        logger.info("⏳ IAM 권한 전파 대기 (30초)...")
        time.sleep(30)

        # 2. EC2 자동 시작 설정
        logger.info("\n2️⃣ EC2 자동 시작 설정")
        success, output = run_python_script('setup_ec2_autostart.py')
        if not success:
            logger.error("EC2 자동 시작 설정 실패")
            return False

        # 3. EventBridge 스케줄 설정
        logger.info("\n3️⃣ EventBridge 스케줄 설정")
        success, output = run_python_script('setup_eventbridge_schedule.py')
        if not success:
            logger.error("EventBridge 스케줄 설정 실패")
            return False

        logger.info("\n" + "=" * 70)
        logger.info("🎉 완전 자동화 시스템 배포 완료!")

        return True

    except Exception as e:
        logger.error(f"❌ 시스템 배포 실패: {e}")
        return False

def test_system() -> bool:
    """시스템 테스트"""
    try:
        logger.info("\n🧪 시스템 테스트 시작")
        logger.info("=" * 50)

        # 1. Lambda 함수 테스트
        logger.info("1️⃣ Lambda 함수 수동 테스트")

        test_event = {
            "pipeline_type": "main_trading",
            "schedule_name": "makenaide-trading-test",
            "kst_time": "TEST",
            "market_timing": "시스템 테스트"
        }

        import json
        import boto3

        lambda_client = boto3.client('lambda')

        response = lambda_client.invoke(
            FunctionName='makenaide-ec2-starter',
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )

        result = json.loads(response['Payload'].read())
        logger.info(f"Lambda 테스트 결과: {result}")

        if response['StatusCode'] == 200:
            logger.info("✅ Lambda 함수 테스트 성공")
        else:
            logger.warning("⚠️ Lambda 함수 테스트 실패")

        # 2. EventBridge 규칙 확인
        logger.info("\n2️⃣ EventBridge 규칙 확인")

        events_client = boto3.client('events')
        rules = events_client.list_rules()

        makenaide_rules = [rule for rule in rules['Rules']
                          if 'makenaide' in rule['Name'].lower()]

        logger.info(f"✅ 생성된 EventBridge 규칙: {len(makenaide_rules)}개")
        for rule in makenaide_rules:
            logger.info(f"   - {rule['Name']}: {rule['ScheduleExpression']} ({rule['State']})")

        # 3. EC2 자동 실행 설정 확인
        logger.info("\n3️⃣ EC2 자동 실행 설정 확인")

        # SSH로 EC2 설정 확인
        try:
            ssh_result = subprocess.run([
                'ssh', '-i', '/Users/13ruce/aws/makenaide-key.pem',
                'ec2-user@52.78.186.226',
                'sudo systemctl is-enabled makenaide-auto.service'
            ], capture_output=True, text=True, timeout=10)

            if 'enabled' in ssh_result.stdout:
                logger.info("✅ EC2 자동 실행 서비스 활성화됨")
            else:
                logger.warning("⚠️ EC2 자동 실행 서비스 확인 필요")

        except Exception as e:
            logger.warning(f"⚠️ EC2 설정 확인 실패: {e}")

        return True

    except Exception as e:
        logger.error(f"❌ 시스템 테스트 실패: {e}")
        return False

def show_next_execution_times() -> None:
    """다음 실행 시간 표시"""
    try:
        from datetime import datetime, timedelta
        import pytz

        logger.info("\n📅 다음 자동 실행 시간 (KST)")
        logger.info("=" * 40)

        # KST 시간대
        kst = pytz.timezone('Asia/Seoul')
        now_kst = datetime.now(kst)

        # 거래 시간대 (KST)
        trading_times = ['02:00', '09:00', '15:00', '18:00', '21:00', '23:00']

        next_executions = []
        for time_str in trading_times:
            hour, minute = map(int, time_str.split(':'))

            # 오늘 해당 시간
            today_time = now_kst.replace(hour=hour, minute=minute, second=0, microsecond=0)

            # 오늘 시간이 지났으면 내일
            if today_time <= now_kst:
                next_time = today_time + timedelta(days=1)
            else:
                next_time = today_time

            next_executions.append((time_str, next_time))

        # 시간순 정렬
        next_executions.sort(key=lambda x: x[1])

        logger.info(f"현재 시각: {now_kst.strftime('%Y-%m-%d %H:%M:%S KST')}")
        logger.info("\n🕒 다음 실행 일정:")

        for i, (time_str, next_time) in enumerate(next_executions[:3]):  # 다음 3회만 표시
            time_diff = next_time - now_kst
            hours = int(time_diff.total_seconds() // 3600)
            minutes = int((time_diff.total_seconds() % 3600) // 60)

            logger.info(f"{i+1}. {next_time.strftime('%Y-%m-%d %H:%M')} KST ({hours}시간 {minutes}분 후)")

    except Exception as e:
        logger.warning(f"⚠️ 실행 시간 계산 실패: {e}")

def show_monitoring_guide() -> None:
    """모니터링 가이드 표시"""
    logger.info("\n📊 시스템 모니터링 가이드")
    logger.info("=" * 50)

    logger.info("🔍 확인 방법:")
    logger.info("1. AWS CloudWatch Logs")
    logger.info("   - Lambda: /aws/lambda/makenaide-ec2-starter")
    logger.info("   - EC2: CloudWatch Agent 또는 SSH 접속")

    logger.info("\n2. SNS 알림")
    logger.info("   - 거래 알림: makenaide-trading-alerts")
    logger.info("   - 시스템 알림: makenaide-system-alerts")
    logger.info("   - 수신: jsj9346@gmail.com")

    logger.info("\n3. EC2 직접 확인")
    logger.info("   - SSH: ssh -i /Users/13ruce/aws/makenaide-key.pem ec2-user@52.78.186.226")
    logger.info("   - 로그: tail -f ~/makenaide/logs/auto_execution.log")
    logger.info("   - 서비스: sudo systemctl status makenaide-auto.service")

    logger.info("\n4. AWS 콘솔")
    logger.info("   - EventBridge: 스케줄 규칙 상태 확인")
    logger.info("   - Lambda: 함수 실행 로그 확인")
    logger.info("   - EC2: 인스턴스 시작/종료 이력 확인")

def main():
    """메인 실행 함수"""
    try:
        # 시스템 배포
        if not deploy_system():
            logger.error("❌ 시스템 배포 실패")
            return False

        # 시스템 테스트
        if not test_system():
            logger.warning("⚠️ 시스템 테스트에서 일부 실패")

        # 다음 실행 시간 표시
        show_next_execution_times()

        # 모니터링 가이드
        show_monitoring_guide()

        logger.info("\n" + "=" * 70)
        logger.info("🎉 Makenaide 완전 자동화 시스템 배포 성공!")
        logger.info("🚀 이제 EventBridge 스케줄에 따라 자동으로 거래 파이프라인이 실행됩니다.")

        return True

    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        print(f"\n🎯 배포 {'성공' if success else '실패'}")

    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")

    except Exception as e:
        print(f"\n❌ 배포 실패: {e}")
        import traceback
        traceback.print_exc()