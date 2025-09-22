"""
Makenaide EC2 자동 시작 Lambda 함수
EventBridge → Lambda → EC2 Start → Makenaide 자동 실행

🎯 기능:
1. EventBridge 스케줄러에서 호출
2. EC2 인스턴스 자동 시작
3. User Data를 통한 Makenaide 자동 실행 설정
4. SNS 알림 (시작 성공/실패)
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Dict, Any

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트
ec2 = boto3.client('ec2')
sns = boto3.client('sns')

# 설정
EC2_INSTANCE_ID = 'i-075ee29859eac9eeb'
SNS_SYSTEM_ALERTS_ARN = 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts'

# EC2 User Data 스크립트 (Makenaide 자동 실행)
USER_DATA_SCRIPT = """#!/bin/bash
# Makenaide 자동 실행 스크립트
# EC2 시작 후 자동으로 Makenaide 파이프라인 실행

# 로그 설정
LOG_FILE="/home/ec2-user/makenaide/auto_execution.log"
exec > >(tee -a $LOG_FILE) 2>&1

echo "=================================================="
echo "🚀 Makenaide 자동 실행 시작: $(date)"
echo "=================================================="

# ec2-user로 전환하여 실행
sudo -u ec2-user bash << 'EOF'
cd /home/ec2-user/makenaide

# Python 가상환경 활성화 (있다면)
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Python 가상환경 활성화"
fi

# 환경변수 설정
export EC2_AUTO_SHUTDOWN=true
export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH

echo "🔧 환경변수 설정 완료"
echo "   - EC2_AUTO_SHUTDOWN: $EC2_AUTO_SHUTDOWN"
echo "   - PYTHONPATH: $PYTHONPATH"

# Makenaide 파이프라인 실행
echo "🎯 Makenaide 파이프라인 실행 시작..."
python3 makenaide.py --risk-level moderate

# 실행 결과 확인
PIPELINE_EXIT_CODE=$?
echo "📊 파이프라인 종료 코드: $PIPELINE_EXIT_CODE"

if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
    echo "✅ Makenaide 파이프라인 성공적으로 완료"
else
    echo "❌ Makenaide 파이프라인 실행 실패"
fi

echo "=================================================="
echo "🏁 Makenaide 자동 실행 완료: $(date)"
echo "=================================================="

EOF

# EC2 자동 종료는 makenaide.py 내부에서 처리됨 (EC2_AUTO_SHUTDOWN=true)
echo "⏳ Makenaide에서 자동 종료 처리 중..."
"""

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 Makenaide EC2 자동 시작 Lambda 실행")
        logger.info(f"📥 Event: {json.dumps(event, ensure_ascii=False)}")

        # 이벤트에서 정보 추출
        pipeline_type = event.get('pipeline_type', 'main_trading')
        schedule_name = event.get('schedule_name', 'unknown')
        kst_time = event.get('kst_time', 'unknown')
        market_timing = event.get('market_timing', 'unknown')

        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')

        logger.info(f"🎯 파이프라인 유형: {pipeline_type}")
        logger.info(f"📅 스케줄: {schedule_name} ({kst_time})")
        logger.info(f"🌍 시장 타이밍: {market_timing}")
        logger.info(f"🔍 실행 ID: {execution_id}")

        # EC2 인스턴스 상태 확인
        response = ec2.describe_instances(InstanceIds=[EC2_INSTANCE_ID])
        instance = response['Reservations'][0]['Instances'][0]
        current_state = instance['State']['Name']

        logger.info(f"🖥️ 현재 EC2 상태: {current_state}")

        if current_state == 'running':
            logger.warning("⚠️ EC2가 이미 실행 중입니다. 중복 실행 방지.")

            # 중복 실행 방지 알림
            send_sns_notification(
                subject="⚠️ Makenaide EC2 중복 실행 방지",
                message=f"""
🚨 Makenaide EC2 중복 실행 방지

EC2 인스턴스가 이미 실행 중이므로 새로운 파이프라인 실행을 건너뜁니다.

📊 상세 정보:
- 인스턴스 ID: {EC2_INSTANCE_ID}
- 현재 상태: {current_state}
- 요청된 스케줄: {schedule_name} ({kst_time})
- 시장 타이밍: {market_timing}
- 실행 ID: {execution_id}

🎯 조치 사항:
현재 실행 중인 파이프라인이 완료되면 자동으로 EC2가 종료됩니다.
다음 스케줄에서 정상적으로 실행될 예정입니다.
                """.strip(),
                category="SYSTEM"
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'EC2 already running, skipped duplicate execution',
                    'instance_state': current_state,
                    'execution_id': execution_id
                }, ensure_ascii=False)
            }

        elif current_state in ['stopped', 'stopping']:
            # EC2 시작
            logger.info("🔄 EC2 인스턴스 시작 중...")

            start_response = ec2.start_instances(InstanceIds=[EC2_INSTANCE_ID])
            logger.info(f"✅ EC2 시작 명령 전송 완료")

            # User Data 업데이트 (자동 실행 스크립트)
            # 참고: 실행 중인 인스턴스의 User Data는 변경할 수 없으므로
            # 별도의 시작 스크립트를 사용해야 함

            # ✅ EC2 시작 성공 (알림 생략, 로그만 기록)
            logger.info("✅ EC2 시작 성공 - SNS 알림 생략 (스팸 방지)")
            logger.info(f"📊 상세 정보: ID={EC2_INSTANCE_ID}, 상태={current_state}→starting, 스케줄={schedule_name}")
            logger.info(f"🎯 실행 ID: {execution_id}, 시장 타이밍: {market_timing}, 파이프라인: {pipeline_type}")
            logger.info("📧 거래 결과는 별도 알림으로 전송됩니다")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'EC2 started successfully',
                    'instance_id': EC2_INSTANCE_ID,
                    'previous_state': current_state,
                    'schedule_name': schedule_name,
                    'execution_id': execution_id
                }, ensure_ascii=False)
            }

        else:
            # 예상치 못한 상태
            logger.error(f"❌ 예상치 못한 EC2 상태: {current_state}")

            send_sns_notification(
                subject="🚨 Makenaide EC2 시작 실패 - 즉시 조치 필요",
                message=f"""
💥 CRITICAL: Makenaide EC2 시작 실패

예상치 못한 EC2 상태로 인해 자동매매 파이프라인을 시작할 수 없습니다.

📊 상세 정보:
- 인스턴스 ID: {EC2_INSTANCE_ID}
- 현재 상태: {current_state} ← 문제 상태
- 예상 상태: stopped
- 요청된 스케줄: {schedule_name} ({kst_time})
- 실행 ID: {execution_id}

🚨 영향:
- 자동매매 파이프라인 실행 불가
- 거래 기회 상실 가능성

🔧 즉시 조치 방법:
1. AWS 콘솔 → EC2 → 인스턴스 ({EC2_INSTANCE_ID}) 확인
2. 상태가 'running'이면 → 수동으로 'stop' 후 재시작
3. 상태가 'pending'이면 → 완료될 때까지 대기 후 재시작
4. 기타 상태면 → 인스턴스 재부팅 또는 지원팀 문의

📞 긴급 시: AWS 콘솔에서 수동 실행 가능
                """.strip(),
                category="CRITICAL"
            )

            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Unexpected EC2 state',
                    'instance_state': current_state,
                    'execution_id': execution_id
                }, ensure_ascii=False)
            }

    except Exception as e:
        logger.error(f"❌ Lambda 실행 중 오류: {e}")

        # 오류 알림
        send_sns_notification(
            subject="🚨 Makenaide Lambda 실행 오류",
            message=f"""
💥 Makenaide EC2 시작 Lambda에서 오류가 발생했습니다.

❌ 오류 내용:
{str(e)}

📊 상세 정보:
- Lambda 함수: makenaide-ec2-starter
- 이벤트: {json.dumps(event, ensure_ascii=False, indent=2)}
- 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

🔧 조치 필요:
Lambda 함수 로그를 확인하고 오류를 수정하세요.
            """.strip(),
            category="SYSTEM"
        )

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'execution_time': datetime.now().isoformat()
            }, ensure_ascii=False)
        }

def send_sns_notification(subject: str, message: str, category: str = "SYSTEM") -> bool:
    """SNS 알림 전송"""
    try:
        response = sns.publish(
            TopicArn=SNS_SYSTEM_ALERTS_ARN,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'category': {
                    'DataType': 'String',
                    'StringValue': category
                },
                'level': {
                    'DataType': 'String',
                    'StringValue': 'INFO' if '✅' in subject or '🚀' in subject else 'WARNING' if '⚠️' in subject else 'CRITICAL'
                }
            }
        )

        logger.info(f"📧 SNS 알림 전송 완료: {response['MessageId']}")
        return True

    except Exception as e:
        logger.error(f"❌ SNS 알림 전송 실패: {e}")
        return False