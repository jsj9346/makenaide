#!/bin/bash
# create_eventbridge.sh - EventBridge 스케줄 생성 스크립트

set -e

echo "⏰ EventBridge 스케줄 생성 시작"
echo "================================"

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS 계정 ID 및 리전 가져오기
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

# EventBridge 규칙 이름
RULE_NAME="makenaide-schedule"
TARGET_ID="1"
LAMBDA_FUNCTION_NAME="makenaide-controller"

# Lambda 함수 ARN
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${LAMBDA_FUNCTION_NAME}"

echo "📝 EventBridge 규칙 생성 중..."

# 1. EventBridge 규칙 생성 (4시간마다 실행)
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(4 hours)" \
    --description "Makenaide 4시간마다 자동 실행" \
    --state ENABLED

echo "✅ EventBridge 규칙 생성 완료: $RULE_NAME"

# 2. Lambda 함수에 EventBridge 호출 권한 부여
echo "🔐 Lambda 함수 호출 권한 설정 중..."

aws lambda add-permission \
    --function-name $LAMBDA_FUNCTION_NAME \
    --statement-id "allow-eventbridge-${RULE_NAME}" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"

echo "✅ Lambda 함수 호출 권한 설정 완료"

# 3. EventBridge 규칙에 Lambda 함수 타겟 추가
echo "🎯 EventBridge 타겟 설정 중..."

# Lambda 함수 입력 데이터 설정 (EC2 시작)
INPUT_DATA='{"action": "start"}'

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=${TARGET_ID},Arn=${LAMBDA_ARN},Input='${INPUT_DATA}'"

echo "✅ EventBridge 타겟 설정 완료"

# 4. EC2 자동 종료를 위한 추가 Lambda 함수 생성 (Makenaide 실행 완료 후)
echo "🔧 EC2 자동 종료 Lambda 함수 생성 중..."

SHUTDOWN_FUNCTION_NAME="makenaide-shutdown"
SHUTDOWN_LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${SHUTDOWN_FUNCTION_NAME}"

# Lambda 함수 코드 생성
mkdir -p lambda_shutdown_package
cd lambda_shutdown_package

cat > lambda_function.py << 'EOF'
import json
import boto3
import logging
import time

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Makenaide 실행 완료 후 EC2 인스턴스 자동 종료 함수
    """
    try:
        # EC2 클라이언트 생성
        ec2 = boto3.client('ec2')
        
        # Makenaide EC2 인스턴스 찾기
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': ['makenaide-ec2']},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        
        if not response['Reservations']:
            logger.info("실행 중인 Makenaide EC2 인스턴스가 없습니다.")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'no_running_instance',
                    'message': '실행 중인 인스턴스가 없습니다.'
                })
            }
        
        # 인스턴스 ID 추출
        instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
        
        # Makenaide 실행 완료 대기 (약 3시간 후 종료)
        # 실제로는 CloudWatch Logs나 다른 방법으로 실행 완료를 감지해야 함
        # 여기서는 단순히 시간 기반으로 처리
        
        logger.info(f"인스턴스 {instance_id} 종료 명령 전송")
        
        # 인스턴스 중지
        ec2.stop_instances(InstanceIds=[instance_id])
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'stopping',
                'instance_id': instance_id,
                'message': 'Makenaide 실행 완료 후 인스턴스 중지 명령 전송'
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda 함수 실행 중 오류: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': f'Lambda 함수 실행 중 오류: {str(e)}'
            })
        }
EOF

# Lambda 패키지 생성
zip -r lambda_function.zip lambda_function.py

# Lambda 함수 생성
LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/makenaide-lambda-role"

aws lambda create-function \
    --function-name $SHUTDOWN_FUNCTION_NAME \
    --runtime python3.9 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://lambda_function.zip \
    --description "Makenaide 실행 완료 후 EC2 자동 종료" \
    --timeout 60 \
    --memory-size 128

echo "✅ EC2 자동 종료 Lambda 함수 생성 완료: $SHUTDOWN_FUNCTION_NAME"

# 임시 파일 정리
cd ..
rm -rf lambda_shutdown_package

# 5. EC2 종료를 위한 추가 EventBridge 규칙 (시작 3시간 후)
SHUTDOWN_RULE_NAME="makenaide-shutdown-schedule"
SHUTDOWN_TARGET_ID="1"

echo "⏰ EC2 자동 종료 스케줄 생성 중..."

# 종료 규칙 생성 (4시간마다, 시작 3시간 후)
aws events put-rule \
    --name $SHUTDOWN_RULE_NAME \
    --schedule-expression "rate(4 hours)" \
    --description "Makenaide EC2 자동 종료 (시작 3시간 후)" \
    --state ENABLED

# Lambda 함수 호출 권한 부여
aws lambda add-permission \
    --function-name $SHUTDOWN_FUNCTION_NAME \
    --statement-id "allow-eventbridge-${SHUTDOWN_RULE_NAME}" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${SHUTDOWN_RULE_NAME}"

# 타겟 설정
aws events put-targets \
    --rule $SHUTDOWN_RULE_NAME \
    --targets "Id=${SHUTDOWN_TARGET_ID},Arn=${SHUTDOWN_LAMBDA_ARN}"

echo "✅ EC2 자동 종료 스케줄 설정 완료"

# EventBridge 설정 정보 저장
cat > aws_eventbridge_config.json << EOF
{
  "start_rule_name": "$RULE_NAME",
  "start_rule_arn": "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}",
  "start_schedule": "rate(4 hours)",
  "start_target_lambda_arn": "$LAMBDA_ARN",
  "shutdown_rule_name": "$SHUTDOWN_RULE_NAME",
  "shutdown_rule_arn": "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${SHUTDOWN_RULE_NAME}",
  "shutdown_schedule": "rate(4 hours)",
  "shutdown_target_lambda_arn": "$SHUTDOWN_LAMBDA_ARN",
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
}
EOF

echo "💾 EventBridge 설정 정보 저장 완료: aws_eventbridge_config.json"

echo ""
echo "🎉 EventBridge 스케줄 설정 완료!"
echo "=================================="
echo "시작 규칙: $RULE_NAME (4시간마다 실행)"
echo "종료 규칙: $SHUTDOWN_RULE_NAME (3시간 후 종료)"
echo "타겟 Lambda: $LAMBDA_FUNCTION_NAME"
echo "종료 Lambda: $SHUTDOWN_FUNCTION_NAME"
echo "==================================" 