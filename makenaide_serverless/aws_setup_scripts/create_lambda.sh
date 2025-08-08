#!/bin/bash
# create_lambda.sh - Lambda 함수 생성 스크립트

set -e

echo "🔧 Lambda 함수 생성 시작"
echo "=========================="

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS 계정 ID 및 리전 가져오기
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

# Lambda 함수 이름
FUNCTION_NAME="makenaide-controller"

# Lambda 함수 코드 생성
echo "📝 Lambda 함수 코드 생성 중..."
mkdir -p lambda_package
cd lambda_package

cat > lambda_function.py << 'EOF'
import json
import boto3
import logging

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Makenaide EC2 인스턴스 자동 시작/중지 Lambda 함수
    """
    try:
        # EC2 클라이언트 생성
        ec2 = boto3.client('ec2')
        
        # 이벤트에서 액션 확인 (start/stop)
        action = event.get('action', 'start')
        
        # Makenaide EC2 인스턴스 태그로 찾기
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': ['makenaide-ec2']},
                {'Name': 'instance-state-name', 'Values': ['running', 'stopped']}
            ]
        )
        
        if not response['Reservations']:
            logger.error("Makenaide EC2 인스턴스를 찾을 수 없습니다.")
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Makenaide EC2 인스턴스를 찾을 수 없습니다.'
                })
            }
        
        # 인스턴스 ID 추출
        instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
        current_state = response['Reservations'][0]['Instances'][0]['State']['Name']
        
        logger.info(f"인스턴스 ID: {instance_id}, 현재 상태: {current_state}")
        
        if action == 'start':
            if current_state == 'running':
                logger.info("인스턴스가 이미 실행 중입니다.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'already_running',
                        'instance_id': instance_id,
                        'message': '인스턴스가 이미 실행 중입니다.'
                    })
                }
            
            # 인스턴스 시작
            ec2.start_instances(InstanceIds=[instance_id])
            logger.info(f"인스턴스 {instance_id} 시작 명령 전송")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'starting',
                    'instance_id': instance_id,
                    'message': '인스턴스 시작 명령이 전송되었습니다.'
                })
            }
            
        elif action == 'stop':
            if current_state == 'stopped':
                logger.info("인스턴스가 이미 중지되어 있습니다.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'already_stopped',
                        'instance_id': instance_id,
                        'message': '인스턴스가 이미 중지되어 있습니다.'
                    })
                }
            
            # 인스턴스 중지
            ec2.stop_instances(InstanceIds=[instance_id])
            logger.info(f"인스턴스 {instance_id} 중지 명령 전송")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'stopping',
                    'instance_id': instance_id,
                    'message': '인스턴스 중지 명령이 전송되었습니다.'
                })
            }
        
        else:
            logger.error(f"지원하지 않는 액션: {action}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'error',
                    'message': f'지원하지 않는 액션: {action}'
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
echo "📦 Lambda 패키지 생성 중..."
zip -r lambda_function.zip lambda_function.py

# Lambda 함수 생성
echo "🚀 Lambda 함수 생성 중..."
LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/makenaide-lambda-role"

aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --runtime python3.9 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://lambda_function.zip \
    --description "Makenaide EC2 자동 시작/중지 함수" \
    --timeout 60 \
    --memory-size 128

echo "✅ Lambda 함수 생성 완료: $FUNCTION_NAME"

# Lambda 함수 ARN 저장
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
echo "Lambda 함수 ARN: $LAMBDA_ARN"

# Lambda 설정 정보 저장
cd ..
cat > aws_lambda_config.json << EOF
{
  "lambda_function_name": "$FUNCTION_NAME",
  "lambda_function_arn": "$LAMBDA_ARN",
  "lambda_role_arn": "$LAMBDA_ROLE_ARN",
  "runtime": "python3.9",
  "handler": "lambda_function.lambda_handler",
  "timeout": 60,
  "memory_size": 128,
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
}
EOF

echo "💾 Lambda 설정 정보 저장 완료: aws_lambda_config.json"

# 임시 파일 정리
rm -rf lambda_package

echo ""
echo "🎉 Lambda 함수 생성 완료!"
echo "=========================="
echo "함수 이름: $FUNCTION_NAME"
echo "함수 ARN: $LAMBDA_ARN"
echo "역할 ARN: $LAMBDA_ROLE_ARN"
echo "==========================" 