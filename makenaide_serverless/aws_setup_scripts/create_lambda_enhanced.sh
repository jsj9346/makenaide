#!/bin/bash
# create_lambda_enhanced.sh - EIP 자동 관리 기능이 포함된 Lambda 함수 생성

set -e

echo "🔧 개선된 Lambda 함수 생성 시작 (EIP 자동 관리 포함)"
echo "=================================================="

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS 계정 ID 및 리전 가져오기
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

# Lambda 함수 이름
FUNCTION_NAME="makenaide-controller"

# Lambda 함수 코드 생성
echo "📝 개선된 Lambda 함수 코드 생성 중..."
mkdir -p lambda_package_enhanced
cd lambda_package_enhanced

cat > lambda_function.py << 'EOF'
import json
import boto3
import logging
import time
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def ensure_elastic_ip_connected(ec2_client, instance_id):
    """
    EC2 인스턴스에 Elastic IP가 연결되어 있는지 확인하고 필요시 연결
    """
    try:
        # Makenaide EIP 조회
        eip_response = ec2_client.describe_addresses(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': ['makenaide-eip']
                }
            ]
        )
        
        if not eip_response['Addresses']:
            logger.warning("Makenaide EIP를 찾을 수 없습니다. 새로 할당합니다.")
            
            # 새 EIP 할당
            alloc_response = ec2_client.allocate_address(Domain='vpc')
            allocation_id = alloc_response['AllocationId']
            public_ip = alloc_response['PublicIp']
            
            # 태그 추가
            ec2_client.create_tags(
                Resources=[allocation_id],
                Tags=[
                    {'Key': 'Name', 'Value': 'makenaide-eip'},
                    {'Key': 'Project', 'Value': 'makenaide'},
                    {'Key': 'Purpose', 'Value': 'trading-bot-static-ip'}
                ]
            )
            
            logger.info(f"새 EIP 할당 완료: {public_ip} (ID: {allocation_id})")
        else:
            eip = eip_response['Addresses'][0]
            allocation_id = eip['AllocationId']
            public_ip = eip['PublicIp']
            connected_instance = eip.get('InstanceId')
            
            logger.info(f"기존 EIP 발견: {public_ip}")
            
            # 이미 현재 인스턴스에 연결되어 있는지 확인
            if connected_instance == instance_id:
                logger.info(f"EIP가 이미 올바르게 연결되어 있습니다: {public_ip}")
                return public_ip
            
            # 다른 인스턴스에 연결되어 있으면 연결 해제
            if connected_instance:
                logger.info(f"EIP가 다른 인스턴스 ({connected_instance})에 연결되어 있어 해제합니다")
                if eip.get('AssociationId'):
                    ec2_client.disassociate_address(AssociationId=eip['AssociationId'])
                    time.sleep(2)  # 연결 해제 대기
        
        # EIP를 현재 인스턴스에 연결
        logger.info(f"EIP {public_ip}를 인스턴스 {instance_id}에 연결 중...")
        
        response = ec2_client.associate_address(
            InstanceId=instance_id,
            AllocationId=allocation_id
        )
        
        logger.info(f"EIP 연결 완료: {public_ip} -> {instance_id}")
        return public_ip
        
    except Exception as e:
        logger.error(f"EIP 연결 처리 중 오류: {str(e)}")
        return None

def wait_for_instance_running(ec2_client, instance_id, max_wait_time=300):
    """
    인스턴스가 running 상태가 될 때까지 대기
    """
    logger.info(f"인스턴스 {instance_id}가 running 상태가 될 때까지 대기...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        try:
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            
            logger.info(f"현재 인스턴스 상태: {state}")
            
            if state == 'running':
                logger.info("인스턴스가 성공적으로 시작되었습니다")
                return True
            elif state in ['stopping', 'stopped', 'terminated']:
                logger.error(f"인스턴스가 예상치 못한 상태입니다: {state}")
                return False
            
            time.sleep(10)  # 10초 대기
            
        except Exception as e:
            logger.error(f"인스턴스 상태 확인 중 오류: {str(e)}")
            return False
    
    logger.error(f"인스턴스가 {max_wait_time}초 내에 시작되지 않았습니다")
    return False

def lambda_handler(event, context):
    """
    Makenaide EC2 인스턴스 자동 시작/중지 Lambda 함수 (EIP 자동 관리 포함)
    """
    try:
        logger.info(f"Lambda 함수 시작 - 이벤트: {json.dumps(event)}")
        
        # EC2 클라이언트 생성
        ec2 = boto3.client('ec2')
        
        # 이벤트에서 액션 확인 (start/stop)
        action = event.get('action', 'start')
        
        # Makenaide EC2 인스턴스 태그로 찾기
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': ['makenaide-ec2']},
                {'Name': 'instance-state-name', 'Values': ['running', 'stopped', 'pending', 'stopping']}
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
        
        # 인스턴스 정보 추출
        instance = response['Reservations'][0]['Instances'][0]
        instance_id = instance['InstanceId']
        current_state = instance['State']['Name']
        
        logger.info(f"인스턴스 ID: {instance_id}, 현재 상태: {current_state}")
        
        if action == 'start':
            if current_state == 'running':
                logger.info("인스턴스가 이미 실행 중입니다. EIP 연결 상태를 확인합니다.")
                
                # 실행 중이어도 EIP 연결 상태 확인
                eip_address = ensure_elastic_ip_connected(ec2, instance_id)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'already_running',
                        'instance_id': instance_id,
                        'eip_address': eip_address,
                        'message': '인스턴스가 이미 실행 중입니다. EIP 연결 상태를 확인했습니다.'
                    })
                }
            
            elif current_state in ['pending', 'stopping']:
                logger.info(f"인스턴스가 전환 중입니다 ({current_state}). 잠시 대기 후 재시도하세요.")
                return {
                    'statusCode': 202,
                    'body': json.dumps({
                        'status': 'transitioning',
                        'instance_id': instance_id,
                        'current_state': current_state,
                        'message': f'인스턴스가 {current_state} 상태입니다. 잠시 후 재시도하세요.'
                    })
                }
            
            # 인스턴스 시작
            logger.info(f"인스턴스 {instance_id} 시작 명령 전송")
            ec2.start_instances(InstanceIds=[instance_id])
            
            # 인스턴스가 시작될 때까지 대기
            if wait_for_instance_running(ec2, instance_id):
                # EIP 연결
                eip_address = ensure_elastic_ip_connected(ec2, instance_id)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'started_successfully',
                        'instance_id': instance_id,
                        'eip_address': eip_address,
                        'message': '인스턴스가 성공적으로 시작되었고 EIP가 연결되었습니다.'
                    })
                }
            else:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'status': 'start_failed',
                        'instance_id': instance_id,
                        'message': '인스턴스 시작에 실패했습니다.'
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
            
            elif current_state in ['pending', 'stopping']:
                logger.info(f"인스턴스가 전환 중입니다 ({current_state}).")
                return {
                    'statusCode': 202,
                    'body': json.dumps({
                        'status': 'transitioning',
                        'instance_id': instance_id,
                        'current_state': current_state,
                        'message': f'인스턴스가 {current_state} 상태입니다.'
                    })
                }
            
            # 인스턴스 중지
            logger.info(f"인스턴스 {instance_id} 중지 명령 전송")
            ec2.stop_instances(InstanceIds=[instance_id])
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'stopping',
                    'instance_id': instance_id,
                    'message': '인스턴스 중지 명령이 전송되었습니다.'
                })
            }
        
        elif action == 'check_eip':
            # EIP 상태만 확인하는 액션
            eip_address = ensure_elastic_ip_connected(ec2, instance_id)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'eip_checked',
                    'instance_id': instance_id,
                    'eip_address': eip_address,
                    'current_state': current_state,
                    'message': 'EIP 연결 상태를 확인했습니다.'
                })
            }
        
        else:
            logger.error(f"지원하지 않는 액션: {action}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'error',
                    'message': f'지원하지 않는 액션: {action} (지원: start, stop, check_eip)'
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
zip -r lambda_function_enhanced.zip lambda_function.py

# 기존 Lambda 함수가 있는지 확인
echo "🔍 기존 Lambda 함수 확인 중..."
if aws lambda get-function --function-name $FUNCTION_NAME >/dev/null 2>&1; then
    echo "📝 기존 Lambda 함수 업데이트 중..."
    
    # 함수 코드 업데이트
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://lambda_function_enhanced.zip
    
    # 함수 설정 업데이트 (타임아웃 및 메모리 증가)
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --timeout 300 \
        --memory-size 256 \
        --description "Makenaide EC2 자동 시작/중지 함수 (EIP 자동 관리 포함)"
    
    echo "✅ Lambda 함수 업데이트 완료: $FUNCTION_NAME"
else
    echo "🚀 새 Lambda 함수 생성 중..."
    LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/makenaide-lambda-role"
    
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.9 \
        --role $LAMBDA_ROLE_ARN \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://lambda_function_enhanced.zip \
        --description "Makenaide EC2 자동 시작/중지 함수 (EIP 자동 관리 포함)" \
        --timeout 300 \
        --memory-size 256
    
    echo "✅ Lambda 함수 생성 완료: $FUNCTION_NAME"
fi

# Lambda 함수 ARN 저장
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
echo "Lambda 함수 ARN: $LAMBDA_ARN"

# Lambda 설정 정보 저장
cd ..
cat > aws_lambda_enhanced_config.json << EOF
{
  "lambda_function_name": "$FUNCTION_NAME",
  "lambda_function_arn": "$LAMBDA_ARN",
  "lambda_role_arn": "arn:aws:iam::${ACCOUNT_ID}:role/makenaide-lambda-role",
  "runtime": "python3.9",
  "handler": "lambda_function.lambda_handler",
  "timeout": 300,
  "memory_size": 256,
  "features": [
    "ec2_start_stop",
    "elastic_ip_auto_management",
    "instance_state_monitoring"
  ],
  "supported_actions": [
    "start",
    "stop", 
    "check_eip"
  ],
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
}
EOF

echo "💾 Lambda 설정 정보 저장 완료: aws_lambda_enhanced_config.json"

# 임시 파일 정리
rm -rf lambda_package_enhanced

echo ""
echo "🎉 개선된 Lambda 함수 배포 완료!"
echo "=================================="
echo "함수 이름: $FUNCTION_NAME"
echo "함수 ARN: $LAMBDA_ARN"
echo "새로운 기능:"
echo "  - 📍 Elastic IP 자동 할당 및 관리"
echo "  - 🔄 인스턴스 시작 시 EIP 자동 연결"
echo "  - 📊 EIP 연결 상태 모니터링"
echo "  - ⏰ 확장된 타임아웃 (5분)"
echo ""
echo "🔧 테스트 방법:"
echo "aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"action\":\"start\"}' response.json"
echo "aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"action\":\"check_eip\"}' response.json"
echo "==================================" 