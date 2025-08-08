#!/usr/bin/env python3
"""
ZIP 파일을 올바르게 생성하여 EC2 컨트롤러 Lambda 함수 생성
"""

import boto3
import json
import zipfile
import os
import tempfile
from botocore.exceptions import ClientError

def create_ec2_controller_with_zip():
    """
    올바른 ZIP 파일로 EC2 컨트롤러 생성
    """
    print("🔄 Creating EC2 controller with proper ZIP...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # Lambda 함수 코드
    function_code = '''import boto3
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'
REGION = 'ap-northeast-2'

def lambda_handler(event, context):
    """거래 신호 수신 시 EC2 인스턴스 제어"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    ec2 = boto3.client('ec2', region_name=REGION)
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    
    try:
        detail = event.get('detail', {})
        source = event.get('source', '')
        detail_type = event.get('detail-type', '')
        
        if source == 'makenaide' and detail_type == 'Trading Signal':
            action = detail.get('action')
            tickers = detail.get('tickers', [])
            signal_id = f"signal_{int(datetime.utcnow().timestamp())}"
            
            logger.info(f"Processing trading signal: {action} for {len(tickers)} tickers")
            
            # EC2 시작
            start_result = start_ec2_instance(ec2, TRADING_INSTANCE_ID)
            if not start_result:
                return {'statusCode': 500, 'body': json.dumps('Failed to start EC2')}
            
            # 거래 파라미터 저장
            save_trading_parameters(dynamodb, signal_id, {
                'action': action,
                'tickers': tickers,
                'signal_strength': detail.get('signal_strength', 'medium'),
                'timestamp': datetime.utcnow().isoformat(),
                'ec2_instance_id': TRADING_INSTANCE_ID,
                'status': 'pending'
            })
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'EC2 started for trading',
                    'signal_id': signal_id,
                    'action': action,
                    'tickers': tickers
                })
            }
        
        return {'statusCode': 200, 'body': json.dumps('Event processed')}
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {str(e)}')}

def start_ec2_instance(ec2_client, instance_id):
    """EC2 인스턴스 시작"""
    try:
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance = response['Reservations'][0]['Instances'][0]
        current_state = instance['State']['Name']
        
        if current_state == 'running':
            logger.info("EC2 already running")
            return True
        elif current_state in ['stopped', 'stopping']:
            ec2_client.start_instances(InstanceIds=[instance_id])
            logger.info(f"Started EC2 instance: {instance_id}")
            return True
        else:
            logger.warning(f"Cannot start instance in state: {current_state}")
            return False
    except Exception as e:
        logger.error(f"Error starting EC2: {str(e)}")
        return False

def save_trading_parameters(dynamodb_resource, signal_id, params):
    """거래 파라미터 저장"""
    try:
        table = dynamodb_resource.Table('makenaide-trading-params')
        item = {
            'signal_id': signal_id,
            'timestamp': datetime.utcnow().date().isoformat(),
            **params
        }
        table.put_item(Item=item)
        logger.info(f"Trading parameters saved: {signal_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving parameters: {str(e)}")
        return False
'''
    
    # 임시 디렉토리에서 ZIP 파일 생성
    with tempfile.TemporaryDirectory() as temp_dir:
        # Python 파일 생성
        py_file = os.path.join(temp_dir, 'lambda_function.py')
        with open(py_file, 'w', encoding='utf-8') as f:
            f.write(function_code)
        
        # ZIP 파일 생성
        zip_file = os.path.join(temp_dir, 'function.zip')
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(py_file, 'lambda_function.py')
        
        # ZIP 파일 읽기
        with open(zip_file, 'rb') as f:
            zip_content = f.read()
        
        print(f"✅ ZIP file created: {len(zip_content)} bytes")
        
        try:
            # 기존 함수 삭제
            try:
                lambda_client.delete_function(FunctionName='makenaide-ec2-controller')
                print("   Deleted existing function")
            except ClientError:
                pass
            
            # 함수 생성
            response = lambda_client.create_function(
                FunctionName='makenaide-ec2-controller',
                Runtime='python3.9',
                Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Description='Makenaide EC2 controller',
                Timeout=300,
                MemorySize=256
            )
            
            function_arn = response['FunctionArn']
            print(f"✅ Created EC2 controller: {function_arn}")
            return function_arn
            
        except ClientError as e:
            print(f"❌ Error creating function: {e.response['Error']['Message']}")
            return None

def create_ec2_shutdown_with_zip():
    """
    EC2 자동 종료 함수 생성
    """
    print("🔄 Creating EC2 shutdown function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    shutdown_code = '''import boto3
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'
REGION = 'ap-northeast-2'

def lambda_handler(event, context):
    """거래 완료 후 EC2 자동 종료"""
    ec2 = boto3.client('ec2', region_name=REGION)
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    
    try:
        # EC2 상태 확인
        response = ec2.describe_instances(InstanceIds=[TRADING_INSTANCE_ID])
        instance = response['Reservations'][0]['Instances'][0]
        current_state = instance['State']['Name']
        
        if current_state != 'running':
            logger.info(f"EC2 not running ({current_state})")
            return {'statusCode': 200, 'body': json.dumps('EC2 not running')}
        
        # 거래 활성 상태 확인
        is_active = check_trading_active(dynamodb)
        
        if is_active:
            logger.info("Trading still active - keeping EC2 running")
            return {'statusCode': 200, 'body': json.dumps('Trading active')}
        else:
            logger.info("No active trading - shutting down EC2")
            ec2.stop_instances(InstanceIds=[TRADING_INSTANCE_ID])
            return {'statusCode': 200, 'body': json.dumps('EC2 shutdown initiated')}
            
    except Exception as e:
        logger.error(f"Shutdown error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {str(e)}')}

def check_trading_active(dynamodb_resource):
    """거래 활성 상태 확인"""
    try:
        table = dynamodb_resource.Table('makenaide-trading-params')
        cutoff_time = (datetime.utcnow() - timedelta(hours=2)).isoformat()
        
        response = table.scan(
            FilterExpression='#ts >= :cutoff AND #status = :status',
            ExpressionAttributeNames={
                '#ts': 'timestamp', 
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':cutoff': cutoff_time,
                ':status': 'pending'
            }
        )
        
        pending_count = len(response['Items'])
        logger.info(f"Pending trades: {pending_count}")
        
        return pending_count > 0
        
    except Exception as e:
        logger.error(f"Error checking trading status: {str(e)}")
        return False  # 안전하게 비활성으로 처리
'''
    
    # 임시 디렉토리에서 ZIP 파일 생성
    with tempfile.TemporaryDirectory() as temp_dir:
        py_file = os.path.join(temp_dir, 'lambda_function.py')
        with open(py_file, 'w', encoding='utf-8') as f:
            f.write(shutdown_code)
        
        zip_file = os.path.join(temp_dir, 'function.zip')
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(py_file, 'lambda_function.py')
        
        with open(zip_file, 'rb') as f:
            zip_content = f.read()
        
        try:
            # 기존 함수 삭제
            try:
                lambda_client.delete_function(FunctionName='makenaide-ec2-shutdown')
            except ClientError:
                pass
            
            # 함수 생성
            response = lambda_client.create_function(
                FunctionName='makenaide-ec2-shutdown',
                Runtime='python3.9',
                Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Description='Makenaide EC2 auto shutdown',
                Timeout=180,
                MemorySize=128
            )
            
            function_arn = response['FunctionArn']
            print(f"✅ Created EC2 shutdown function: {function_arn}")
            return function_arn
            
        except ClientError as e:
            print(f"❌ Error creating shutdown function: {e.response['Error']['Message']}")
            return None

def setup_eventbridge_connections():
    """
    EventBridge 규칙 연결 설정
    """
    print("🔄 Setting up EventBridge connections...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    try:
        # 1. EC2 컨트롤러를 거래 신호 규칙에 연결
        events_client.put_targets(
            Rule='makenaide-trading-signal',
            Targets=[
                {
                    'Id': '2',  # Phase 6와 다른 ID
                    'Arn': 'arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-ec2-controller'
                }
            ]
        )
        print("✅ Connected EC2 controller to trading signal")
        
        # 2. EC2 컨트롤러 권한 추가
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-ec2-controller',
                StatementId='eventbridge-ec2-controller',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-trading-signal'
            )
            print("✅ Added EC2 controller permission")
        except ClientError:
            print("ℹ️  EC2 controller permission already exists")
        
        # 3. 자동 종료 스케줄 규칙 생성
        try:
            events_client.put_rule(
                Name='makenaide-ec2-shutdown-check',
                ScheduleExpression='rate(10 minutes)',
                Description='Check and shutdown EC2 after trading completion',
                State='ENABLED'
            )
            print("✅ Created shutdown schedule rule")
        except ClientError:
            print("ℹ️  Shutdown rule already exists")
        
        # 4. 종료 함수를 스케줄에 연결
        events_client.put_targets(
            Rule='makenaide-ec2-shutdown-check',
            Targets=[
                {
                    'Id': '1',
                    'Arn': 'arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-ec2-shutdown'
                }
            ]
        )
        print("✅ Connected shutdown function to schedule")
        
        # 5. 종료 함수 권한 추가
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-ec2-shutdown',
                StatementId='eventbridge-shutdown',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-ec2-shutdown-check'
            )
            print("✅ Added shutdown function permission")
        except ClientError:
            print("ℹ️  Shutdown permission already exists")
        
        return True
        
    except ClientError as e:
        print(f"❌ Error setting up EventBridge: {e.response['Error']['Message']}")
        return False

def main():
    """메인 실행"""
    print("🚀 Makenaide EC2 Controller Setup (ZIP Method)")
    print("=" * 60)
    
    # 1. EC2 컨트롤러 생성
    controller_arn = create_ec2_controller_with_zip()
    if not controller_arn:
        return False
    
    # 2. 자동 종료 함수 생성
    shutdown_arn = create_ec2_shutdown_with_zip()
    if not shutdown_arn:
        return False
    
    # 3. EventBridge 연결
    if not setup_eventbridge_connections():
        return False
    
    print(f"\n🎉 EC2 controller setup completed!")
    
    print(f"\n📋 Functions Created:")
    print(f"   • makenaide-ec2-controller: 거래 신호 수신 → EC2 시작")
    print(f"   • makenaide-ec2-shutdown: 10분마다 체크 → 거래 완료 시 EC2 종료")
    
    print(f"\n🔄 Automated Trading Flow:")
    print(f"   1. Phase 0-5: 서버리스 분석 (비용 효율)")
    print(f"   2. 거래 신호 발생 → EC2 자동 시작")
    print(f"   3. EC2에서 실제 업비트 거래 실행")
    print(f"   4. 거래 완료 → EC2 자동 종료")
    
    print(f"\n💰 Final Cost Structure:")
    print(f"   • 서버리스 분석: $42/월 (Lambda + DynamoDB + RDS 30분)")
    print(f"   • 거래 실행: $3/월 (EC2 평균 5-10분/일)")
    print(f"   • 총 비용: $45/월 (기존 $420 → 90% 절약)")
    
    print(f"\n⚠️  다음 단계:")
    print(f"   1. 업비트 개발자 센터에서 고정 IP 등록: 3.35.129.198")
    print(f"   2. EC2에 업비트 거래 코드 배포")
    print(f"   3. 전체 파이프라인 통합 테스트")
    
    return True

if __name__ == "__main__":
    main()