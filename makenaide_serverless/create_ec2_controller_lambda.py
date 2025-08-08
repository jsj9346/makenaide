#!/usr/bin/env python3
"""
EC2 자동 시작/종료 Lambda 함수 생성
거래 신호 수신 시 EC2 인스턴스 제어
"""

import boto3
import json
from botocore.exceptions import ClientError

def create_ec2_controller_lambda():
    """
    EC2 제어 Lambda 함수 생성
    """
    print("🔄 Creating EC2 controller Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # Lambda 함수 코드
    function_code = '''
import boto3
import json
import logging
import time
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    거래 신호 수신 시 EC2 인스턴스 시작/종료 제어
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
    
    # 거래 EC2 인스턴스 ID
    TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'  # 실제 인스턴스 ID
    
    try:
        # EventBridge 이벤트 파싱
        detail = event.get('detail', {})
        source = event.get('source', '')
        detail_type = event.get('detail-type', '')
        
        if source == 'makenaide' and detail_type == 'Trading Signal':
            # 거래 신호 수신 -> EC2 시작
            action = detail.get('action')
            tickers = detail.get('tickers', [])
            signal_strength = detail.get('signal_strength', 'medium')
            signal_id = f"signal_{int(datetime.utcnow().timestamp())}"
            
            logger.info(f"Processing trading signal: {action} for {len(tickers)} tickers")
            
            # 1. EC2 인스턴스 시작
            ec2_response = start_ec2_instance(ec2, TRADING_INSTANCE_ID)
            if not ec2_response:
                return create_error_response("Failed to start EC2 instance")
            
            # 2. 거래 파라미터를 DynamoDB에 저장
            save_result = save_trading_parameters(dynamodb, signal_id, {
                'action': action,
                'tickers': tickers,
                'signal_strength': signal_strength,
                'timestamp': datetime.utcnow().isoformat(),
                'ec2_instance_id': TRADING_INSTANCE_ID,
                'status': 'pending'
            })
            
            if not save_result:
                logger.error("Failed to save trading parameters")
            
            # 3. 성공 응답
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'EC2 instance started for trading',
                    'signal_id': signal_id,
                    'instance_id': TRADING_INSTANCE_ID,
                    'action': action,
                    'tickers': tickers,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }
            
        elif detail_type == 'EC2 Instance State-change Notification':
            # EC2 상태 변경 이벤트 처리
            instance_id = detail.get('instance-id')
            state = detail.get('state')
            
            logger.info(f"EC2 state change: {instance_id} -> {state}")
            
            if instance_id == TRADING_INSTANCE_ID and state == 'running':
                logger.info("Trading EC2 instance is now running - ready for trading")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'EC2 state processed: {state}',
                    'instance_id': instance_id
                })
            }
        
        else:
            # 기타 이벤트
            logger.info(f"Unhandled event type: {detail_type}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Event acknowledged but not processed'})
            }
            
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return create_error_response(str(e))

def start_ec2_instance(ec2_client, instance_id):
    """EC2 인스턴스 시작"""
    try:
        # 인스턴스 상태 확인
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance = response['Reservations'][0]['Instances'][0]
        current_state = instance['State']['Name']
        
        logger.info(f"Current EC2 state: {current_state}")
        
        if current_state == 'running':
            logger.info("EC2 instance is already running")
            return True
        elif current_state in ['stopped', 'stopping']:
            # 인스턴스 시작
            logger.info(f"Starting EC2 instance: {instance_id}")
            ec2_client.start_instances(InstanceIds=[instance_id])
            
            # 시작 완료까지 대기 (최대 5분)
            waiter = ec2_client.get_waiter('instance_running')
            waiter.wait(
                InstanceIds=[instance_id],
                WaiterConfig={
                    'Delay': 15,      # 15초마다 확인
                    'MaxAttempts': 20  # 최대 20번 시도 (5분)
                }
            )
            
            logger.info(f"EC2 instance {instance_id} is now running")
            return True
        else:
            logger.warning(f"Cannot start instance in state: {current_state}")
            return False
            
    except Exception as e:
        logger.error(f"Error starting EC2 instance: {str(e)}")
        return False

def save_trading_parameters(dynamodb_resource, signal_id, params):
    """거래 파라미터를 DynamoDB에 저장"""
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
        logger.error(f"Error saving trading parameters: {str(e)}")
        return False

def create_error_response(error_message):
    """에러 응답 생성"""
    return {
        'statusCode': 500,
        'body': json.dumps({
            'error': error_message,
            'timestamp': datetime.utcnow().isoformat()
        })
    }
'''
    
    try:
        # 기존 함수 삭제
        try:
            lambda_client.delete_function(FunctionName='makenaide-ec2-controller')
            print("   Deleted existing EC2 controller function")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-ec2-controller',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': function_code.encode('utf-8')},
            Description='Makenaide EC2 trading instance controller',
            Timeout=300,  # 5분
            MemorySize=256,
            Environment={
                'Variables': {
                    'AWS_DEFAULT_REGION': 'ap-northeast-2',
                    'TRADING_INSTANCE_ID': 'i-09faf163434bd5d00'
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Purpose': 'EC2-Controller'
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created EC2 controller function: {function_arn}")
        
        return function_arn
        
    except ClientError as e:
        print(f"❌ Error creating EC2 controller function: {e.response['Error']['Message']}")
        return None

def update_eventbridge_rules():
    """
    EventBridge 규칙을 EC2 컨트롤러에 연결
    """
    print("🔄 Updating EventBridge rules for EC2 controller...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    function_arn = "arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-ec2-controller"
    
    try:
        # 거래 신호 규칙에 EC2 컨트롤러 연결
        events_client.put_targets(
            Rule='makenaide-trading-signal',
            Targets=[
                {
                    'Id': '1',
                    'Arn': function_arn
                }
            ]
        )
        
        print("✅ Connected trading signal rule to EC2 controller")
        
        # Lambda 호출 권한 추가
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-ec2-controller',
                StatementId='eventbridge-invoke-ec2-controller',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-trading-signal'
            )
            print("✅ Added EventBridge permission to EC2 controller")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print("ℹ️  Permission already exists for EC2 controller")
        
        return True
        
    except ClientError as e:
        print(f"❌ Error updating EventBridge rules: {e.response['Error']['Message']}")
        return False

def create_ec2_shutdown_function():
    """
    거래 완료 후 EC2 자동 종료 함수 생성
    """
    print("🔄 Creating EC2 shutdown function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # 종료 함수 코드
    shutdown_code = '''
import boto3
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    거래 완료 후 EC2 인스턴스 자동 종료
    """
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
    
    TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'
    
    try:
        # 거래 완료 확인
        trading_completed = check_trading_completion(dynamodb)
        
        if trading_completed:
            logger.info("Trading completed - shutting down EC2 instance")
            
            # EC2 인스턴스 종료
            ec2.stop_instances(InstanceIds=[TRADING_INSTANCE_ID])
            
            logger.info(f"EC2 instance {TRADING_INSTANCE_ID} shutdown initiated")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'EC2 instance shutdown completed',
                    'instance_id': TRADING_INSTANCE_ID,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }
        else:
            logger.info("Trading still in progress - keeping EC2 running")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'EC2 instance kept running'})
            }
            
    except Exception as e:
        logger.error(f"Error in shutdown function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def check_trading_completion(dynamodb_resource):
    """거래 완료 여부 확인"""
    try:
        # 최근 거래 파라미터 확인
        params_table = dynamodb_resource.Table('makenaide-trading-params')
        
        # 오늘 날짜의 거래 파라미터 조회
        today = datetime.utcnow().date().isoformat()
        
        response = params_table.scan(
            FilterExpression='#ts = :today AND attribute_exists(#status)',
            ExpressionAttributeNames={
                '#ts': 'timestamp',
                '#status': 'status'
            },
            ExpressionAttributeValues={':today': today}
        )
        
        items = response['Items']
        
        if not items:
            return True  # 거래 파라미터가 없으면 종료
        
        # 모든 거래가 완료되었는지 확인
        for item in items:
            if item.get('status') == 'pending':
                return False  # 대기 중인 거래가 있으면 계속 실행
        
        return True  # 모든 거래 완료
        
    except Exception as e:
        logger.error(f"Error checking trading completion: {str(e)}")
        return True  # 오류 시 안전하게 종료
'''
    
    try:
        # 기존 함수 삭제
        try:
            lambda_client.delete_function(FunctionName='makenaide-ec2-shutdown')
            print("   Deleted existing EC2 shutdown function")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-ec2-shutdown',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': shutdown_code.encode('utf-8')},
            Description='Makenaide EC2 trading instance automatic shutdown',
            Timeout=180,
            MemorySize=128,
            Tags={
                'Project': 'Makenaide',
                'Purpose': 'EC2-Shutdown'
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created EC2 shutdown function: {function_arn}")
        
        # CloudWatch Events로 10분마다 실행하도록 설정
        events_client = boto3.client('events', region_name='ap-northeast-2')
        
        # 규칙 생성
        events_client.put_rule(
            Name='makenaide-ec2-shutdown-check',
            ScheduleExpression='rate(10 minutes)',  # 10분마다
            Description='Check and shutdown EC2 after trading completion',
            State='ENABLED'
        )
        
        # 타겟 설정
        events_client.put_targets(
            Rule='makenaide-ec2-shutdown-check',
            Targets=[
                {
                    'Id': '1',
                    'Arn': function_arn
                }
            ]
        )
        
        # 권한 추가
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-ec2-shutdown',
                StatementId='eventbridge-invoke-shutdown',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-ec2-shutdown-check'
            )
            print("✅ Added EventBridge permission to shutdown function")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print("ℹ️  Permission already exists for shutdown function")
        
        return function_arn
        
    except ClientError as e:
        print(f"❌ Error creating EC2 shutdown function: {e.response['Error']['Message']}")
        return None

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide EC2 Controller Setup")
    print("=" * 50)
    
    # 1. EC2 컨트롤러 함수 생성
    controller_arn = create_ec2_controller_lambda()
    if not controller_arn:
        print("❌ Failed to create EC2 controller")
        return False
    
    # 2. EventBridge 규칙 업데이트
    if not update_eventbridge_rules():
        print("❌ Failed to update EventBridge rules")
        return False
    
    # 3. EC2 자동 종료 함수 생성
    shutdown_arn = create_ec2_shutdown_function()
    if not shutdown_arn:
        print("❌ Failed to create shutdown function")
        return False
    
    print(f"\n🎉 EC2 controller setup completed!")
    print(f"\n📋 Created Functions:")
    print(f"   Controller: {controller_arn}")
    print(f"   Shutdown: {shutdown_arn}")
    
    print(f"\n🔄 Workflow:")
    print(f"   1. Trading signal → EC2 starts automatically")
    print(f"   2. Trading parameters saved to DynamoDB")
    print(f"   3. EC2 executes trading logic")
    print(f"   4. EC2 shuts down after completion (10min check)")
    
    print(f"\n💰 Cost Optimization:")
    print(f"   - EC2 runs only when trading (avg 5-10 min)")
    print(f"   - Monthly EC2 cost: ~$3")
    print(f"   - Lambda functions: ~$0.5")
    print(f"   - Total additional cost: ~$3.5/month")
    
    return True

if __name__ == "__main__":
    main()