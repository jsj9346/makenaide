#!/usr/bin/env python3
"""
EC2 자동 시작/종료 Lambda 함수 생성 (환경변수 수정)
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

# 상수 설정
TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'
REGION = 'ap-northeast-2'

def lambda_handler(event, context):
    """
    거래 신호 수신 시 EC2 인스턴스 시작/종료 제어
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    ec2 = boto3.client('ec2', region_name=REGION)
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    
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
            
            # 3. SNS 알림 발송
            send_notification(
                'system',
                f'거래 신호 수신 - EC2 인스턴스 시작: {action} {len(tickers)}개 종목',
                f'EC2 자동 시작: {action.upper()} 거래',
                {
                    'signal_id': signal_id,
                    'action': action,
                    'tickers': tickers,
                    'instance_id': TRADING_INSTANCE_ID
                }
            )
            
            # 4. 성공 응답
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
                
                send_notification(
                    'system',
                    f'거래 EC2 인스턴스가 시작되었습니다: {instance_id}',
                    'EC2 시작 완료',
                    {'instance_id': instance_id, 'state': state}
                )
            
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
        
        # 오류 알림
        send_notification(
            'system',
            f'EC2 컨트롤러 오류: {str(e)}',
            'EC2 컨트롤러 시스템 오류',
            {'error': str(e), 'event': event}
        )
        
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
            
            logger.info(f"EC2 instance {instance_id} start initiated")
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

def send_notification(notification_type, message, subject, details=None):
    """SNS 알림 발송"""
    try:
        lambda_client = boto3.client('lambda', region_name=REGION)
        
        payload = {
            'type': notification_type,
            'message': message,
            'subject': subject,
            'details': details or {},
            'source': 'ec2-controller',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        lambda_client.invoke(
            FunctionName='makenaide-notification-handler',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        
        logger.info(f"Notification sent: {subject}")
        
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")

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
        
        # 새 함수 생성 (환경변수 제거)
        response = lambda_client.create_function(
            FunctionName='makenaide-ec2-controller',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': function_code.encode('utf-8')},
            Description='Makenaide EC2 trading instance controller',
            Timeout=300,  # 5분
            MemorySize=256,
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
        # 1. 거래 신호 규칙에 EC2 컨트롤러 연결
        events_client.put_targets(
            Rule='makenaide-trading-signal',
            Targets=[
                {
                    'Id': '2',  # 기존 Phase 6와 다른 ID 사용
                    'Arn': function_arn
                }
            ]
        )
        
        print("✅ Connected trading signal rule to EC2 controller")
        
        # 2. Lambda 호출 권한 추가
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

TRADING_INSTANCE_ID = 'i-09faf163434bd5d00'
REGION = 'ap-northeast-2'

def lambda_handler(event, context):
    """
    거래 완료 후 EC2 인스턴스 자동 종료
    """
    ec2 = boto3.client('ec2', region_name=REGION)
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    
    try:
        # EC2 인스턴스 상태 확인
        response = ec2.describe_instances(InstanceIds=[TRADING_INSTANCE_ID])
        instance = response['Reservations'][0]['Instances'][0]
        current_state = instance['State']['Name']
        
        logger.info(f"Current EC2 state: {current_state}")
        
        if current_state != 'running':
            logger.info(f"EC2 instance is not running ({current_state}) - no action needed")
            return create_success_response("EC2 not running")
        
        # 거래 완료 확인
        trading_active, pending_count = check_trading_status(dynamodb)
        
        if trading_active:
            logger.info(f"Trading still active ({pending_count} pending) - keeping EC2 running")
            return create_success_response(f"EC2 kept running - {pending_count} pending trades")
        else:
            logger.info("No active trading detected - shutting down EC2 instance")
            
            # EC2 인스턴스 종료
            ec2.stop_instances(InstanceIds=[TRADING_INSTANCE_ID])
            logger.info(f"EC2 instance {TRADING_INSTANCE_ID} shutdown initiated")
            
            # 종료 알림
            send_shutdown_notification(
                f'거래 완료 - EC2 인스턴스 자동 종료: {TRADING_INSTANCE_ID}',
                'EC2 자동 종료 완료',
                {'instance_id': TRADING_INSTANCE_ID, 'reason': 'trading_completed'}
            )
            
            return create_success_response("EC2 shutdown initiated")
            
    except Exception as e:
        logger.error(f"Error in shutdown function: {str(e)}")
        return create_error_response(str(e))

def check_trading_status(dynamodb_resource):
    """거래 상태 확인"""
    try:
        params_table = dynamodb_resource.Table('makenaide-trading-params')
        
        # 최근 24시간 내 거래 파라미터 확인
        cutoff_time = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        
        response = params_table.scan(
            FilterExpression='#ts >= :cutoff',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={':cutoff': cutoff_time}
        )
        
        items = response['Items']
        pending_count = 0
        
        if not items:
            logger.info("No recent trading parameters found")
            return False, 0
        
        # pending 상태인 항목 수 계산
        for item in items:
            if item.get('status') == 'pending':
                pending_count += 1
        
        # pending이 있으면 거래 활성
        is_active = pending_count > 0
        
        logger.info(f"Trading status check: active={is_active}, pending={pending_count}")
        return is_active, pending_count
        
    except Exception as e:
        logger.error(f"Error checking trading status: {str(e)}")
        return False, 0  # 오류 시 안전하게 비활성으로 처리

def send_shutdown_notification(message, subject, details):
    """종료 알림 발송"""
    try:
        lambda_client = boto3.client('lambda', region_name=REGION)
        
        payload = {
            'type': 'system',
            'message': message,
            'subject': subject,
            'details': details,
            'source': 'ec2-shutdown',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        lambda_client.invoke(
            FunctionName='makenaide-notification-handler',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        
        logger.info(f"Shutdown notification sent")
        
    except Exception as e:
        logger.error(f"Error sending shutdown notification: {str(e)}")

def create_success_response(message):
    """성공 응답 생성"""
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': message,
            'instance_id': TRADING_INSTANCE_ID,
            'timestamp': datetime.utcnow().isoformat()
        })
    }

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
        try:
            events_client.put_rule(
                Name='makenaide-ec2-shutdown-check',
                ScheduleExpression='rate(10 minutes)',  # 10분마다
                Description='Check and shutdown EC2 after trading completion',
                State='ENABLED'
            )
            print("✅ Created EC2 shutdown schedule rule")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceConflictException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
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
        print("✅ Connected shutdown rule to function")
        
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
    print(f"   Controller: makenaide-ec2-controller")
    print(f"   Shutdown: makenaide-ec2-shutdown")
    
    print(f"\n🔄 Automated Workflow:")
    print(f"   1. 거래 신호 발생 → EC2 자동 시작")
    print(f"   2. 거래 파라미터 DynamoDB 저장")
    print(f"   3. EC2에서 실제 거래 실행")
    print(f"   4. 10분마다 거래 완료 체크")
    print(f"   5. 거래 완료 시 EC2 자동 종료")
    
    print(f"\n💰 Cost Impact:")
    print(f"   - EC2 운영시간: 평균 5-10분/일")
    print(f"   - 월 EC2 비용: ~$3")
    print(f"   - Lambda 비용: ~$0.5")
    print(f"   - 전체 시스템: $48/월 (87% 절약)")
    
    return True

if __name__ == "__main__":
    main()