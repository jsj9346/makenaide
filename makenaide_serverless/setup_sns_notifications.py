#!/usr/bin/env python3
"""
Makenaide SNS 알림 시스템 설정
거래 및 오류 실시간 모니터링 알림
"""

import boto3
import json
from botocore.exceptions import ClientError

def create_sns_topics():
    """
    SNS 토픽들을 생성합니다.
    """
    print("🔄 Creating SNS topics for Makenaide notifications...")
    
    sns_client = boto3.client('sns', region_name='ap-northeast-2')
    
    # SNS 토픽 정의
    topics_config = [
        {
            'name': 'makenaide-trading-alerts',
            'display_name': 'Makenaide Trading Alerts',
            'description': '거래 실행, 수익/손실, 포트폴리오 변화 알림'
        },
        {
            'name': 'makenaide-system-alerts',
            'display_name': 'Makenaide System Alerts', 
            'description': '시스템 오류, Lambda 실패, 파이프라인 이슈 알림'
        },
        {
            'name': 'makenaide-daily-reports',
            'display_name': 'Makenaide Daily Reports',
            'description': '일일 파이프라인 실행 결과 및 성과 리포트'
        }
    ]
    
    created_topics = {}
    
    for topic in topics_config:
        try:
            print(f"\n🔄 Creating SNS topic: {topic['name']}")
            
            # SNS 토픽 생성
            response = sns_client.create_topic(
                Name=topic['name'],
                Attributes={
                    'DisplayName': topic['display_name'],
                    'Description': topic['description']
                },
                Tags=[
                    {'Key': 'Project', 'Value': 'Makenaide'},
                    {'Key': 'Purpose', 'Value': 'Notifications'},
                    {'Key': 'Environment', 'Value': 'Production'}
                ]
            )
            
            topic_arn = response['TopicArn']
            created_topics[topic['name']] = topic_arn
            
            print(f"✅ Created SNS topic: {topic['name']}")
            print(f"   ARN: {topic_arn}")
            print(f"   Description: {topic['description']}")
            
        except ClientError as e:
            print(f"❌ Error creating topic {topic['name']}: {e.response['Error']['Message']}")
    
    return created_topics

def create_lambda_sns_policy():
    """
    Lambda 함수들이 SNS에 메시지를 발송할 수 있도록 IAM 정책을 생성합니다.
    """
    print(f"\n🔄 Creating IAM policy for Lambda SNS access...")
    
    iam = boto3.client('iam', region_name='ap-northeast-2')
    
    # SNS 발송 정책 생성
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "MakenaideSNSPublishAccess",
                "Effect": "Allow",
                "Action": [
                    "sns:Publish",
                    "sns:GetTopicAttributes",
                    "sns:ListTopics"
                ],
                "Resource": [
                    "arn:aws:sns:ap-northeast-2:901361833359:makenaide-*"
                ]
            }
        ]
    }
    
    policy_name = "MakenaideSNSLambdaAccess"
    
    try:
        # 기존 정책 삭제 (있다면)
        try:
            iam.delete_policy(PolicyArn=f"arn:aws:iam::901361833359:policy/{policy_name}")
            print(f"   Deleted existing policy: {policy_name}")
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 정책 생성
        response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description="Makenaide Lambda functions SNS publish access policy"
        )
        
        policy_arn = response['Policy']['Arn']
        print(f"✅ Created IAM policy: {policy_arn}")
        
        # Lambda 실행 역할에 정책 연결
        lambda_roles = ["makenaide-lambda-execution-role", "makenaide-lambda-role"]
        
        for role_name in lambda_roles:
            try:
                iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                print(f"✅ Attached policy to role: {role_name}")
            except ClientError as e:
                print(f"⚠️  Could not attach to {role_name}: {e.response['Error']['Message']}")
        
        return policy_arn
        
    except ClientError as e:
        print(f"❌ Error creating IAM policy: {e.response['Error']['Message']}")
        return None

def create_notification_lambda():
    """
    통합 알림 처리 Lambda 함수를 생성합니다.
    """
    print(f"\n🔄 Creating notification handler Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # 알림 처리 함수 코드
    function_code = '''
import boto3
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS 토픽 ARNs
TOPICS = {
    "trading": "arn:aws:sns:ap-northeast-2:901361833359:makenaide-trading-alerts",
    "system": "arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts", 
    "reports": "arn:aws:sns:ap-northeast-2:901361833359:makenaide-daily-reports"
}

def lambda_handler(event, context):
    """
    다양한 알림을 처리하여 적절한 SNS 토픽으로 전송합니다.
    """
    sns = boto3.client('sns', region_name='ap-northeast-2')
    
    try:
        # 이벤트 타입 확인
        notification_type = event.get('type', 'system')
        message = event.get('message', 'Makenaide 알림')
        subject = event.get('subject', 'Makenaide 알림')
        details = event.get('details', {})
        
        # 메시지 포맷팅
        formatted_message = format_message(notification_type, message, details)
        
        # 적절한 토픽 선택
        topic_arn = TOPICS.get(notification_type, TOPICS['system'])
        
        # SNS로 메시지 발송
        response = sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=formatted_message,
            MessageAttributes={
                'notification_type': {
                    'DataType': 'String',
                    'StringValue': notification_type
                },
                'timestamp': {
                    'DataType': 'String', 
                    'StringValue': datetime.utcnow().isoformat()
                },
                'source': {
                    'DataType': 'String',
                    'StringValue': event.get('source', 'makenaide')
                }
            }
        )
        
        logger.info(f"Notification sent: {notification_type} - {subject}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notification sent successfully',
                'message_id': response['MessageId'],
                'topic': topic_arn
            })
        }
        
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def format_message(notification_type, message, details):
    """메시지 타입별로 포맷팅"""
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    if notification_type == 'trading':
        # 거래 알림 포맷
        ticker = details.get('ticker', 'N/A')
        action = details.get('action', 'N/A')
        amount = details.get('amount', 'N/A')
        price = details.get('price', 'N/A')
        
        formatted = f"""
🚨 거래 알림 - {timestamp}

메시지: {message}

거래 정보:
- 티커: {ticker}
- 액션: {action}
- 수량: {amount}
- 가격: {price}

자세한 내용은 CloudWatch 로그를 확인하세요.
        """
        
    elif notification_type == 'system':
        # 시스템 알림 포맷
        error_type = details.get('error_type', 'N/A')
        function_name = details.get('function_name', 'N/A')
        
        formatted = f"""
⚠️ 시스템 알림 - {timestamp}

메시지: {message}

시스템 정보:
- 오류 유형: {error_type}
- 함수명: {function_name}
- 추가 정보: {json.dumps(details, indent=2)}

즉시 확인이 필요합니다.
        """
        
    elif notification_type == 'reports':
        # 리포트 알림 포맷
        phase = details.get('phase', 'N/A')
        status = details.get('status', 'N/A')
        processed_count = details.get('processed_count', 'N/A')
        
        formatted = f"""
📊 일일 리포트 - {timestamp}

메시지: {message}

실행 정보:
- Phase: {phase}
- 상태: {status}
- 처리 건수: {processed_count}
- 세부 결과: {json.dumps(details, indent=2)}
        """
        
    else:
        # 기본 포맷
        formatted = f"""
📢 Makenaide 알림 - {timestamp}

메시지: {message}

세부 정보:
{json.dumps(details, indent=2)}
        """
    
    return formatted.strip()
'''
    
    try:
        # 기존 함수 삭제 (있다면)
        try:
            lambda_client.delete_function(FunctionName='makenaide-notification-handler')
            print("   Deleted existing notification handler function")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-notification-handler',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': function_code.encode('utf-8')},
            Description='Makenaide unified notification handler',
            Timeout=60,
            MemorySize=128,
            Tags={
                'Project': 'Makenaide',
                'Purpose': 'Notifications'
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created notification handler: {function_arn}")
        
        return function_arn
        
    except ClientError as e:
        print(f"❌ Error creating notification handler: {e.response['Error']['Message']}")
        return None

def create_usage_examples():
    """
    SNS 알림 사용 예제 코드를 생성합니다.
    """
    example_code = '''
# Lambda 함수에서 SNS 알림 발송 예제

import boto3
import json
from datetime import datetime

def send_notification(notification_type, message, subject, details=None):
    """
    통합 알림 함수를 호출하여 SNS 알림을 발송합니다.
    
    Args:
        notification_type (str): 'trading', 'system', 'reports'
        message (str): 알림 메시지
        subject (str): 알림 제목
        details (dict): 추가 세부 정보
    """
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    payload = {
        'type': notification_type,
        'message': message,
        'subject': subject,
        'details': details or {},
        'source': 'makenaide',
        'timestamp': datetime.utcnow().isoformat()
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName='makenaide-notification-handler',
            InvocationType='Event',  # 비동기 호출
            Payload=json.dumps(payload)
        )
        
        print(f"✅ Notification sent: {notification_type} - {subject}")
        return True
        
    except Exception as e:
        print(f"❌ Error sending notification: {e}")
        return False

# 사용 예시들

# 1. 거래 알림
def notify_trade_executed(ticker, action, amount, price):
    send_notification(
        notification_type='trading',
        message=f'{ticker} {action} 거래가 실행되었습니다.',
        subject=f'거래 실행: {ticker} {action}',
        details={
            'ticker': ticker,
            'action': action,
            'amount': amount,
            'price': price,
            'executed_at': datetime.utcnow().isoformat()
        }
    )

# 2. 시스템 오류 알림
def notify_system_error(function_name, error_message, error_details):
    send_notification(
        notification_type='system',
        message=f'{function_name}에서 오류가 발생했습니다: {error_message}',
        subject=f'시스템 오류: {function_name}',
        details={
            'function_name': function_name,
            'error_type': 'execution_error',
            'error_message': error_message,
            'error_details': error_details,
            'occurred_at': datetime.utcnow().isoformat()
        }
    )

# 3. 일일 리포트 알림  
def notify_daily_report(phase, processed_count, success_count, error_count):
    send_notification(
        notification_type='reports',
        message=f'Phase {phase} 일일 실행이 완료되었습니다.',
        subject=f'일일 리포트: Phase {phase}',
        details={
            'phase': phase,
            'status': 'completed',
            'processed_count': processed_count,
            'success_count': success_count,
            'error_count': error_count,
            'completion_time': datetime.utcnow().isoformat()
        }
    )

# Lambda 함수에서 사용 예시
def lambda_handler(event, context):
    try:
        # 비즈니스 로직 실행
        result = execute_trading_logic()
        
        # 성공 시 거래 알림
        if result['trades']:
            for trade in result['trades']:
                notify_trade_executed(
                    ticker=trade['ticker'],
                    action=trade['action'],
                    amount=trade['amount'],
                    price=trade['price']
                )
        
        # 일일 리포트 알림
        notify_daily_report(
            phase=1,
            processed_count=result['processed'],
            success_count=result['success'],
            error_count=result['errors']
        )
        
        return {'statusCode': 200, 'body': 'Success'}
        
    except Exception as e:
        # 오류 시 시스템 알림
        notify_system_error(
            function_name=context.function_name,
            error_message=str(e),
            error_details=str(e.__class__.__name__)
        )
        
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def execute_trading_logic():
    """실제 거래 로직 (예시)"""
    return {
        'trades': [
            {'ticker': 'KRW-BTC', 'action': 'buy', 'amount': '0.001', 'price': '50000000'},
            {'ticker': 'KRW-ETH', 'action': 'sell', 'amount': '0.1', 'price': '3000000'}
        ],
        'processed': 105,
        'success': 103,
        'errors': 2
    }
'''
    
    print(f"\n📝 SNS notification examples saved to: sns_notification_examples.py")
    
    with open('/Users/13ruce/makenaide/sns_notification_examples.py', 'w', encoding='utf-8') as f:
        f.write(example_code)
    
    return True

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide SNS Notifications Setup")
    print("=" * 50)
    
    # 1. SNS 토픽 생성
    topics = create_sns_topics()
    if not topics:
        print("❌ Failed to create SNS topics. Exiting.")
        return False
    
    print(f"\n✅ Created {len(topics)} SNS topics")
    
    # 2. Lambda 권한 설정
    policy_arn = create_lambda_sns_policy()
    if not policy_arn:
        print("⚠️  Lambda SNS permissions setup failed, but continuing...")
    
    # 3. 알림 처리 함수 생성
    notification_handler_arn = create_notification_lambda()
    if not notification_handler_arn:
        print("⚠️  Notification handler creation failed, but continuing...")
    
    # 4. 사용 예제 코드 생성
    create_usage_examples()
    
    print(f"\n🎉 SNS notification system setup completed!")
    print(f"   Topics created: {len(topics)}")
    print(f"   Handler function: {'✅' if notification_handler_arn else '❌'}")
    
    print(f"\n📋 SNS Topics:")
    for name, arn in topics.items():
        print(f"   📢 {name}: {arn}")
    
    print(f"\n📝 Next steps:")
    print(f"   1. Subscribe to SNS topics (email, SMS, Slack webhook)")
    print(f"   2. Update Lambda functions to send notifications")
    print(f"   3. Test notification system with sample messages")
    print(f"   4. Configure CloudWatch alarms to trigger notifications")
    
    return True

if __name__ == "__main__":
    main()