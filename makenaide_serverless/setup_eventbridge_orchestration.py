#!/usr/bin/env python3
"""
Makenaide EventBridge Orchestration Setup Script
Lambda 함수 간 이벤트 기반 자동화된 파이프라인 오케스트레이션
"""

import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

def create_eventbridge_rules():
    """
    EventBridge 규칙들을 생성합니다.
    """
    print("🔄 Creating EventBridge rules for pipeline orchestration...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # RDS 제어를 위한 Lambda 함수들 존재 여부 확인
    lambda_functions = {
        'ticker-scanner': 'lambda_ticker_scanner_phase0',
        'data-collector': 'lambda_selective_data_collector_phase1',
        'comprehensive-filter': 'lambda_comprehensive_filter_phase2',
        'gpt-analyzer': 'lambda_gpt_analysis_phase3',
        '4h-analyzer': 'lambda_phase4_4h_analysis',
        'condition-checker': 'lambda_phase5_condition_check',
        'trade-executor': 'lambda_phase6_trade_execution'
    }
    
    # Lambda 함수 존재 여부 확인
    existing_functions = []
    try:
        response = lambda_client.list_functions()
        function_names = [f['FunctionName'] for f in response['Functions']]
        
        for name, function_name in lambda_functions.items():
            if any(function_name in fn for fn in function_names):
                existing_functions.append(name)
                print(f"✅ Found Lambda function: {name}")
            else:
                print(f"⚠️  Lambda function not found: {name} ({function_name})")
                
    except ClientError as e:
        print(f"❌ Error checking Lambda functions: {e.response['Error']['Message']}")
        return False
    
    if len(existing_functions) < 6:
        print(f"⚠️  Only found {len(existing_functions)}/7 Lambda functions")
        print("   Continuing with EventBridge setup for existing functions...")
    
    # EventBridge 규칙들 정의
    rules_config = [
        {
            'name': 'makenaide-daily-pipeline-start',
            'description': 'Start daily Makenaide pipeline at 9:00 AM KST',
            'schedule': 'cron(0 0 * * ? *)',  # 9:00 AM KST = 0:00 UTC
            'target_function': 'lambda_ticker_scanner_phase0',
            'enabled': True
        },
        {
            'name': 'makenaide-rds-startup',
            'description': 'Start RDS instance 20 minutes before pipeline',
            'schedule': 'cron(40 23 * * ? *)',  # 8:40 AM KST = 23:40 UTC
            'target_function': 'rds_controller',  # RDS 제어 함수
            'enabled': True,
            'input': json.dumps({"action": "start"})
        }
    ]
    
    # Phase 간 연결을 위한 커스텀 이벤트 규칙들
    phase_rules = [
        {
            'name': 'makenaide-phase0-completed',
            'description': 'Trigger Phase 1 when Phase 0 completes',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Phase Completed"],
                "detail": {
                    "phase": ["0"],
                    "status": ["success"]
                }
            },
            'target_function': 'lambda_selective_data_collector_phase1'
        },
        {
            'name': 'makenaide-phase1-completed',
            'description': 'Trigger Phase 2 when Phase 1 completes',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Phase Completed"],
                "detail": {
                    "phase": ["1"],
                    "status": ["success"]
                }
            },
            'target_function': 'lambda_comprehensive_filter_phase2'
        },
        {
            'name': 'makenaide-phase2-completed',
            'description': 'Trigger Phase 3 when Phase 2 completes',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Phase Completed"],
                "detail": {
                    "phase": ["2"],
                    "status": ["success"]
                }
            },
            'target_function': 'lambda_gpt_analysis_phase3'
        },
        {
            'name': 'makenaide-phase3-completed',
            'description': 'Trigger Phase 4 when Phase 3 completes',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Phase Completed"],
                "detail": {
                    "phase": ["3"],
                    "status": ["success"]
                }
            },
            'target_function': 'lambda_phase4_4h_analysis'
        },
        {
            'name': 'makenaide-phase4-completed',
            'description': 'Trigger Phase 5 when Phase 4 completes',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Phase Completed"],
                "detail": {
                    "phase": ["4"],
                    "status": ["success"]
                }
            },
            'target_function': 'lambda_phase5_condition_check'
        },
        {
            'name': 'makenaide-trading-signal',
            'description': 'Trigger EC2 trading when signal is generated',
            'event_pattern': {
                "source": ["makenaide"],
                "detail-type": ["Trading Signal"],
                "detail": {
                    "action": ["buy", "sell"],
                    "signal_strength": ["high"]
                }
            },
            'target_function': 'ec2_trading_controller'  # EC2 시작 함수
        }
    ]
    
    created_rules = []
    
    # 스케줄 기반 규칙 생성
    for rule in rules_config:
        try:
            print(f"\n🔄 Creating scheduled rule: {rule['name']}")
            
            # 기존 규칙 삭제 (있다면)
            try:
                events_client.delete_rule(Name=rule['name'], Force=True)
                print(f"   Deleted existing rule: {rule['name']}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    print(f"   Warning: {e.response['Error']['Message']}")
            
            # 새 규칙 생성
            response = events_client.put_rule(
                Name=rule['name'],
                ScheduleExpression=rule['schedule'],
                Description=rule['description'],
                State='ENABLED' if rule['enabled'] else 'DISABLED'
            )
            
            created_rules.append(rule['name'])
            print(f"✅ Created scheduled rule: {rule['name']}")
            print(f"   ARN: {response['RuleArn']}")
            
        except ClientError as e:
            print(f"❌ Error creating rule {rule['name']}: {e.response['Error']['Message']}")
    
    # 이벤트 패턴 기반 규칙 생성
    for rule in phase_rules:
        try:
            print(f"\n🔄 Creating event pattern rule: {rule['name']}")
            
            # 기존 규칙 삭제 (있다면)
            try:
                events_client.delete_rule(Name=rule['name'], Force=True)
                print(f"   Deleted existing rule: {rule['name']}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    print(f"   Warning: {e.response['Error']['Message']}")
            
            # 새 규칙 생성
            response = events_client.put_rule(
                Name=rule['name'],
                EventPattern=json.dumps(rule['event_pattern']),
                Description=rule['description'],
                State='ENABLED'
            )
            
            created_rules.append(rule['name'])
            print(f"✅ Created event pattern rule: {rule['name']}")
            print(f"   ARN: {response['RuleArn']}")
            
        except ClientError as e:
            print(f"❌ Error creating rule {rule['name']}: {e.response['Error']['Message']}")
    
    print(f"\n🎉 Created {len(created_rules)} EventBridge rules!")
    return created_rules

def setup_lambda_permissions():
    """
    EventBridge가 Lambda 함수를 호출할 수 있도록 권한을 설정합니다.
    """
    print(f"\n🔄 Setting up Lambda permissions for EventBridge...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # EventBridge가 호출할 Lambda 함수들
    lambda_functions = [
        'lambda_ticker_scanner_phase0',
        'lambda_selective_data_collector_phase1', 
        'lambda_comprehensive_filter_phase2',
        'lambda_gpt_analysis_phase3',
        'lambda_phase4_4h_analysis',
        'lambda_phase5_condition_check',
        'lambda_phase6_trade_execution'
    ]
    
    permissions_added = 0
    
    for function_name in lambda_functions:
        try:
            # EventBridge 호출 권한 추가
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId=f'eventbridge-invoke-{function_name}',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=f'arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-*'
            )
            
            permissions_added += 1
            print(f"✅ Added EventBridge permission to: {function_name}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceConflictException':
                print(f"ℹ️  Permission already exists for: {function_name}")
            elif error_code == 'ResourceNotFoundException':
                print(f"⚠️  Function not found: {function_name}")
            else:
                print(f"❌ Error adding permission to {function_name}: {e.response['Error']['Message']}")
    
    print(f"\n✅ Added permissions to {permissions_added} Lambda functions")
    return permissions_added > 0

def create_rds_controller_function():
    """
    RDS 시작/종료를 제어하는 Lambda 함수를 생성합니다.
    """
    print(f"\n🔄 Creating RDS controller Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # RDS 제어 함수 코드
    function_code = '''
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    RDS 인스턴스 시작/종료를 제어합니다.
    """
    rds = boto3.client('rds', region_name='ap-northeast-2')
    
    # 이벤트에서 액션 가져오기
    action = event.get('action', 'start')
    db_instance_id = 'makenaide'  # RDS 인스턴스 ID
    
    try:
        if action == 'start':
            logger.info(f"Starting RDS instance: {db_instance_id}")
            response = rds.start_db_instance(DBInstanceIdentifier=db_instance_id)
            logger.info(f"RDS start initiated: {response['DBInstance']['DBInstanceStatus']}")
            
        elif action == 'stop':
            logger.info(f"Stopping RDS instance: {db_instance_id}")
            response = rds.stop_db_instance(
                DBInstanceIdentifier=db_instance_id,
                DBSnapshotIdentifier=f"{db_instance_id}-auto-stop-{int(context.aws_request_id[:8], 16)}"
            )
            logger.info(f"RDS stop initiated: {response['DBInstance']['DBInstanceStatus']}")
            
        else:
            logger.error(f"Invalid action: {action}")
            return {
                'statusCode': 400,
                'body': json.dumps(f'Invalid action: {action}')
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': action,
                'db_instance': db_instance_id,
                'status': 'initiated'
            })
        }
        
    except Exception as e:
        logger.error(f"Error controlling RDS: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
'''
    
    try:
        # 기존 함수 삭제 (있다면)
        try:
            lambda_client.delete_function(FunctionName='makenaide-rds-controller')
            print("   Deleted existing RDS controller function")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-rds-controller',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={'ZipFile': function_code.encode()},
            Description='Makenaide RDS instance start/stop controller',
            Timeout=300,
            MemorySize=128,
            Tags={
                'Project': 'Makenaide',
                'Purpose': 'RDS-Control'
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created RDS controller function: {function_arn}")
        
        # EventBridge 호출 권한 추가
        lambda_client.add_permission(
            FunctionName='makenaide-rds-controller',
            StatementId='eventbridge-invoke-rds-controller',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com'
        )
        
        print("✅ Added EventBridge permission to RDS controller")
        return function_arn
        
    except ClientError as e:
        print(f"❌ Error creating RDS controller function: {e.response['Error']['Message']}")
        return None

def create_event_example():
    """
    Phase 간 이벤트 발송 예제를 생성합니다.
    """
    example_code = '''
# Lambda 함수에서 다음 Phase를 트리거하는 EventBridge 이벤트 발송 예제

import boto3
import json
from datetime import datetime

def send_phase_completed_event(phase_number, status, data=None):
    """
    Phase 완료 이벤트를 EventBridge로 전송합니다.
    
    Args:
        phase_number (int): 완료된 Phase 번호 (0-6)
        status (str): 'success' 또는 'failure'
        data (dict): 추가 데이터 (선택사항)
    """
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
    detail = {
        "phase": str(phase_number),
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data or {}
    }
    
    try:
        response = events_client.put_events(
            Entries=[
                {
                    'Source': 'makenaide',
                    'DetailType': 'Phase Completed',
                    'Detail': json.dumps(detail),
                    'Time': datetime.utcnow()
                }
            ]
        )
        
        print(f"✅ Event sent: Phase {phase_number} {status}")
        return response
        
    except Exception as e:
        print(f"❌ Error sending event: {e}")
        return None

def send_trading_signal_event(action, tickers, signal_strength="high"):
    """
    거래 신호 이벤트를 EventBridge로 전송합니다.
    
    Args:
        action (str): 'buy' 또는 'sell'
        tickers (list): 거래 대상 티커들
        signal_strength (str): 신호 강도 ('high', 'medium', 'low')
    """
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
    detail = {
        "action": action,
        "tickers": tickers,
        "signal_strength": signal_strength,
        "timestamp": datetime.utcnow().isoformat(),
        "generated_by": "Phase5-ConditionCheck"
    }
    
    try:
        response = events_client.put_events(
            Entries=[
                {
                    'Source': 'makenaide',
                    'DetailType': 'Trading Signal',
                    'Detail': json.dumps(detail),
                    'Time': datetime.utcnow()
                }
            ]
        )
        
        print(f"✅ Trading signal sent: {action} {len(tickers)} tickers")
        return response
        
    except Exception as e:
        print(f"❌ Error sending trading signal: {e}")
        return None

# Lambda 함수에서 사용 예시
def lambda_handler(event, context):
    try:
        # Phase 로직 실행
        # ...
        
        # Phase 완료 시 다음 Phase 트리거
        send_phase_completed_event(
            phase_number=1,  # 현재 Phase 번호
            status="success",
            data={"processed_tickers": 105, "filtered_count": 23}
        )
        
        # 거래 신호 발생 시 (Phase 5에서)
        if trading_candidates:
            send_trading_signal_event(
                action="buy",
                tickers=["KRW-BTC", "KRW-ETH"],
                signal_strength="high"
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Phase completed successfully')
        }
        
    except Exception as e:
        # 실패 시 실패 이벤트 전송
        send_phase_completed_event(
            phase_number=1,
            status="failure",
            data={"error": str(e)}
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
'''
    
    print(f"\n📝 Example EventBridge integration code saved to: eventbridge_integration_example.py")
    
    with open('/Users/13ruce/makenaide/eventbridge_integration_example.py', 'w', encoding='utf-8') as f:
        f.write(example_code)
    
    return True

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide EventBridge Orchestration Setup")
    print("=" * 60)
    
    # 1. EventBridge 규칙 생성
    created_rules = create_eventbridge_rules()
    if not created_rules:
        print("❌ Failed to create EventBridge rules. Exiting.")
        return False
    
    # 2. Lambda 권한 설정
    if not setup_lambda_permissions():
        print("⚠️  Lambda permissions setup had issues, but continuing...")
    
    # 3. RDS 제어 함수 생성
    rds_controller_arn = create_rds_controller_function()
    if not rds_controller_arn:
        print("⚠️  RDS controller creation failed, but continuing...")
    
    # 4. 예제 코드 생성
    create_event_example()
    
    print(f"\n🎉 EventBridge orchestration setup completed!")
    print(f"   Created rules: {len(created_rules)}")
    print(f"   RDS controller: {'✅' if rds_controller_arn else '❌'}")
    
    print(f"\n📋 Next steps:")
    print(f"   1. Update existing Lambda functions to send EventBridge events")
    print(f"   2. Test the pipeline with manual triggers")
    print(f"   3. Monitor CloudWatch logs for event flow")
    print(f"   4. Configure RDS instance ID in RDS controller if different")
    
    return True

if __name__ == "__main__":
    main()