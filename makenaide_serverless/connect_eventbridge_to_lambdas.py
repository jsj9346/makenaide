#!/usr/bin/env python3
"""
EventBridge 규칙과 실제 Lambda 함수를 연결
"""

import boto3
import json
from botocore.exceptions import ClientError

def connect_eventbridge_targets():
    """
    생성된 EventBridge 규칙에 실제 Lambda 함수들을 타겟으로 추가합니다.
    """
    print("🔄 Connecting EventBridge rules to Lambda functions...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # 실제 Lambda 함수 매핑
    lambda_mapping = {
        'makenaide-daily-pipeline-start': 'makenaide-scanner',
        'makenaide-phase0-completed': 'makenaide-data-collector', 
        'makenaide-phase1-completed': 'makenaide-comprehensive-filter-phase2',
        'makenaide-phase2-completed': 'makenaide-gpt-analysis-phase3',
        'makenaide-phase3-completed': 'makenaide-4h-analysis-phase4',
        'makenaide-phase4-completed': 'makenaide-condition-check-phase5',
        'makenaide-trading-signal': 'makenaide-trade-execution-phase6'
    }
    
    connected_targets = 0
    
    for rule_name, function_name in lambda_mapping.items():
        try:
            print(f"\n🔄 Connecting rule '{rule_name}' to function '{function_name}'")
            
            # 기존 타겟 제거
            try:
                response = events_client.list_targets_by_rule(Rule=rule_name)
                if response['Targets']:
                    target_ids = [target['Id'] for target in response['Targets']]
                    events_client.remove_targets(Rule=rule_name, Ids=target_ids)
                    print(f"   Removed existing targets: {len(target_ids)}")
            except ClientError as e:
                print(f"   No existing targets to remove: {e.response['Error']['Code']}")
            
            # Lambda 함수 ARN 생성
            function_arn = f"arn:aws:lambda:ap-northeast-2:901361833359:function:{function_name}"
            
            # 새 타겟 추가
            events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': function_arn
                    }
                ]
            )
            
            # Lambda 호출 권한 추가
            try:
                lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=f'eventbridge-invoke-{rule_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f'arn:aws:events:ap-northeast-2:901361833359:rule/{rule_name}'
                )
                print(f"✅ Added EventBridge permission to {function_name}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceConflictException':
                    print(f"ℹ️  Permission already exists for {function_name}")
                else:
                    print(f"❌ Error adding permission: {e.response['Error']['Message']}")
            
            connected_targets += 1
            print(f"✅ Connected {rule_name} → {function_name}")
            
        except ClientError as e:
            print(f"❌ Error connecting {rule_name}: {e.response['Error']['Message']}")
    
    return connected_targets

def create_rds_controller():
    """
    RDS 제어 Lambda 함수를 생성합니다.
    """
    print(f"\n🔄 Creating RDS controller Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # 간단한 RDS 제어 함수 생성 (ZIP 파일 없이)
    function_code = """
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    rds = boto3.client('rds', region_name='ap-northeast-2')
    action = event.get('action', 'start')
    db_instance_id = 'makenaide'
    
    try:
        if action == 'start':
            logger.info(f"Starting RDS instance: {db_instance_id}")
            response = rds.start_db_instance(DBInstanceIdentifier=db_instance_id)
            return {'statusCode': 200, 'body': json.dumps('RDS start initiated')}
        elif action == 'stop':
            logger.info(f"Stopping RDS instance: {db_instance_id}")
            response = rds.stop_db_instance(DBInstanceIdentifier=db_instance_id)
            return {'statusCode': 200, 'body': json.dumps('RDS stop initiated')}
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {str(e)}')}
"""
    
    try:
        # RDS 제어 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-rds-controller',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='index.lambda_handler',
            Code={
                'ZipFile': function_code.encode('utf-8')
            },
            Description='Makenaide RDS instance start/stop controller',
            Timeout=300,
            MemorySize=128
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created RDS controller: {function_arn}")
        
        # RDS 시작 규칙에 연결
        events_client = boto3.client('events', region_name='ap-northeast-2')
        
        events_client.put_targets(
            Rule='makenaide-rds-startup',
            Targets=[
                {
                    'Id': '1',
                    'Arn': function_arn,
                    'Input': json.dumps({"action": "start"})
                }
            ]
        )
        
        # 권한 추가
        lambda_client.add_permission(
            FunctionName='makenaide-rds-controller',
            StatementId='eventbridge-invoke-rds-startup',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-rds-startup'
        )
        
        print(f"✅ Connected RDS startup rule to RDS controller")
        return function_arn
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceConflictException':
            print(f"ℹ️  RDS controller function already exists")
            return f"arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-rds-controller"
        else:
            print(f"❌ Error creating RDS controller: {e.response['Error']['Message']}")
            return None

def verify_connections():
    """
    EventBridge 규칙과 타겟 연결을 검증합니다.
    """
    print(f"\n🔍 Verifying EventBridge connections...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
    # 생성된 규칙들 확인
    rules = [
        'makenaide-daily-pipeline-start',
        'makenaide-rds-startup',
        'makenaide-phase0-completed',
        'makenaide-phase1-completed',
        'makenaide-phase2-completed', 
        'makenaide-phase3-completed',
        'makenaide-phase4-completed',
        'makenaide-trading-signal'
    ]
    
    connected_rules = 0
    
    for rule_name in rules:
        try:
            # 규칙 상태 확인
            rule_response = events_client.describe_rule(Name=rule_name)
            rule_state = rule_response['State']
            
            # 타겟 확인
            targets_response = events_client.list_targets_by_rule(Rule=rule_name)
            target_count = len(targets_response['Targets'])
            
            if target_count > 0:
                connected_rules += 1
                status = "✅"
            else:
                status = "⚠️"
            
            print(f"   {status} {rule_name}: {rule_state}, {target_count} targets")
            
        except ClientError as e:
            print(f"   ❌ {rule_name}: {e.response['Error']['Message']}")
    
    print(f"\n📊 Connection Summary:")
    print(f"   Total rules: {len(rules)}")
    print(f"   Connected rules: {connected_rules}")
    print(f"   Connection rate: {connected_rules/len(rules)*100:.1f}%")
    
    return connected_rules

def main():
    """메인 실행 함수"""
    print("🚀 EventBridge Lambda Connection Setup")
    print("=" * 50)
    
    # 1. EventBridge 규칙과 Lambda 함수 연결
    connected_count = connect_eventbridge_targets()
    print(f"\n✅ Connected {connected_count} EventBridge rules to Lambda functions")
    
    # 2. RDS 제어 함수 생성
    rds_controller_arn = create_rds_controller()
    if rds_controller_arn:
        print(f"✅ RDS controller ready")
    
    # 3. 연결 상태 검증
    verify_connections()
    
    print(f"\n🎉 EventBridge orchestration completed!")
    print(f"\n📋 Pipeline Flow:")
    print(f"   08:40 UTC → RDS 시작")
    print(f"   09:00 UTC → Phase 0 (Scanner) 시작")
    print(f"   Phase 0 완료 → Phase 1 (Data Collector) 시작")
    print(f"   Phase 1 완료 → Phase 2 (Filter) 시작")
    print(f"   Phase 2 완료 → Phase 3 (GPT Analysis) 시작")
    print(f"   Phase 3 완료 → Phase 4 (4H Analysis) 시작")
    print(f"   Phase 4 완료 → Phase 5 (Condition Check) 시작")
    print(f"   거래 신호 발생 → Phase 6 (Trade Execution) 시작")

if __name__ == "__main__":
    main()