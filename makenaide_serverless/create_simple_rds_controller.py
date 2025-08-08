#!/usr/bin/env python3
"""
간단한 RDS 제어 Lambda 함수 생성 (ZIP 파일 문제 우회)
"""

import boto3
import json
import zipfile
import os
from botocore.exceptions import ClientError

def create_simple_rds_controller():
    """
    ZIP 파일을 직접 생성하여 RDS 제어 Lambda 함수를 만듭니다.
    """
    print("🔄 Creating RDS controller with proper ZIP file...")
    
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    # 임시 Python 파일 생성
    python_code = '''import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """RDS 인스턴스 시작/종료 제어"""
    rds = boto3.client('rds', region_name='ap-northeast-2')
    
    action = event.get('action', 'start')
    db_instance_id = 'makenaide'
    
    try:
        if action == 'start':
            logger.info(f"Starting RDS instance: {db_instance_id}")
            response = rds.start_db_instance(DBInstanceIdentifier=db_instance_id)
            status = response['DBInstance']['DBInstanceStatus']
            logger.info(f"RDS start initiated: {status}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'action': 'start',
                    'db_instance': db_instance_id,
                    'status': status
                })
            }
            
        elif action == 'stop':
            logger.info(f"Stopping RDS instance: {db_instance_id}")
            response = rds.stop_db_instance(
                DBInstanceIdentifier=db_instance_id,
                DBSnapshotIdentifier=f"{db_instance_id}-auto-{int(context.aws_request_id[:8], 16)}"
            )
            status = response['DBInstance']['DBInstanceStatus']
            logger.info(f"RDS stop initiated: {status}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'action': 'stop',
                    'db_instance': db_instance_id,
                    'status': status
                })
            }
        else:
            logger.error(f"Invalid action: {action}")
            return {
                'statusCode': 400,
                'body': json.dumps(f'Invalid action: {action}')
            }
            
    except Exception as e:
        logger.error(f"Error controlling RDS: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
'''
    
    # 임시 파일 생성
    temp_dir = '/tmp'
    lambda_py = os.path.join(temp_dir, 'lambda_function.py')
    zip_file = os.path.join(temp_dir, 'rds_controller.zip')
    
    try:
        # Python 파일 작성
        with open(lambda_py, 'w') as f:
            f.write(python_code)
        
        # ZIP 파일 생성
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(lambda_py, 'lambda_function.py')
        
        print("✅ Created ZIP file for RDS controller")
        
        # 기존 함수 삭제
        try:
            lambda_client.delete_function(FunctionName='makenaide-rds-controller')
            print("   Deleted existing RDS controller function")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # ZIP 파일 읽기
        with open(zip_file, 'rb') as f:
            zip_content = f.read()
        
        # Lambda 함수 생성
        response = lambda_client.create_function(
            FunctionName='makenaide-rds-controller',
            Runtime='python3.9',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Makenaide RDS instance start/stop controller',
            Timeout=300,
            MemorySize=128,
            Tags={
                'Project': 'Makenaide',
                'Purpose': 'RDS-Control'
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"✅ Created RDS controller: {function_arn}")
        
        # EventBridge 연결
        events_client = boto3.client('events', region_name='ap-northeast-2')
        
        # RDS 시작 규칙에 타겟 추가
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
        
        # EventBridge 호출 권한 추가
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-rds-controller',
                StatementId='eventbridge-invoke-rds-startup',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn='arn:aws:events:ap-northeast-2:901361833359:rule/makenaide-rds-startup'
            )
            print("✅ Added EventBridge permission to RDS controller")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print("ℹ️  Permission already exists for RDS controller")
        
        print("✅ Connected RDS startup rule to RDS controller")
        
        # 임시 파일 정리
        os.remove(lambda_py)
        os.remove(zip_file)
        
        return function_arn
        
    except Exception as e:
        print(f"❌ Error creating RDS controller: {str(e)}")
        # 임시 파일 정리
        try:
            os.remove(lambda_py)
            os.remove(zip_file)
        except:
            pass
        return None

def verify_final_connections():
    """
    최종 EventBridge 연결 상태를 확인합니다.
    """
    print(f"\n🔍 Final EventBridge connection verification...")
    
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
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
            rule_response = events_client.describe_rule(Name=rule_name)
            targets_response = events_client.list_targets_by_rule(Rule=rule_name)
            target_count = len(targets_response['Targets'])
            
            if target_count > 0:
                connected_rules += 1
                status = "✅"
            else:
                status = "❌"
                
            print(f"   {status} {rule_name}: {target_count} targets")
            
        except ClientError as e:
            print(f"   ❌ {rule_name}: {e.response['Error']['Message']}")
    
    connection_rate = connected_rules / len(rules) * 100
    print(f"\n📊 Final Connection Summary:")
    print(f"   Total rules: {len(rules)}")
    print(f"   Connected rules: {connected_rules}")
    print(f"   Connection rate: {connection_rate:.1f}%")
    
    return connection_rate

def main():
    """메인 실행 함수"""
    print("🚀 Simple RDS Controller Creation")
    print("=" * 40)
    
    # RDS 제어 함수 생성
    rds_controller_arn = create_simple_rds_controller()
    
    if rds_controller_arn:
        print(f"\n🎉 RDS controller created successfully!")
        print(f"   Function ARN: {rds_controller_arn}")
        
        # 최종 연결 상태 확인
        connection_rate = verify_final_connections()
        
        if connection_rate == 100.0:
            print(f"\n🎉 100% EventBridge orchestration achieved!")
            print(f"   All 8 rules connected to Lambda targets")
            print(f"\n📋 Complete Pipeline Flow:")
            print(f"   23:40 UTC → RDS 자동 시작 (08:40 KST)")
            print(f"   00:00 UTC → Phase 0 시작 (09:00 KST)")
            print(f"   Phase 0 완료 → Phase 1 → 2 → 3 → 4 → 5 → 6")
            print(f"   비용 최적화: RDS 30분 사용으로 90% 절약")
        else:
            print(f"\n⚠️  Connection rate: {connection_rate:.1f}%")
    else:
        print(f"\n❌ Failed to create RDS controller")
    
    return rds_controller_arn is not None

if __name__ == "__main__":
    main()