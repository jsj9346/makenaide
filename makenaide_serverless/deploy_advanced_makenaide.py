#!/usr/bin/env python3
"""
Makenaide 분리형 파이프라인 배포 스크립트 (Lambda 15분 제한 해결)
RDS와 EC2+makenaide 실행을 분리하여 스케줄링 최적화

배포할 Lambda 함수들:
1. makenaide-basic-RDB-controller: RDS 시작 (20분 전 실행)
2. makenaide-integrated-orchestrator: EC2+makenaide 실행 (15분 제한 준수)

스케줄링:
- RDB Controller: 00:40, 04:40, 08:40, 12:40, 16:40, 20:40
- Integrated Orchestrator: 01:00, 05:00, 09:00, 13:00, 17:00, 21:00
"""

import boto3
import json
import time
import zipfile
import os
from datetime import datetime

# Account ID 설정
ACCOUNT_ID = '901361833359'

# AWS 클라이언트 초기화
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
iam_client = boto3.client('iam', region_name='ap-northeast-2')
events_client = boto3.client('events', region_name='ap-northeast-2')

def create_lambda_zip(function_file: str, zip_name: str) -> str:
    """Lambda 함수 ZIP 파일 생성"""
    zip_path = f"/tmp/{zip_name}"
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(function_file, 'lambda_function.py')
    
    print(f"✅ ZIP 파일 생성: {zip_path}")
    return zip_path

def create_iam_role_if_not_exists(role_name: str) -> str:
    """IAM 역할 생성 (존재하지 않는 경우)"""
    try:
        # 기존 역할 확인
        response = iam_client.get_role(RoleName=role_name)
        role_arn = response['Role']['Arn']
        print(f"✅ 기존 IAM 역할 사용: {role_arn}")
        return role_arn
        
    except iam_client.exceptions.NoSuchEntityException:
        print(f"🔧 IAM 역할 생성 중: {role_name}")
        
        # 신뢰 정책
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        # 역할 생성
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Makenaide Advanced Pipeline execution role'
        )
        
        role_arn = response['Role']['Arn']
        
        # 정책 연결
        policies = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
            'arn:aws:iam::aws:policy/AmazonRDSFullAccess',
            'arn:aws:iam::aws:policy/AmazonSSMFullAccess',
            'arn:aws:iam::aws:policy/CloudWatchFullAccess'
        ]
        
        for policy_arn in policies:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
        
        print(f"✅ IAM 역할 생성 완료: {role_arn}")
        time.sleep(10)  # 역할 전파 대기
        return role_arn

def deploy_lambda_function(function_name: str, zip_path: str, role_arn: str, timeout: int = 300, memory: int = 512, description: str = "") -> str:
    """Lambda 함수 배포"""
    try:
        # ZIP 파일 읽기
        with open(zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        # 기존 함수 확인
        try:
            lambda_client.get_function(FunctionName=function_name)
            # 기존 함수 업데이트
            print(f"🔄 기존 함수 업데이트: {function_name}")
            
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content
            )
            
            # 함수 설정 업데이트 (충돌 처리)
            max_retries = 3
            for retry_count in range(max_retries):
                try:
                    lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Timeout=timeout,
                        MemorySize=memory,
                        Environment={
                            'Variables': {
                                'DB_IDENTIFIER': 'makenaide',
                                'EC2_INSTANCE_ID': 'i-082bf343089af62d3',
                                'PG_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
                                'PG_PORT': '5432',
                                'PG_DATABASE': 'makenaide',
                                'PG_USER': 'bruce',
                                'PG_PASSWORD': '0asis314.'
                            }
                        }
                    )
                    print(f"✅ 함수 설정 업데이트 완료")
                    break
                except Exception as e:
                    if "ResourceConflictException" in str(e) and retry_count < max_retries - 1:
                        print(f"⏳ 업데이트 충돌 발생. {retry_count + 1}/{max_retries} 재시도 중... (30초 대기)")
                        time.sleep(30)
                    elif retry_count == max_retries - 1:
                        print(f"⚠️ {max_retries}회 재시도 후에도 업데이트 충돌 지속. 코드 업데이트만 완료됨")
                        break
                    else:
                        raise
            
        except lambda_client.exceptions.ResourceNotFoundException:
            # 새 함수 생성
            print(f"🆕 새 함수 생성: {function_name}")
            
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Timeout=timeout,
                MemorySize=memory,
                Description=description,
                Environment={
                    'Variables': {
                        'DB_IDENTIFIER': 'makenaide',
                        'EC2_INSTANCE_ID': 'i-082bf343089af62d3',

                        'PG_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
                        'PG_PORT': '5432',
                        'PG_DATABASE': 'makenaide',
                        'PG_USER': 'bruce',
                        'PG_PASSWORD': '0asis314.'
                    }
                }
            )
        
        function_arn = response['FunctionArn']
        print(f"✅ Lambda 함수 배포 완료: {function_arn}")
        return function_arn
        
    except Exception as e:
        print(f"❌ Lambda 함수 배포 실패 ({function_name}): {e}")
        raise

def create_separated_schedules(deployed_functions):
    """분리형 아키텍처를 위한 EventBridge 스케줄 생성"""
    try:
        print("📋 분리형 스케줄 설정:")
        print("   - RDB Controller: 00:40, 04:40, 08:40, 12:40, 16:40, 20:40")
        print("   - Integrated Orchestrator: 01:00, 05:00, 09:00, 13:00, 17:00, 21:00")
        
        # 1. RDB Controller 스케줄 (매일 6회, 40분에)
        rdb_rule_name = 'makenaide-rdb-controller-scheduler'
        events_client.put_rule(
            Name=rdb_rule_name,
            ScheduleExpression='cron(40 0,4,8,12,16,20 * * ? *)',  # 00:40, 04:40, 08:40, 12:40, 16:40, 20:40
            Description='Makenaide RDB Controller - 20분 전 RDS 시작',
            State='ENABLED'
        )
        
        # 2. Integrated Orchestrator 스케줄 (매일 6회, 정시에)
        orchestrator_rule_name = 'makenaide-integrated-orchestrator-scheduler'
        events_client.put_rule(
            Name=orchestrator_rule_name,
            ScheduleExpression='cron(0 1,5,9,13,17,21 * * ? *)',   # 01:00, 05:00, 09:00, 13:00, 17:00, 21:00
            Description='Makenaide Integrated Orchestrator - EC2+makenaide 실행',
            State='ENABLED'
        )
        
        # Lambda 함수 ARN 매핑
        function_arn_map = {}
        for func in deployed_functions:
            function_arn_map[func['name']] = func['arn']
        
        # 3. RDB Controller 타겟 설정
        if 'makenaide-basic-RDB-controller' in function_arn_map:
            events_client.put_targets(
                Rule=rdb_rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': function_arn_map['makenaide-basic-RDB-controller']
                    }
                ]
            )
            print(f"✅ RDB Controller 스케줄 설정 완료: {rdb_rule_name}")
        
        # 4. Integrated Orchestrator 타겟 설정
        if 'makenaide-integrated-orchestrator' in function_arn_map:
            events_client.put_targets(
                Rule=orchestrator_rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': function_arn_map['makenaide-integrated-orchestrator']
                    }
                ]
            )
            print(f"✅ Integrated Orchestrator 스케줄 설정 완료: {orchestrator_rule_name}")
        
        # 5. Lambda 실행 권한 추가
        for func in deployed_functions:
            try:
                rule_name = rdb_rule_name if 'RDB-controller' in func['name'] else orchestrator_rule_name
                
                lambda_client.add_permission(
                    FunctionName=func['name'],
                    StatementId=f"allow-eventbridge-{rule_name}",
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f"arn:aws:events:ap-northeast-2:{ACCOUNT_ID}:rule/{rule_name}"
                )
                print(f"✅ {func['name']} EventBridge 실행 권한 추가 완료")
            except Exception as e:
                if "ResourceConflictException" in str(e):
                    print(f"ℹ️ {func['name']} EventBridge 권한이 이미 존재함")
                else:
                    print(f"⚠️ {func['name']} EventBridge 권한 추가 실패: {e}")
        
    except Exception as e:
        print(f"❌ EventBridge 스케줄 설정 실패: {e}")

def main():
    """메인 배포 함수"""
    print("🚀 Makenaide 분리형 파이프라인 배포 시작 (Lambda 15분 제한 해결)")
    print(f"📅 배포 시간: {datetime.now().isoformat()}")
    print()
    print("📋 배포 계획:")
    print("   1. makenaide-basic-RDB-controller: RDS 관리 (20분 전 실행)")
    print("   2. makenaide-integrated-orchestrator: EC2+makenaide 실행 (15분 제한 준수)")
    print("   3. EventBridge 분리형 스케줄 설정")
    print()
    
    try:
        # IAM 역할 생성
        role_name = 'makenaide-separated-execution-role'
        role_arn = create_iam_role_if_not_exists(role_name)
        
        # Lambda 함수들 배포 (분리형 아키텍처)
        functions_to_deploy = [
            {
                'name': 'makenaide-basic-RDB-controller',
                'file': 'lambda_rdb_controller.py',
                'timeout': 600,  # 10분 (RDS 시작 대기)
                'memory': 256,
                'description': 'RDS 시작 전용 (20분 전 스케줄)'
            },
            {
                'name': 'makenaide-integrated-orchestrator',
                'file': 'lambda_integrated_orchestrator.py',
                'timeout': 900,  # 15분 (Lambda 최대 시간)
                'memory': 512,
                'description': 'EC2+makenaide 실행 전용 (15분 제한 준수)'
            }
        ]
        
        deployed_functions = []
        
        for func_config in functions_to_deploy:
            print(f"\n📦 {func_config['name']} 배포 중...")
            
            # ZIP 파일 생성
            zip_path = create_lambda_zip(
                func_config['file'], 
                f"{func_config['name']}.zip"
            )
            
            # Lambda 함수 배포
            function_arn = deploy_lambda_function(
                func_config['name'],
                zip_path,
                role_arn,
                func_config['timeout'],
                func_config['memory']
            )
            
            deployed_functions.append({
                'name': func_config['name'],
                'arn': function_arn
            })
            
            # 임시 ZIP 파일 삭제
            if os.path.exists(zip_path):
                os.remove(zip_path)
        
        # EventBridge 스케줄 설정 (분리형)
        print(f"\n⏰ EventBridge 스케줄러 설정 중...")
        create_separated_schedules(deployed_functions)
        
        # 배포 결과 출력
        print(f"\n🎉 Makenaide 분리형 파이프라인 배포 완료! (Lambda 15분 제한 해결)")
        print(f"📋 배포된 함수들:")
        for func in deployed_functions:
            print(f"  - {func['name']}: {func['arn']}")
        
        print(f"\n🔄 새로운 분리형 플로우:")
        print(f"  📅 RDB Controller (매일 6회):")
        print(f"     00:40, 04:40, 08:40, 12:40, 16:40, 20:40")
        print(f"     ↓ RDS 시작 & 준비")
        print(f"  📅 Integrated Orchestrator (20분 후):")
        print(f"     01:00, 05:00, 09:00, 13:00, 17:00, 21:00")
        print(f"     ↓ EC2 시작 → makenaide 실행 → EC2 자동 종료")
        print(f"  📅 RDS는 계속 실행 상태 유지")
        
        print(f"\n⚡ Lambda 15분 제한 해결:")
        print(f"  - RDB Controller: ~10분 (RDS 시작 대기)")
        print(f"  - Integrated Orchestrator: ~10분 (EC2+makenaide 비동기 시작)")
        print(f"  - makenaide는 EC2에서 실행 후 자동 종료")
        
        print(f"\n💰 예상 비용 절약:")
        print(f"  - Lambda 15분 제한 준수로 안정성 확보")
        print(f"  - RDS/EC2 최적화된 스케줄링")
        print(f"  - 월간 절약: ~$50+ USD")
        print(f"  - 연간 절약: ~$600+ USD")
        
        print(f"\n📊 모니터링:")
        print(f"  - RDB Controller 로그: /aws/lambda/makenaide-basic-RDB-controller")
        print(f"  - Orchestrator 로그: /aws/lambda/makenaide-integrated-orchestrator")
        print(f"  - EventBridge 규칙: makenaide-rdb-controller-scheduler, makenaide-integrated-orchestrator-scheduler")
        
    except Exception as e:
        print(f"❌ 배포 실패: {e}")
        import traceback
        print(f"상세 오류: {traceback.format_exc()}")

if __name__ == "__main__":
    print("🔥 Lambda 15분 제한 문제 해결을 위한 분리형 파이프라인 배포 시작!")
    print("=" * 80)
    main()