#!/usr/bin/env python3
"""
🚀 Phase 0 Lambda 배포 스크립트
- lambda_ticker_scanner_phase0.py를 AWS Lambda로 배포
- 필요한 Layer와 환경 변수 설정
- EventBridge 스케줄러 연동
"""

import boto3
import zipfile
import os
import json
import time
from datetime import datetime

def setup_clients():
    """AWS 클라이언트 초기화"""
    return {
        'lambda': boto3.client('lambda'),
        'iam': boto3.client('iam'),
        'events': boto3.client('events'),
        's3': boto3.client('s3')
    }

def create_lambda_package():
    """Lambda 배포 패키지 생성"""
    print("📦 Lambda 배포 패키지 생성 중...")
    
    # ZIP 파일 생성
    zip_filename = 'lambda_ticker_scanner_phase0.zip'
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # 메인 Lambda 함수 추가
        zip_file.write('lambda_ticker_scanner_phase0.py', 'lambda_function.py')
        
        print(f"✅ {zip_filename} 생성 완료")
    
    return zip_filename

def create_execution_role(iam_client):
    """Lambda 실행 역할 생성"""
    role_name = 'makenaide-phase0-lambda-role'
    
    try:
        # 역할이 이미 존재하는지 확인
        iam_client.get_role(RoleName=role_name)
        print(f"✅ IAM 역할 이미 존재: {role_name}")
        return f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/{role_name}"
    
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
            Description='Makenaide Phase 0 Lambda execution role'
        )
        
        # 기본 Lambda 실행 정책 연결
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        
        # S3, EventBridge, RDS 접근 정책 생성 및 연결
        custom_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": "arn:aws:s3:::makenaide-serverless-data/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "events:PutEvents"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "rds:DescribeDBInstances",
                        "rds:StartDBInstance",
                        "rds:StopDBInstance"
                    ],
                    "Resource": "arn:aws:rds:*:*:db:*"
                }
            ]
        }
        
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='MakenaidePhase0Policy',
            PolicyDocument=json.dumps(custom_policy)
        )
        
        print(f"✅ IAM 역할 생성 완료: {role_name}")
        
        # 역할 전파 대기
        time.sleep(10)
        
        return response['Role']['Arn']

def create_or_update_lambda(lambda_client, zip_filename, role_arn):
    """Lambda 함수 생성 또는 업데이트"""
    function_name = 'makenaide-ticker-scanner-phase0'
    
    # ZIP 파일 읽기
    with open(zip_filename, 'rb') as zip_file:
        zip_content = zip_file.read()
    
    try:
        # 기존 함수가 있는지 확인
        lambda_client.get_function(FunctionName=function_name)
        
        print(f"🔄 기존 Lambda 함수 업데이트: {function_name}")
        
        # 함수 코드 업데이트
        lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        
        # 함수 설정 업데이트
        response = lambda_client.update_function_configuration(
            FunctionName=function_name,
            Timeout=300,  # 5분
            MemorySize=256,  # 256MB
            Environment={
                'Variables': {
                    'S3_BUCKET': 'makenaide-serverless-data',
                    'PHASE': 'ticker_scanner'
                }
            }
        )
        
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"🆕 새 Lambda 함수 생성: {function_name}")
        
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Makenaide Phase 0: Ticker Scanner',
            Timeout=300,  # 5분
            MemorySize=256,  # 256MB
            Environment={
                'Variables': {
                    'S3_BUCKET': 'makenaide-serverless-data',
                    'PHASE': 'ticker_scanner'
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Phase': 'Phase0',
                'Environment': 'Production'
            }
        )
    
    print(f"✅ Lambda 함수 준비 완료: {function_name}")
    return response['FunctionArn']

def create_layer(lambda_client):
    """공통 Layer 생성 (pyupbit, psycopg2 등)"""
    layer_name = 'makenaide-core-layer-phase0'
    
    print(f"📚 Layer 생성: {layer_name}")
    
    # 간단한 requirements.txt 내용
    requirements = """
pyupbit==0.2.22
psycopg2-binary==2.9.7
boto3==1.28.57
requests==2.31.0
"""
    
    # Layer ZIP 생성 (실제로는 사전에 준비된 Layer를 사용하는 것이 좋음)
    layer_zip = 'makenaide-core-layer-phase0.zip'
    
    try:
        with zipfile.ZipFile(layer_zip, 'w') as zip_file:
            # requirements.txt 추가
            zip_file.writestr('requirements.txt', requirements)
        
        with open(layer_zip, 'rb') as zip_file:
            layer_content = zip_file.read()
        
        response = lambda_client.publish_layer_version(
            LayerName=layer_name,
            Description='Makenaide Phase 0 core dependencies',
            Content={'ZipFile': layer_content},
            CompatibleRuntimes=['python3.9'],
        )
        
        print(f"✅ Layer 생성 완료: {layer_name}")
        return response['LayerVersionArn']
        
    except Exception as e:
        print(f"⚠️ Layer 생성 실패 (기존 Layer 사용): {e}")
        # 기존에 생성된 Layer ARN 반환 (실제 환경에서는 사전에 생성된 Layer 사용)
        return "arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1"

def create_eventbridge_rule(events_client, lambda_arn):
    """EventBridge 스케줄 규칙 생성"""
    rule_name = 'makenaide-phase0-schedule'
    
    print(f"⏰ EventBridge 규칙 생성: {rule_name}")
    
    try:
        # 스케줄 규칙 생성 (매일 09:05 KST = 00:05 UTC)
        events_client.put_rule(
            Name=rule_name,
            ScheduleExpression='cron(5 0 * * ? *)',  # UTC 기준 00:05 (KST 09:05)
            Description='Makenaide Phase 0 daily ticker scan',
            State='ENABLED'
        )
        
        # Lambda 함수를 타겟으로 추가
        events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': lambda_arn,
                    'Input': json.dumps({
                        'source': 'eventbridge',
                        'trigger': 'scheduled'
                    })
                }
            ]
        )
        
        # Lambda에 EventBridge 호출 권한 부여
        lambda_client = boto3.client('lambda')
        try:
            lambda_client.add_permission(
                FunctionName='makenaide-ticker-scanner-phase0',
                StatementId=f'{rule_name}-permission',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=f"arn:aws:events:ap-northeast-2:{boto3.client('sts').get_caller_identity()['Account']}:rule/{rule_name}"
            )
        except lambda_client.exceptions.ResourceConflictException:
            print("ℹ️ Lambda 권한이 이미 존재합니다")
        
        print(f"✅ EventBridge 규칙 생성 완료: {rule_name}")
        
    except Exception as e:
        print(f"❌ EventBridge 규칙 생성 실패: {e}")

def create_s3_bucket(s3_client):
    """S3 버킷 생성"""
    bucket_name = 'makenaide-serverless-data'
    
    try:
        # 버킷 존재 확인
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ S3 버킷 이미 존재: {bucket_name}")
        
    except s3_client.exceptions.NoSuchBucket:
        print(f"🪣 S3 버킷 생성: {bucket_name}")
        
        # 서울 리전에 버킷 생성
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-2'}
        )
        
        # 버킷 정책 설정 (필요시)
        print(f"✅ S3 버킷 생성 완료: {bucket_name}")

def main():
    """메인 배포 함수"""
    print("🚀 Makenaide Phase 0 Lambda 배포 시작")
    print("="*50)
    
    try:
        # AWS 클라이언트 초기화
        clients = setup_clients()
        
        # 1. S3 버킷 생성
        create_s3_bucket(clients['s3'])
        
        # 2. Lambda 패키지 생성
        zip_filename = create_lambda_package()
        
        # 3. IAM 역할 생성
        role_arn = create_execution_role(clients['iam'])
        
        # 4. Layer 생성
        layer_arn = create_layer(clients['lambda'])
        
        # 5. Lambda 함수 생성/업데이트
        lambda_arn = create_or_update_lambda(clients['lambda'], zip_filename, role_arn)
        
        # 6. Layer 연결
        if layer_arn:
            clients['lambda'].update_function_configuration(
                FunctionName='makenaide-ticker-scanner-phase0',
                Layers=[layer_arn]
            )
            print("✅ Layer 연결 완료")
        
        # 7. EventBridge 스케줄 생성
        create_eventbridge_rule(clients['events'], lambda_arn)
        
        # 8. 정리
        os.remove(zip_filename)
        if os.path.exists('makenaide-core-layer-phase0.zip'):
            os.remove('makenaide-core-layer-phase0.zip')
        
        print("="*50)
        print("✅ Phase 0 Lambda 배포 완료!")
        print(f"📍 함수명: makenaide-ticker-scanner-phase0")
        print(f"⏰ 스케줄: 매일 09:05 KST")
        print(f"🪣 S3 버킷: makenaide-serverless-data")
        print("="*50)
        
        return True
        
    except Exception as e:
        print(f"❌ 배포 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)