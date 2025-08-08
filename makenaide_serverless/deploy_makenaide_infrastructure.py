#!/usr/bin/env python3
"""
Makenaide 인프라 배포 스크립트
Lambda 함수, Step Functions, API Gateway, CloudWatch 알림을 일괄 배포
"""

import boto3
import json
import zipfile
import os
import time
import logging
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트 초기화
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
iam_client = boto3.client('iam', region_name='ap-northeast-2')
stepfunctions_client = boto3.client('stepfunctions', region_name='ap-northeast-2')
apigateway_client = boto3.client('apigateway', region_name='ap-northeast-2')
sqs_client = boto3.client('sqs', region_name='ap-northeast-2')

def create_lambda_execution_role() -> str:
    """Lambda 실행을 위한 IAM 역할 생성"""
    try:
        # 신뢰 정책
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        # IAM 역할 생성
        role_response = iam_client.create_role(
            RoleName='makenaide-lambda-execution-role',
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Makenaide Lambda execution role'
        )
        
        role_arn = role_response['Role']['Arn']
        
        # 기본 Lambda 실행 정책 연결
        iam_client.attach_role_policy(
            RoleName='makenaide-lambda-execution-role',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        
        # VPC 액세스 정책 연결 (RDS 접근용)
        iam_client.attach_role_policy(
            RoleName='makenaide-lambda-execution-role',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
        )
        
        # 추가 권한 정책 생성 및 연결
        additional_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:SendMessage",
                        "sqs:ReceiveMessage", 
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogStreams",
                        "logs:GetLogEvents",
                        "cloudwatch:PutMetricData",
                        "ec2:CreateNetworkInterface",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DeleteNetworkInterface"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        iam_client.put_role_policy(
            RoleName='makenaide-lambda-execution-role',
            PolicyName='MakenaideLambdaAdditionalPolicy',
            PolicyDocument=json.dumps(additional_policy)
        )
        
        logger.info(f"✅ IAM 역할 생성 완료: {role_arn}")
        
        # 역할 전파 대기
        time.sleep(10)
        
        return role_arn
        
    except Exception as e:
        if 'EntityAlreadyExists' in str(e):
            # 이미 존재하는 경우 ARN 반환
            role_response = iam_client.get_role(RoleName='makenaide-lambda-execution-role')
            role_arn = role_response['Role']['Arn']
            logger.info(f"✅ 기존 IAM 역할 사용: {role_arn}")
            return role_arn
        else:
            logger.error(f"❌ IAM 역할 생성 실패: {e}")
            raise

def create_lambda_deployment_package(function_name: str, source_file: str) -> str:
    """Lambda 배포 패키지 생성"""
    try:
        package_name = f"{function_name}.zip"
        
        with zipfile.ZipFile(package_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(source_file, 'lambda_function.py')
        
        logger.info(f"✅ Lambda 패키지 생성: {package_name}")
        return package_name
        
    except Exception as e:
        logger.error(f"❌ Lambda 패키지 생성 실패: {e}")
        raise

def deploy_lambda_function(function_name: str, source_file: str, role_arn: str, 
                          environment_vars: Dict[str, str], timeout: int = 300, 
                          layer_arn: str = None) -> str:
    """Lambda 함수 배포"""
    try:
        # 배포 패키지 생성
        package_file = create_lambda_deployment_package(function_name, source_file)
        
        # 패키지 읽기
        with open(package_file, 'rb') as f:
            package_data = f.read()
        
        try:
            # 함수가 이미 존재하는지 확인
            lambda_client.get_function(FunctionName=function_name)
            
            # 기존 함수 업데이트
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=package_data
            )
            
            # 환경변수 업데이트 (재시도 로직 포함)
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Environment={'Variables': environment_vars},
                        Timeout=timeout
                    )
                    break
                except lambda_client.exceptions.ResourceConflictException:
                    if attempt < max_retries - 1:
                        logger.info(f"⏳ Lambda 업데이트 대기 중... ({attempt + 1}/{max_retries})")
                        time.sleep(10)
                    else:
                        raise
            
            logger.info(f"✅ Lambda 함수 업데이트: {function_name}")
            
        except lambda_client.exceptions.ResourceNotFoundException:
            # 새 함수 생성
            create_params = {
                'FunctionName': function_name,
                'Runtime': 'python3.9',
                'Role': role_arn,
                'Handler': 'lambda_function.lambda_handler',
                'Code': {'ZipFile': package_data},
                'Environment': {'Variables': environment_vars},
                'Timeout': timeout,
                'MemorySize': 512 if 'collector' in function_name else 256
            }
            
            # Layer가 제공된 경우 추가
            if layer_arn:
                create_params['Layers'] = [layer_arn]
            
            response = lambda_client.create_function(**create_params)
            
            logger.info(f"✅ Lambda 함수 생성: {function_name}")
        
        # 패키지 파일 정리
        os.remove(package_file)
        
        return response['FunctionArn']
        
    except Exception as e:
        logger.error(f"❌ Lambda 함수 배포 실패 ({function_name}): {e}")
        raise

def create_sqs_queues() -> Dict[str, str]:
    """SQS 큐 생성"""
    try:
        queues = {}
        
        queue_configs = [
            {
                'name': 'makenaide-ohlcv-collection',
                'visibility_timeout': 900,  # 15분
                'message_retention_period': 1209600  # 14일
            },
            {
                'name': 'makenaide-filter',
                'visibility_timeout': 300,  # 5분
                'message_retention_period': 86400  # 1일
            }
        ]
        
        for config in queue_configs:
            try:
                response = sqs_client.create_queue(
                    QueueName=config['name'],
                    Attributes={
                        'VisibilityTimeout': str(config['visibility_timeout']),
                        'MessageRetentionPeriod': str(config['message_retention_period'])
                    }
                )
                queues[config['name']] = response['QueueUrl']
                logger.info(f"✅ SQS 큐 생성: {config['name']}")
                
            except Exception as e:
                if 'QueueAlreadyExists' in str(e):
                    # 기존 큐 URL 조회
                    response = sqs_client.get_queue_url(QueueName=config['name'])
                    queues[config['name']] = response['QueueUrl']
                    logger.info(f"✅ 기존 SQS 큐 사용: {config['name']}")
                else:
                    raise
        
        return queues
        
    except Exception as e:
        logger.error(f"❌ SQS 큐 생성 실패: {e}")
        raise

def deploy_step_functions(lambda_functions: Dict[str, str]) -> str:
    """Step Functions 워크플로우 배포"""
    try:
        # Step Functions 정의 파일 읽기
        with open('step_functions_definition.json', 'r', encoding='utf-8') as f:
            definition = f.read()
        
        # Step Functions 실행 역할 생성
        try:
            sf_trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            sf_role_response = iam_client.create_role(
                RoleName='makenaide-stepfunctions-execution-role',
                AssumeRolePolicyDocument=json.dumps(sf_trust_policy)
            )
            
            sf_role_arn = sf_role_response['Role']['Arn']
            
            # Lambda 호출 권한 정책
            sf_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["lambda:InvokeFunction"],
                        "Resource": list(lambda_functions.values())
                    }
                ]
            }
            
            iam_client.put_role_policy(
                RoleName='makenaide-stepfunctions-execution-role',
                PolicyName='MakenaideLambdaInvokePolicy',
                PolicyDocument=json.dumps(sf_policy)
            )
            
            time.sleep(5)  # 역할 전파 대기
            
        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                sf_role_response = iam_client.get_role(RoleName='makenaide-stepfunctions-execution-role')
                sf_role_arn = sf_role_response['Role']['Arn']
            else:
                raise
        
        try:
            # 기존 State Machine 확인
            response = stepfunctions_client.describe_state_machine(
                stateMachineArn=f"arn:aws:states:ap-northeast-2:901361833359:stateMachine:makenaide-workflow"
            )
            
            # 기존 워크플로우 업데이트
            response = stepfunctions_client.update_state_machine(
                stateMachineArn=response['stateMachineArn'],
                definition=definition,
                roleArn=sf_role_arn
            )
            
            logger.info("✅ Step Functions 워크플로우 업데이트")
            
        except stepfunctions_client.exceptions.StateMachineDoesNotExist:
            # 새 워크플로우 생성
            response = stepfunctions_client.create_state_machine(
                name='makenaide-workflow',
                definition=definition,
                roleArn=sf_role_arn,
                type='STANDARD'
            )
            
            logger.info("✅ Step Functions 워크플로우 생성")
        
        return response['stateMachineArn']
        
    except Exception as e:
        logger.error(f"❌ Step Functions 배포 실패: {e}")
        raise

def deploy_api_gateway(lambda_function_arn: str) -> str:
    """API Gateway 배포"""
    try:
        # REST API 생성
        api_response = apigateway_client.create_rest_api(
            name='makenaide-api',
            description='Makenaide 시스템 REST API',
            endpointConfiguration={'types': ['REGIONAL']}
        )
        
        api_id = api_response['id']
        
        # 루트 리소스 ID 조회
        resources_response = apigateway_client.get_resources(restApiId=api_id)
        root_resource_id = None
        for resource in resources_response['items']:
            if resource['path'] == '/':
                root_resource_id = resource['id']
                break
        
        # API 엔드포인트 정의
        endpoints = [
            {'path': 'status', 'method': 'GET'},
            {'path': 'ticker', 'method': 'GET'},
            {'path': 'logs', 'method': 'GET'},
            {'path': 'performance', 'method': 'GET'},
            {'path': 'tickers', 'method': 'GET'}
        ]
        
        for endpoint in endpoints:
            # 리소스 생성
            resource_response = apigateway_client.create_resource(
                restApiId=api_id,
                parentId=root_resource_id,
                pathPart=endpoint['path']
            )
            
            resource_id = resource_response['id']
            
            # 메소드 생성
            apigateway_client.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=endpoint['method'],
                authorizationType='NONE'
            )
            
            # Lambda 통합 설정
            apigateway_client.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=endpoint['method'],
                type='AWS_PROXY',
                integrationHttpMethod='POST',
                uri=f"arn:aws:apigateway:ap-northeast-2:lambda:path/2015-03-31/functions/{lambda_function_arn}/invocations"
            )
            
            # CORS 옵션 메소드 추가
            apigateway_client.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                authorizationType='NONE'
            )
            
            apigateway_client.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                type='MOCK',
                requestTemplates={'application/json': '{"statusCode": 200}'}
            )
            
            # OPTIONS 응답 설정
            apigateway_client.put_method_response(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': False,
                    'method.response.header.Access-Control-Allow-Methods': False,
                    'method.response.header.Access-Control-Allow-Origin': False
                }
            )
            
            apigateway_client.put_integration_response(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                    'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS'",
                    'method.response.header.Access-Control-Allow-Origin': "'*'"
                }
            )
        
        # API 배포
        deployment_response = apigateway_client.create_deployment(
            restApiId=api_id,
            stageName='prod',
            description='Production deployment'
        )
        
        # Lambda 권한 추가 (API Gateway가 Lambda 호출할 수 있도록)
        lambda_client.add_permission(
            FunctionName=lambda_function_arn.split(':')[-1],
            StatementId='AllowAPIGatewayInvoke',
            Action='lambda:InvokeFunction',
            Principal='apigateway.amazonaws.com',
            SourceArn=f"arn:aws:execute-api:ap-northeast-2:901361833359:{api_id}/*/*"
        )
        
        api_url = f"https://{api_id}.execute-api.ap-northeast-2.amazonaws.com/prod"
        
        logger.info(f"✅ API Gateway 배포 완료: {api_url}")
        return api_url
        
    except Exception as e:
        logger.error(f"❌ API Gateway 배포 실패: {e}")
        raise

def main():
    """메인 배포 함수"""
    try:
        logger.info("🚀 Makenaide 인프라 배포 시작")
        
        # 환경변수 설정 (EC2에서 설정한 실제 자격증명 사용)
        from get_ec2_env_vars import get_ec2_env_vars
        environment_vars = get_ec2_env_vars()
        
        # 1. IAM 역할 생성
        role_arn = create_lambda_execution_role()
        
        # 2. SQS 큐 생성
        queues = create_sqs_queues()
        environment_vars['OHLCV_QUEUE_URL'] = queues['makenaide-ohlcv-collection']
        
        # 3. Lambda 함수들 배포 (Layer 포함)
        lambda_functions = {}
        layer_arn = "arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-dependencies:3"
        
        # 티커 스캐너
        lambda_functions['ticker-scanner'] = deploy_lambda_function(
            'makenaide-ticker-scanner',
            'lambda_ticker_scanner.py',
            role_arn,
            environment_vars,
            300,
            layer_arn
        )
        
        # OHLCV 수집기
        lambda_functions['ohlcv-collector'] = deploy_lambda_function(
            'makenaide-ohlcv-collector',
            'lambda_ohlcv_collector.py',
            role_arn,
            environment_vars,
            900,
            layer_arn
        )
        
        # API Gateway 백엔드
        lambda_functions['api-gateway'] = deploy_lambda_function(
            'makenaide-api-gateway',
            'lambda_api_gateway.py',
            role_arn,
            environment_vars,
            300,
            layer_arn
        )
        
        # 4. Step Functions 배포
        stepfunctions_arn = deploy_step_functions(lambda_functions)
        
        # 5. API Gateway 배포
        api_url = deploy_api_gateway(lambda_functions['api-gateway'])
        
        # 6. CloudWatch 알림 설정
        from cloudwatch_alarms import main as setup_cloudwatch
        topic_arn = setup_cloudwatch()
        
        # 배포 완료 요약
        logger.info("=" * 60)
        logger.info("✅ Makenaide 인프라 배포 완료!")
        logger.info("=" * 60)
        logger.info(f"🔗 API Gateway URL: {api_url}")
        logger.info(f"📊 Step Functions ARN: {stepfunctions_arn}")
        logger.info(f"📧 SNS Topic ARN: {topic_arn}")
        logger.info("📋 Lambda 함수들:")
        for name, arn in lambda_functions.items():
            logger.info(f"   - {name}: {arn}")
        logger.info("=" * 60)
        logger.info("📌 추가 작업 필요:")
        logger.info("   1. 환경변수에서 실제 DB 패스워드와 API 키로 교체")
        logger.info("   2. EventBridge 스케줄 설정 (4시간 간격)")
        logger.info("   3. SNS 토픽에 이메일 구독 추가")
        logger.info("=" * 60)
        
        return {
            'api_url': api_url,
            'stepfunctions_arn': stepfunctions_arn,
            'lambda_functions': lambda_functions,
            'sns_topic_arn': topic_arn
        }
        
    except Exception as e:
        logger.error(f"❌ 인프라 배포 실패: {e}")
        raise

if __name__ == "__main__":
    main() 