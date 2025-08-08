#!/usr/bin/env python3
"""
🚀 Makenaide Complete Serverless System Deployment
- Phase 2-6 Lambda functions deployment
- DynamoDB tables, S3 bucket, Secrets Manager setup
- EventBridge rules and SNS notifications
- Complete serverless architecture deployment
"""

import boto3
import json
import os
import sys
import zipfile
import time
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MakenaideSeverlessDeployer:
    """Makenaide 서버리스 시스템 배포 클래스"""
    
    def __init__(self):
        self.session = boto3.Session()
        self.lambda_client = self.session.client('lambda')
        self.s3_client = self.session.client('s3')
        self.dynamodb = self.session.client('dynamodb')
        self.events_client = self.session.client('events')
        self.sns_client = self.session.client('sns')
        self.secrets_client = self.session.client('secretsmanager')
        self.iam_client = self.session.client('iam')
        
        # 배포 설정
        self.config = {
            'region': 'ap-northeast-2',
            's3_bucket': 'makenaide-serverless-data',
            'lambda_role': 'makenaide-lambda-execution-role',
            'function_timeout': 900,  # 15분
            'memory_size': 1024,
            'python_runtime': 'python3.11',
            'environment_variables': {
                'S3_BUCKET': 'makenaide-serverless-data',
                'REGION': 'ap-northeast-2',
                'LOOKBACK_DAYS': '30',
                'VOLUME_MULTIPLIER': '1.5',
                'MA_SLOPE_THRESHOLD': '0.5',
                'ADX_THRESHOLD': '20',
                'RSI_LOWER': '40',
                'RSI_UPPER': '70',
                'OPENAI_SECRET_NAME': 'makenaide/openai-api-key',
                'UPBIT_SECRET_NAME': 'makenaide/upbit-api-keys',
                'MAX_POSITION_PCT': '10.0',
                'MAX_TOTAL_EXPOSURE': '50.0',
                'MAX_DAILY_TRADES': '5',
                'STOP_LOSS_PCT': '8.0',
                'TAKE_PROFIT_PCT': '25.0'
            }
        }
        
        # Lambda 함수 정의
        self.lambda_functions = {
            'makenaide-phase2-comprehensive-filter': {
                'file': 'lambda_comprehensive_filter_phase2_adaptive.py',
                'handler': 'lambda_handler',
                'description': 'Phase 2: Comprehensive filtering with market condition detection',
                'layers': ['arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9']
            },
            'makenaide-phase3-gpt-analysis': {
                'file': 'lambda_gpt_analysis_phase3_v2.py',
                'handler': 'lambda_handler',
                'description': 'Phase 3: GPT-4 analysis with chart generation',
                'layers': ['arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9']
            },
            'makenaide-phase4-4h-analysis': {
                'file': 'lambda_phase4_4h_analysis.py',
                'handler': 'lambda_handler',
                'description': 'Phase 4: 4-hour technical analysis and timing',
                'layers': ['arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9']
            },
            'makenaide-phase5-condition-check': {
                'file': 'lambda_phase5_condition_check.py',
                'handler': 'lambda_handler',
                'description': 'Phase 5: Final condition validation and risk management',
                'layers': ['arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9']
            },
            'makenaide-phase6-trade-execution': {
                'file': 'lambda_phase6_trade_execution.py',
                'handler': 'lambda_handler',
                'description': 'Phase 6: Trade execution and position management',
                'layers': ['arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9']
            }
        }

    def check_and_create_s3_bucket(self) -> bool:
        """S3 버킷 확인 및 생성"""
        try:
            logger.info(f"🪣 S3 버킷 '{self.config['s3_bucket']}' 확인 중...")
            
            # 버킷 존재 여부 확인
            try:
                self.s3_client.head_bucket(Bucket=self.config['s3_bucket'])
                logger.info(f"✅ S3 버킷이 이미 존재합니다: {self.config['s3_bucket']}")
                return True
            except:
                logger.info("S3 버킷이 없어 새로 생성합니다...")
            
            # 버킷 생성
            self.s3_client.create_bucket(
                Bucket=self.config['s3_bucket'],
                CreateBucketConfiguration={
                    'LocationConstraint': self.config['region']
                }
            )
            
            # 폴더 구조 생성
            folders = ['phase1/', 'phase2/', 'phase3/', 'phase4/', 'phase5/', 'phase6/', 
                      'phase2/backups/', 'phase3/charts/', 'phase4/backups/', 'phase5/backups/']
            
            for folder in folders:
                self.s3_client.put_object(
                    Bucket=self.config['s3_bucket'],
                    Key=folder,
                    Body=''
                )
            
            logger.info(f"✅ S3 버킷 생성 완료: {self.config['s3_bucket']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 버킷 생성 실패: {e}")
            return False

    def create_dynamodb_tables(self) -> bool:
        """DynamoDB 테이블 생성"""
        try:
            logger.info("🗄️ DynamoDB 테이블 생성 중...")
            
            # Trades 테이블
            try:
                self.dynamodb.create_table(
                    TableName='makenaide-trades',
                    KeySchema=[
                        {'AttributeName': 'trade_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'trade_id', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'S'},
                        {'AttributeName': 'ticker', 'AttributeType': 'S'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'ticker-timestamp-index',
                            'KeySchema': [
                                {'AttributeName': 'ticker', 'KeyType': 'HASH'},
                                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                            ],
                            'Projection': {'ProjectionType': 'ALL'}
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                logger.info("✅ makenaide-trades 테이블 생성 완료")
            except self.dynamodb.exceptions.ResourceInUseException:
                logger.info("ℹ️ makenaide-trades 테이블이 이미 존재합니다")
            
            # Positions 테이블
            try:
                self.dynamodb.create_table(
                    TableName='makenaide-positions',
                    KeySchema=[
                        {'AttributeName': 'ticker', 'KeyType': 'HASH'},
                        {'AttributeName': 'position_id', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'ticker', 'AttributeType': 'S'},
                        {'AttributeName': 'position_id', 'AttributeType': 'S'},
                        {'AttributeName': 'status', 'AttributeType': 'S'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'status-ticker-index',
                            'KeySchema': [
                                {'AttributeName': 'status', 'KeyType': 'HASH'},
                                {'AttributeName': 'ticker', 'KeyType': 'RANGE'}
                            ],
                            'Projection': {'ProjectionType': 'ALL'}
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                logger.info("✅ makenaide-positions 테이블 생성 완료")
            except self.dynamodb.exceptions.ResourceInUseException:
                logger.info("ℹ️ makenaide-positions 테이블이 이미 존재합니다")
            
            # 테이블 활성화 대기
            time.sleep(10)
            return True
            
        except Exception as e:
            logger.error(f"❌ DynamoDB 테이블 생성 실패: {e}")
            return False

    def setup_secrets_manager(self) -> bool:
        """Secrets Manager 설정"""
        try:
            logger.info("🔐 Secrets Manager 설정 중...")
            
            # OpenAI API Key 시크릿
            try:
                self.secrets_client.create_secret(
                    Name='makenaide/openai-api-key',
                    Description='Makenaide OpenAI API Key',
                    SecretString=json.dumps({
                        'api_key': 'YOUR_OPENAI_API_KEY_HERE'
                    })
                )
                logger.info("✅ OpenAI API Key 시크릿 생성 완료")
            except self.secrets_client.exceptions.ResourceExistsException:
                logger.info("ℹ️ OpenAI API Key 시크릿이 이미 존재합니다")
            
            # Upbit API Keys 시크릿
            try:
                self.secrets_client.create_secret(
                    Name='makenaide/upbit-api-keys',
                    Description='Makenaide Upbit API Keys',
                    SecretString=json.dumps({
                        'access_key': 'YOUR_UPBIT_ACCESS_KEY_HERE',
                        'secret_key': 'YOUR_UPBIT_SECRET_KEY_HERE'
                    })
                )
                logger.info("✅ Upbit API Keys 시크릿 생성 완료")
            except self.secrets_client.exceptions.ResourceExistsException:
                logger.info("ℹ️ Upbit API Keys 시크릿이 이미 존재합니다")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Secrets Manager 설정 실패: {e}")
            return False

    def create_lambda_execution_role(self) -> str:
        """Lambda 실행 역할 생성"""
        try:
            logger.info("🔑 Lambda 실행 역할 확인/생성 중...")
            
            role_name = self.config['lambda_role']
            
            # 역할 존재 여부 확인
            try:
                response = self.iam_client.get_role(RoleName=role_name)
                logger.info(f"ℹ️ 역할이 이미 존재합니다: {role_name}")
                return response['Role']['Arn']
            except self.iam_client.exceptions.NoSuchEntityException:
                pass
            
            # 신뢰 정책
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # 역할 생성
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                Description='Makenaide Lambda execution role with comprehensive permissions'
            )
            
            # 정책 연결
            policies = [
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
                'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess',
                'arn:aws:iam::aws:policy/AmazonSNSFullAccess',
                'arn:aws:iam::aws:policy/SecretsManagerReadWrite'
            ]
            
            for policy_arn in policies:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            
            time.sleep(10)  # 역할 전파 대기
            
            logger.info(f"✅ Lambda 실행 역할 생성 완료: {role_name}")
            return response['Role']['Arn']
            
        except Exception as e:
            logger.error(f"❌ Lambda 실행 역할 생성 실패: {e}")
            return None

    def create_deployment_package(self, function_file: str) -> str:
        """배포 패키지 생성"""
        try:
            logger.info(f"📦 배포 패키지 생성 중: {function_file}")
            
            zip_filename = f"{function_file.replace('.py', '')}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Lambda 함수 파일 추가
                if os.path.exists(function_file):
                    zipf.write(function_file, 'lambda_function.py')
                else:
                    logger.error(f"❌ 함수 파일을 찾을 수 없습니다: {function_file}")
                    return None
                
                # requirements.txt가 있다면 추가
                if os.path.exists('requirements.txt'):
                    zipf.write('requirements.txt')
            
            logger.info(f"✅ 배포 패키지 생성 완료: {zip_filename}")
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 배포 패키지 생성 실패: {e}")
            return None

    def deploy_lambda_function(self, function_name: str, function_config: dict, role_arn: str) -> bool:
        """Lambda 함수 배포"""
        try:
            logger.info(f"🚀 Lambda 함수 배포 중: {function_name}")
            
            # 배포 패키지 생성
            zip_file = self.create_deployment_package(function_config['file'])
            if not zip_file:
                return False
            
            # 함수 코드 읽기
            with open(zip_file, 'rb') as f:
                zip_content = f.read()
            
            try:
                # 함수 생성
                response = self.lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime=self.config['python_runtime'],
                    Role=role_arn,
                    Handler=f"lambda_function.{function_config['handler']}",
                    Code={'ZipFile': zip_content},
                    Description=function_config['description'],
                    Timeout=self.config['function_timeout'],
                    MemorySize=self.config['memory_size'],
                    Environment={'Variables': self.config['environment_variables']},
                    Layers=function_config.get('layers', [])
                )
                logger.info(f"✅ Lambda 함수 생성 완료: {function_name}")
                
            except self.lambda_client.exceptions.ResourceConflictException:
                # 함수가 이미 존재하면 업데이트
                logger.info(f"ℹ️ 기존 함수 업데이트: {function_name}")
                
                # 함수 코드 업데이트
                self.lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_content
                )
                
                # 함수 설정 업데이트
                self.lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Runtime=self.config['python_runtime'],
                    Role=role_arn,
                    Handler=f"lambda_function.{function_config['handler']}",
                    Description=function_config['description'],
                    Timeout=self.config['function_timeout'],
                    MemorySize=self.config['memory_size'],
                    Environment={'Variables': self.config['environment_variables']},
                    Layers=function_config.get('layers', [])
                )
                logger.info(f"✅ Lambda 함수 업데이트 완료: {function_name}")
            
            # 배포 패키지 파일 삭제
            if os.path.exists(zip_file):
                os.remove(zip_file)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 배포 실패 {function_name}: {e}")
            return False

    def setup_eventbridge_rules(self) -> bool:
        """EventBridge 규칙 설정"""
        try:
            logger.info("⚡ EventBridge 규칙 설정 중...")
            
            # Phase 2 → Phase 3 규칙
            rule_name = 'makenaide-phase2-to-phase3'
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["makenaide.phase2"],
                        "detail-type": ["Phase 2 Comprehensive Filter Completed"]
                    }),
                    State='ENABLED',
                    Description='Trigger Phase 3 after Phase 2 completion'
                )
                
                # 타겟 추가
                self.events_client.put_targets(
                    Rule=rule_name,
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': f"arn:aws:lambda:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:function:makenaide-phase3-gpt-analysis"
                        }
                    ]
                )
                
                # Lambda 권한 추가
                try:
                    self.lambda_client.add_permission(
                        FunctionName='makenaide-phase3-gpt-analysis',
                        StatementId=f'{rule_name}-permission',
                        Action='lambda:InvokeFunction',
                        Principal='events.amazonaws.com',
                        SourceArn=f"arn:aws:events:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:rule/{rule_name}"
                    )
                except:
                    pass  # 권한이 이미 있을 수 있음
                    
                logger.info(f"✅ EventBridge 규칙 생성: {rule_name}")
                
            except Exception as e:
                logger.warning(f"⚠️ EventBridge 규칙 설정 실패 {rule_name}: {e}")
            
            # Phase 3 → Phase 4 규칙
            rule_name = 'makenaide-phase3-to-phase4'
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["makenaide.phase3"],
                        "detail-type": ["Phase 3 GPT Analysis Completed"]
                    }),
                    State='ENABLED',
                    Description='Trigger Phase 4 after Phase 3 completion'
                )
                
                self.events_client.put_targets(
                    Rule=rule_name,
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': f"arn:aws:lambda:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:function:makenaide-phase4-4h-analysis"
                        }
                    ]
                )
                
                try:
                    self.lambda_client.add_permission(
                        FunctionName='makenaide-phase4-4h-analysis',
                        StatementId=f'{rule_name}-permission',
                        Action='lambda:InvokeFunction',
                        Principal='events.amazonaws.com',
                        SourceArn=f"arn:aws:events:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:rule/{rule_name}"
                    )
                except:
                    pass
                    
                logger.info(f"✅ EventBridge 규칙 생성: {rule_name}")
                
            except Exception as e:
                logger.warning(f"⚠️ EventBridge 규칙 설정 실패 {rule_name}: {e}")
            
            # Phase 4 → Phase 5 규칙
            rule_name = 'makenaide-phase4-to-phase5'
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["makenaide.phase4"],
                        "detail-type": ["Phase 4 4H Analysis Completed"]
                    }),
                    State='ENABLED',
                    Description='Trigger Phase 5 after Phase 4 completion'
                )
                
                self.events_client.put_targets(
                    Rule=rule_name,
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': f"arn:aws:lambda:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:function:makenaide-phase5-condition-check"
                        }
                    ]
                )
                
                try:
                    self.lambda_client.add_permission(
                        FunctionName='makenaide-phase5-condition-check',
                        StatementId=f'{rule_name}-permission',
                        Action='lambda:InvokeFunction',
                        Principal='events.amazonaws.com',
                        SourceArn=f"arn:aws:events:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:rule/{rule_name}"
                    )
                except:
                    pass
                    
                logger.info(f"✅ EventBridge 규칙 생성: {rule_name}")
                
            except Exception as e:
                logger.warning(f"⚠️ EventBridge 규칙 설정 실패 {rule_name}: {e}")
            
            # Phase 5 → Phase 6 규칙
            rule_name = 'makenaide-phase5-to-phase6'
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["makenaide.phase5"],
                        "detail-type": ["Phase 5 Final Check Completed"]
                    }),
                    State='ENABLED',
                    Description='Trigger Phase 6 after Phase 5 completion'
                )
                
                self.events_client.put_targets(
                    Rule=rule_name,
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': f"arn:aws:lambda:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:function:makenaide-phase6-trade-execution"
                        }
                    ]
                )
                
                try:
                    self.lambda_client.add_permission(
                        FunctionName='makenaide-phase6-trade-execution',
                        StatementId=f'{rule_name}-permission',
                        Action='lambda:InvokeFunction',
                        Principal='events.amazonaws.com',
                        SourceArn=f"arn:aws:events:{self.config['region']}:{boto3.client('sts').get_caller_identity()['Account']}:rule/{rule_name}"
                    )
                except:
                    pass
                    
                logger.info(f"✅ EventBridge 규칙 생성: {rule_name}")
                
            except Exception as e:
                logger.warning(f"⚠️ EventBridge 규칙 설정 실패 {rule_name}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ EventBridge 규칙 설정 실패: {e}")
            return False

    def setup_sns_notifications(self) -> bool:
        """SNS 알림 설정"""
        try:
            logger.info("📧 SNS 알림 설정 중...")
            
            # SNS 토픽 생성
            topic_name = 'makenaide-trading-alerts'
            try:
                response = self.sns_client.create_topic(Name=topic_name)
                topic_arn = response['TopicArn']
                logger.info(f"✅ SNS 토픽 생성 완료: {topic_name}")
                
                # 환경 변수에 토픽 ARN 추가
                self.config['environment_variables']['SNS_TOPIC_ARN'] = topic_arn
                
                return True
                
            except Exception as e:
                logger.warning(f"⚠️ SNS 토픽 생성 실패: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ SNS 설정 실패: {e}")
            return False

    def deploy_complete_system(self) -> bool:
        """전체 시스템 배포"""
        try:
            logger.info("🚀 Makenaide 서버리스 시스템 배포 시작")
            
            # 1. S3 버킷 생성
            if not self.check_and_create_s3_bucket():
                logger.error("❌ S3 버킷 생성 실패")
                return False
            
            # 2. DynamoDB 테이블 생성
            if not self.create_dynamodb_tables():
                logger.error("❌ DynamoDB 테이블 생성 실패")
                return False
            
            # 3. Secrets Manager 설정
            if not self.setup_secrets_manager():
                logger.error("❌ Secrets Manager 설정 실패")
                return False
            
            # 4. Lambda 실행 역할 생성
            role_arn = self.create_lambda_execution_role()
            if not role_arn:
                logger.error("❌ Lambda 실행 역할 생성 실패")
                return False
            
            # 5. SNS 설정
            if not self.setup_sns_notifications():
                logger.warning("⚠️ SNS 설정 실패, 계속 진행")
            
            # 6. Lambda 함수들 배포
            logger.info("📦 Lambda 함수 배포 시작...")
            deployed_functions = []
            
            for function_name, function_config in self.lambda_functions.items():
                if self.deploy_lambda_function(function_name, function_config, role_arn):
                    deployed_functions.append(function_name)
                    logger.info(f"✅ {function_name} 배포 완료")
                else:
                    logger.error(f"❌ {function_name} 배포 실패")
            
            if len(deployed_functions) != len(self.lambda_functions):
                logger.warning(f"⚠️ 일부 Lambda 함수 배포 실패: {len(deployed_functions)}/{len(self.lambda_functions)}")
            
            # 7. EventBridge 규칙 설정 (Lambda 함수 배포 후)
            if not self.setup_eventbridge_rules():
                logger.warning("⚠️ EventBridge 규칙 설정 실패")
            
            logger.info("🎉 Makenaide 서버리스 시스템 배포 완료!")
            logger.info(f"📊 배포된 Lambda 함수: {len(deployed_functions)}개")
            logger.info(f"📈 S3 버킷: {self.config['s3_bucket']}")
            logger.info("🔐 Secrets Manager에서 API 키를 설정하세요:")
            logger.info("   - makenaide/openai-api-key")
            logger.info("   - makenaide/upbit-api-keys")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 전체 시스템 배포 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("🚀 Makenaide Complete Serverless System Deployment")
        print("=" * 60)
        
        deployer = MakenaideSeverlessDeployer()
        
        if deployer.deploy_complete_system():
            print("\n✅ 배포 성공!")
            print("\n📋 다음 단계:")
            print("1. AWS Secrets Manager에서 API 키 설정")
            print("   - makenaide/openai-api-key")
            print("   - makenaide/upbit-api-keys")
            print("2. 테스트 실행")
            print("3. 모니터링 및 알림 설정")
            return True
        else:
            print("\n❌ 배포 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)