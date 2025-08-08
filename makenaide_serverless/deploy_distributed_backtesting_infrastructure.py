#!/usr/bin/env python3
"""
분산 백테스팅 인프라 배포 스크립트

Phase 1: 핵심 인프라 구축
- SQS 큐 시스템 (작업 큐, 결과 큐, DLQ)
- DynamoDB 테이블 (작업 추적, 결과 저장) 
- 기본 Lambda 함수 프레임워크

Author: Distributed Backtesting Infrastructure
Version: 1.0.0
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedBacktestingInfrastructure:
    """분산 백테스팅 인프라 배포 클래스"""
    
    def __init__(self, region_name: str = 'ap-northeast-2'):
        self.region = region_name
        self.sqs_client = boto3.client('sqs', region_name=region_name)
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.iam_client = boto3.client('iam', region_name=region_name)
        
        # 리소스 이름 접두사
        self.resource_prefix = "makenaide-distributed-backtest"
        
        logger.info(f"🚀 분산 백테스팅 인프라 배포 준비 완료 (리전: {region_name})")
    
    def deploy_sqs_queues(self) -> Dict[str, str]:
        """SQS 큐 시스템 배포"""
        try:
            logger.info("📡 SQS 큐 시스템 배포 시작")
            
            queue_configs = {
                # 1. 백테스트 작업 큐 (메인)
                f"{self.resource_prefix}-job-queue": {
                    "Attributes": {
                        'VisibilityTimeout': '900',  # 15분
                        'MessageRetentionPeriod': '1209600',  # 14일
                        'ReceiveMessageWaitTimeSeconds': '20',  # Long polling
                        'RedrivePolicy': json.dumps({
                            'deadLetterTargetArn': f"arn:aws:sqs:{self.region}:{{AccountId}}:{self.resource_prefix}-dlq",
                            'maxReceiveCount': 3
                        })
                    }
                },
                
                # 2. 우선순위 작업 큐 (FIFO)
                f"{self.resource_prefix}-priority-queue.fifo": {
                    "Attributes": {
                        'FifoQueue': 'true',
                        'ContentBasedDeduplication': 'true',
                        'VisibilityTimeout': '600',
                        'MessageRetentionPeriod': '1209600'
                    }
                },
                
                # 3. 결과 수집 큐
                f"{self.resource_prefix}-result-queue": {
                    "Attributes": {
                        'VisibilityTimeout': '300',  # 5분
                        'MessageRetentionPeriod': '604800',  # 7일
                        'ReceiveMessageWaitTimeSeconds': '20'
                    }
                },
                
                # 4. Dead Letter Queue
                f"{self.resource_prefix}-dlq": {
                    "Attributes": {
                        'MessageRetentionPeriod': '1209600'  # 14일
                    }
                }
            }
            
            created_queues = {}
            
            for queue_name, config in queue_configs.items():
                try:
                    # DLQ는 다른 큐들보다 먼저 생성되어야 함
                    if 'dlq' in queue_name:
                        response = self.sqs_client.create_queue(
                            QueueName=queue_name,
                            Attributes=config['Attributes']
                        )
                        created_queues[queue_name] = response['QueueUrl']
                        logger.info(f"✅ DLQ 생성 완료: {queue_name}")
                
                except self.sqs_client.exceptions.QueueNameExists:
                    # 이미 존재하는 큐의 URL 가져오기
                    response = self.sqs_client.get_queue_url(QueueName=queue_name)
                    created_queues[queue_name] = response['QueueUrl']
                    logger.info(f"🔄 기존 DLQ 사용: {queue_name}")
            
            # 나머지 큐 생성 (DLQ ARN을 알고 나서)
            account_id = boto3.client('sts').get_caller_identity()['Account']
            
            for queue_name, config in queue_configs.items():
                if 'dlq' in queue_name:
                    continue  # 이미 처리됨
                
                try:
                    # RedrivePolicy의 AccountId 치환
                    if 'RedrivePolicy' in config['Attributes']:
                        redrive_policy = config['Attributes']['RedrivePolicy']
                        config['Attributes']['RedrivePolicy'] = redrive_policy.replace(
                            '{AccountId}', account_id
                        )
                    
                    response = self.sqs_client.create_queue(
                        QueueName=queue_name,
                        Attributes=config['Attributes']
                    )
                    created_queues[queue_name] = response['QueueUrl']
                    logger.info(f"✅ 큐 생성 완료: {queue_name}")
                    
                except self.sqs_client.exceptions.QueueNameExists:
                    response = self.sqs_client.get_queue_url(QueueName=queue_name)
                    created_queues[queue_name] = response['QueueUrl']
                    logger.info(f"🔄 기존 큐 사용: {queue_name}")
                
                except Exception as e:
                    logger.error(f"❌ 큐 생성 실패 ({queue_name}): {e}")
            
            logger.info(f"🎉 SQS 큐 시스템 배포 완료: {len(created_queues)}개 큐")
            return created_queues
            
        except Exception as e:
            logger.error(f"❌ SQS 큐 시스템 배포 실패: {e}")
            return {}
    
    def deploy_dynamodb_tables(self) -> Dict[str, str]:
        """DynamoDB 테이블 배포"""
        try:
            logger.info("🗄️ DynamoDB 테이블 배포 시작")
            
            table_configs = {
                # 1. 백테스트 작업 추적 테이블
                f"{self.resource_prefix}-jobs": {
                    "KeySchema": [
                        {
                            'AttributeName': 'job_id',
                            'KeyType': 'HASH'
                        }
                    ],
                    "AttributeDefinitions": [
                        {
                            'AttributeName': 'job_id',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'status',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'created_at',
                            'AttributeType': 'S'
                        }
                    ],
                    "GlobalSecondaryIndexes": [
                        {
                            'IndexName': 'status-created_at-index',
                            'KeySchema': [
                                {
                                    'AttributeName': 'status',
                                    'KeyType': 'HASH'
                                },
                                {
                                    'AttributeName': 'created_at', 
                                    'KeyType': 'RANGE'
                                }
                            ],
                            'Projection': {
                                'ProjectionType': 'ALL'
                            }
                        }
                    ]
                },
                
                # 2. 백테스트 결과 저장 테이블
                f"{self.resource_prefix}-results": {
                    "KeySchema": [
                        {
                            'AttributeName': 'job_id',
                            'KeyType': 'HASH'
                        }
                    ],
                    "AttributeDefinitions": [
                        {
                            'AttributeName': 'job_id',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'strategy_name',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'completed_at',
                            'AttributeType': 'S'
                        }
                    ],
                    "GlobalSecondaryIndexes": [
                        {
                            'IndexName': 'strategy-completed_at-index',
                            'KeySchema': [
                                {
                                    'AttributeName': 'strategy_name',
                                    'KeyType': 'HASH'
                                },
                                {
                                    'AttributeName': 'completed_at',
                                    'KeyType': 'RANGE'
                                }
                            ],
                            'Projection': {
                                'ProjectionType': 'ALL'
                            }
                        }
                    ]
                },
                
                # 3. 백테스트 세션 관리 테이블
                f"{self.resource_prefix}-sessions": {
                    "KeySchema": [
                        {
                            'AttributeName': 'session_id',
                            'KeyType': 'HASH'
                        }
                    ],
                    "AttributeDefinitions": [
                        {
                            'AttributeName': 'session_id',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'created_at',
                            'AttributeType': 'S'
                        }
                    ],
                    "GlobalSecondaryIndexes": [
                        {
                            'IndexName': 'created_at-index',
                            'KeySchema': [
                                {
                                    'AttributeName': 'created_at',
                                    'KeyType': 'HASH'
                                }
                            ],
                            'Projection': {
                                'ProjectionType': 'ALL'
                            }
                        }
                    ]
                }
            }
            
            created_tables = {}
            
            for table_name, config in table_configs.items():
                try:
                    table = self.dynamodb.create_table(
                        TableName=table_name,
                        BillingMode='PAY_PER_REQUEST',
                        **config
                    )
                    
                    # 테이블 생성 대기
                    table.wait_until_exists()
                    created_tables[table_name] = table.table_arn
                    logger.info(f"✅ 테이블 생성 완료: {table_name}")
                    
                except Exception as e:
                    if "already exists" in str(e) or "ResourceInUseException" in str(e):
                        table = self.dynamodb.Table(table_name)
                        created_tables[table_name] = table.table_arn
                        logger.info(f"🔄 기존 테이블 사용: {table_name}")
                    else:
                        logger.error(f"❌ 테이블 생성 실패 ({table_name}): {e}")
            
            logger.info(f"🎉 DynamoDB 테이블 배포 완료: {len(created_tables)}개 테이블")
            return created_tables
            
        except Exception as e:
            logger.error(f"❌ DynamoDB 테이블 배포 실패: {e}")
            return {}
    
    def create_lambda_execution_role(self) -> str:
        """Lambda 실행 역할 생성"""
        try:
            role_name = f"{self.resource_prefix}-lambda-role"
            
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
            
            # 권한 정책
            permission_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream", 
                            "logs:PutLogEvents"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:SendMessage",
                            "sqs:GetQueueAttributes"
                        ],
                        "Resource": f"arn:aws:sqs:{self.region}:*:{self.resource_prefix}-*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:GetItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:Query",
                            "dynamodb:Scan"
                        ],
                        "Resource": f"arn:aws:dynamodb:{self.region}:*:table/{self.resource_prefix}-*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject"
                        ],
                        "Resource": "arn:aws:s3:::makenaide-*/*"
                    }
                ]
            }
            
            try:
                # 역할 생성
                response = self.iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description="Distributed Backtesting Lambda Execution Role"
                )
                role_arn = response['Role']['Arn']
                logger.info(f"✅ Lambda 실행 역할 생성: {role_name}")
                
                # 권한 정책 첨부
                self.iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=f"{role_name}-policy", 
                    PolicyDocument=json.dumps(permission_policy)
                )
                
                # 기본 Lambda 실행 역할 첨부
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
                )
                
                return role_arn
                
            except Exception as e:
                if "already exists" in str(e):
                    response = self.iam_client.get_role(RoleName=role_name)
                    role_arn = response['Role']['Arn']
                    logger.info(f"🔄 기존 Lambda 역할 사용: {role_name}")
                    return role_arn
                else:
                    raise e
            
        except Exception as e:
            logger.error(f"❌ Lambda 실행 역할 생성 실패: {e}")
            return ""
    
    def deploy_infrastructure(self) -> Dict[str, Any]:
        """전체 인프라 배포"""
        try:
            logger.info("🏗️ 분산 백테스팅 인프라 전체 배포 시작")
            deployment_start = datetime.now()
            
            results = {
                "deployment_timestamp": deployment_start.isoformat(),
                "region": self.region,
                "resource_prefix": self.resource_prefix
            }
            
            # 1. SQS 큐 시스템 배포
            logger.info("\n📡 1단계: SQS 큐 시스템 배포")
            sqs_queues = self.deploy_sqs_queues()
            results["sqs_queues"] = sqs_queues
            
            # 2. DynamoDB 테이블 배포
            logger.info("\n🗄️ 2단계: DynamoDB 테이블 배포")
            dynamodb_tables = self.deploy_dynamodb_tables()
            results["dynamodb_tables"] = dynamodb_tables
            
            # 3. Lambda 실행 역할 생성
            logger.info("\n🔐 3단계: Lambda 실행 역할 생성")
            lambda_role_arn = self.create_lambda_execution_role()
            results["lambda_role_arn"] = lambda_role_arn
            
            # 배포 완료 시간
            deployment_end = datetime.now()
            deployment_duration = (deployment_end - deployment_start).total_seconds()
            results["deployment_duration_seconds"] = deployment_duration
            
            # 배포 요약
            logger.info(f"\n🎉 분산 백테스팅 인프라 배포 완료!")
            logger.info(f"   ⏱️  배포 소요 시간: {deployment_duration:.2f}초")
            logger.info(f"   📡 SQS 큐: {len(sqs_queues)}개")
            logger.info(f"   🗄️  DynamoDB 테이블: {len(dynamodb_tables)}개")
            logger.info(f"   🔐 Lambda 역할: {'생성됨' if lambda_role_arn else '실패'}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 인프라 배포 실패: {e}")
            return {"error": str(e)}

def main():
    """메인 실행 함수"""
    print("🚀 분산 백테스팅 인프라 배포 시작")
    print("=" * 80)
    
    try:
        # 인프라 배포 실행
        infrastructure = DistributedBacktestingInfrastructure()
        results = infrastructure.deploy_infrastructure()
        
        if "error" not in results:
            print("\n✅ Phase 1: 핵심 인프라 구축 완료!")
            print("📋 배포된 리소스:")
            
            # SQS 큐 목록
            if results.get("sqs_queues"):
                print(f"   📡 SQS 큐 ({len(results['sqs_queues'])}개):")
                for queue_name in results["sqs_queues"].keys():
                    print(f"      • {queue_name}")
            
            # DynamoDB 테이블 목록
            if results.get("dynamodb_tables"):
                print(f"   🗄️  DynamoDB 테이블 ({len(results['dynamodb_tables'])}개):")
                for table_name in results["dynamodb_tables"].keys():
                    print(f"      • {table_name}")
            
            # Lambda 역할
            if results.get("lambda_role_arn"):
                role_name = results["lambda_role_arn"].split("/")[-1]
                print(f"   🔐 Lambda 실행 역할: {role_name}")
            
            print(f"\n🎯 다음 단계: Phase 2 - 분산 처리 엔진 개발")
            
        else:
            print(f"❌ 인프라 배포 실패: {results['error']}")
    
    except Exception as e:
        print(f"❌ 전체 배포 실패: {e}")

if __name__ == "__main__":
    main()