#!/usr/bin/env python3
"""
📊 Market Sentiment Check Lambda 배포 스크립트
- 시장 상황 분석하여 하락장 시 EC2/RDS 자동 종료
- 비용 절감과 거래 안전성을 동시에 확보
"""

import boto3
import json
import logging
import zipfile
import os
import shutil
from pathlib import Path
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketSentimentDeployer:
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        
        self.function_name = 'makenaide-market-sentiment-check'
        self.layer_arn = 'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1'
        self.role_arn = 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role'
        
    def create_deployment_package(self) -> str:
        """배포 패키지 생성"""
        try:
            logger.info("배포 패키지 생성 중...")
            
            # 임시 디렉토리 생성
            temp_dir = Path('lambda_sentiment_package')
            temp_dir.mkdir(exist_ok=True)
            
            # 메인 Lambda 파일 복사
            shutil.copy2('lambda_market_sentiment_check.py', temp_dir / 'lambda_function.py')
            
            # ZIP 파일 생성
            zip_path = 'makenaide-market-sentiment.zip'
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in temp_dir.rglob('*'):
                    if file_path.is_file():
                        arcname = file_path.relative_to(temp_dir)
                        zipf.write(file_path, arcname)
            
            # 임시 디렉토리 정리
            shutil.rmtree(temp_dir)
            
            logger.info(f"배포 패키지 생성 완료: {zip_path}")
            return zip_path
            
        except Exception as e:
            logger.error(f"배포 패키지 생성 실패: {e}")
            return None

    def update_lambda_role_permissions(self):
        """Lambda 실행 역할에 필요한 권한 추가"""
        try:
            logger.info("Lambda 실행 역할 권한 업데이트 중...")
            
            # EC2/RDS 제어 권한 정책
            instance_control_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ec2:StartInstances",
                            "ec2:StopInstances",
                            "ec2:DescribeInstances",
                            "rds:StartDBInstance",
                            "rds:StopDBInstance",
                            "rds:DescribeDBInstances"
                        ],
                        "Resource": "*"
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
                            "sns:Publish"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            
            # 정책 생성/업데이트
            policy_name = 'makenaide-instance-control-policy'
            try:
                self.iam_client.create_policy(
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(instance_control_policy),
                    Description='Makenaide EC2/RDS instance control policy'
                )
                logger.info("인스턴스 제어 정책 생성 완료")
            except Exception as e:
                if 'EntityAlreadyExists' in str(e):
                    logger.info("인스턴스 제어 정책이 이미 존재함")
                else:
                    logger.error(f"정책 생성 실패: {e}")
            
            # 역할에 정책 연결
            policy_arn = f"arn:aws:iam::901361833359:policy/{policy_name}"
            try:
                self.iam_client.attach_role_policy(
                    RoleName='makenaide-lambda-execution-role',
                    PolicyArn=policy_arn
                )
                logger.info("역할에 정책 연결 완료")
            except Exception as e:
                if 'EntityAlreadyExists' in str(e):
                    logger.info("정책이 이미 역할에 연결됨")
                else:
                    logger.error(f"정책 연결 실패: {e}")
                    
        except Exception as e:
            logger.error(f"Lambda 역할 권한 업데이트 실패: {e}")

    def deploy_lambda_function(self, zip_path: str) -> bool:
        """Lambda 함수 배포"""
        try:
            logger.info(f"Lambda 함수 배포: {self.function_name}")
            
            # ZIP 파일 읽기
            with open(zip_path, 'rb') as f:
                zip_content = f.read()
            
            # Lambda 함수 생성/업데이트
            try:
                # 기존 함수 업데이트
                response = self.lambda_client.update_function_code(
                    FunctionName=self.function_name,
                    ZipFile=zip_content
                )
                logger.info("기존 Lambda 함수 코드 업데이트 완료")
                
                # 함수 설정 업데이트
                self.lambda_client.update_function_configuration(
                    FunctionName=self.function_name,
                    Runtime='python3.9',
                    Handler='lambda_function.lambda_handler',
                    Role=self.role_arn,
                    Timeout=300,  # 5분
                    MemorySize=512,
                    Layers=[self.layer_arn],
                    Environment={
                        'Variables': {
                            'EC2_INSTANCE_IDS': '',  # 수동 설정 필요
                            'RDS_INSTANCE_ID': '',   # 수동 설정 필요
                            'SNS_TOPIC_ARN': ''      # 수동 설정 필요
                        }
                    }
                )
                logger.info("Lambda 함수 설정 업데이트 완료")
                
            except self.lambda_client.exceptions.ResourceNotFoundException:
                # 새 함수 생성
                response = self.lambda_client.create_function(
                    FunctionName=self.function_name,
                    Runtime='python3.9',
                    Role=self.role_arn,
                    Handler='lambda_function.lambda_handler',
                    Code={'ZipFile': zip_content},
                    Description='Market sentiment analysis and resource control',
                    Timeout=300,
                    MemorySize=512,
                    Layers=[self.layer_arn],
                    Environment={
                        'Variables': {
                            'EC2_INSTANCE_IDS': '',  # 수동 설정 필요
                            'RDS_INSTANCE_ID': '',   # 수동 설정 필요
                            'SNS_TOPIC_ARN': ''      # 수동 설정 필요
                        }
                    }
                )
                logger.info("새 Lambda 함수 생성 완료")
            
            function_arn = response['FunctionArn']
            logger.info(f"Lambda 함수 ARN: {function_arn}")
            return True
            
        except Exception as e:
            logger.error(f"Lambda 함수 배포 실패: {e}")
            return False

    def setup_eventbridge_schedule(self) -> bool:
        """EventBridge 스케줄 설정"""
        try:
            logger.info("EventBridge 스케줄 설정 중...")
            
            rule_name = 'makenaide-market-sentiment-daily'
            
            # 스케줄 규칙 생성 (매일 08:30 KST)
            # UTC로 변환: 08:30 KST = 23:30 UTC (전일)
            schedule_expression = 'cron(30 23 * * ? *)'
            
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    ScheduleExpression=schedule_expression,
                    Description='Daily market sentiment check at 08:30 KST',
                    State='ENABLED'
                )
                logger.info("EventBridge 규칙 생성 완료")
                
                # Lambda 함수를 타겟으로 추가
                function_arn = f"arn:aws:lambda:ap-northeast-2:901361833359:function:{self.function_name}"
                
                self.events_client.put_targets(
                    Rule=rule_name,
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': function_arn
                        }
                    ]
                )
                logger.info("EventBridge 타겟 설정 완료")
                
                # Lambda 함수에 EventBridge 실행 권한 부여
                try:
                    self.lambda_client.add_permission(
                        FunctionName=self.function_name,
                        StatementId='allow-eventbridge',
                        Action='lambda:InvokeFunction',
                        Principal='events.amazonaws.com',
                        SourceArn=f"arn:aws:events:ap-northeast-2:901361833359:rule/{rule_name}"
                    )
                    logger.info("Lambda 실행 권한 부여 완료")
                except Exception as e:
                    if 'ResourceConflictException' in str(e):
                        logger.info("Lambda 실행 권한이 이미 존재함")
                    else:
                        logger.error(f"Lambda 실행 권한 부여 실패: {e}")
                
                return True
                
            except Exception as e:
                logger.error(f"EventBridge 설정 실패: {e}")
                return False
                
        except Exception as e:
            logger.error(f"EventBridge 스케줄 설정 실패: {e}")
            return False

    def cleanup_deployment_files(self, zip_path: str):
        """배포 파일 정리"""
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
                logger.info("배포 파일 정리 완료")
        except Exception as e:
            logger.error(f"파일 정리 실패: {e}")

    def print_manual_setup_instructions(self):
        """수동 설정 안내"""
        logger.info("\n" + "="*80)
        logger.info("🔧 수동 설정이 필요한 항목:")
        logger.info("="*80)
        
        print(f"""
1. EC2 인스턴스 ID 설정:
   aws lambda update-function-configuration \\
     --function-name {self.function_name} \\
     --environment Variables='{{
       "EC2_INSTANCE_IDS": "i-1234567890abcdef0,i-0987654321fedcba0",
       "RDS_INSTANCE_ID": "makenaide-db-instance",
       "SNS_TOPIC_ARN": "arn:aws:sns:ap-northeast-2:901361833359:makenaide-alerts"
     }}'

2. SNS 토픽 생성 (필요시):
   aws sns create-topic --name makenaide-alerts
   aws sns subscribe --topic-arn <생성된-topic-arn> --protocol email --notification-endpoint your-email@example.com

3. 스케줄 확인:
   - 매일 08:30 KST (23:30 UTC 전일)에 실행
   - 시장 상황 분석 후 BULL/NEUTRAL → 파이프라인 시작
   - BEAR → EC2/RDS 종료 및 비용 절감

4. 테스트 실행:
   aws lambda invoke --function-name {self.function_name} /tmp/test-result.json
        """)
        
        logger.info("="*80)

def main():
    """메인 실행"""
    try:
        logger.info("🚀 Market Sentiment Check Lambda 배포 시작")
        
        deployer = MarketSentimentDeployer()
        
        # 1. Lambda 역할 권한 업데이트
        deployer.update_lambda_role_permissions()
        
        # 2. 배포 패키지 생성
        zip_path = deployer.create_deployment_package()
        if not zip_path:
            logger.error("배포 패키지 생성 실패")
            return False
        
        # 3. Lambda 함수 배포
        lambda_deployed = deployer.deploy_lambda_function(zip_path)
        if not lambda_deployed:
            logger.error("Lambda 함수 배포 실패")
            return False
        
        # 4. EventBridge 스케줄 설정
        schedule_setup = deployer.setup_eventbridge_schedule()
        if not schedule_setup:
            logger.error("EventBridge 스케줄 설정 실패")
            return False
        
        # 5. 배포 파일 정리
        deployer.cleanup_deployment_files(zip_path)
        
        # 6. 수동 설정 안내
        deployer.print_manual_setup_instructions()
        
        logger.info("✅ Market Sentiment Check Lambda 배포 완료!")
        return True
        
    except Exception as e:
        logger.error(f"배포 실패: {e}")
        return False

if __name__ == '__main__':
    main()