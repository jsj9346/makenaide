#!/usr/bin/env python3
"""
💼 Phase 6: Trade Execution Lambda 배포 스크립트
- 거래 실행 Lambda 함수 배포
- EventBridge 연동 설정
- Secrets Manager 설정 (Upbit API)
"""

import boto3
import json
import logging
import zipfile
import os
import shutil
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Phase6Deployer:
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        self.secrets_client = boto3.client('secretsmanager')
        
        self.function_name = 'makenaide-trade-execution-phase6'
        self.layer_arn = 'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1'
        self.role_arn = 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role'
        
    def create_deployment_package(self) -> str:
        """배포 패키지 생성"""
        try:
            logger.info("배포 패키지 생성 중...")
            
            # Lambda 파일 복사
            shutil.copy2('lambda_trade_execution_phase6.py', 'lambda_function.py')
            
            # ZIP 파일 생성
            zip_path = 'makenaide-phase6-trade-execution.zip'
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write('lambda_function.py')
            
            logger.info(f"배포 패키지 생성 완료: {zip_path}")
            return zip_path
            
        except Exception as e:
            logger.error(f"배포 패키지 생성 실패: {e}")
            return None

    def setup_secrets_manager(self):
        """Secrets Manager에 Upbit API 키 설정"""
        try:
            logger.info("Secrets Manager Upbit API 키 설정 중...")
            
            secret_name = "makenaide/upbit-api"
            
            # 더미 API 키 (실제 사용시에는 실제 키로 교체)
            dummy_secret = {
                "access_key": "DUMMY_ACCESS_KEY_FOR_SIMULATION",
                "secret_key": "DUMMY_SECRET_KEY_FOR_SIMULATION",
                "note": "시뮬레이션 모드용 더미 키. 실거래시 실제 키로 교체 필요"
            }
            
            try:
                # 시크릿 생성
                self.secrets_client.create_secret(
                    Name=secret_name,
                    Description='Upbit API credentials for Makenaide trading bot',
                    SecretString=json.dumps(dummy_secret)
                )
                logger.info("Upbit API 시크릿 생성 완료")
                
            except Exception as e:
                if 'ResourceExistsException' in str(e):
                    logger.info("Upbit API 시크릿이 이미 존재함")
                    
                    # 기존 시크릿 업데이트
                    self.secrets_client.update_secret(
                        SecretId=secret_name,
                        SecretString=json.dumps(dummy_secret)
                    )
                    logger.info("Upbit API 시크릿 업데이트 완료")
                else:
                    logger.error(f"Upbit API 시크릿 설정 실패: {e}")
                    
        except Exception as e:
            logger.error(f"Secrets Manager 설정 실패: {e}")

    def update_lambda_role_permissions(self):
        """Lambda 실행 역할에 필요한 권한 추가"""
        try:
            logger.info("Lambda 실행 역할 권한 업데이트 중...")
            
            # Phase 6 전용 권한 정책
            phase6_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:PutItem",
                            "dynamodb:GetItem", 
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:Scan",
                            "dynamodb:Query"
                        ],
                        "Resource": [
                            "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades",
                            "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades/index/*",
                            "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions",
                            "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions/index/*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "secretsmanager:GetSecretValue"
                        ],
                        "Resource": [
                            "arn:aws:secretsmanager:ap-northeast-2:901361833359:secret:makenaide/upbit-api*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "events:PutEvents"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            
            # 정책 생성/업데이트
            policy_name = 'makenaide-phase6-policy'
            try:
                self.iam_client.create_policy(
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(phase6_policy),
                    Description='Makenaide Phase 6 trade execution policy'
                )
                logger.info("Phase 6 정책 생성 완료")
            except Exception as e:
                if 'EntityAlreadyExists' in str(e):
                    logger.info("Phase 6 정책이 이미 존재함")
                else:
                    logger.error(f"Phase 6 정책 생성 실패: {e}")
            
            # 역할에 정책 연결
            policy_arn = f"arn:aws:iam::901361833359:policy/{policy_name}"
            try:
                self.iam_client.attach_role_policy(
                    RoleName='makenaide-lambda-execution-role',
                    PolicyArn=policy_arn
                )
                logger.info("역할에 Phase 6 정책 연결 완료")
            except Exception as e:
                if 'EntityAlreadyExists' in str(e):
                    logger.info("Phase 6 정책이 이미 역할에 연결됨")
                else:
                    logger.error(f"Phase 6 정책 연결 실패: {e}")
                    
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
                    MemorySize=1024,  # 거래 처리를 위해 메모리 증가
                    Layers=[self.layer_arn],
                    Environment={
                        'Variables': {
                            'UPBIT_SECRET_NAME': 'makenaide/upbit-api',
                            'SIMULATION_MODE': 'true',  # 기본 시뮬레이션 모드
                            'MAX_POSITIONS': '5',
                            'POSITION_SIZE_KRW': '100000'
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
                    Description='Phase 6: Trade execution with Upbit API integration',
                    Timeout=300,
                    MemorySize=1024,
                    Layers=[self.layer_arn],
                    Environment={
                        'Variables': {
                            'UPBIT_SECRET_NAME': 'makenaide/upbit-api',
                            'SIMULATION_MODE': 'true',
                            'MAX_POSITIONS': '5',
                            'POSITION_SIZE_KRW': '100000'
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

    def setup_eventbridge_integration(self) -> bool:
        """EventBridge Phase 5 → Phase 6 연동 설정"""
        try:
            logger.info("EventBridge Phase 5 → Phase 6 연동 설정 중...")
            
            rule_name = 'makenaide-phase5-to-phase6'
            
            # 이벤트 패턴 설정 (Phase 5 완료 시 트리거)
            event_pattern = {
                "source": ["makenaide.condition_check"],
                "detail-type": ["Condition Check Completed"]
            }
            
            try:
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps(event_pattern),
                    Description='Trigger Phase 6 when Phase 5 completes',
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
                        StatementId='allow-eventbridge-phase5-to-phase6',
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
            logger.error(f"EventBridge 연동 설정 실패: {e}")
            return False

    def cleanup_deployment_files(self, zip_path: str):
        """배포 파일 정리"""
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
            if os.path.exists('lambda_function.py'):
                os.remove('lambda_function.py')
            logger.info("배포 파일 정리 완료")
        except Exception as e:
            logger.error(f"파일 정리 실패: {e}")

    def print_manual_setup_instructions(self):
        """수동 설정 안내"""
        logger.info("\n" + "="*80)
        logger.info("🔧 Phase 6 배포 완료 - 추가 설정 안내:")
        logger.info("="*80)
        
        print(f"""
1. DynamoDB 테이블 생성 (AWS 콘솔에서):
   - 테이블명: makenaide-trades
   - 파티션 키: trade_id (String)
   - 테이블명: makenaide-positions  
   - 파티션 키: ticker (String)

2. Upbit API 키 설정 (실거래시):
   aws secretsmanager update-secret \\
     --secret-id makenaide/upbit-api \\
     --secret-string '{{"access_key": "실제키", "secret_key": "실제키"}}'

3. 시뮬레이션 모드 해제 (실거래시):
   aws lambda update-function-configuration \\
     --function-name {self.function_name} \\
     --environment Variables='{{
       "UPBIT_SECRET_NAME": "makenaide/upbit-api",
       "SIMULATION_MODE": "false",
       "MAX_POSITIONS": "5",
       "POSITION_SIZE_KRW": "100000"
     }}'

4. 테스트 실행:
   aws lambda invoke --function-name {self.function_name} /tmp/phase6-test.json

5. 현재 설정:
   - 시뮬레이션 모드: 활성화 (안전)
   - 종목당 투자금액: 10만원
   - 최대 동시 보유: 5종목
   - 손절 기준: 8%
   - 익절 단계: 20%, 40%, 80%
        """)
        
        logger.info("="*80)

def main():
    """메인 실행"""
    try:
        logger.info("🚀 Phase 6: Trade Execution Lambda 배포 시작")
        
        deployer = Phase6Deployer()
        
        # 1. Secrets Manager 설정
        deployer.setup_secrets_manager()
        
        # 2. Lambda 역할 권한 업데이트
        deployer.update_lambda_role_permissions()
        
        # 3. 배포 패키지 생성
        zip_path = deployer.create_deployment_package()
        if not zip_path:
            logger.error("배포 패키지 생성 실패")
            return False
        
        # 4. Lambda 함수 배포
        lambda_deployed = deployer.deploy_lambda_function(zip_path)
        if not lambda_deployed:
            logger.error("Lambda 함수 배포 실패")
            return False
        
        # 5. EventBridge 연동 설정
        eventbridge_setup = deployer.setup_eventbridge_integration()
        if not eventbridge_setup:
            logger.error("EventBridge 연동 설정 실패")
            return False
        
        # 6. 배포 파일 정리
        deployer.cleanup_deployment_files(zip_path)
        
        # 7. 수동 설정 안내
        deployer.print_manual_setup_instructions()
        
        logger.info("✅ Phase 6: Trade Execution Lambda 배포 완료!")
        return True
        
    except Exception as e:
        logger.error(f"배포 실패: {e}")
        return False

if __name__ == '__main__':
    main()