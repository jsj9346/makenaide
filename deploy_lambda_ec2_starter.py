#!/usr/bin/env python3
"""
Makenaide EC2 Starter Lambda 함수 배포 스크립트
"""

import boto3
import zipfile
import json
import os
import logging
from typing import Dict

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')

# 설정
FUNCTION_NAME = 'makenaide-ec2-starter'
ROLE_NAME = 'makenaide-ec2-starter-role'
REGION = 'ap-northeast-2'
ACCOUNT_ID = '901361833359'

def create_lambda_role() -> str:
    """Lambda 실행 역할 생성"""
    try:
        # IAM 역할 정책 문서
        assume_role_policy = {
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

        # Lambda 실행 권한 정책
        lambda_execution_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "ec2:DescribeInstances",
                        "ec2:StartInstances",
                        "ec2:StopInstances"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sns:Publish"
                    ],
                    "Resource": [
                        f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:makenaide-system-alerts",
                        f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:makenaide-trading-alerts"
                    ]
                }
            ]
        }

        # IAM 역할 생성
        try:
            role_response = iam_client.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                Description='Makenaide EC2 Starter Lambda execution role'
            )
            logger.info(f"✅ IAM 역할 생성 완료: {ROLE_NAME}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            logger.info(f"ℹ️ IAM 역할 이미 존재: {ROLE_NAME}")
            role_response = iam_client.get_role(RoleName=ROLE_NAME)

        role_arn = role_response['Role']['Arn']

        # 인라인 정책 연결
        iam_client.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName='MakenaideEC2StarterPolicy',
            PolicyDocument=json.dumps(lambda_execution_policy)
        )

        logger.info(f"✅ IAM 정책 연결 완료: {ROLE_NAME}")
        return role_arn

    except Exception as e:
        logger.error(f"❌ IAM 역할 생성 실패: {e}")
        raise

def create_lambda_package() -> str:
    """Lambda 배포 패키지 생성"""
    try:
        zip_filename = 'lambda_ec2_starter.zip'

        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Lambda 함수 코드 추가
            zip_file.write('lambda_ec2_starter.py', 'lambda_function.py')

        logger.info(f"✅ Lambda 패키지 생성 완료: {zip_filename}")
        return zip_filename

    except Exception as e:
        logger.error(f"❌ Lambda 패키지 생성 실패: {e}")
        raise

def deploy_lambda_function(role_arn: str, zip_filename: str) -> str:
    """Lambda 함수 배포"""
    try:
        # ZIP 파일 읽기
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()

        # 기존 함수 확인
        try:
            lambda_client.get_function(FunctionName=FUNCTION_NAME)
            function_exists = True
            logger.info(f"ℹ️ 기존 Lambda 함수 발견: {FUNCTION_NAME}")
        except lambda_client.exceptions.ResourceNotFoundException:
            function_exists = False

        if function_exists:
            # 기존 함수 업데이트
            response = lambda_client.update_function_code(
                FunctionName=FUNCTION_NAME,
                ZipFile=zip_content
            )

            # 함수 설정 업데이트
            lambda_client.update_function_configuration(
                FunctionName=FUNCTION_NAME,
                Runtime='python3.11',
                Handler='lambda_function.lambda_handler',
                Role=role_arn,
                Timeout=300,  # 5분
                MemorySize=128,
                Description='Makenaide EC2 자동 시작 Lambda 함수'
            )

            logger.info(f"✅ Lambda 함수 업데이트 완료: {FUNCTION_NAME}")

        else:
            # 새 함수 생성
            response = lambda_client.create_function(
                FunctionName=FUNCTION_NAME,
                Runtime='python3.11',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Description='Makenaide EC2 자동 시작 Lambda 함수',
                Timeout=300,  # 5분
                MemorySize=128,
                Publish=True
            )

            logger.info(f"✅ Lambda 함수 생성 완료: {FUNCTION_NAME}")

        function_arn = response['FunctionArn']
        logger.info(f"📍 Lambda ARN: {function_arn}")

        return function_arn

    except Exception as e:
        logger.error(f"❌ Lambda 함수 배포 실패: {e}")
        raise

def cleanup_files(zip_filename: str):
    """임시 파일 정리"""
    try:
        if os.path.exists(zip_filename):
            os.remove(zip_filename)
            logger.info(f"🗑️ 임시 파일 삭제: {zip_filename}")
    except Exception as e:
        logger.warning(f"⚠️ 임시 파일 삭제 실패: {e}")

def main():
    """메인 실행 함수"""
    logger.info("🚀 Makenaide EC2 Starter Lambda 배포 시작")
    logger.info("=" * 60)

    zip_filename = None

    try:
        # 1. IAM 역할 생성
        logger.info("1️⃣ IAM 역할 생성 중...")
        role_arn = create_lambda_role()

        # 2. Lambda 패키지 생성
        logger.info("2️⃣ Lambda 패키지 생성 중...")
        zip_filename = create_lambda_package()

        # 3. Lambda 함수 배포
        logger.info("3️⃣ Lambda 함수 배포 중...")
        function_arn = deploy_lambda_function(role_arn, zip_filename)

        # 4. 결과 요약
        logger.info("\n" + "=" * 60)
        logger.info("🎉 Lambda 배포 완료!")
        logger.info(f"📍 함수 ARN: {function_arn}")
        logger.info(f"🔧 IAM 역할: {role_arn}")

        logger.info("\n🎯 다음 단계:")
        logger.info("1. EventBridge 스케줄 설정")
        logger.info("2. EC2 자동 실행 스크립트 설정")
        logger.info("3. 전체 파이프라인 테스트")

        return True

    except Exception as e:
        logger.error(f"❌ 배포 실패: {e}")
        return False

    finally:
        # 임시 파일 정리
        if zip_filename:
            cleanup_files(zip_filename)

if __name__ == "__main__":
    try:
        success = main()
        print(f"\n🎯 배포 {'성공' if success else '실패'}")

    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")

    except Exception as e:
        print(f"\n❌ 배포 실패: {e}")
        import traceback
        traceback.print_exc()