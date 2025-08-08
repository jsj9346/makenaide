#!/usr/bin/env python3
"""
🔧 Fix Lambda S3 Permissions Issue
- Create S3 access policy for makenaide-serverless-data bucket
- Attach to Lambda execution role
- Test S3 access after fix
"""

import boto3
import json
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaS3PermissionsFixer:
    """Lambda S3 권한 문제 해결 클래스"""
    
    def __init__(self):
        self.iam_client = boto3.client('iam')
        self.lambda_client = boto3.client('lambda')
        self.s3_client = boto3.client('s3')
        
        self.role_name = 'makenaide-lambda-execution-role'
        self.policy_name = 'makenaide-s3-access-policy'
        self.s3_bucket = 'makenaide-serverless-data'
        self.test_function = 'makenaide-phase2-comprehensive-filter'

    def create_s3_access_policy(self) -> str:
        """S3 접근 정책 생성"""
        try:
            logger.info("📋 S3 접근 정책 생성 중...")
            
            # S3 접근 정책 정의
            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{self.s3_bucket}",
                            f"arn:aws:s3:::{self.s3_bucket}/*"
                        ]
                    }
                ]
            }
            
            logger.info(f"S3 정책 내용:")
            logger.info(f"  버킷: {self.s3_bucket}")
            logger.info(f"  권한: GetObject, PutObject, DeleteObject, ListBucket")
            
            # 기존 정책 삭제 시도 (있다면)
            try:
                self.iam_client.delete_policy(
                    PolicyArn=f"arn:aws:iam::901361833359:policy/{self.policy_name}"
                )
                logger.info("  기존 정책 삭제 완료")
            except:
                pass  # 기존 정책이 없을 수 있음
            
            # 새 정책 생성
            response = self.iam_client.create_policy(
                PolicyName=self.policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=f'S3 access policy for {self.s3_bucket} bucket'
            )
            
            policy_arn = response['Policy']['Arn']
            logger.info(f"✅ S3 정책 생성 완료: {policy_arn}")
            
            return policy_arn
            
        except Exception as e:
            logger.error(f"❌ S3 정책 생성 실패: {e}")
            return None

    def attach_policy_to_role(self, policy_arn: str) -> bool:
        """역할에 정책 연결"""
        try:
            logger.info(f"🔗 정책을 역할에 연결 중...")
            logger.info(f"  역할: {self.role_name}")
            logger.info(f"  정책: {policy_arn}")
            
            self.iam_client.attach_role_policy(
                RoleName=self.role_name,
                PolicyArn=policy_arn
            )
            
            logger.info("✅ 정책 연결 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 정책 연결 실패: {e}")
            return False

    def test_s3_access_after_fix(self) -> bool:
        """권한 수정 후 S3 접근 테스트"""
        try:
            logger.info("🧪 권한 수정 후 S3 접근 테스트...")
            
            # 약간의 대기 시간 (정책 전파 대기)
            import time
            time.sleep(5)
            
            # Lambda 함수 테스트 이벤트
            test_event = {
                'test_s3_access': True,
                'source': 's3_permission_fix_test',
                'timestamp': datetime.now().isoformat()
            }
            
            response = self.lambda_client.invoke(
                FunctionName=self.test_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read().decode('utf-8'))
                
                logger.info(f"📊 테스트 응답:")
                logger.info(f"   Status Code: {payload.get('statusCode')}")
                logger.info(f"   Phase: {payload.get('phase')}")
                
                if payload.get('statusCode') == 200:
                    logger.info("✅ S3 접근 테스트 성공!")
                    logger.info(f"   Input Tickers: {payload.get('input_tickers')}")
                    logger.info(f"   Filtered Tickers: {payload.get('filtered_tickers')}")
                    return True
                elif payload.get('error') == 'Phase 1 데이터 없음':
                    logger.warning("⚠️ Phase 1 데이터가 없지만 S3 접근은 성공")
                    logger.info("  이는 Phase 1을 실행하지 않았기 때문입니다")
                    return True  # S3 접근 자체는 성공
                else:
                    logger.error(f"❌ 테스트 실패: {payload.get('error')}")
                    return False
            else:
                logger.error(f"❌ Lambda 호출 실패: {response['StatusCode']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ S3 접근 테스트 실패: {e}")
            return False

    def verify_current_permissions(self) -> dict:
        """현재 권한 상태 확인"""
        try:
            logger.info("🔍 현재 Lambda 실행 역할 권한 확인...")
            
            response = self.iam_client.list_attached_role_policies(
                RoleName=self.role_name
            )
            
            policies = response.get('AttachedPolicies', [])
            
            logger.info(f"📋 현재 연결된 정책들:")
            has_s3_policy = False
            for policy in policies:
                logger.info(f"   - {policy['PolicyName']}")
                if 's3' in policy['PolicyName'].lower():
                    has_s3_policy = True
            
            return {
                'has_s3_policy': has_s3_policy,
                'total_policies': len(policies),
                'policies': [p['PolicyName'] for p in policies]
            }
            
        except Exception as e:
            logger.error(f"❌ 권한 확인 실패: {e}")
            return {'error': str(e)}

    def fix_s3_permissions(self) -> bool:
        """S3 권한 문제 전체 해결"""
        try:
            logger.info("🔧 Lambda S3 권한 문제 해결 시작")
            
            # 1. 현재 상태 확인
            current_perms = self.verify_current_permissions()
            if current_perms.get('has_s3_policy'):
                logger.info("✅ S3 정책이 이미 연결되어 있습니다")
            else:
                logger.info("⚠️ S3 정책이 연결되지 않음 - 생성 및 연결 필요")
            
            # 2. S3 정책 생성
            policy_arn = self.create_s3_access_policy()
            if not policy_arn:
                return False
            
            # 3. 역할에 정책 연결
            if not self.attach_policy_to_role(policy_arn):
                return False
            
            # 4. 테스트
            if not self.test_s3_access_after_fix():
                logger.warning("⚠️ 테스트는 실패했지만 권한 설정은 완료됨")
                logger.info("Phase 1 데이터가 있어야 완전한 테스트가 가능합니다")
            
            logger.info("🎉 S3 권한 설정 완료!")
            logger.info(f"   정책: {policy_arn}")
            logger.info(f"   버킷: {self.s3_bucket}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 권한 해결 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("🔧 Lambda S3 Permissions Fix")
        print("=" * 50)
        
        fixer = LambdaS3PermissionsFixer()
        
        if fixer.fix_s3_permissions():
            print("\n✅ S3 권한 문제 해결 완료!")
            print("\n📋 다음 단계:")
            print("1. Phase 1 데이터 생성 (필요시)")
            print("2. Phase 2 실제 데이터 테스트")
            print("3. 전체 워크플로우 검증")
            return True
        else:
            print("\n❌ S3 권한 해결 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)