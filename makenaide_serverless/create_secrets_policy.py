#!/usr/bin/env python3
"""
Lambda Secrets Manager 정책 생성 스크립트
"""

import boto3
import json
from botocore.exceptions import ClientError

def create_lambda_secrets_policy():
    """
    Lambda 함수들이 Secrets Manager에 접근할 수 있도록 IAM 정책을 생성합니다.
    """
    print(f"🔄 Creating IAM policy for Lambda Secrets Manager access...")
    
    iam = boto3.client('iam', region_name='ap-northeast-2')
    
    # Secrets Manager 접근 정책 생성
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "MakenaidSecretsManagerAccess",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret"
                ],
                "Resource": [
                    "arn:aws:secretsmanager:ap-northeast-2:901361833359:secret:makenaide/upbit/api-keys*"
                ]
            }
        ]
    }
    
    policy_name = "MakenaideSecretsManagerLambdaAccess"
    
    try:
        # 기존 정책 삭제 (있다면)
        try:
            iam.delete_policy(PolicyArn=f"arn:aws:iam::901361833359:policy/{policy_name}")
            print(f"   Deleted existing policy: {policy_name}")
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                print(f"   Warning: {e.response['Error']['Message']}")
        
        # 새 정책 생성
        response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description="Makenaide Lambda functions Secrets Manager access policy"
        )
        
        policy_arn = response['Policy']['Arn']
        print(f"✅ Created IAM policy: {policy_arn}")
        
        # Lambda 실행 역할에 정책 연결
        lambda_roles = ["makenaide-lambda-execution-role", "makenaide-lambda-role"]
        
        for role_name in lambda_roles:
            try:
                iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                print(f"✅ Attached policy to role: {role_name}")
            except ClientError as e:
                print(f"⚠️  Could not attach to {role_name}: {e.response['Error']['Message']}")
        
        return policy_arn
        
    except ClientError as e:
        print(f"❌ Error creating IAM policy: {e.response['Error']['Message']}")
        return None

if __name__ == "__main__":
    print("🚀 Makenaide Secrets Manager Policy Setup")
    print("=" * 50)
    
    policy_arn = create_lambda_secrets_policy()
    
    if policy_arn:
        print(f"\n✅ Lambda Secrets Manager policy created successfully!")
        print(f"   Policy ARN: {policy_arn}")
        print(f"\n📝 Next steps:")
        print(f"   1. Create Upbit API secret manually in AWS Console")
        print(f"   2. Use secret name: makenaide/upbit/api-keys")
        print(f"   3. Lambda functions will have access automatically")
    else:
        print(f"\n❌ Policy creation failed")