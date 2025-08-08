#!/usr/bin/env python3
"""
Makenaide Secrets Manager Setup Script
업비트 API 키 보안 저장 및 암호화 관리
"""

import boto3
import json
import getpass
from botocore.exceptions import ClientError

def create_upbit_api_secret():
    """
    업비트 API 키를 AWS Secrets Manager에 안전하게 저장합니다.
    """
    print("🔐 Upbit API Keys Setup for AWS Secrets Manager")
    print("=" * 60)
    
    # Secrets Manager 클라이언트 생성
    secrets_client = boto3.client('secretsmanager', region_name='ap-northeast-2')
    
    # Secret 이름 정의
    secret_name = "makenaide/upbit/api-keys"
    
    # 기존 시크릿 확인
    try:
        response = secrets_client.describe_secret(SecretId=secret_name)
        print(f"⚠️  Secret '{secret_name}' already exists!")
        print(f"   Created: {response['CreatedDate'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Last Modified: {response['LastChangedDate'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        update_choice = input("\n🔄 Update existing secret? (y/N): ").lower()
        if update_choice != 'y':
            print("ℹ️  Keeping existing secret unchanged.")
            return secret_name
            
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            print(f"❌ Error checking secret: {e.response['Error']['Message']}")
            return None
        # 시크릿이 존재하지 않으면 생성 진행
    
    # API 키 입력받기
    print(f"\n📝 Please enter your Upbit API credentials:")
    print(f"   (These will be encrypted and stored securely in AWS)")
    
    try:
        access_key = getpass.getpass("🔑 Upbit Access Key: ").strip()
        secret_key = getpass.getpass("🔑 Upbit Secret Key: ").strip()
        
        if not access_key or not secret_key:
            print("❌ Both Access Key and Secret Key are required!")
            return None
            
        # API 키 검증 (간단한 형식 확인)
        if len(access_key) < 20 or len(secret_key) < 40:
            print("⚠️  Warning: API keys seem to be shorter than expected.")
            proceed = input("Continue anyway? (y/N): ").lower()
            if proceed != 'y':
                return None
        
        # Secret 값 구성
        secret_value = {
            "upbit_access_key": access_key,
            "upbit_secret_key": secret_key,
            "description": "Makenaide Upbit API Keys for automated trading",
            "created_by": "Makenaide Setup Script",
            "environment": "production"
        }
        
        # Secret 생성 또는 업데이트
        try:
            if 'response' in locals():  # 기존 시크릿이 있으면 업데이트
                secrets_client.update_secret(
                    SecretId=secret_name,
                    SecretString=json.dumps(secret_value),
                    Description="Makenaide Upbit API Keys - Updated"
                )
                print(f"✅ Successfully updated secret: {secret_name}")
            else:  # 새 시크릿 생성
                secrets_client.create_secret(
                    Name=secret_name,
                    Description="Makenaide Upbit API Keys for automated cryptocurrency trading",
                    SecretString=json.dumps(secret_value),
                    Tags=[
                        {'Key': 'Project', 'Value': 'Makenaide'},
                        {'Key': 'Environment', 'Value': 'Production'},
                        {'Key': 'Purpose', 'Value': 'UpbitAPI'},
                        {'Key': 'Sensitive', 'Value': 'True'}
                    ]
                )
                print(f"✅ Successfully created secret: {secret_name}")
                
        except ClientError as e:
            print(f"❌ Error managing secret: {e.response['Error']['Message']}")
            return None
            
        return secret_name
        
    except KeyboardInterrupt:
        print(f"\n❌ Setup cancelled by user.")
        return None

def create_lambda_secrets_policy():
    """
    Lambda 함수들이 Secrets Manager에 접근할 수 있도록 IAM 정책을 생성합니다.
    """
    print(f"\n🔄 Creating IAM policy for Lambda Secrets Manager access...")
    
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

def test_secret_access():
    """
    생성된 시크릿에 대한 접근을 테스트합니다.
    """
    print(f"\n🧪 Testing secret access...")
    
    secrets_client = boto3.client('secretsmanager', region_name='ap-northeast-2')
    secret_name = "makenaide/upbit/api-keys"
    
    try:
        # 시크릿 값 조회 테스트
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        
        # 민감한 정보는 마스킹해서 표시
        access_key = secret_data.get('upbit_access_key', '')
        secret_key = secret_data.get('upbit_secret_key', '')
        
        masked_access = access_key[:8] + '*' * (len(access_key) - 12) + access_key[-4:] if len(access_key) > 12 else '*' * len(access_key)
        masked_secret = secret_key[:8] + '*' * (len(secret_key) - 12) + secret_key[-4:] if len(secret_key) > 12 else '*' * len(secret_key)
        
        print(f"✅ Secret access test successful!")
        print(f"   Access Key: {masked_access}")
        print(f"   Secret Key: {masked_secret}")
        print(f"   Description: {secret_data.get('description', 'N/A')}")
        
        return True
        
    except ClientError as e:
        print(f"❌ Secret access test failed: {e.response['Error']['Message']}")
        return False

def create_secret_access_function_example():
    """
    Lambda 함수에서 시크릿에 접근하는 예제 코드를 생성합니다.
    """
    example_code = '''
# Lambda 함수에서 Upbit API 키 사용 예제
import boto3
import json
from botocore.exceptions import ClientError

def get_upbit_api_keys():
    """AWS Secrets Manager에서 Upbit API 키를 가져옵니다."""
    
    secret_name = "makenaide/upbit/api-keys"
    region_name = "ap-northeast-2"
    
    # Secrets Manager 클라이언트 생성
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        # 시크릿 값 조회
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        
        return {
            'access_key': secret['upbit_access_key'],
            'secret_key': secret['upbit_secret_key']
        }
        
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

# Lambda 함수에서 사용 예시
def lambda_handler(event, context):
    try:
        # API 키 가져오기
        api_keys = get_upbit_api_keys()
        access_key = api_keys['access_key']
        secret_key = api_keys['secret_key']
        
        # 여기에서 Upbit API 호출 로직 구현
        # ...
        
        return {
            'statusCode': 200,
            'body': json.dumps('API keys loaded successfully')
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
'''
    
    print(f"\n📝 Example Lambda function code saved to: lambda_secrets_example.py")
    
    with open('/Users/13ruce/makenaide/lambda_secrets_example.py', 'w', encoding='utf-8') as f:
        f.write(example_code)
    
    return True

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide Secrets Manager Setup")
    print("=" * 50)
    
    # 1. Upbit API 시크릿 생성
    secret_name = create_upbit_api_secret()
    if not secret_name:
        print("❌ Failed to create/update secret. Exiting.")
        return False
    
    # 2. Lambda 권한 설정
    policy_arn = create_lambda_secrets_policy()
    if not policy_arn:
        print("⚠️  Lambda permissions setup failed, but continuing...")
    
    # 3. 시크릿 접근 테스트
    if test_secret_access():
        print("✅ Secret setup and testing completed successfully!")
    else:
        print("⚠️  Secret created but access test failed.")
    
    # 4. 예제 코드 생성
    create_secret_access_function_example()
    
    print(f"\n🎉 Secrets Manager setup completed!")
    print(f"   Secret Name: {secret_name}")
    print(f"   Region: ap-northeast-2")
    print(f"   Policy ARN: {policy_arn or 'Failed to create'}")
    
    return True

if __name__ == "__main__":
    main()