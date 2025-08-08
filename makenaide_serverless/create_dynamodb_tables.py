#!/usr/bin/env python3
"""
Makenaide DynamoDB Tables Creation Script
거래 이력 및 포지션 추적을 위한 DynamoDB 테이블 생성
"""

import boto3
import json
from botocore.exceptions import ClientError

def create_dynamodb_tables():
    """
    Makenaide 프로젝트에 필요한 DynamoDB 테이블들을 생성합니다.
    
    테이블 구조:
    1. makenaide-trades: 거래 실행 이력
    2. makenaide-positions: 현재 포지션 상태
    """
    
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.client('dynamodb', region_name='ap-northeast-2')
    
    # 1. trades 테이블 생성
    trades_table_config = {
        'TableName': 'makenaide-trades',
        'KeySchema': [
            {
                'AttributeName': 'trade_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'trade_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ticker',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'created_date',
                'AttributeType': 'S'
            }
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'ticker-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'ticker',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'BillingMode': 'PAY_PER_REQUEST'
            },
            {
                'IndexName': 'created-date-index',
                'KeySchema': [
                    {
                        'AttributeName': 'created_date',
                        'KeyType': 'HASH'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'BillingMode': 'PAY_PER_REQUEST'
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',  # 온디맨드 과금 (비용 최적화)
        'Tags': [
            {
                'Key': 'Project',
                'Value': 'Makenaide'
            },
            {
                'Key': 'Environment',
                'Value': 'Production'
            },
            {
                'Key': 'Purpose',
                'Value': 'TradeHistory'
            }
        ]
    }
    
    # 2. positions 테이블 생성
    positions_table_config = {
        'TableName': 'makenaide-positions',
        'KeySchema': [
            {
                'AttributeName': 'ticker',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'ticker',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'last_updated',
                'AttributeType': 'S'
            }
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'last-updated-index',
                'KeySchema': [
                    {
                        'AttributeName': 'last_updated',
                        'KeyType': 'HASH'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'BillingMode': 'PAY_PER_REQUEST'
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'Tags': [
            {
                'Key': 'Project',
                'Value': 'Makenaide'
            },
            {
                'Key': 'Environment',
                'Value': 'Production'
            },
            {
                'Key': 'Purpose',
                'Value': 'PositionTracking'
            }
        ]
    }
    
    tables_to_create = [
        ('makenaide-trades', trades_table_config),
        ('makenaide-positions', positions_table_config)
    ]
    
    created_tables = []
    
    for table_name, config in tables_to_create:
        try:
            print(f"\n🔄 Creating DynamoDB table: {table_name}")
            
            # 테이블 존재 여부 확인
            try:
                response = dynamodb.describe_table(TableName=table_name)
                print(f"✅ Table {table_name} already exists")
                print(f"   Status: {response['Table']['TableStatus']}")
                print(f"   Items: {response['Table'].get('ItemCount', 'Unknown')}")
                continue
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    raise e
                # 테이블이 존재하지 않으면 생성 진행
            
            # 테이블 생성
            response = dynamodb.create_table(**config)
            created_tables.append(table_name)
            
            print(f"✅ Table creation initiated: {table_name}")
            print(f"   Table ARN: {response['TableDescription']['TableArn']}")
            print(f"   Status: {response['TableDescription']['TableStatus']}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'ResourceInUseException':
                print(f"⚠️  Table {table_name} already exists")
            else:
                print(f"❌ Error creating table {table_name}: {error_code} - {error_message}")
                return False
    
    # 생성된 테이블들의 상태 확인
    if created_tables:
        print(f"\n🔄 Waiting for tables to become active...")
        
        for table_name in created_tables:
            try:
                waiter = dynamodb.get_waiter('table_exists')
                waiter.wait(
                    TableName=table_name,
                    WaiterConfig={
                        'Delay': 5,
                        'MaxAttempts': 40  # 최대 200초 대기
                    }
                )
                
                # 테이블 상태 최종 확인
                response = dynamodb.describe_table(TableName=table_name)
                table_status = response['Table']['TableStatus']
                
                if table_status == 'ACTIVE':
                    print(f"✅ Table {table_name} is now ACTIVE")
                else:
                    print(f"⚠️  Table {table_name} status: {table_status}")
                    
            except Exception as e:
                print(f"❌ Error waiting for table {table_name}: {str(e)}")
    
    print(f"\n🎉 DynamoDB tables setup completed!")
    print(f"   Created tables: {len(created_tables)}")
    
    # 테이블 정보 요약 출력
    print(f"\n📊 Table Summary:")
    for table_name, _ in tables_to_create:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            table_info = response['Table']
            
            print(f"\n📋 {table_name}:")
            print(f"   Status: {table_info['TableStatus']}")
            print(f"   Billing: {table_info['BillingModeSummary']['BillingMode']}")
            print(f"   Creation: {table_info['CreationDateTime'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            # GSI 정보
            if 'GlobalSecondaryIndexes' in table_info:
                gsi_count = len(table_info['GlobalSecondaryIndexes'])
                print(f"   GSI Count: {gsi_count}")
                for gsi in table_info['GlobalSecondaryIndexes']:
                    print(f"     - {gsi['IndexName']}: {gsi['IndexStatus']}")
                    
        except Exception as e:
            print(f"   Error getting info: {str(e)}")
    
    return True

def create_lambda_permissions():
    """
    Lambda 함수들이 DynamoDB 테이블에 접근할 수 있도록 IAM 정책을 생성합니다.
    """
    print(f"\n🔄 Creating IAM policy for Lambda DynamoDB access...")
    
    iam = boto3.client('iam', region_name='ap-northeast-2')
    
    # DynamoDB 접근 정책 생성
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "MakenaideDynamoDBAccess",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:BatchGetItem",
                    "dynamodb:BatchWriteItem"
                ],
                "Resource": [
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades/*",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions/*"
                ]
            }
        ]
    }
    
    policy_name = "MakenaideDynamoDBLambdaAccess"
    
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
            Description="Makenaide Lambda functions DynamoDB access policy"
        )
        
        policy_arn = response['Policy']['Arn']
        print(f"✅ Created IAM policy: {policy_arn}")
        
        # Lambda 실행 역할에 정책 연결
        lambda_role = "lambda-execution-role"
        try:
            iam.attach_role_policy(
                RoleName=lambda_role,
                PolicyArn=policy_arn
            )
            print(f"✅ Attached policy to role: {lambda_role}")
        except ClientError as e:
            print(f"⚠️  Could not attach to {lambda_role}: {e.response['Error']['Message']}")
            print(f"   Please manually attach policy ARN: {policy_arn}")
        
        return policy_arn
        
    except ClientError as e:
        print(f"❌ Error creating IAM policy: {e.response['Error']['Message']}")
        return None

if __name__ == "__main__":
    print("🚀 Makenaide DynamoDB Tables Setup")
    print("=" * 50)
    
    # DynamoDB 테이블 생성
    if create_dynamodb_tables():
        # Lambda 권한 설정
        create_lambda_permissions()
        print(f"\n✅ All setup completed successfully!")
    else:
        print(f"\n❌ Setup failed. Please check the errors above.")