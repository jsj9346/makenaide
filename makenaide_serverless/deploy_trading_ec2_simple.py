#!/usr/bin/env python3
"""
SQS 없이 EC2 거래 환경 설정 (간소화 버전)
파라미터는 S3 또는 DynamoDB를 통해 전달
"""

import boto3
import json
import time
from botocore.exceptions import ClientError

def get_existing_resources():
    """
    이미 생성된 리소스들 확인
    """
    print("🔍 Checking existing resources...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    # Elastic IP 확인
    addresses = ec2.describe_addresses(
        Filters=[{'Name': 'tag:Name', 'Values': ['makenaide-trading-ip']}]
    )
    
    elastic_ip = None
    allocation_id = None
    if addresses['Addresses']:
        elastic_ip = addresses['Addresses'][0]['PublicIp']
        allocation_id = addresses['Addresses'][0]['AllocationId']
        print(f"✅ Found Elastic IP: {elastic_ip}")
    
    # 보안 그룹 확인
    security_groups = ec2.describe_security_groups(
        Filters=[{'Name': 'group-name', 'Values': ['makenaide-trading-sg']}]
    )
    
    security_group_id = None
    if security_groups['SecurityGroups']:
        security_group_id = security_groups['SecurityGroups'][0]['GroupId']
        print(f"✅ Found security group: {security_group_id}")
    
    return elastic_ip, allocation_id, security_group_id

def create_trading_ec2_optimized():
    """
    최적화된 거래용 EC2 인스턴스 생성 (SQS 없이)
    """
    print("🔄 Creating optimized trading EC2 instance...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    # 기존 리소스 확인
    elastic_ip, allocation_id, security_group_id = get_existing_resources()
    
    if not security_group_id:
        print("❌ Security group not found")
        return None
    
    try:
        # 최신 Amazon Linux 2 AMI
        amis = ec2.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )
        
        latest_ami = sorted(amis['Images'], key=lambda x: x['CreationDate'])[-1]
        ami_id = latest_ami['ImageId']
        
        # User Data - 거래 환경 설정
        user_data = '''#!/bin/bash
yum update -y
yum install -y python3 python3-pip git htop

# Python 패키지 설치
pip3 install boto3 requests PyJWT pandas numpy

# 거래 디렉토리 생성
mkdir -p /home/ec2-user/makenaide-trading
cd /home/ec2-user/makenaide-trading

# 로그 디렉토리
mkdir -p /var/log/makenaide
chown ec2-user:ec2-user /var/log/makenaide

# 환경 변수 설정
echo 'export AWS_DEFAULT_REGION=ap-northeast-2' >> /home/ec2-user/.bashrc
echo 'export PYTHONPATH=/home/ec2-user/makenaide-trading:$PYTHONPATH' >> /home/ec2-user/.bashrc

# 시간대 설정 (한국 시간)
timedatectl set-timezone Asia/Seoul

echo "$(date): EC2 trading environment setup completed" > /var/log/makenaide/setup.log
'''
        
        # EC2 인스턴스 생성
        response = ec2.run_instances(
            ImageId=ami_id,
            MinCount=1,
            MaxCount=1,
            InstanceType='t3.micro',
            SecurityGroupIds=[security_group_id],
            UserData=user_data,
            IamInstanceProfile={'Name': 'makenaide-trading-ec2-profile'},
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Name', 'Value': 'makenaide-trading'},
                        {'Key': 'Project', 'Value': 'Makenaide'},
                        {'Key': 'Purpose', 'Value': 'Trading-Execution'},
                        {'Key': 'AutoStart', 'Value': 'EventBridge'},
                        {'Key': 'CostOptimized', 'Value': 'True'}
                    ]
                }
            ]
        )
        
        instance_id = response['Instances'][0]['InstanceId']
        print(f"✅ EC2 instance created: {instance_id}")
        
        # 인스턴스 시작 대기
        print("   Waiting for instance to start...")
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        
        # Elastic IP 연결
        if allocation_id:
            print(f"🔄 Associating Elastic IP...")
            ec2.associate_address(
                InstanceId=instance_id,
                AllocationId=allocation_id
            )
            print(f"✅ Elastic IP {elastic_ip} associated to {instance_id}")
        
        # 인스턴스 정보 가져오기
        instances = ec2.describe_instances(InstanceIds=[instance_id])
        instance = instances['Reservations'][0]['Instances'][0]
        
        print(f"\n📋 EC2 Instance Details:")
        print(f"   Instance ID: {instance_id}")
        print(f"   Instance Type: t3.micro")
        print(f"   Public IP: {elastic_ip}")
        print(f"   Private IP: {instance.get('PrivateIpAddress')}")
        print(f"   Security Group: {security_group_id}")
        
        return instance_id, elastic_ip
        
    except ClientError as e:
        print(f"❌ Error creating EC2 instance: {e.response['Error']['Message']}")
        return None, None

def create_trading_parameter_table():
    """
    거래 파라미터 전달용 DynamoDB 테이블 생성 (SQS 대신)
    """
    print("🔄 Creating trading parameters table...")
    
    dynamodb = boto3.client('dynamodb', region_name='ap-northeast-2')
    
    try:
        table_config = {
            'TableName': 'makenaide-trading-params',
            'KeySchema': [
                {'AttributeName': 'signal_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'signal_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            'BillingMode': 'PAY_PER_REQUEST',
            'Tags': [
                {'Key': 'Project', 'Value': 'Makenaide'},
                {'Key': 'Purpose', 'Value': 'Trading-Parameters'}
            ]
        }
        
        response = dynamodb.create_table(**table_config)
        table_name = response['TableDescription']['TableName']
        table_arn = response['TableDescription']['TableArn']
        
        print(f"✅ Created trading parameters table: {table_name}")
        print(f"   ARN: {table_arn}")
        
        return table_name, table_arn
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceInUseException':
            print("ℹ️  Trading parameters table already exists")
            # 기존 테이블 정보 가져오기
            response = dynamodb.describe_table(TableName='makenaide-trading-params')
            table_arn = response['Table']['TableArn']
            return 'makenaide-trading-params', table_arn
        
        print(f"❌ Error creating trading parameters table: {e.response['Error']['Message']}")
        return None, None

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide EC2 Trading Environment (Optimized)")
    print("=" * 60)
    
    # 1. 거래 파라미터 테이블 생성
    table_name, table_arn = create_trading_parameter_table()
    if not table_name:
        print("❌ Failed to create trading parameters table")
        return False
    
    # 2. EC2 인스턴스 생성
    instance_id, elastic_ip = create_trading_ec2_optimized()
    if not instance_id:
        print("❌ Failed to create EC2 instance")
        return False
    
    print(f"\n🎉 Trading environment deployment completed!")
    print(f"\n📋 Resource Summary:")
    print(f"   EC2 Instance: {instance_id}")
    print(f"   Fixed IP: {elastic_ip}")
    print(f"   Parameter Table: {table_name}")
    
    print(f"\n⚠️  Important Next Steps:")
    print(f"   1. 업비트 개발자 센터에서 IP 등록: {elastic_ip}")
    print(f"   2. SSH로 EC2 접속: ssh -i your-key.pem ec2-user@{elastic_ip}")
    print(f"   3. 거래 코드 배포 및 테스트")
    
    print(f"\n💰 Cost Impact:")
    print(f"   Additional monthly cost: ~$3")
    print(f"   Total system cost: $45/month (90% savings maintained)")
    
    return True

if __name__ == "__main__":
    main()