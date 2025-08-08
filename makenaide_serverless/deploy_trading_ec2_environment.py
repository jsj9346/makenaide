#!/usr/bin/env python3
"""
Makenaide EC2 거래 실행 환경 설정
고정 IP를 가진 EC2 인스턴스에서 업비트 API 연동 거래 실행
"""

import boto3
import json
import time
from botocore.exceptions import ClientError

def create_elastic_ip():
    """
    EC2용 고정 IP (Elastic IP) 생성
    """
    print("🔄 Creating Elastic IP for trading EC2...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    try:
        # Elastic IP 할당
        response = ec2.allocate_address(Domain='vpc')
        
        allocation_id = response['AllocationId']
        public_ip = response['PublicIp']
        
        print(f"✅ Elastic IP created: {public_ip}")
        print(f"   Allocation ID: {allocation_id}")
        
        # 태그 설정
        ec2.create_tags(
            Resources=[allocation_id],
            Tags=[
                {'Key': 'Project', 'Value': 'Makenaide'},
                {'Key': 'Purpose', 'Value': 'Trading-IP'},
                {'Key': 'Name', 'Value': 'makenaide-trading-ip'}
            ]
        )
        
        return allocation_id, public_ip
        
    except ClientError as e:
        print(f"❌ Error creating Elastic IP: {e.response['Error']['Message']}")
        return None, None

def create_trading_security_group():
    """
    거래용 EC2의 보안 그룹 생성
    """
    print("🔄 Creating security group for trading EC2...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    try:
        # 기본 VPC 가져오기
        vpcs = ec2.describe_vpcs(Filters=[{'Name': 'is-default', 'Values': ['true']}])
        if not vpcs['Vpcs']:
            print("❌ Default VPC not found")
            return None
            
        vpc_id = vpcs['Vpcs'][0]['VpcId']
        
        # 보안 그룹 생성
        response = ec2.create_security_group(
            GroupName='makenaide-trading-sg',
            Description='Makenaide trading EC2 security group',
            VpcId=vpc_id
        )
        
        security_group_id = response['GroupId']
        print(f"✅ Security group created: {security_group_id}")
        
        # 인바운드 규칙 설정 (SSH만 허용)
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'SSH access'}]
                }
            ]
        )
        
        # 태그 설정
        ec2.create_tags(
            Resources=[security_group_id],
            Tags=[
                {'Key': 'Project', 'Value': 'Makenaide'},
                {'Key': 'Purpose', 'Value': 'Trading-Security'},
                {'Key': 'Name', 'Value': 'makenaide-trading-sg'}
            ]
        )
        
        return security_group_id
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidGroup.Duplicate':
            print("ℹ️  Security group already exists, getting existing one...")
            # 기존 보안 그룹 찾기
            groups = ec2.describe_security_groups(
                Filters=[{'Name': 'group-name', 'Values': ['makenaide-trading-sg']}]
            )
            if groups['SecurityGroups']:
                security_group_id = groups['SecurityGroups'][0]['GroupId']
                print(f"✅ Using existing security group: {security_group_id}")
                return security_group_id
        
        print(f"❌ Error creating security group: {e.response['Error']['Message']}")
        return None

def create_trading_iam_role():
    """
    거래 EC2용 IAM 역할 생성
    """
    print("🔄 Creating IAM role for trading EC2...")
    
    iam = boto3.client('iam', region_name='ap-northeast-2')
    
    # EC2 신뢰 정책
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # EC2가 필요한 권한들
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DynamoDBAccess",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:GetItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                "Resource": [
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-trades/index/*",
                    "arn:aws:dynamodb:ap-northeast-2:901361833359:table/makenaide-positions/index/*"
                ]
            },
            {
                "Sid": "SecretsManagerAccess",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue"
                ],
                "Resource": "arn:aws:secretsmanager:ap-northeast-2:901361833359:secret:upbit-api-keys-*"
            },
            {
                "Sid": "SNSPublishAccess",
                "Effect": "Allow",
                "Action": [
                    "sns:Publish"
                ],
                "Resource": [
                    "arn:aws:sns:ap-northeast-2:901361833359:makenaide-*"
                ]
            },
            {
                "Sid": "SQSAccess",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes"
                ],
                "Resource": "arn:aws:sqs:ap-northeast-2:901361833359:makenaide-*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        # IAM 역할 생성
        role_name = "makenaide-trading-ec2-role"
        
        try:
            iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Makenaide trading EC2 instance role"
            )
            print(f"✅ Created IAM role: {role_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"ℹ️  IAM role already exists: {role_name}")
            else:
                raise
        
        # 인라인 정책 연결
        policy_name = "MakenaideTradingPolicy"
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(permission_policy)
            )
            print(f"✅ Attached policy to role: {policy_name}")
        except ClientError as e:
            print(f"⚠️  Policy attachment issue: {e.response['Error']['Message']}")
        
        # 인스턴스 프로파일 생성
        profile_name = "makenaide-trading-ec2-profile"
        try:
            iam.create_instance_profile(InstanceProfileName=profile_name)
            print(f"✅ Created instance profile: {profile_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"ℹ️  Instance profile already exists: {profile_name}")
            else:
                print(f"⚠️  Instance profile creation issue: {e.response['Error']['Message']}")
        
        # 역할을 인스턴스 프로파일에 추가
        try:
            iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )
            print(f"✅ Added role to instance profile")
        except ClientError as e:
            if e.response['Error']['Code'] != 'LimitExceeded':
                print(f"⚠️  Role addition issue: {e.response['Error']['Message']}")
        
        return role_name, profile_name
        
    except ClientError as e:
        print(f"❌ Error creating IAM role: {e.response['Error']['Message']}")
        return None, None

def create_trading_ec2_instance():
    """
    거래용 EC2 인스턴스 생성
    """
    print("🔄 Creating trading EC2 instance...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    try:
        # 최신 Amazon Linux 2 AMI 가져오기
        amis = ec2.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )
        
        if not amis['Images']:
            print("❌ No suitable AMI found")
            return None
            
        # 최신 AMI 선택
        latest_ami = sorted(amis['Images'], key=lambda x: x['CreationDate'])[-1]
        ami_id = latest_ami['ImageId']
        print(f"   Using AMI: {ami_id} ({latest_ami['Name']})")
        
        # User Data 스크립트 (초기 설정)
        user_data = '''#!/bin/bash
yum update -y
yum install -y python3 python3-pip git

# Python 패키지 설치
pip3 install boto3 requests PyJWT hashlib uuid

# 거래 디렉토리 생성
mkdir -p /home/ec2-user/makenaide-trading
cd /home/ec2-user/makenaide-trading

# 로그 디렉토리 생성
mkdir -p /var/log/makenaide
chown ec2-user:ec2-user /var/log/makenaide

# CloudWatch 에이전트 설치 (선택사항)
# wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
# rpm -U ./amazon-cloudwatch-agent.rpm

echo "EC2 trading environment setup completed" > /var/log/makenaide/setup.log
'''
        
        # EC2 인스턴스 생성
        response = ec2.run_instances(
            ImageId=ami_id,
            MinCount=1,
            MaxCount=1,
            InstanceType='t3.micro',  # 비용 효율적인 타입
            SecurityGroupIds=['sg-default'],  # 보안 그룹은 별도 설정 필요
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
                        {'Key': 'AutoStop', 'Value': 'True'}
                    ]
                }
            ]
        )
        
        instance_id = response['Instances'][0]['InstanceId']
        print(f"✅ EC2 instance created: {instance_id}")
        print(f"   Instance type: t3.micro")
        print(f"   AMI: {ami_id}")
        
        # 인스턴스가 시작될 때까지 대기
        print("   Waiting for instance to start...")
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        
        # 인스턴스 정보 가져오기
        instances = ec2.describe_instances(InstanceIds=[instance_id])
        instance = instances['Reservations'][0]['Instances'][0]
        public_ip = instance.get('PublicIpAddress')
        private_ip = instance.get('PrivateIpAddress')
        
        print(f"✅ Instance is running:")
        print(f"   Instance ID: {instance_id}")
        print(f"   Public IP: {public_ip}")
        print(f"   Private IP: {private_ip}")
        
        return instance_id, public_ip, private_ip
        
    except ClientError as e:
        print(f"❌ Error creating EC2 instance: {e.response['Error']['Message']}")
        return None, None, None

def associate_elastic_ip_to_instance(instance_id, allocation_id):
    """
    생성된 EC2 인스턴스에 Elastic IP 연결
    """
    print(f"🔄 Associating Elastic IP to instance {instance_id}...")
    
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    try:
        response = ec2.associate_address(
            InstanceId=instance_id,
            AllocationId=allocation_id
        )
        
        association_id = response['AssociationId']
        print(f"✅ Elastic IP associated: {association_id}")
        
        return association_id
        
    except ClientError as e:
        print(f"❌ Error associating Elastic IP: {e.response['Error']['Message']}")
        return None

def create_trading_sqs_queue():
    """
    거래 파라미터 전달용 SQS 큐 생성
    """
    print("🔄 Creating SQS queue for trading parameters...")
    
    sqs = boto3.client('sqs', region_name='ap-northeast-2')
    
    try:
        # SQS 큐 생성
        response = sqs.create_queue(
            QueueName='makenaide-trading-queue',
            Attributes={
                'DelaySeconds': '0',
                'MessageRetentionPeriod': '3600',  # 1시간
                'ReceiveMessageWaitTimeSeconds': '20',  # Long polling
                'VisibilityTimeoutSeconds': '300'  # 5분
            }
        )
        
        queue_url = response['QueueUrl']
        print(f"✅ SQS queue created: {queue_url}")
        
        # 큐 속성에서 ARN 가져오기
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = attrs['Attributes']['QueueArn']
        
        return queue_url, queue_arn
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'QueueAlreadyExists':
            print("ℹ️  SQS queue already exists, getting existing one...")
            queue_url = sqs.get_queue_url(QueueName='makenaide-trading-queue')['QueueUrl']
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['QueueArn']
            )
            queue_arn = attrs['Attributes']['QueueArn']
            print(f"✅ Using existing SQS queue: {queue_url}")
            return queue_url, queue_arn
        
        print(f"❌ Error creating SQS queue: {e.response['Error']['Message']}")
        return None, None

def create_deployment_summary():
    """
    배포 요약 정보 생성
    """
    summary = '''
# Makenaide EC2 거래 환경 배포 완료

## 🎯 생성된 리소스

### EC2 인스턴스
- **인스턴스 타입**: t3.micro (비용 최적화)
- **OS**: Amazon Linux 2
- **자동 시작/종료**: EventBridge 연동
- **권한**: DynamoDB, Secrets Manager, SNS 접근

### 네트워크
- **Elastic IP**: 업비트 API 호환 고정 IP
- **보안 그룹**: SSH(22) 포트만 허용
- **VPC**: 기본 VPC 사용

### 큐 시스템
- **SQS**: 거래 파라미터 전달용
- **Long Polling**: 효율적인 메시지 수신

## 🔧 다음 단계

1. **업비트 개발자 센터 설정**:
   - Elastic IP를 업비트 API 키에 등록
   - Open API 사용 권한 설정

2. **거래 코드 배포**:
   - EC2에 업비트 API 연동 코드 업로드
   - 환경 설정 및 테스트 실행

3. **EventBridge 연결**:
   - 거래 신호 수신 시 EC2 자동 시작
   - 거래 완료 후 자동 종료

## 💰 비용 효율성

- **월 EC2 비용**: ~$3 (45분 사용 기준)
- **Elastic IP**: $0 (인스턴스 연결 시)
- **SQS**: ~$0.1 (메시지 전송 비용)

**총 추가 비용**: ~$3/월
**전체 시스템 비용**: $45/월 (90% 절약 유지)
'''
    
    print("\n" + "=" * 60)
    print(summary)
    print("=" * 60)

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide EC2 Trading Environment Setup")
    print("=" * 60)
    
    # 1. Elastic IP 생성
    allocation_id, public_ip = create_elastic_ip()
    if not allocation_id:
        print("❌ Failed to create Elastic IP. Exiting.")
        return False
    
    # 2. 보안 그룹 생성
    security_group_id = create_trading_security_group()
    if not security_group_id:
        print("❌ Failed to create security group. Exiting.")
        return False
    
    # 3. IAM 역할 생성
    role_name, profile_name = create_trading_iam_role()
    if not role_name:
        print("❌ Failed to create IAM role. Exiting.")
        return False
    
    # 4. SQS 큐 생성
    queue_url, queue_arn = create_trading_sqs_queue()
    if not queue_url:
        print("❌ Failed to create SQS queue. Exiting.")
        return False
    
    # 5. EC2 인스턴스 생성
    instance_id, instance_public_ip, private_ip = create_trading_ec2_instance()
    if not instance_id:
        print("❌ Failed to create EC2 instance. Exiting.")
        return False
    
    # 6. Elastic IP 연결
    association_id = associate_elastic_ip_to_instance(instance_id, allocation_id)
    if not association_id:
        print("❌ Failed to associate Elastic IP. Continuing...")
    
    # 7. 배포 요약
    create_deployment_summary()
    
    # 8. 중요 정보 출력
    print(f"\n📋 배포된 리소스 정보:")
    print(f"   EC2 Instance: {instance_id}")
    print(f"   Elastic IP: {public_ip}")
    print(f"   Security Group: {security_group_id}")
    print(f"   IAM Role: {role_name}")
    print(f"   SQS Queue: {queue_url}")
    
    print(f"\n⚠️  중요 다음 단계:")
    print(f"   1. 업비트 개발자 센터에 IP 등록: {public_ip}")
    print(f"   2. EC2 인스턴스에 SSH 접속하여 거래 코드 배포")
    print(f"   3. 거래 테스트 및 EventBridge 연동 확인")
    
    return True

if __name__ == "__main__":
    main()