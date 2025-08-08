#!/usr/bin/env python3
import boto3
import time

def recreate_ec2():
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')
    
    # 기존 인스턴스 종료
    print('기존 인스턴스 종료 중...')
    try:
        ec2.terminate_instances(InstanceIds=['i-06c74b2f9cc4a9de3'])
        print('기존 인스턴스 종료 명령 완료')
    except Exception as e:
        print(f'기존 인스턴스 종료 오류 (무시 가능): {e}')
    
    # 새 인스턴스 생성용 UserData 스크립트
    user_data = """#!/bin/bash
yum update -y
yum install -y python3 python3-pip git postgresql15
pip3 install --upgrade pip
mkdir -p /home/ec2-user/makenaide
chown ec2-user:ec2-user /home/ec2-user/makenaide
"""
    
    print('새 인스턴스 생성 중...')
    response = ec2.run_instances(
        ImageId='ami-0c2d3e23e757b5d84',  # Amazon Linux 2023
        MinCount=1,
        MaxCount=1,
        InstanceType='t3.medium',
        KeyName='makenaide-key',  # 기존 키 사용
        SecurityGroupIds=['sg-0cef8405c3be93398'],
        SubnetId='subnet-01587b4c50fbb34d1',
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'makenaide-ec2-v2'}]
        }],
        UserData=user_data
    )
    
    instance_id = response['Instances'][0]['InstanceId']
    print(f'새 인스턴스 생성 완료: {instance_id}')
    
    # 인스턴스 실행 대기
    print('인스턴스 부팅 대기 중...')
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    print('인스턴스 실행 중')
    
    # EIP 연결
    print('EIP 연결 중...')
    try:
        ec2.associate_address(
            InstanceId=instance_id,
            AllocationId='eipalloc-0385f7da6f462b2cc'
        )
        print('EIP 연결 완료')
    except Exception as e:
        print(f'EIP 연결 오류: {e}')
    
    print(f'완료! SSH 접속: ssh -i /Users/13ruce/aws/makenaide-key.pem ec2-user@52.78.186.226')
    print(f'새 인스턴스 ID: {instance_id}')
    
    return instance_id

if __name__ == '__main__':
    recreate_ec2() 