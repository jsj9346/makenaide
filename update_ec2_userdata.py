#!/usr/bin/env python3
"""
EC2 User Data 업데이트 스크립트
설정 파일 자동 다운로드 로직 추가
"""

import boto3
import base64

def update_ec2_userdata():
    """EC2 User Data에 설정 파일 다운로드 로직 추가"""

    new_userdata = '''#!/bin/bash

# 기존 설정
dnf update -y
dnf install -y python3 python3-pip git postgresql15
pip3 install --upgrade pip
mkdir -p /home/ec2-user/makenaide
chown ec2-user:ec2-user /home/ec2-user/makenaide

# 새로운 추가: config 디렉토리 생성 및 설정 파일 다운로드
mkdir -p /home/ec2-user/makenaide/config
chown ec2-user:ec2-user /home/ec2-user/makenaide/config

# S3에서 설정 파일 다운로드 (선택적)
if aws s3 ls s3://makenaide-config-deploy/config/filter_rules_config.yaml > /dev/null 2>&1; then
    aws s3 cp s3://makenaide-config-deploy/config/filter_rules_config.yaml /home/ec2-user/makenaide/config/
    chown ec2-user:ec2-user /home/ec2-user/makenaide/config/filter_rules_config.yaml
    echo "✅ S3에서 설정 파일 다운로드 완료" >> /home/ec2-user/setup_log.txt
else
    echo "ℹ️ S3 설정 파일 없음, 기본값 사용" >> /home/ec2-user/setup_log.txt
fi

# Python 설치 완료 메시지
echo "Python 설치 완료 $(date)" > /home/ec2-user/setup_complete.txt
echo "설정 파일 다운로드 시도 완료 $(date)" >> /home/ec2-user/setup_complete.txt
'''

    # Base64 인코딩
    userdata_encoded = base64.b64encode(new_userdata.encode('utf-8')).decode('utf-8')

    # EC2 인스턴스 중지 (User Data 업데이트를 위해)
    ec2 = boto3.client('ec2', region_name='ap-northeast-2')

    try:
        # 인스턴스 중지
        print("🔄 EC2 인스턴스 중지 중...")
        ec2.stop_instances(InstanceIds=['i-082bf343089af62d3'])

        # 중지 확인
        waiter = ec2.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=['i-082bf343089af62d3'])
        print("✅ EC2 인스턴스 중지 완료")

        # User Data 업데이트
        print("🔧 User Data 업데이트 중...")
        ec2.modify_instance_attribute(
            InstanceId='i-082bf343089af62d3',
            UserData={'Value': userdata_encoded}
        )
        print("✅ User Data 업데이트 완료")

        # 인스턴스 시작
        print("🚀 EC2 인스턴스 시작 중...")
        ec2.start_instances(InstanceIds=['i-082bf343089af62d3'])
        print("✅ EC2 인스턴스 시작 명령 완료")

        print("""
🎉 EC2 User Data 업데이트 완료!

📋 추가된 기능:
• S3에서 설정 파일 자동 다운로드
• config 디렉토리 자동 생성
• 설정 파일 권한 자동 설정

🔄 다음 EC2 시작 시:
1. S3에서 최신 설정 파일 다운로드
2. Makenaide 실행 시 새로운 임계값 적용
3. 파이프라인 활성화 예상
        """)

    except Exception as e:
        print(f"❌ User Data 업데이트 실패: {e}")

if __name__ == "__main__":
    update_ec2_userdata()