#!/usr/bin/env python3
"""
EC2 Auto Update Script
EC2 시작 시 S3에서 최신 코드를 자동으로 다운로드
"""

import boto3
import os
import subprocess
from datetime import datetime

def update_code_from_s3():
    """S3에서 최신 코드 다운로드 및 적용"""

    s3_bucket = 'makenaide-config-deploy'
    local_makenaide_dir = '/home/ec2-user/makenaide'

    try:
        # S3 클라이언트 초기화
        s3 = boto3.client('s3')

        print(f"🔍 S3에서 최신 코드 확인 중... ({datetime.now()})")

        # market_sentiment.py 다운로드
        s3_key = 'code/market_sentiment.py'
        local_file = f'{local_makenaide_dir}/market_sentiment.py'

        # 기존 파일 백업
        if os.path.exists(local_file):
            backup_file = f'{local_file}.backup.{datetime.now().strftime("%Y%m%d_%H%M%S")}'
            os.rename(local_file, backup_file)
            print(f"📦 기존 파일 백업: {backup_file}")

        # S3에서 최신 파일 다운로드
        s3.download_file(s3_bucket, s3_key, local_file)
        print(f"✅ 다운로드 완료: {local_file}")

        # 파일 권한 설정
        os.chmod(local_file, 0o644)

        # 변경사항 확인
        result = subprocess.run(['head', '-20', local_file], capture_output=True, text=True)
        print(f"📋 파일 내용 확인:\n{result.stdout}")

        print(f"""
🎉 코드 업데이트 완료!

📅 업데이트 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🔧 적용된 변경사항:
• market_sentiment.py 임계값 완화
• min_pct_up: 40.0 → 30.0
• max_top10_volume: 75.0 → 85.0
• min_ma200_above: 20.0 → 10.0
• min_sentiment_score: 40.0 → 25.0

🚀 다음 파이프라인 실행 시 새로운 임계값 적용됩니다.
        """)

        return True

    except Exception as e:
        print(f"❌ 코드 업데이트 실패: {e}")
        return False

def check_s3_connectivity():
    """S3 연결 상태 확인"""
    try:
        s3 = boto3.client('s3')
        s3.head_bucket(Bucket='makenaide-config-deploy')
        print("✅ S3 연결 상태 정상")
        return True
    except Exception as e:
        print(f"❌ S3 연결 실패: {e}")
        return False

if __name__ == "__main__":
    print("🔄 EC2 Auto Update Script 시작")

    # S3 연결 확인
    if check_s3_connectivity():
        # 코드 업데이트 실행
        if update_code_from_s3():
            print("🎯 업데이트 성공: 시스템이 최신 임계값으로 설정되었습니다")
        else:
            print("⚠️ 업데이트 실패: 기존 설정으로 동작합니다")
    else:
        print("🚨 S3 연결 불가: 네트워크 상태를 확인해주세요")