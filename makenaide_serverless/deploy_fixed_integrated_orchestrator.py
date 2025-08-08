#!/usr/bin/env python3
"""
Lambda 함수 배포 스크립트: Makenaide Integrated Orchestrator 수정본
- undefined variable 버그 수정
- EC2 종료 후 RDS도 자동 종료하도록 개선
"""

import boto3
import zipfile
import os
import sys
import time
from datetime import datetime

# AWS 클라이언트 초기화
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

def create_deployment_package():
    """배포 패키지 생성"""
    print("📦 배포 패키지 생성 중...")
    
    zip_filename = 'makenaide-integrated-orchestrator-fixed.zip'
    
    # ZIP 파일 생성
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Lambda 함수 코드 추가
        zipf.write('lambda_integrated_orchestrator.py', 'lambda_function.py')
    
    print(f"✅ 배포 패키지 생성 완료: {zip_filename}")
    print(f"📊 파일 크기: {os.path.getsize(zip_filename) / 1024:.2f} KB")
    
    return zip_filename

def update_lambda_function(zip_filename: str):
    """Lambda 함수 업데이트"""
    function_name = 'makenaide-integrated-orchestrator'
    
    print(f"🚀 Lambda 함수 업데이트 중: {function_name}")
    
    try:
        # ZIP 파일 읽기
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        # Lambda 함수 업데이트
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        
        print("✅ Lambda 함수 업데이트 성공!")
        print(f"📋 함수 ARN: {response['FunctionArn']}")
        print(f"📋 버전: {response['Version']}")
        print(f"📋 마지막 수정: {response['LastModified']}")
        
        # 함수 설정 업데이트 (타임아웃 15분 확인)
        config_response = lambda_client.update_function_configuration(
            FunctionName=function_name,
            Timeout=900,  # 15분
            MemorySize=512,
            Description='Makenaide Integrated Orchestrator - EC2+makenaide 실행 및 자동 종료 (수정본)'
        )
        
        print("✅ Lambda 함수 설정 업데이트 완료!")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 업데이트 실패: {e}")
        return False

def main():
    """메인 함수"""
    print("🎯 Makenaide Integrated Orchestrator 수정본 배포 시작")
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n📋 수정 사항:")
    print("1. undefined variable 'makenaide_result' 버그 수정")
    print("2. EC2 종료 모니터링 및 RDS 자동 종료 기능 추가")
    print("3. 타임아웃 조정으로 Lambda 15분 제한 준수")
    
    # 현재 디렉토리 확인
    if not os.path.exists('lambda_integrated_orchestrator.py'):
        print("❌ lambda_integrated_orchestrator.py 파일을 찾을 수 없습니다.")
        print("📂 현재 디렉토리를 확인하세요.")
        sys.exit(1)
    
    # 배포 패키지 생성
    zip_filename = create_deployment_package()
    
    # Lambda 함수 업데이트
    if update_lambda_function(zip_filename):
        print("\n🎉 배포 완료!")
        print("📝 다음 스케줄 실행 시 변경사항이 적용됩니다:")
        print("   - 01:00, 05:00, 09:00, 13:00, 17:00, 21:00 (KST)")
        
        # 정리
        os.remove(zip_filename)
        print(f"\n🧹 임시 파일 삭제 완료: {zip_filename}")
    else:
        print("\n❌ 배포 실패!")
        sys.exit(1)

if __name__ == "__main__":
    main()