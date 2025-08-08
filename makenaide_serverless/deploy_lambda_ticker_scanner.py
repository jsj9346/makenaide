#!/usr/bin/env python3
"""
Lambda 티커 스캐너 배포 스크립트
"""

import boto3
import json
import zipfile
import os
import sys
from datetime import datetime

def create_deployment_package():
    """배포용 ZIP 패키지 생성"""
    try:
        print("🔄 Lambda 배포 패키지 생성 중...")
        
        # ZIP 파일 생성
        with zipfile.ZipFile('lambda_ticker_scanner_deploy.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
            # 메인 Lambda 함수 파일 추가
            zipf.write('lambda_ticker_scanner.py', 'lambda_ticker_scanner.py')
            print("✅ lambda_ticker_scanner.py 추가됨")
        
        print("✅ 배포 패키지 생성 완료: lambda_ticker_scanner_deploy.zip")
        return True
        
    except Exception as e:
        print(f"❌ 배포 패키지 생성 실패: {e}")
        return False

def update_lambda_function():
    """Lambda 함수 업데이트"""
    try:
        print("🔄 Lambda 함수 업데이트 중...")
        
        # AWS Lambda 클라이언트 생성
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 함수 코드 업데이트
        with open('lambda_ticker_scanner_deploy.zip', 'rb') as zip_file:
            response = lambda_client.update_function_code(
                FunctionName='makenaide-ticker-scanner',
                ZipFile=zip_file.read()
            )
        
        print(f"✅ Lambda 함수 업데이트 완료")
        print(f"   - 함수명: {response['FunctionName']}")
        print(f"   - 마지막 수정: {response['LastModified']}")
        print(f"   - 코드 크기: {response['CodeSize']} bytes")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 업데이트 실패: {e}")
        return False

def test_lambda_function():
    """Lambda 함수 테스트"""
    try:
        print("🔄 Lambda 함수 테스트 중...")
        
        # AWS Lambda 클라이언트 생성
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 함수 호출
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        # 응답 파싱
        payload = json.loads(response['Payload'].read())
        
        if response['StatusCode'] == 200:
            print("✅ Lambda 함수 테스트 성공")
            
            if 'body' in payload:
                body = json.loads(payload['body'])
                print(f"   - 메시지: {body.get('message', 'N/A')}")
                print(f"   - 처리된 티커 수: {body.get('volume_filtered', 'N/A')}")
                print(f"   - 실행 시간: {body.get('execution_time', 'N/A')}초")
            
        else:
            print(f"⚠️ Lambda 함수 실행 중 오류 발생")
            print(f"   - Status Code: {response['StatusCode']}")
            print(f"   - Payload: {payload}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide 티커 스캐너 Lambda 배포 시작")
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 배포 패키지 생성
    if not create_deployment_package():
        sys.exit(1)
    
    # 2. Lambda 함수 업데이트
    if not update_lambda_function():
        sys.exit(1)
    
    # 3. 함수 테스트
    if not test_lambda_function():
        print("⚠️ 함수는 배포되었지만 테스트에서 문제가 발견되었습니다.")
    
    print("✅ Lambda 배포 완료")

if __name__ == "__main__":
    main() 