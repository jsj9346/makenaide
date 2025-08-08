#!/usr/bin/env python3
"""
AWS Lambda 함수 업데이트 스크립트
makenaide-ticker-scanner 함수를 수정된 코드로 업데이트
"""

import boto3
import zipfile
import os
import json
import time
from datetime import datetime

def create_lambda_deployment_package():
    """Lambda 배포 패키지 생성"""
    print("📦 Lambda 배포 패키지 생성 중...")
    
    # ZIP 파일 생성
    zip_filename = 'lambda_ticker_scanner_fixed.zip'
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # 메인 Lambda 함수 파일 추가
        zipf.write('lambda_ticker_scanner_fixed.py', 'lambda_function.py')
        
        print(f"✅ {zip_filename} 생성 완료")
    
    return zip_filename

def update_lambda_function():
    """Lambda 함수 코드 업데이트"""
    try:
        # AWS Lambda 클라이언트 초기화
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 배포 패키지 생성
        zip_filename = create_lambda_deployment_package()
        
        # ZIP 파일 읽기
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        print("🔄 Lambda 함수 업데이트 중...")
        
        # Lambda 함수 코드 업데이트
        response = lambda_client.update_function_code(
            FunctionName='makenaide-ticker-scanner',
            ZipFile=zip_content
        )
        
        print(f"✅ Lambda 함수 업데이트 완료")
        print(f"   - 함수명: {response['FunctionName']}")
        print(f"   - 런타임: {response['Runtime']}")
        print(f"   - 핸들러: {response['Handler']}")
        print(f"   - 최종 수정: {response['LastModified']}")
        
        # 환경변수 확인 및 설정
        print("\n🔧 환경변수 확인 중...")
        
        config_response = lambda_client.get_function_configuration(
            FunctionName='makenaide-ticker-scanner'
        )
        
        current_env = config_response.get('Environment', {}).get('Variables', {})
        
        # 필수 환경변수 정의
        required_env_vars = {
            'PG_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
            'PG_PORT': '5432',
            'PG_DATABASE': 'makenaide',
            'PG_USER': 'bruce',
            'PG_PASSWORD': '0asis314.'
        }
        
        # 환경변수 업데이트 필요 여부 확인
        needs_update = False
        for key, value in required_env_vars.items():
            if current_env.get(key) != value:
                needs_update = True
                break
        
        if needs_update:
            print("🔧 환경변수 업데이트 중...")
            lambda_client.update_function_configuration(
                FunctionName='makenaide-ticker-scanner',
                Environment={
                    'Variables': required_env_vars
                }
            )
            print("✅ 환경변수 업데이트 완료")
        else:
            print("✅ 환경변수는 이미 올바르게 설정되어 있습니다")
        
        # 임시 파일 정리
        os.remove(zip_filename)
        print(f"🗑️ 임시 파일 {zip_filename} 삭제 완료")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 업데이트 실패: {e}")
        return False

def test_updated_lambda():
    """업데이트된 Lambda 함수 테스트"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        print("\n🧪 업데이트된 Lambda 함수 테스트 중...")
        
        # Lambda 함수 호출
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'test': True,
                'timestamp': datetime.now().isoformat()
            })
        )
        
        # 응답 처리
        payload = json.loads(response['Payload'].read())
        
        print(f"✅ Lambda 함수 테스트 완료")
        print(f"   - Status Code: {payload.get('statusCode')}")
        
        # 응답 본문 파싱
        if 'body' in payload:
            body = json.loads(payload['body'])
            print(f"   - 메시지: {body.get('message')}")
            print(f"   - 티커 수: {body.get('ticker_count')}")
            print(f"   - 실행 시간: {body.get('execution_time'):.2f}초")
            print(f"   - 버전: {body.get('version')}")
            
            if body.get('update_result'):
                update_result = body['update_result']
                print(f"   - API 티커: {update_result.get('total_api_tickers')}개")
                print(f"   - 신규 추가: {update_result.get('new_tickers_added')}개")
                print(f"   - 업데이트: {update_result.get('existing_tickers_updated')}개")
        
        return payload.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Lambda 함수 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide 티커 스캐너 Lambda 함수 업데이트 시작")
    print("="*60)
    
    # 1. Lambda 함수 업데이트
    if not update_lambda_function():
        print("❌ Lambda 함수 업데이트 실패")
        return False
    
    # 2. 업데이트 후 잠시 대기
    print("\n⏳ Lambda 함수 준비 대기 중... (5초)")
    time.sleep(5)
    
    # 3. 업데이트된 함수 테스트
    if not test_updated_lambda():
        print("❌ Lambda 함수 테스트 실패")
        return False
    
    print("\n" + "="*60)
    print("✅ Makenaide 티커 스캐너 Lambda 함수 업데이트 완료!")
    print("✅ 이제 AWS 콘솔에서 함수를 테스트하거나 EventBridge로 트리거할 수 있습니다.")
    
    return True

if __name__ == "__main__":
    main()