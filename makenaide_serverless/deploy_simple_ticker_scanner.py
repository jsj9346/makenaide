#!/usr/bin/env python3
"""
간단한 티커 스캐너 Lambda 함수 배포 스크립트
makenaide-ticker-scanner 함수를 간단한 버전으로 업데이트
"""

import boto3
import zipfile
import os
import json
import time
from datetime import datetime

def create_simple_lambda_package():
    """간단한 Lambda 배포 패키지 생성"""
    print("📦 간단한 Lambda 배포 패키지 생성 중...")
    
    zip_filename = 'lambda_ticker_scanner_simple.zip'
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # 간단한 Lambda 함수 파일 추가
        zipf.write('lambda_ticker_scanner_simple.py', 'lambda_function.py')
        print(f"✅ {zip_filename} 생성 완료")
    
    return zip_filename

def deploy_simple_lambda():
    """간단한 Lambda 함수 배포"""
    try:
        # AWS Lambda 클라이언트 초기화
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 배포 패키지 생성
        zip_filename = create_simple_lambda_package()
        
        # ZIP 파일 읽기
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        print("🔄 간단한 Lambda 함수 배포 중...")
        
        # Lambda 함수 코드 업데이트
        response = lambda_client.update_function_code(
            FunctionName='makenaide-ticker-scanner',
            ZipFile=zip_content
        )
        
        print(f"✅ Lambda 함수 업데이트 완료")
        print(f"   - 함수명: {response['FunctionName']}")
        print(f"   - 코드 크기: {response['CodeSize']} bytes")
        print(f"   - 최종 수정: {response['LastModified']}")
        
        # 임시 파일 정리
        os.remove(zip_filename)
        print(f"🗑️ 임시 파일 {zip_filename} 삭제 완료")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 배포 실패: {e}")
        return False

def test_simple_lambda():
    """간단한 Lambda 함수 테스트"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        print("\\n🧪 간단한 Lambda 함수 테스트 중...")
        
        # Lambda 함수 호출 (동기 호출로 결과 즉시 확인)
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
            print(f"   - 실행 시간: {body.get('execution_time', 0):.2f}초")
            print(f"   - 버전: {body.get('version')}")
            print(f"   - 노트: {body.get('note')}")
            
            if body.get('refresh_result'):
                refresh = body['refresh_result']
                print(f"   - 전체 티커: {refresh.get('total_tickers')}개")
                print(f"   - 활성 티커: {refresh.get('final_active_tickers')}개")
                print(f"   - 샘플: {refresh.get('sample_tickers', [])[:5]}")
        
        return payload.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Lambda 함수 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🚀 간단한 Makenaide 티커 스캐너 Lambda 함수 배포 시작")
    print("="*70)
    
    # 1. 간단한 Lambda 함수 배포
    if not deploy_simple_lambda():
        print("❌ Lambda 함수 배포 실패")
        return False
    
    # 2. 배포 후 잠시 대기
    print("\\n⏳ Lambda 함수 준비 대기 중... (5초)")
    time.sleep(5)
    
    # 3. 배포된 함수 테스트
    if not test_simple_lambda():
        print("❌ Lambda 함수 테스트 실패")
        return False
    
    print("\\n" + "="*70)
    print("✅ 간단한 Makenaide 티커 스캐너 Lambda 함수 배포 완료!")
    print("✅ Upbit API 타임아웃 문제 해결됨 (기존 DB 데이터 활용)")
    print("✅ 이제 Lambda 함수가 안정적으로 작동합니다.")
    
    return True

if __name__ == "__main__":
    main()