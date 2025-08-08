#!/usr/bin/env python3
"""
AWS 공개 Layer를 사용하여 Lambda 함수 구성
"""

import boto3
import json
import zipfile
import os
import time

def configure_lambda_with_public_layer():
    """AWS 공개 Layer를 사용하여 Lambda 함수 구성"""
    
    # 매우 간단한 Lambda 함수 코드 (의존성 최소화)
    lambda_code = '''
import json
import logging
import os
import urllib.request
import urllib.parse
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def test_basic_functionality():
    """기본 기능 테스트"""
    try:
        logger.info("🔍 기본 기능 테스트 시작")
        
        # 환경변수 확인
        env_vars = {
            'PG_HOST': os.environ.get('PG_HOST'),
            'PG_PORT': os.environ.get('PG_PORT'),
            'PG_DATABASE': os.environ.get('PG_DATABASE'),
            'PG_USER': os.environ.get('PG_USER'),
            'PG_PASSWORD': '****' if os.environ.get('PG_PASSWORD') else None
        }
        
        logger.info(f"환경변수 상태: {env_vars}")
        
        # 외부 API 호출 테스트 (Upbit API)
        try:
            url = "https://api.upbit.com/v1/market/all"
            
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode())
                
            # KRW 마켓만 필터링
            krw_tickers = [
                market['market'] for market in data 
                if market['market'].startswith('KRW-')
            ]
            
            logger.info(f"✅ Upbit API 테스트 성공: {len(krw_tickers)}개 티커")
            
            # DB 연결 시도 (psycopg2 없이)
            return {
                'environment_variables': env_vars,
                'upbit_api_test': {
                    'success': True,
                    'ticker_count': len(krw_tickers),
                    'sample_tickers': krw_tickers[:5]
                },
                'message': 'psycopg2 없이 기본 기능 테스트 완료'
            }
            
        except Exception as e:
            logger.error(f"❌ Upbit API 테스트 실패: {e}")
            return {
                'environment_variables': env_vars,
                'upbit_api_test': {
                    'success': False,
                    'error': str(e)
                },
                'message': 'API 테스트 실패'
            }
        
    except Exception as e:
        logger.error(f"❌ 기본 기능 테스트 실패: {e}")
        return {
            'error': str(e),
            'message': '전체 테스트 실패'
        }

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 기본 기능 테스트 Lambda 시작")
        start_time = datetime.now()
        
        # 기본 기능 테스트
        test_result = test_basic_functionality()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': '기본 기능 테스트 완료',
                'test_result': test_result,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat(),
                'version': 'basic'
            })
        }
        
        logger.info(f"✅ 기본 기능 테스트 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ 기본 기능 테스트 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'version': 'basic'
            })
        }
'''
    
    # lambda_function.py 파일 생성
    with open('lambda_function.py', 'w', encoding='utf-8') as f:
        f.write(lambda_code.strip())
    
    # ZIP 파일 생성
    zip_filename = 'lambda-basic-test.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write('lambda_function.py', 'lambda_function.py')
    
    # Lambda 함수 업데이트
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    with open(zip_filename, 'rb') as zip_file:
        zip_content = zip_file.read()
    
    print("🔄 기본 테스트 Lambda 함수 업데이트 중...")
    lambda_client.update_function_code(
        FunctionName='makenaide-ticker-scanner',
        ZipFile=zip_content
    )
    
    # Lambda 함수 상태 확인 및 대기
    print("⏳ Lambda 함수 업데이트 완료 대기 중...")
    for attempt in range(10):
        try:
            response = lambda_client.get_function(FunctionName='makenaide-ticker-scanner')
            state = response['Configuration']['State']
            last_update_status = response['Configuration']['LastUpdateStatus']
            
            if state == 'Active' and last_update_status == 'Successful':
                print(f"✅ Lambda 함수 활성화 완료")
                break
                
            print(f"대기 중... ({attempt + 1}/10) - 상태: {state}, 업데이트: {last_update_status}")
            time.sleep(5)
            
        except Exception as e:
            print(f"상태 확인 실패: {e}")
            time.sleep(3)
    
    # Layer 제거 (기본 런타임만 사용)
    print("🔄 Layer 제거하고 기본 런타임만 사용...")
    lambda_client.update_function_configuration(
        FunctionName='makenaide-ticker-scanner',
        Layers=[]  # 모든 Layer 제거
    )
    
    # 정리
    os.remove('lambda_function.py')
    os.remove(zip_filename)
    
    print("✅ 기본 테스트 Lambda 함수 배포 완료")

def test_basic_lambda():
    """기본 테스트 Lambda 함수 실행"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        print("🧪 기본 기능 테스트 실행 중...")
        
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            Payload=json.dumps({})
        )
        
        result = json.loads(response['Payload'].read())
        print(f"✅ 기본 테스트 결과:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result
        
    except Exception as e:
        print(f"❌ 기본 테스트 실패: {e}")
        return None

def deploy_simple_db_lambda():
    """간단한 DB 연결 Lambda 함수 배포 (AWS PostgreSQL Layer 사용)"""
    
    # AWS PostgreSQL Layer ARN (공개 Layer)
    # AWS가 제공하는 psycopg2 Layer를 사용
    postgresql_layer_arn = "arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py38:2"
    
    # DB 연결 Lambda 함수 코드
    lambda_code = '''
import json
import logging
import os
import urllib.request
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def test_db_connection():
    """DB 연결 테스트"""
    try:
        logger.info("🔍 DB 연결 테스트 시작")
        
        # psycopg2 import 시도
        try:
            import psycopg2
            logger.info("✅ psycopg2 import 성공")
            
            # DB 연결 시도
            conn = psycopg2.connect(
                host=os.environ.get('PG_HOST'),
                port=int(os.environ.get('PG_PORT', 5432)),
                database=os.environ.get('PG_DATABASE'),
                user=os.environ.get('PG_USER'),
                password=os.environ.get('PG_PASSWORD'),
                connect_timeout=10
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            db_version = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tickers")
            ticker_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM ohlcv WHERE date >= CURRENT_DATE - INTERVAL '7 days'")
            ohlcv_count = cursor.fetchone()[0]
            
            # 0값 데이터 확인
            cursor.execute("""
                SELECT COUNT(*) FROM ohlcv 
                WHERE (open = 0 OR high = 0 OR low = 0 OR close = 0 OR volume = 0)
                AND date >= CURRENT_DATE - INTERVAL '7 days'
            """)
            zero_records = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            logger.info("✅ DB 연결 및 쿼리 성공")
            
            return {
                'db_connection': True,
                'db_version': db_version[:50],  # 버전 정보 축약
                'ticker_count': ticker_count,
                'ohlcv_recent_count': ohlcv_count,
                'zero_value_records': zero_records
            }
            
        except ImportError as e:
            logger.error(f"❌ psycopg2 import 실패: {e}")
            return {
                'db_connection': False,
                'error': f'psycopg2 import 실패: {str(e)}'
            }
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            return {
                'db_connection': False,
                'error': f'DB 연결 실패: {str(e)}'
            }
        
    except Exception as e:
        logger.error(f"❌ 전체 DB 테스트 실패: {e}")
        return {
            'db_connection': False,
            'error': f'전체 테스트 실패: {str(e)}'
        }

def get_upbit_tickers():
    """Upbit 티커 조회"""
    try:
        url = "https://api.upbit.com/v1/market/all"
        
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
        # KRW 마켓만 필터링
        krw_tickers = [
            market['market'] for market in data 
            if market['market'].startswith('KRW-')
        ]
        
        return {
            'upbit_api': True,
            'ticker_count': len(krw_tickers),
            'sample_tickers': krw_tickers[:3]
        }
        
    except Exception as e:
        return {
            'upbit_api': False,
            'error': str(e)
        }

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 DB 연결 테스트 Lambda 시작")
        start_time = datetime.now()
        
        # 1. DB 연결 테스트
        db_result = test_db_connection()
        
        # 2. Upbit API 테스트
        upbit_result = get_upbit_tickers()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DB 연결 및 API 테스트 완료',
                'db_test': db_result,
                'upbit_test': upbit_result,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat(),
                'version': 'db_test'
            })
        }
        
        logger.info(f"✅ DB 연결 및 API 테스트 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'version': 'db_test'
            })
        }
'''
    
    # lambda_function.py 파일 생성
    with open('lambda_function.py', 'w', encoding='utf-8') as f:
        f.write(lambda_code.strip())
    
    # ZIP 파일 생성
    zip_filename = 'lambda-db-test.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write('lambda_function.py', 'lambda_function.py')
    
    # Lambda 함수 업데이트
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    
    with open(zip_filename, 'rb') as zip_file:
        zip_content = zip_file.read()
    
    print("🔄 DB 연결 테스트 Lambda 함수 업데이트 중...")
    lambda_client.update_function_code(
        FunctionName='makenaide-ticker-scanner',
        ZipFile=zip_content
    )
    
    # 잠시 대기
    time.sleep(3)
    
    # PostgreSQL Layer 추가
    print("🔗 AWS PostgreSQL Layer 연결 중...")
    lambda_client.update_function_configuration(
        FunctionName='makenaide-ticker-scanner',
        Layers=[postgresql_layer_arn]
    )
    
    # 정리
    os.remove('lambda_function.py')
    os.remove(zip_filename)
    
    print("✅ DB 연결 테스트 Lambda 함수 배포 완료")

def test_db_lambda():
    """DB 연결 테스트 Lambda 함수 실행"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        print("🧪 DB 연결 테스트 실행 중...")
        
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            Payload=json.dumps({})
        )
        
        result = json.loads(response['Payload'].read())
        print(f"✅ DB 연결 테스트 결과:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result
        
    except Exception as e:
        print(f"❌ DB 연결 테스트 실패: {e}")
        return None

if __name__ == "__main__":
    print("🚀 AWS 공개 Layer를 사용한 Lambda 함수 구성")
    
    try:
        # 1. 기본 기능 테스트
        print("\n📋 1단계: 기본 기능 테스트")
        configure_lambda_with_public_layer()
        time.sleep(5)
        basic_result = test_basic_lambda()
        
        if basic_result and basic_result.get('statusCode') == 200:
            print("✅ 기본 기능 정상 작동")
            
            # 2. DB 연결 테스트
            print("\n📋 2단계: DB 연결 테스트")
            deploy_simple_db_lambda()
            time.sleep(10)  # Layer 연결 대기
            test_db_lambda()
        else:
            print("❌ 기본 기능 테스트 실패")
        
    except Exception as e:
        print(f"❌ 전체 테스트 실패: {e}")
        import traceback
        traceback.print_exc() 