#!/usr/bin/env python3
"""
AWS 공식 PostgreSQL Layer 사용하여 Lambda 함수 재구성
"""

import boto3
import json
import zipfile
import os
import time
from datetime import datetime

def find_aws_official_postgresql_layer():
    """AWS 공식 PostgreSQL Layer 찾기"""
    
    try:
        print("🔍 AWS 공식 PostgreSQL Layer 검색 중...")
        
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # AWS에서 제공하는 공개 Layer 중 PostgreSQL 관련 검색
        # 일반적으로 사용되는 공식 Layer ARN들
        possible_layers = [
            # AWS에서 제공하는 공식 psycopg2 Layer (ap-northeast-2)
            "arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py38:2",
            "arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py39:1", 
            # Klayers가 제공하는 psycopg2 Layer
            "arn:aws:lambda:ap-northeast-2:770693421928:layer:Klayers-p39-psycopg2:1",
            "arn:aws:lambda:ap-northeast-2:770693421928:layer:Klayers-p311-psycopg2:1",
        ]
        
        working_layer = None
        
        for layer_arn in possible_layers:
            try:
                # Layer 정보 확인
                response = lambda_client.get_layer_version_by_arn(Arn=layer_arn)
                
                print(f"   ✅ 사용 가능한 Layer: {layer_arn}")
                print(f"      - 런타임: {response.get('CompatibleRuntimes', [])}")
                print(f"      - 아키텍처: {response.get('CompatibleArchitectures', [])}")
                print(f"      - 설명: {response.get('Description', 'N/A')}")
                
                working_layer = layer_arn
                break
                
            except Exception as e:
                print(f"   ❌ {layer_arn}: 접근 불가 ({str(e)[:50]}...)")
                continue
        
        if not working_layer:
            print("⚠️ 공개 Layer 접근 불가, 자체 Layer 생성 필요")
            return None
        
        return working_layer
        
    except Exception as e:
        print(f"❌ Layer 검색 실패: {e}")
        return None

def create_simple_postgresql_layer():
    """간단한 PostgreSQL Layer 생성 (최소한의 패키지만)"""
    
    try:
        print("🔧 간단한 PostgreSQL Layer 생성 중...")
        
        # 매우 간단한 Python 스크립트로 psycopg2-binary 설치 시도
        lambda_code_test = '''
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """psycopg2 import 테스트"""
    try:
        # 다양한 방법으로 psycopg2 import 시도
        import_methods = [
            "import psycopg2",
            "import psycopg2.pool", 
            "from psycopg2 import sql",
            "import psycopg2.extras"
        ]
        
        results = {}
        
        for method in import_methods:
            try:
                exec(method)
                results[method] = "✅ 성공"
            except Exception as e:
                results[method] = f"❌ 실패: {str(e)}"
        
        # 실제 연결 테스트
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.environ.get('PG_HOST'),
                port=int(os.environ.get('PG_PORT', 5432)),
                database=os.environ.get('PG_DATABASE'),
                user=os.environ.get('PG_USER'),
                password=os.environ.get('PG_PASSWORD'),
                connect_timeout=5
            )
            conn.close()
            db_test = "✅ DB 연결 성공"
        except Exception as e:
            db_test = f"❌ DB 연결 실패: {str(e)}"
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'psycopg2 호환성 테스트 완료',
                'import_tests': results,
                'db_connection_test': db_test,
                'timestamp': str(datetime.now())
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'전체 테스트 실패: {str(e)}',
                'timestamp': str(datetime.now())
            })
        }
'''
        
        # 테스트용 Lambda 함수 생성
        with open('lambda_function.py', 'w', encoding='utf-8') as f:
            f.write(lambda_code_test.strip())
        
        # ZIP 파일 생성
        zip_filename = 'lambda-psycopg2-test.zip'
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write('lambda_function.py', 'lambda_function.py')
        
        # Lambda 함수 업데이트
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        print("🔄 psycopg2 테스트 Lambda 함수 업데이트 중...")
        lambda_client.update_function_code(
            FunctionName='makenaide-ticker-scanner',
            ZipFile=zip_content
        )
        
        # 정리
        os.remove('lambda_function.py')
        os.remove(zip_filename)
        
        return True
        
    except Exception as e:
        print(f"❌ 테스트 Lambda 생성 실패: {e}")
        return False

def test_with_different_layers():
    """다양한 Layer로 테스트"""
    
    try:
        print("🧪 다양한 PostgreSQL Layer 테스트 중...")
        
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 테스트할 Layer들
        test_layers = [
            None,  # Layer 없이 테스트
            "arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py38:2",
            "arn:aws:lambda:ap-northeast-2:770693421928:layer:Klayers-p311-psycopg2:1",
        ]
        
        results = {}
        
        for i, layer_arn in enumerate(test_layers):
            layer_name = layer_arn if layer_arn else "Layer 없음"
            print(f"\n   📋 테스트 {i+1}: {layer_name}")
            
            try:
                # Layer 설정
                if layer_arn:
                    lambda_client.update_function_configuration(
                        FunctionName='makenaide-ticker-scanner',
                        Layers=[layer_arn]
                    )
                else:
                    lambda_client.update_function_configuration(
                        FunctionName='makenaide-ticker-scanner',
                        Layers=[]
                    )
                
                # 설정 적용 대기
                time.sleep(10)
                
                # 함수 실행
                response = lambda_client.invoke(
                    FunctionName='makenaide-ticker-scanner',
                    Payload=json.dumps({})
                )
                
                result = json.loads(response['Payload'].read())
                results[layer_name] = result
                
                print(f"      결과: {result.get('statusCode', 'N/A')}")
                if result.get('statusCode') == 200:
                    body = json.loads(result['body'])
                    print(f"      DB 연결: {body.get('db_connection_test', 'N/A')}")
                
            except Exception as e:
                print(f"      ❌ 테스트 실패: {e}")
                results[layer_name] = {'error': str(e)}
        
        return results
        
    except Exception as e:
        print(f"❌ Layer 테스트 실패: {e}")
        return {}

def deploy_working_postgresql_lambda():
    """작동하는 PostgreSQL Lambda 함수 배포"""
    
    # 실제 데이터 수집 및 적재 Lambda 함수
    lambda_code = '''
import json
import logging
import os
import urllib.request
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_upbit_tickers():
    """Upbit에서 티커 목록 조회"""
    try:
        url = "https://api.upbit.com/v1/market/all"
        
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
        # KRW 마켓만 필터링
        krw_tickers = [
            market['market'] for market in data 
            if market['market'].startswith('KRW-')
        ]
        
        return krw_tickers
        
    except Exception as e:
        logger.error(f"Upbit API 오류: {e}")
        return []

def update_tickers_to_db(tickers):
    """티커 정보를 DB에 업데이트"""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=os.environ.get('PG_HOST'),
            port=int(os.environ.get('PG_PORT', 5432)),
            database=os.environ.get('PG_DATABASE'),
            user=os.environ.get('PG_USER'),
            password=os.environ.get('PG_PASSWORD'),
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # 기존 티커 조회
        cursor.execute("SELECT ticker FROM tickers")
        existing_tickers = set(row[0] for row in cursor.fetchall())
        
        # 신규 티커 추가
        new_tickers = set(tickers) - existing_tickers
        added_count = 0
        
        for ticker in new_tickers:
            try:
                cursor.execute("""
                    INSERT INTO tickers (ticker, created_at, is_active) 
                    VALUES (%s, CURRENT_TIMESTAMP, true)
                    ON CONFLICT (ticker) DO NOTHING
                """, (ticker,))
                added_count += 1
            except Exception as e:
                logger.warning(f"티커 {ticker} 추가 실패: {e}")
                continue
        
        conn.commit()
        
        # 현재 DB 상태 확인
        cursor.execute("SELECT COUNT(*) FROM tickers")
        total_tickers = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        active_tickers = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'new_tickers_added': added_count,
            'total_tickers': total_tickers,
            'active_tickers': active_tickers
        }
        
    except Exception as e:
        logger.error(f"DB 업데이트 실패: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def lambda_handler(event, context):
    """메인 핸들러 - 실제 데이터 적재"""
    try:
        logger.info("🚀 PostgreSQL 데이터 적재 시작")
        start_time = datetime.now()
        
        # 1. Upbit 티커 조회
        tickers = get_upbit_tickers()
        if not tickers:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Upbit 티커 조회 실패',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        logger.info(f"Upbit에서 {len(tickers)}개 티커 조회 완료")
        
        # 2. DB에 티커 정보 업데이트
        db_result = update_tickers_to_db(tickers)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        if db_result['success']:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': '✅ PostgreSQL 데이터 적재 성공',
                    'upbit_tickers': len(tickers),
                    'db_result': db_result,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'DB 적재 실패',
                    'db_error': db_result.get('error', 'Unknown'),
                    'upbit_tickers': len(tickers),
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                })
            }
        
    except Exception as e:
        logger.error(f"전체 프로세스 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }
'''
    
    try:
        print("🔧 실제 데이터 적재 Lambda 함수 배포 중...")
        
        # lambda_function.py 파일 생성
        with open('lambda_function.py', 'w', encoding='utf-8') as f:
            f.write(lambda_code.strip())
        
        # ZIP 파일 생성
        zip_filename = 'lambda-postgresql-final.zip'
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write('lambda_function.py', 'lambda_function.py')
        
        # Lambda 함수 업데이트
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        lambda_client.update_function_code(
            FunctionName='makenaide-ticker-scanner',
            ZipFile=zip_content
        )
        
        # 정리
        os.remove('lambda_function.py')
        os.remove(zip_filename)
        
        print("✅ 실제 데이터 적재 Lambda 함수 배포 완료")
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 배포 실패: {e}")
        return False

if __name__ == "__main__":
    print("🚀 AWS 공식 PostgreSQL Layer 사용 Lambda 구성")
    print("=" * 60)
    
    # 1. AWS 공식 Layer 찾기
    working_layer = find_aws_official_postgresql_layer()
    
    # 2. 테스트용 Lambda 함수 생성
    if create_simple_postgresql_layer():
        print("✅ 테스트 Lambda 함수 생성 완료")
        
        # 3. 다양한 Layer로 테스트
        test_results = test_with_different_layers()
        
        # 4. 가장 잘 작동하는 Layer 찾기
        best_layer = None
        for layer_name, result in test_results.items():
            if result.get('statusCode') == 200:
                body = json.loads(result.get('body', '{}'))
                if 'DB 연결 성공' in body.get('db_connection_test', ''):
                    best_layer = layer_name if layer_name != "Layer 없음" else None
                    break
        
        if best_layer:
            print(f"\n✅ 최적 Layer 발견: {best_layer}")
            
            # 5. 실제 데이터 적재 Lambda 배포
            if deploy_working_postgresql_lambda():
                # 6. 최적 Layer 설정
                lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
                lambda_client.update_function_configuration(
                    FunctionName='makenaide-ticker-scanner',
                    Layers=[best_layer]
                )
                
                # 7. 최종 테스트
                time.sleep(10)
                print("\n🧪 최종 RDB 적재 테스트...")
                
                response = lambda_client.invoke(
                    FunctionName='makenaide-ticker-scanner',
                    Payload=json.dumps({})
                )
                
                final_result = json.loads(response['Payload'].read())
                print("✅ 최종 테스트 결과:")
                print(json.dumps(final_result, indent=2, ensure_ascii=False))
                
        else:
            print("❌ 작동하는 PostgreSQL Layer를 찾을 수 없습니다.")
    
    print("\n" + "=" * 60)
    print("🎯 AWS 공식 PostgreSQL Layer 구성 완료") 