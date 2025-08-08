#!/usr/bin/env python3
"""
Lambda 함수 재배포 스크립트 - 문법 오류 수정 버전
"""

import boto3
import zipfile
import os
import json
from datetime import datetime

def create_deployment_package():
    """배포 패키지 생성"""
    
    # 임시 lambda_function.py 파일 생성 (최적화된 버전 사용)
    lambda_code = '''#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide 티커 스캐너 (수정 버전)
기능: Upbit REST API 직접 호출 → 신규 티커 감지 → DB 업데이트 → 블랙리스트 필터링 → 거래량 필터링 → SQS 전송
"""

import json
import boto3
import logging
import os
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
sqs = boto3.client('sqs')

# Upbit API 엔드포인트
UPBIT_API_BASE = "https://api.upbit.com/v1"
UPBIT_MARKET_ALL_URL = f"{UPBIT_API_BASE}/market/all"

def get_db_connection():
    """PostgreSQL DB 연결 - psycopg2를 동적으로 import"""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as e:
        logger.error(f"psycopg2 import 실패: {e}")
        raise Exception("psycopg2가 사용 불가능합니다.")
    
    try:
        pg_host = os.environ.get('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com')
        pg_port = int(os.environ.get('PG_PORT', '5432'))
        pg_database = os.environ.get('PG_DATABASE', 'makenaide')
        pg_user = os.environ.get('PG_USER', 'bruce')
        pg_password = os.environ.get('PG_PASSWORD')
        
        if not pg_password:
            raise Exception("PG_PASSWORD 환경변수가 설정되지 않았습니다.")
        
        logger.info(f"DB 연결 시도: {pg_host}:{pg_port}/{pg_database}")
        
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise

def get_upbit_krw_tickers() -> List[str]:
    """Upbit REST API를 직접 호출하여 KRW 마켓 티커 목록 조회"""
    try:
        logger.info("📡 Upbit REST API로 마켓 정보 조회 중...")
        
        response = requests.get(UPBIT_MARKET_ALL_URL, timeout=10)
        response.raise_for_status()
        
        markets_data = response.json()
        
        # KRW 마켓만 필터링
        krw_tickers = [
            market['market'] for market in markets_data 
            if market['market'].startswith('KRW-')
        ]
        
        logger.info(f"✅ Upbit REST API에서 {len(krw_tickers)}개 KRW 티커 조회 완료")
        return krw_tickers
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Upbit API 요청 실패: {e}")
        raise Exception(f"Upbit API 티커 목록 조회 실패: {str(e)}")
    except (ValueError, KeyError) as e:
        logger.error(f"❌ Upbit API 응답 파싱 실패: {e}")
        raise Exception(f"Upbit API 응답 형식 오류: {str(e)}")

def load_blacklist_from_db() -> Dict[str, Any]:
    """DB에서 블랙리스트 로드"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, reason FROM blacklist WHERE is_active = true")
        results = cursor.fetchall()
        
        blacklist = {}
        for ticker, reason in results:
            blacklist[ticker] = reason
            
        cursor.close()
        conn.close()
        
        logger.info(f"블랙리스트 로드 완료: {len(blacklist)}개")
        return blacklist
        
    except Exception as e:
        logger.error(f"블랙리스트 로드 실패: {e}")
        return {}

def update_tickers():
    """티커 정보 업데이트 함수"""
    try:
        logger.info("🔄 티커 정보 업데이트 시작")
        
        blacklist = load_blacklist_from_db()
        current_tickers = get_upbit_krw_tickers()
        
        if not current_tickers:
            return False, "Upbit API 티커 목록 조회 실패"

        filtered_tickers = [ticker for ticker in current_tickers if ticker not in blacklist]
        
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT ticker, updated_at FROM tickers")
            existing_rows = cursor.fetchall()
            existing_ticker_times = {row[0]: row[1] for row in existing_rows}

            new_tickers = set(filtered_tickers) - set(existing_ticker_times.keys())
            if new_tickers:
                for new_ticker in new_tickers:
                    cursor.execute(
                        "INSERT INTO tickers (ticker, created_at) VALUES (%s, CURRENT_TIMESTAMP)",
                        (new_ticker,)
                    )
                conn.commit()
                logger.info(f"🎉 신규 티커 감지 및 추가됨: {sorted(new_tickers)}")

            return True, {
                'total_api_tickers': len(current_tickers),
                'filtered_tickers': len(filtered_tickers),
                'new_tickers': len(new_tickers)
            }

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"❌ 티커 업데이트 중 오류: {str(e)}")
        return False, str(e)

def get_active_tickers() -> List[str]:
    """활성 티커 목록 조회"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker FROM tickers WHERE is_active = true ORDER BY ticker")
        tickers = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        logger.info(f"활성 티커 조회 완료: {len(tickers)}개")
        return tickers
        
    except Exception as e:
        logger.error(f"활성 티커 조회 실패: {e}")
        return []

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 Makenaide 티커 스캐너 시작")
        start_time = datetime.now()
        
        # 1. 티커 정보 업데이트
        update_success, update_result = update_tickers()
        if not update_success:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'티커 업데이트 실패: {update_result}',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 2. 활성 티커 조회
        tickers = get_active_tickers()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': '티커 스캐닝 완료',
                'update_result': update_result,
                'total_tickers': len(tickers),
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            })
        }
        
        logger.info(f"✅ 티커 스캐닝 완료: {len(tickers)}개 티커 처리")
        return result
        
    except Exception as e:
        logger.error(f"❌ 티커 스캐닝 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }
'''
    
    # lambda_function.py 파일 생성
    with open('lambda_function.py', 'w', encoding='utf-8') as f:
        f.write(lambda_code)
    
    # ZIP 파일 생성
    zip_filename = 'lambda-deployment-fixed.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write('lambda_function.py', 'lambda_function.py')
    
    print(f"배포 패키지 생성 완료: {zip_filename}")
    return zip_filename

def deploy_lambda():
    """Lambda 함수 배포"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # 배포 패키지 생성
        zip_filename = create_deployment_package()
        
        # ZIP 파일 읽기
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        # Lambda 함수 코드 업데이트
        response = lambda_client.update_function_code(
            FunctionName='makenaide-ticker-scanner',
            ZipFile=zip_content
        )
        
        print(f"✅ Lambda 함수 업데이트 완료: {response['LastModified']}")
        
        # 정리
        os.remove('lambda_function.py')
        os.remove(zip_filename)
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda 함수 배포 실패: {e}")
        return False

def test_lambda():
    """Lambda 함수 테스트"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            Payload=json.dumps({})
        )
        
        result = json.loads(response['Payload'].read())
        print(f"✅ Lambda 함수 실행 결과:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result
        
    except Exception as e:
        print(f"❌ Lambda 함수 테스트 실패: {e}")
        return None

if __name__ == "__main__":
    print("🚀 Lambda 함수 재배포 및 테스트 시작")
    
    # 1. 배포
    if deploy_lambda():
        print("✅ 배포 완료")
        
        # 2. 테스트
        print("\n🧪 Lambda 함수 테스트 중...")
        test_lambda()
    else:
        print("❌ 배포 실패") 