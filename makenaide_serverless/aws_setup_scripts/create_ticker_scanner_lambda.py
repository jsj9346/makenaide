#!/usr/bin/env python3
"""
티커 스캔 전용 Lambda 함수 생성 스크립트

🎯 목적:
- 4시간마다 실행되는 티커 스캔 프로세스를 독립적인 Lambda 함수로 분리
- EC2 실행 시간 단축을 통한 비용 최적화 달성
- 스캔 결과를 SQS나 EventBridge를 통해 다음 단계로 전달

💰 예상 비용 절감: 약 30-40%
"""

import boto3
import json
import os
from datetime import datetime

def create_ticker_scanner_lambda():
    """티커 스캔 Lambda 함수 생성"""
    
    # AWS 클라이언트 초기화
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    iam_client = boto3.client('iam')
    
    # Lambda 함수 코드 생성
    lambda_code = '''
import json
import boto3
import psycopg2
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    티커 스캔 Lambda 함수
    
    기능:
    1. DB에서 모든 티커 조회
    2. 블랙리스트 필터링 적용
    3. 기본 거래량/시가총액 필터링
    4. 스캔 결과를 SQS로 전송하여 다음 단계 트리거
    """
    try:
        logger.info("🔍 티커 스캔 Lambda 함수 시작")
        
        # 환경변수에서 DB 연결 정보 가져오기
        db_config = {
            'host': os.environ['DB_HOST'],
            'port': os.environ['DB_PORT'], 
            'database': os.environ['DB_NAME'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD']
        }
        
        # DB 연결
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 1. 모든 티커 조회
        cursor.execute("SELECT ticker FROM tickers")
        all_tickers = [row[0] for row in cursor.fetchall()]
        logger.info(f"📊 DB에서 {len(all_tickers)}개 티커 로드됨")
        
        # 2. 블랙리스트 적용 (환경변수에서 읽기)
        blacklist_str = os.environ.get('TICKER_BLACKLIST', '')
        blacklist = blacklist_str.split(',') if blacklist_str else []
        filtered_tickers = [ticker for ticker in all_tickers if ticker not in blacklist]
        logger.info(f"🚫 블랙리스트 적용 후 {len(filtered_tickers)}개 티커")
        
        # 3. 기본 거래량 필터링 (24시간 거래대금 > 1억원)
        volume_filtered_tickers = []
        for ticker in filtered_tickers[:100]:  # 성능을 위해 제한
            try:
                cursor.execute("""
                    SELECT volume * close as trade_amount
                    FROM ohlcv 
                    WHERE ticker = %s 
                    ORDER BY date DESC 
                    LIMIT 1
                """, (ticker,))
                
                result = cursor.fetchone()
                if result and result[0] and result[0] > 100000000:  # 1억원 이상
                    volume_filtered_tickers.append(ticker)
            except Exception as e:
                logger.warning(f"⚠️ {ticker} 거래량 확인 실패: {e}")
                continue
        
        logger.info(f"💰 거래량 필터링 후 {len(volume_filtered_tickers)}개 티커")
        
        # 4. 결과를 SQS로 전송
        sqs = boto3.client('sqs')
        queue_url = os.environ.get('OHLCV_COLLECTION_QUEUE_URL')
        
        if queue_url and volume_filtered_tickers:
            # 티커들을 청크로 나누어 전송 (Lambda 동시 실행 제한 고려)
            chunk_size = 10
            chunks = [volume_filtered_tickers[i:i+chunk_size] 
                     for i in range(0, len(volume_filtered_tickers), chunk_size)]
            
            sent_count = 0
            for chunk in chunks:
                message = {
                    'action': 'collect_ohlcv',
                    'tickers': chunk,
                    'timestamp': datetime.utcnow().isoformat(),
                    'scan_session_id': context.aws_request_id
                }
                
                try:
                    sqs.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(message)
                    )
                    sent_count += len(chunk)
                except Exception as e:
                    logger.error(f"❌ SQS 메시지 전송 실패: {e}")
            
            logger.info(f"📤 {sent_count}개 티커를 OHLCV 수집 큐로 전송")
        
        # 5. 응답 반환
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'total_tickers': len(all_tickers),
                'filtered_tickers': len(volume_filtered_tickers),
                'sent_to_queue': len(volume_filtered_tickers) if queue_url else 0,
                'scan_session_id': context.aws_request_id,
                'execution_time': context.get_remaining_time_in_millis()
            })
        }
        
        # DB 연결 정리
        cursor.close()
        conn.close()
        
        logger.info(f"✅ 티커 스캔 완료: {len(volume_filtered_tickers)}개 선별")
        return response
        
    except Exception as e:
        logger.error(f"❌ 티커 스캔 Lambda 오류: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error_message': str(e),
                'scan_session_id': context.aws_request_id
            })
        }
'''
    
    # Lambda 패키지 생성
    package_dir = 'ticker_scanner_package'
    os.makedirs(package_dir, exist_ok=True)
    
    # lambda_function.py 파일 생성
    with open(f'{package_dir}/lambda_function.py', 'w', encoding='utf-8') as f:
        f.write(lambda_code)
    
    # requirements.txt 생성
    with open(f'{package_dir}/requirements.txt', 'w') as f:
        f.write("""psycopg2-binary==2.9.7
boto3==1.28.44
""")
    
    # ZIP 패키지 생성
    import zipfile
    import shutil
    
    zip_filename = 'ticker_scanner_lambda.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, package_dir)
                zipf.write(file_path, arcname)
    
    # Lambda 함수 생성/업데이트
    function_name = 'makenaide-ticker-scanner'
    
    try:
        # 기존 함수 확인
        lambda_client.get_function(FunctionName=function_name)
        
        # 기존 함수 업데이트
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        print(f"✅ Lambda 함수 업데이트 완료: {function_name}")
        
    except lambda_client.exceptions.ResourceNotFoundException:
        # 새 함수 생성
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.9',
            Role=f'arn:aws:iam::{boto3.client("sts").get_caller_identity()["Account"]}:role/makenaide-lambda-role',
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Makenaide 티커 스캔 전용 Lambda 함수',
            Timeout=300,  # 5분
            MemorySize=512,  # 512MB
            Environment={
                'Variables': {
                    'DB_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
                    'DB_PORT': '5432',
                    'DB_NAME': 'makenaide',
                    'DB_USER': 'bruce',
                    'DB_PASSWORD': 'REPLACE_WITH_ACTUAL_PASSWORD',
                    'TICKER_BLACKLIST': 'KRW-USDT,KRW-USDC,KRW-DAI',
                    'OHLCV_COLLECTION_QUEUE_URL': 'https://sqs.ap-northeast-2.amazonaws.com/ACCOUNT_ID/makenaide-ohlcv-collection'
                }
            }
        )
        print(f"✅ Lambda 함수 생성 완료: {function_name}")
    
    # 정리
    shutil.rmtree(package_dir)
    os.remove(zip_filename)
    
    return response['FunctionArn']

if __name__ == '__main__':
    print("🚀 티커 스캔 Lambda 함수 생성 시작...")
    try:
        function_arn = create_ticker_scanner_lambda()
        print(f"✅ 티커 스캔 Lambda 함수 생성/업데이트 완료")
        print(f"📋 함수 ARN: {function_arn}")
        print("\n🎯 다음 단계:")
        print("1. SQS 큐 생성 (OHLCV 수집용)")
        print("2. EventBridge 스케줄 설정 (4시간 간격)")
        print("3. 환경변수 실제 값으로 업데이트")
    except Exception as e:
        print(f"❌ 오류 발생: {e}") 