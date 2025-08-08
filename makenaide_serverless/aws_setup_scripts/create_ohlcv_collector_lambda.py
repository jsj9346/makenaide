#!/usr/bin/env python3
"""
OHLCV 수집 전용 Lambda 함수 생성 스크립트

🎯 목적:
- 개별 티커별 OHLCV 데이터 수집을 병렬 Lambda로 처리
- SQS 트리거 방식으로 확장성과 안정성 확보
- 기술적 지표 계산까지 포함한 완전한 데이터 처리

💰 예상 비용 절감: 약 40-50%
"""

import boto3
import json
import os

def create_ohlcv_collector_lambda():
    """OHLCV 수집 Lambda 함수 생성"""
    
    # AWS 클라이언트 초기화
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
    sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
    
    # Lambda 함수 코드 생성
    lambda_code = '''
import json
import boto3
import psycopg2
import os
import logging
import time
from datetime import datetime, date
import pyupbit
import pandas as pd
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def safe_pyupbit_get_ohlcv(ticker, interval="day", count=450, max_retries=3):
    """안전한 pyupbit OHLCV 데이터 조회"""
    for attempt in range(max_retries):
        try:
            time.sleep(0.1 * attempt)  # 재시도 시 지연
            df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"⚠️ {ticker} API 조회 시도 {attempt+1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
    return None

def calculate_basic_indicators(df):
    """기본 기술적 지표 계산"""
    try:
        # 이동평균선
        df['ma_20'] = df['close'].rolling(window=20).mean()
        df['ma_50'] = df['close'].rolling(window=50).mean()
        df['ma_200'] = df['close'].rolling(window=200).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        # MACD
        exp1 = df['close'].ewm(span=12).mean()
        exp2 = df['close'].ewm(span=26).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # 볼린저 밴드
        bb_period = 20
        bb_std = 2
        df['bb_middle'] = df['close'].rolling(window=bb_period).mean()
        bb_std_dev = df['close'].rolling(window=bb_period).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std_dev * bb_std)
        df['bb_lower'] = df['bb_middle'] - (bb_std_dev * bb_std)
        
        # 거래량 지표
        df['volume_20ma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_20ma']
        
        return df
        
    except Exception as e:
        logger.error(f"❌ 기술적 지표 계산 오류: {e}")
        return df

def save_ohlcv_to_db(ticker, df, db_config):
    """OHLCV 데이터를 DB에 저장"""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 데이터 준비
        records = []
        for index, row in df.iterrows():
            if pd.isna(row['close']) or row['close'] <= 0:
                continue
                
            record = (
                ticker,
                index.date() if hasattr(index, 'date') else index,
                float(row['open']) if not pd.isna(row['open']) else None,
                float(row['high']) if not pd.isna(row['high']) else None,
                float(row['low']) if not pd.isna(row['low']) else None,
                float(row['close']) if not pd.isna(row['close']) else None,
                float(row['volume']) if not pd.isna(row['volume']) else None,
                float(row.get('ma_20')) if not pd.isna(row.get('ma_20', np.nan)) else None,
                float(row.get('ma_50')) if not pd.isna(row.get('ma_50', np.nan)) else None,
                float(row.get('ma_200')) if not pd.isna(row.get('ma_200', np.nan)) else None,
                float(row.get('bb_upper')) if not pd.isna(row.get('bb_upper', np.nan)) else None,
                float(row.get('bb_lower')) if not pd.isna(row.get('bb_lower', np.nan)) else None,
                float(row.get('macd_histogram')) if not pd.isna(row.get('macd_histogram', np.nan)) else None,
                float(row.get('rsi_14')) if not pd.isna(row.get('rsi_14', np.nan)) else None,
                float(row.get('volume_20ma')) if not pd.isna(row.get('volume_20ma', np.nan)) else None,
                float(row.get('volume_ratio')) if not pd.isna(row.get('volume_ratio', np.nan)) else None
            )
            records.append(record)
        
        if records:
            # UPSERT 쿼리 실행
            upsert_query = """
                INSERT INTO ohlcv (
                    ticker, date, open, high, low, close, volume,
                    ma_20, ma_50, ma_200, bb_upper, bb_lower, 
                    macd_histogram, rsi_14, volume_20ma, volume_ratio
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) 
                DO UPDATE SET 
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    ma_20 = EXCLUDED.ma_20,
                    ma_50 = EXCLUDED.ma_50,
                    ma_200 = EXCLUDED.ma_200,
                    bb_upper = EXCLUDED.bb_upper,
                    bb_lower = EXCLUDED.bb_lower,
                    macd_histogram = EXCLUDED.macd_histogram,
                    rsi_14 = EXCLUDED.rsi_14,
                    volume_20ma = EXCLUDED.volume_20ma,
                    volume_ratio = EXCLUDED.volume_ratio
            """
            
            cursor.executemany(upsert_query, records)
            conn.commit()
            
            logger.info(f"✅ {ticker} OHLCV 데이터 {len(records)}개 저장 완료")
            
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ {ticker} DB 저장 오류: {e}")
        return False

def lambda_handler(event, context):
    """
    OHLCV 수집 Lambda 함수
    
    SQS 메시지로부터 티커 목록을 받아서 OHLCV 데이터를 수집하고 처리
    """
    try:
        logger.info("📊 OHLCV 수집 Lambda 함수 시작")
        
        # 환경변수에서 DB 연결 정보 가져오기
        db_config = {
            'host': os.environ['DB_HOST'],
            'port': os.environ['DB_PORT'],
            'database': os.environ['DB_NAME'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD']
        }
        
        processed_tickers = []
        failed_tickers = []
        
        # SQS Records 처리
        if 'Records' in event:
            for record in event['Records']:
                try:
                    message_body = json.loads(record['body'])
                    tickers = message_body.get('tickers', [])
                    
                    logger.info(f"📋 처리할 티커: {len(tickers)}개")
                    
                    for ticker in tickers:
                        try:
                            # 1. OHLCV 데이터 수집
                            df = safe_pyupbit_get_ohlcv(ticker, interval="day", count=450)
                            
                            if df is None or df.empty:
                                logger.warning(f"⚠️ {ticker} OHLCV 데이터 수집 실패")
                                failed_tickers.append(ticker)
                                continue
                            
                            # 2. 기술적 지표 계산
                            df_with_indicators = calculate_basic_indicators(df)
                            
                            # 3. DB 저장
                            save_success = save_ohlcv_to_db(ticker, df_with_indicators, db_config)
                            
                            if save_success:
                                processed_tickers.append(ticker)
                                logger.info(f"✅ {ticker} 처리 완료")
                            else:
                                failed_tickers.append(ticker)
                                
                        except Exception as e:
                            logger.error(f"❌ {ticker} 처리 중 오류: {e}")
                            failed_tickers.append(ticker)
                            
                except Exception as e:
                    logger.error(f"❌ SQS 메시지 처리 오류: {e}")
        else:
            # 직접 호출의 경우
            tickers = event.get('tickers', [])
            logger.info(f"📋 직접 호출 - 처리할 티커: {len(tickers)}개")
            
            for ticker in tickers:
                try:
                    df = safe_pyupbit_get_ohlcv(ticker, interval="day", count=450)
                    if df is not None and not df.empty:
                        df_with_indicators = calculate_basic_indicators(df)
                        if save_ohlcv_to_db(ticker, df_with_indicators, db_config):
                            processed_tickers.append(ticker)
                        else:
                            failed_tickers.append(ticker)
                    else:
                        failed_tickers.append(ticker)
                except Exception as e:
                    logger.error(f"❌ {ticker} 처리 중 오류: {e}")
                    failed_tickers.append(ticker)
        
        # 4. 후속 처리 트리거 (필터링 Lambda)
        if processed_tickers:
            try:
                sqs = boto3.client('sqs')
                filter_queue_url = os.environ.get('FILTER_QUEUE_URL')
                
                if filter_queue_url:
                    filter_message = {
                        'action': 'filter_tickers',
                        'tickers': processed_tickers,
                        'timestamp': datetime.utcnow().isoformat(),
                        'session_id': context.aws_request_id
                    }
                    
                    sqs.send_message(
                        QueueUrl=filter_queue_url,
                        MessageBody=json.dumps(filter_message)
                    )
                    
                    logger.info(f"📤 {len(processed_tickers)}개 티커를 필터링 큐로 전송")
            except Exception as e:
                logger.warning(f"⚠️ 필터링 큐 전송 실패: {e}")
        
        # 5. 응답 반환
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'processed_tickers': len(processed_tickers),
                'failed_tickers': len(failed_tickers),
                'processed_list': processed_tickers,
                'failed_list': failed_tickers,
                'session_id': context.aws_request_id
            })
        }
        
        logger.info(f"✅ OHLCV 수집 완료: 성공 {len(processed_tickers)}개, 실패 {len(failed_tickers)}개")
        return response
        
    except Exception as e:
        logger.error(f"❌ OHLCV 수집 Lambda 오류: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error_message': str(e),
                'session_id': context.aws_request_id
            })
        }
'''
    
    # Lambda 패키지 생성
    package_dir = 'ohlcv_collector_package'
    os.makedirs(package_dir, exist_ok=True)
    
    # lambda_function.py 파일 생성
    with open(f'{package_dir}/lambda_function.py', 'w', encoding='utf-8') as f:
        f.write(lambda_code)
    
    # requirements.txt 생성
    with open(f'{package_dir}/requirements.txt', 'w') as f:
        f.write("""psycopg2-binary==2.9.7
boto3==1.28.44
pyupbit==0.2.31
pandas==2.0.3
numpy==1.24.3
""")
    
    # ZIP 패키지 생성
    import zipfile
    import shutil
    
    zip_filename = 'ohlcv_collector_lambda.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, package_dir)
                zipf.write(file_path, arcname)
    
    # Lambda 함수 생성/업데이트
    function_name = 'makenaide-ohlcv-collector'
    
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
            Description='Makenaide OHLCV 수집 전용 Lambda 함수',
            Timeout=900,  # 15분 (최대)
            MemorySize=1024,  # 1GB
            Environment={
                'Variables': {
                    'DB_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
                    'DB_PORT': '5432',
                    'DB_NAME': 'makenaide',
                    'DB_USER': 'bruce',
                    'DB_PASSWORD': 'REPLACE_WITH_ACTUAL_PASSWORD',
                    'FILTER_QUEUE_URL': 'https://sqs.ap-northeast-2.amazonaws.com/ACCOUNT_ID/makenaide-filter'
                }
            }
        )
        print(f"✅ Lambda 함수 생성 완료: {function_name}")
    
    # 정리
    shutil.rmtree(package_dir)
    os.remove(zip_filename)
    
    return response['FunctionArn']

def create_sqs_queues():
    """필요한 SQS 큐들 생성"""
    sqs = boto3.client('sqs', region_name='ap-northeast-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    
    queues = [
        {
            'name': 'makenaide-ohlcv-collection',
            'description': 'OHLCV 수집 작업 큐'
        },
        {
            'name': 'makenaide-filter',
            'description': '티커 필터링 작업 큐'
        }
    ]
    
    created_queues = {}
    
    for queue in queues:
        try:
            response = sqs.create_queue(
                QueueName=queue['name'],
                Attributes={
                    'VisibilityTimeoutSeconds': '900',  # 15분
                    'MessageRetentionPeriod': '1209600',  # 14일
                    'ReceiveMessageWaitTimeSeconds': '20'  # 롱 폴링
                }
            )
            
            queue_url = response['QueueUrl']
            created_queues[queue['name']] = queue_url
            print(f"✅ SQS 큐 생성 완료: {queue['name']}")
            
        except sqs.exceptions.QueueNameExists:
            # 기존 큐 URL 가져오기
            response = sqs.get_queue_url(QueueName=queue['name'])
            queue_url = response['QueueUrl']
            created_queues[queue['name']] = queue_url
            print(f"✅ 기존 SQS 큐 사용: {queue['name']}")
    
    return created_queues

if __name__ == '__main__':
    print("🚀 OHLCV 수집 Lambda 함수 생성 시작...")
    try:
        # 1. SQS 큐 생성
        print("📨 SQS 큐 생성 중...")
        queues = create_sqs_queues()
        
        # 2. Lambda 함수 생성
        function_arn = create_ohlcv_collector_lambda()
        print(f"✅ OHLCV 수집 Lambda 함수 생성/업데이트 완료")
        print(f"📋 함수 ARN: {function_arn}")
        
        print("\n📋 생성된 SQS 큐:")
        for name, url in queues.items():
            print(f"  - {name}: {url}")
        
        print("\n🎯 다음 단계:")
        print("1. SQS 트리거 설정 (OHLCV 수집 Lambda ← OHLCV 큐)")
        print("2. 필터링 Lambda 함수 생성")
        print("3. 환경변수 실제 값으로 업데이트")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}") 