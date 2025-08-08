#!/usr/bin/env python3
"""
Lambda 티커 데이터 수정 함수
Lambda 환경에서 직접 티커 데이터를 삽입하여 문제 해결
"""

import json
import logging
import os
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fix_tickers_data():
    """Lambda 환경에서 직접 티커 데이터를 삽입"""
    try:
        import psycopg2
        
        # DB 연결
        pg_host = os.environ.get('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com')
        pg_port = int(os.environ.get('PG_PORT', '5432'))
        pg_database = os.environ.get('PG_DATABASE', 'makenaide')
        pg_user = os.environ.get('PG_USER', 'bruce')
        pg_password = os.environ.get('PG_PASSWORD')
        
        logger.info(f"DB 연결: {pg_host}:{pg_port}/{pg_database}")
        
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password,
            connect_timeout=15
        )
        cursor = conn.cursor()
        
        # 1. 현재 상태 확인
        cursor.execute("SELECT COUNT(*) FROM tickers")
        current_count = cursor.fetchone()[0]
        logger.info(f"현재 티커 수: {current_count}")
        
        # 2. 샘플 KRW 티커들을 직접 삽입
        sample_tickers = [
            'KRW-BTC', 'KRW-ETH', 'KRW-ADA', 'KRW-DOT', 'KRW-LINK',
            'KRW-XRP', 'KRW-SOL', 'KRW-AVAX', 'KRW-MATIC', 'KRW-ATOM',
            'KRW-NEAR', 'KRW-ALGO', 'KRW-MANA', 'KRW-SAND', 'KRW-AXS',
            'KRW-KLAY', 'KRW-ICX', 'KRW-QTUM', 'KRW-OMG', 'KRW-CRO',
            'KRW-ENJ', 'KRW-ANKR', 'KRW-STORJ', 'KRW-GRT', 'KRW-MED',
            'KRW-TFUEL', 'KRW-VET', 'KRW-CHZ', 'KRW-THETA', 'KRW-SNT'
        ]
        
        logger.info(f"삽입할 샘플 티커: {len(sample_tickers)}개")
        
        inserted_count = 0
        for ticker in sample_tickers:
            try:
                cursor.execute("""
                    INSERT INTO tickers (ticker, created_at, updated_at, is_active) 
                    VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true)
                    ON CONFLICT (ticker) DO UPDATE SET 
                        updated_at = CURRENT_TIMESTAMP,
                        is_active = true
                """, (ticker,))
                inserted_count += 1
            except Exception as e:
                logger.warning(f"티커 {ticker} 삽입 실패: {e}")
        
        conn.commit()
        logger.info(f"티커 삽입 완료: {inserted_count}개")
        
        # 3. 결과 확인
        cursor.execute("SELECT COUNT(*) FROM tickers")
        final_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        active_count = cursor.fetchone()[0]
        
        # 4. 삽입된 데이터 샘플 확인
        cursor.execute("SELECT ticker, is_active, created_at FROM tickers ORDER BY created_at DESC LIMIT 5")
        sample_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        result = {
            'initial_count': current_count,
            'inserted_count': inserted_count,
            'final_total': final_count,
            'active_count': active_count,
            'sample_data': [
                {
                    'ticker': row[0],
                    'is_active': row[1],
                    'created_at': row[2].isoformat() if row[2] else None
                }
                for row in sample_data
            ]
        }
        
        logger.info(f"✅ 티커 데이터 수정 완료: {result}")
        return True, result
        
    except Exception as e:
        logger.error(f"❌ 티커 데이터 수정 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False, str(e)

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🔧 Lambda 티커 데이터 수정 시작")
        
        success, result = fix_tickers_data()
        
        if success:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': '티커 데이터 수정 성공',
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }, ensure_ascii=False)
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'티커 데이터 수정 실패: {result}',
                    'timestamp': datetime.now().isoformat()
                }, ensure_ascii=False)
            }
        
    except Exception as e:
        logger.error(f"❌ Lambda 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, ensure_ascii=False)
        }

# 로컬 테스트용
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2, ensure_ascii=False))