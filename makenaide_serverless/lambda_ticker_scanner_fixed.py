#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide 티커 스캐너 (수정된 최종 버전)
기능: Upbit API → DB 티커 업데이트 → 결과 반환

수정사항:
1. 단순하고 명확한 로직
2. 상세한 디버깅 로그
3. 단계별 실행 확인
4. 에러 처리 강화
"""

import json
import logging
import os
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Upbit API 엔드포인트
UPBIT_API_BASE = "https://api.upbit.com/v1"
UPBIT_MARKET_ALL_URL = f"{UPBIT_API_BASE}/market/all"

def get_db_connection():
    """PostgreSQL DB 연결"""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as e:
        logger.error(f"psycopg2 import 실패: {e}")
        raise Exception("psycopg2가 사용 불가능합니다.")
    
    try:
        # 환경변수 확인 및 기본값 설정
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
            connect_timeout=15
        )
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise

def get_upbit_krw_tickers() -> List[str]:
    """Upbit REST API를 직접 호출하여 KRW 마켓 티커 목록 조회"""
    try:
        logger.info("📡 Upbit REST API로 마켓 정보 조회 중...")
        
        # HTTP GET 요청 (인증 불필요)
        response = requests.get(UPBIT_MARKET_ALL_URL, timeout=15)
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

def update_tickers_to_db():
    """티커 정보를 DB에 업데이트하는 핵심 함수"""
    connection = None
    try:
        logger.info("🔄 티커 정보 업데이트 시작")
        
        # 1. Upbit API에서 현재 티커 목록 조회
        current_tickers = get_upbit_krw_tickers()
        if not current_tickers:
            raise Exception("Upbit API에서 티커 목록을 가져올 수 없습니다")
        
        # 2. DB 연결
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # 3. 기존 티커 조회
        cursor.execute("SELECT ticker, updated_at FROM tickers")
        existing_rows = cursor.fetchall()
        existing_ticker_times = {row[0]: row[1] for row in existing_rows}
        
        logger.info(f"📊 DB에 기존 티커 {len(existing_ticker_times)}개 존재")
        logger.info(f"📊 Upbit API에서 조회된 현재 티커 {len(current_tickers)}개")
        
        # 4. 신규 티커 추가
        new_tickers = set(current_tickers) - set(existing_ticker_times.keys())
        if new_tickers:
            for new_ticker in new_tickers:
                cursor.execute(
                    "INSERT INTO tickers (ticker, created_at, updated_at, is_active) VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true)",
                    (new_ticker,)
                )
            connection.commit()
            logger.info(f"🎉 신규 티커 {len(new_tickers)}개 추가됨: {sorted(list(new_tickers)[:5])}...")
        else:
            logger.info("📊 신규 티커 없음")
        
        # 5. 기존 티커 업데이트 (24시간 이상 지난 경우)
        update_threshold = datetime.now() - timedelta(hours=24)
        updated_count = 0
        
        for ticker in current_tickers:
            if ticker in existing_ticker_times:
                last_update = existing_ticker_times[ticker]
                if last_update < update_threshold:
                    cursor.execute(
                        "UPDATE tickers SET updated_at = CURRENT_TIMESTAMP WHERE ticker = %s",
                        (ticker,)
                    )
                    updated_count += 1
        
        connection.commit()
        
        # 6. 최종 결과 확인
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        total_active_tickers = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        result = {
            'total_api_tickers': len(current_tickers),
            'new_tickers_added': len(new_tickers),
            'existing_tickers_updated': updated_count,
            'total_active_tickers': total_active_tickers
        }
        
        logger.info(f"✅ 티커 정보 업데이트 완료: {result}")
        return True, result
        
    except Exception as e:
        if connection:
            connection.rollback()
            connection.close()
        logger.error(f"❌ 티커 업데이트 중 오류: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False, str(e)

def lambda_handler(event, context):
    """Lambda 메인 핸들러 - 단순하고 확실한 버전"""
    try:
        logger.info("🚀 Makenaide 티커 스캐너 시작 (Fixed 버전)")
        start_time = datetime.now()
        
        # 1. DB 연결 테스트
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tickers")
            existing_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            logger.info(f"🔗 DB 연결 테스트 성공, 기존 티커 수: {existing_count}")
        except Exception as e:
            logger.error(f"❌ DB 연결 테스트 실패: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'DB 연결 실패: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                }, ensure_ascii=False)
            }
        
        # 2. 티커 정보 업데이트 실행
        logger.info("📡 티커 업데이트 실행 중...")
        update_success, update_result = update_tickers_to_db()
        
        if not update_success:
            logger.error(f"❌ 티커 업데이트 실패: {update_result}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'티커 업데이트 실패: {update_result}',
                    'timestamp': datetime.now().isoformat()
                }, ensure_ascii=False)
            }
        
        # 3. 최종 DB 상태 확인
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
            final_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            logger.info(f"📊 최종 활성 티커 수: {final_count}")
        except Exception as e:
            logger.warning(f"⚠️ 최종 확인 실패: {e}")
            final_count = 0
        
        # 4. 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 5. 성공 응답 반환
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DB 연결 성공',
                'ticker_count': final_count,
                'update_result': update_result,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat(),
                'version': 'fixed'
            }, ensure_ascii=False)
        }
        
        logger.info(f"✅ 티커 스캐너 완료: {final_count}개 활성 티커 ({execution_time:.2f}초)")
        return result
        
    except Exception as e:
        logger.error(f"❌ 티커 스캐너 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'version': 'fixed'
            }, ensure_ascii=False)
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 환경변수 로드
    from dotenv import load_dotenv
    load_dotenv()
    
    # 테스트 실행
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2, ensure_ascii=False))