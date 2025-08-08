#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide 티커 스캐너 (간단한 DB 업데이트 버전)
기능: 기존 DB 티커 활성화 → 결과 반환

특징:
- Upbit API 호출 없이 기존 데이터 활용
- 빠른 실행 (네트워크 타임아웃 없음)
- 단순하고 안정적인 로직
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

def refresh_ticker_status():
    """기존 티커들의 상태를 새로고침하는 간단한 함수"""
    connection = None
    try:
        logger.info("🔄 티커 상태 새로고침 시작")
        
        # DB 연결
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # 1. 현재 DB의 티커 현황 조회
        cursor.execute("SELECT COUNT(*) FROM tickers")
        total_tickers = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        active_tickers = cursor.fetchone()[0]
        
        logger.info(f"📊 DB 현황: 전체 {total_tickers}개, 활성 {active_tickers}개")
        
        # 2. 비활성 티커들을 활성화 (단순 업데이트)
        cursor.execute("UPDATE tickers SET is_active = true WHERE is_active = false OR is_active IS NULL")
        updated_count = cursor.rowcount
        
        # 3. updated_at 타임스탬프 갱신 (24시간 이상 지난 것들)
        update_threshold = datetime.now() - timedelta(hours=24)
        cursor.execute(
            "UPDATE tickers SET updated_at = CURRENT_TIMESTAMP WHERE updated_at < %s OR updated_at IS NULL",
            (update_threshold,)
        )
        timestamp_updated = cursor.rowcount
        
        connection.commit()
        
        # 4. 최종 결과 확인
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        final_active_tickers = cursor.fetchone()[0]
        
        # 5. 샘플 티커 조회
        cursor.execute("SELECT ticker FROM tickers WHERE is_active = true ORDER BY ticker LIMIT 10")
        sample_tickers = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        
        result = {
            'total_tickers': total_tickers,
            'previously_active': active_tickers,
            'activated_count': updated_count,
            'timestamp_updated': timestamp_updated,
            'final_active_tickers': final_active_tickers,
            'sample_tickers': sample_tickers
        }
        
        logger.info(f"✅ 티커 상태 새로고침 완료: {result}")
        return True, result
        
    except Exception as e:
        if connection:
            connection.rollback()
            connection.close()
        logger.error(f"❌ 티커 상태 새로고침 중 오류: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False, str(e)

def lambda_handler(event, context):
    """Lambda 메인 핸들러 - 간단한 DB 업데이트 버전"""
    try:
        logger.info("🚀 Makenaide 티커 스캐너 시작 (Simple 버전)")
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
        
        # 2. 티커 상태 새로고침 (Upbit API 호출 없음)
        logger.info("📡 티커 상태 새로고침 실행 중...")
        refresh_success, refresh_result = refresh_ticker_status()
        
        if not refresh_success:
            logger.error(f"❌ 티커 상태 새로고침 실패: {refresh_result}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'티커 상태 새로고침 실패: {refresh_result}',
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
            final_count = refresh_result.get('final_active_tickers', 0)
        
        # 4. 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 5. 성공 응답 반환
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DB 연결 성공',
                'ticker_count': final_count,
                'refresh_result': refresh_result,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat(),
                'version': 'simple',
                'note': 'Upbit API 호출 없이 기존 DB 데이터 활용'
            }, ensure_ascii=False)
        }
        
        logger.info(f"✅ 간단한 티커 스캐너 완료: {final_count}개 활성 티커 ({execution_time:.2f}초)")
        return result
        
    except Exception as e:
        logger.error(f"❌ 간단한 티커 스캐너 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'version': 'simple'
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