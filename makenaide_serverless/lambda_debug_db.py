#!/usr/bin/env python3
"""
Lambda DB 디버깅 함수
실제로 어떤 DB와 테이블에 연결되고 있는지 확인
"""

import json
import logging
import os

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def debug_db_connection():
    """DB 연결 상태와 테이블 구조 디버깅"""
    try:
        import psycopg2
        import psycopg2.extras
        
        # 환경변수 확인
        pg_host = os.environ.get('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com')
        pg_port = int(os.environ.get('PG_PORT', '5432'))
        pg_database = os.environ.get('PG_DATABASE', 'makenaide')
        pg_user = os.environ.get('PG_USER', 'bruce')
        pg_password = os.environ.get('PG_PASSWORD')
        
        logger.info(f"연결 정보: {pg_host}:{pg_port}/{pg_database} (user: {pg_user})")
        
        # DB 연결
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password,
            connect_timeout=15
        )
        cursor = conn.cursor()
        
        # 1. 현재 DB 이름 확인
        cursor.execute("SELECT current_database()")
        current_db = cursor.fetchone()[0]
        logger.info(f"현재 DB: {current_db}")
        
        # 2. 현재 스키마 확인
        cursor.execute("SELECT current_schema()")
        current_schema = cursor.fetchone()[0]
        logger.info(f"현재 스키마: {current_schema}")
        
        # 3. 모든 테이블 목록 조회
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"사용 가능한 테이블: {tables}")
        
        # 4. tickers 테이블 존재 여부 확인
        if 'tickers' in tables:
            logger.info("✅ tickers 테이블 존재함")
            
            # 테이블 구조 확인
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'tickers' 
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            logger.info(f"tickers 테이블 구조:")
            for col_name, data_type, nullable in columns:
                logger.info(f"  - {col_name}: {data_type} (nullable: {nullable})")
            
            # 테이블 데이터 확인
            cursor.execute("SELECT COUNT(*) FROM tickers")
            total_count = cursor.fetchone()[0]
            logger.info(f"총 티커 수: {total_count}")
            
            if total_count > 0:
                # is_active 컬럼 확인
                try:
                    cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
                    active_count = cursor.fetchone()[0]
                    logger.info(f"활성 티커 수: {active_count}")
                except Exception as e:
                    logger.error(f"is_active 컬럼 조회 실패: {e}")
                
                # 샘플 데이터 조회
                cursor.execute("SELECT ticker, is_active, created_at FROM tickers LIMIT 5")
                sample_data = cursor.fetchall()
                logger.info(f"샘플 데이터:")
                for ticker, is_active, created_at in sample_data:
                    logger.info(f"  - {ticker}: active={is_active}, created={created_at}")
            else:
                logger.warning("tickers 테이블이 비어있습니다!")
        else:
            logger.error("❌ tickers 테이블이 존재하지 않습니다!")
        
        cursor.close()
        conn.close()
        
        return {
            'database': current_db,
            'schema': current_schema,
            'tables': tables,
            'tickers_exists': 'tickers' in tables,
            'total_tickers': total_count if 'tickers' in tables else 0
        }
        
    except Exception as e:
        logger.error(f"DB 디버깅 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return {'error': str(e)}

def lambda_handler(event, context):
    """Lambda 메인 핸들러 - DB 디버깅"""
    try:
        logger.info("🔍 Lambda DB 디버깅 시작")
        
        debug_result = debug_db_connection()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DB 디버깅 완료',
                'debug_result': debug_result,
                'timestamp': '2025-07-30T19:17:00'
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda 디버깅 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': '2025-07-30T19:17:00'
            }, ensure_ascii=False)
        }

# 로컬 테스트용
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2, ensure_ascii=False))