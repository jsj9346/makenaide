#!/usr/bin/env python3
"""
DB 연결 테스트 전용 스크립트
Lambda 환경에서 psycopg2 연결 여부만 확인
"""

import json
import logging
import os
from datetime import datetime

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Lambda 함수 진입점 - 연결 테스트만"""
    try:
        logger.info("🚀 DB 연결 테스트 시작")
        
        # psycopg2 import 테스트
        try:
            import psycopg2
            logger.info("✅ psycopg2 import 성공")
        except ImportError as e:
            logger.error(f"❌ psycopg2 import 실패: {e}")
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': f'psycopg2 import 실패: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                }
            }
        
        # DB 연결 설정
        db_config = {
            'host': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
            'port': 5432,
            'database': 'makenaide',
            'user': 'bruce',
            'password': '0asis314.'
        }
        
        # DB 연결 테스트
        try:
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # 간단한 쿼리 테스트
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            logger.info(f"✅ DB 연결 성공: {version[0]}")
            
            return {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'message': 'DB 연결 성공',
                    'database_version': version[0],
                    'timestamp': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': f'DB 연결 실패: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                }
            }
            
    except Exception as e:
        logger.error(f"❌ Lambda 함수 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': f'Lambda 함수 실행 실패: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
        }

if __name__ == "__main__":
    # 로컬 테스트
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))