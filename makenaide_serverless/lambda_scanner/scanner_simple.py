#!/usr/bin/env python3
"""
Scanner Simple - API 없이 테스트용
pyupbit 종속성 없이 DB 연결 및 기본 기능만 테스트
"""

import json
import logging
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DatabaseManager:
    """Lambda 환경용 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def get_connection(self):
        """DB 연결 획득"""
        if self.connection is None:
            import psycopg2
            try:
                self.connection = psycopg2.connect(**self.config)
                self.connection.autocommit = False
                logger.info("✅ PostgreSQL 연결 성공")
            except Exception as e:
                logger.error(f"❌ DB 연결 실패: {e}")
                raise
        return self.connection
    
    def execute_query(self, query: str, params: tuple = None, fetchone: bool = False, fetchall: bool = False):
        """쿼리 실행"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            
            if fetchone:
                return cursor.fetchone()
            elif fetchall:
                return cursor.fetchall()
            else:
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def commit(self):
        if self.connection:
            self.connection.commit()
    
    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

def lambda_handler(event, context):
    """단순화된 Lambda 핸들러 (DB 연결 테스트용)"""
    try:
        logger.info("🚀 Scanner Simple 테스트 시작")
        
        # DB 설정
        db_config = {
            'host': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
            'port': 5432,
            'database': 'makenaide',
            'user': 'bruce',
            'password': '0asis314.'
        }
        
        # DB 연결 테스트
        db = DatabaseManager(db_config)
        
        # 기존 티커 조회 테스트
        existing_tickers = db.execute_query(
            "SELECT ticker, updated_at FROM tickers LIMIT 5",
            fetchall=True
        )
        
        logger.info(f"✅ 기존 티커 조회 성공: {len(existing_tickers)}개")
        
        # 테스트용 더미 티커 데이터
        mock_tickers = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
        
        db.close()
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'message': 'DB 연결 및 기본 쿼리 성공',
                'existing_tickers_count': len(existing_tickers),
                'mock_tickers': mock_tickers,
                'timestamp': datetime.now().isoformat(),
                'lambda_version': 'SCANNER_SIMPLE_v1.0'
            }
        }
        
    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        }

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))