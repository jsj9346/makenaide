#!/usr/bin/env python3
"""
DB Initializer - psycopg2 없이 구조 검증용
실제 DB 연결 없이 스키마 생성 SQL과 로직 검증
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MockDatabaseManager:
    """Mock DB 매니저 - 실제 연결 없이 로직 테스트"""
    
    def __init__(self, config: dict):
        self.config = config
        self.mock_tables = {}  # 가상 테이블 상태
        
    def execute_query(self, query: str, params=None, fetchone=False, fetchall=False):
        """Mock 쿼리 실행"""
        logger.info(f"📝 Mock Query: {query[:100]}...")
        
        # 테이블 존재 확인 쿼리 시뮬레이션
        if "information_schema.tables" in query:
            if fetchone:
                return [False]  # 테이블이 없다고 가정
            elif fetchall:
                return []  # 빈 결과
        
        # COUNT 쿼리 시뮬레이션
        if "COUNT(*)" in query:
            if fetchone:
                return [0]  # 레코드 0개
                
        return 1  # 기본 rowcount
    
    def commit(self):
        logger.info("✅ Mock 트랜잭션 커밋")
    
    def rollback(self):
        logger.info("🔄 Mock 트랜잭션 롤백")
    
    def close(self):
        logger.info("🔐 Mock 연결 종료")

class SchemaGenerator:
    """스키마 생성 SQL 검증"""
    
    def generate_core_schema_sql(self) -> str:
        """핵심 스키마 SQL 생성"""
        return """
        -- 성과 요약 테이블
        CREATE TABLE IF NOT EXISTS performance_summary (
            id SERIAL PRIMARY KEY,
            period_start DATE,
            period_end DATE,
            initial_cash REAL,
            final_valuation REAL,
            net_profit REAL,
            win_rate REAL,
            profit_factor REAL,
            max_drawdown REAL,
            num_trades INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 거래 로그 테이블
        CREATE TABLE IF NOT EXISTS trade_log (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL,
            qty REAL,
            price REAL,
            buy_price REAL,
            score REAL,
            confidence REAL,
            trade_amount_krw REAL,
            bought_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT,
            error_msg TEXT,
            kelly_ratio REAL,
            swing_score REAL,
            strategy_combo TEXT,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 티커 테이블
        CREATE TABLE IF NOT EXISTS tickers (
            ticker VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT true,
            PRIMARY KEY (ticker)
        );
        """
    
    def validate_sql_syntax(self, sql: str) -> Dict[str, Any]:
        """SQL 구문 검증 (기본적인 검사)"""
        issues = []
        
        # 기본 구문 검사
        if "CREATE TABLE" not in sql.upper():
            issues.append("CREATE TABLE 문이 없습니다")
        
        if "PRIMARY KEY" not in sql.upper():
            issues.append("PRIMARY KEY가 정의되지 않았습니다") 
            
        # 테이블 개수 확인
        table_count = sql.upper().count("CREATE TABLE")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'table_count': table_count,
            'sql_length': len(sql)
        }

def lambda_handler(event, context):
    """psycopg2 없이 구조 검증"""
    try:
        logger.info("🧪 DB Initializer 구조 검증 시작")
        
        # Mock DB 매니저 생성
        db_config = {
            'host': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
            'port': 5432,
            'database': 'makenaide',
            'user': 'bruce',
            'password': '0asis314.'
        }
        
        db = MockDatabaseManager(db_config)
        schema_gen = SchemaGenerator()
        
        # 스키마 SQL 생성 및 검증
        core_sql = schema_gen.generate_core_schema_sql()
        validation = schema_gen.validate_sql_syntax(core_sql)
        
        # Mock 쿼리 실행 테스트
        db.execute_query("SELECT COUNT(*) FROM information_schema.tables", fetchone=True)
        db.execute_query(core_sql)
        db.commit()
        db.close()
        
        logger.info("✅ 구조 검증 완료")
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'message': 'DB Initializer 구조 검증 성공',
                'validation': validation,
                'core_schema_preview': core_sql[:500] + "...",
                'mock_db_config': {k: v if k != 'password' else '***' for k, v in db_config.items()},
                'timestamp': datetime.now().isoformat(),
                'version': 'NO_PSYCOPG2_v1.0'
            }
        }
        
    except Exception as e:
        logger.error(f"❌ 구조 검증 실패: {e}")
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