#!/usr/bin/env python3
"""
DB Initializer - aws-psycopg2 사용 버전
AWS Lambda 환경에 최적화된 psycopg2 패키지 사용
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

# AWS Lambda 환경에 최적화된 psycopg2 임포트
try:
    import psycopg2
    import pg8000.native as pg8000
    PSYCOPG2_AVAILABLE = True
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("✅ psycopg2 import 성공")
except ImportError as e:
    PSYCOPG2_AVAILABLE = False
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.error(f"❌ psycopg2 import 실패: {e}")
    # 대안으로 pg8000 시도
    try:
        import pg8000.native as pg8000
        logger.info("✅ pg8000 대안 사용")
        PG8000_AVAILABLE = True
    except ImportError:
        logger.error("❌ pg8000도 사용 불가")
        PG8000_AVAILABLE = False

class DatabaseManager:
    """AWS Lambda 환경용 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """DB 연결"""
        try:
            if PSYCOPG2_AVAILABLE:
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
                )
                self.cursor = self.connection.cursor()
                logger.info("✅ psycopg2로 DB 연결 성공")
            elif PG8000_AVAILABLE:
                self.connection = pg8000.Connection(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    ssl_context=True  # SSL 연결 활성화
                )
                logger.info("✅ pg8000으로 DB 연결 성공 (SSL 활성화)")
            else:
                raise Exception("PostgreSQL 드라이버가 없습니다")
                
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            raise
    
    def execute_query(self, query: str, params=None, fetchone=False, fetchall=False):
        """쿼리 실행"""
        try:
            if PSYCOPG2_AVAILABLE:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                
                if fetchone:
                    return self.cursor.fetchone()
                elif fetchall:
                    return self.cursor.fetchall()
                else:
                    return self.cursor.rowcount
                    
            elif PG8000_AVAILABLE:
                # pg8000.native 연결에서 직접 SQL 실행
                if fetchone:
                    result = self.connection.run(query, stream=params)
                    return result[0] if result else None
                elif fetchall:
                    return self.connection.run(query, stream=params)
                else:
                    self.connection.run(query, stream=params)
                    return 1
            else:
                return self._mock_execute(query, params, fetchone, fetchall)
                
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            raise
    
    def _mock_execute(self, query: str, params=None, fetchone=False, fetchall=False):
        """Mock 쿼리 실행 (연결 없을 때)"""
        logger.info(f"📝 Mock Query: {query[:100]}...")
        
        if "information_schema.tables" in query:
            if fetchone:
                return [False]
            elif fetchall:
                return []
        
        if "COUNT(*)" in query:
            if fetchone:
                return [0]
                
        return 1
    
    def commit(self):
        """트랜잭션 커밋"""
        if self.connection and PSYCOPG2_AVAILABLE:
            self.connection.commit()
            logger.info("✅ 트랜잭션 커밋")
        else:
            logger.info("✅ Mock 트랜잭션 커밋")
    
    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection and PSYCOPG2_AVAILABLE:
            self.connection.rollback()
            logger.info("🔄 트랜잭션 롤백")
        else:
            logger.info("🔄 Mock 트랜잭션 롤백")
    
    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔐 DB 연결 종료")

class SchemaInitializer:
    """스키마 초기화 클래스"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def create_core_schema(self) -> bool:
        """핵심 스키마 생성"""
        try:
            logger.info("🚀 핵심 스키마 생성 시작")
            
            # 메인 스키마 SQL
            main_schema_sql = """
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

            -- 인덱스 생성
            CREATE INDEX IF NOT EXISTS idx_trade_log_ticker ON trade_log(ticker);
            CREATE INDEX IF NOT EXISTS idx_trade_log_executed_at ON trade_log(executed_at);
            CREATE INDEX IF NOT EXISTS idx_tickers_is_active ON tickers(is_active);
            """
            
            # 스키마 생성 실행
            self.db.execute_query(main_schema_sql)
            logger.info("✅ 핵심 스키마 생성 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 스키마 생성 실패: {e}")
            return False
    
    def validate_schema(self) -> Dict[str, Any]:
        """스키마 검증"""
        try:
            # 테이블 존재 확인
            required_tables = ['performance_summary', 'trade_log', 'tickers']
            existing_tables = []
            
            for table in required_tables:
                result = self.db.execute_query(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                    (table,),
                    fetchone=True
                )
                if result and result[0] > 0:
                    existing_tables.append(table)
            
            validation_result = {
                'required_tables': required_tables,
                'existing_tables': existing_tables,
                'all_present': len(existing_tables) == len(required_tables),
                'missing_tables': list(set(required_tables) - set(existing_tables))
            }
            
            logger.info(f"📊 스키마 검증 결과: {validation_result}")
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 스키마 검증 실패: {e}")
            return {'error': str(e)}

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 DB Initializer 시작")
        
        # DB 설정
        db_config = {
            'host': os.environ.get('DB_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
            'port': int(os.environ.get('DB_PORT', 5432)),
            'database': os.environ.get('DB_NAME', 'makenaide'),
            'user': os.environ.get('DB_USER', 'bruce'),
            'password': os.environ.get('DB_PASSWORD', '0asis314.')
        }
        
        # DB 매니저 생성 및 연결
        db = DatabaseManager(db_config)
        db.connect()
        
        # 스키마 초기화
        initializer = SchemaInitializer(db)
        
        # 스키마 생성
        schema_created = initializer.create_core_schema()
        
        # 스키마 검증
        validation_result = initializer.validate_schema()
        
        # 트랜잭션 커밋
        db.commit()
        db.close()
        
        logger.info("✅ DB Initializer 완료")
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'message': 'DB 스키마 초기화 성공',
                'schema_created': schema_created,
                'validation': validation_result,
                'psycopg2_available': PSYCOPG2_AVAILABLE,
                'timestamp': datetime.now().isoformat(),
                'version': 'AWS_PSYCOPG2_v1.0'
            }
        }
        
    except Exception as e:
        logger.error(f"❌ DB Initializer 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e),
                'psycopg2_available': PSYCOPG2_AVAILABLE,
                'timestamp': datetime.now().isoformat()
            }
        }

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))