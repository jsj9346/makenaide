"""
Lambda 함수들의 공통 베이스 클래스
"""
import os
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Any, Optional

# 환경변수 처리
def load_env_vars():
    """Lambda 환경변수 로드"""
    return {
        'UPBIT_ACCESS_KEY': os.getenv('UPBIT_ACCESS_KEY'),
        'UPBIT_SECRET_KEY': os.getenv('UPBIT_SECRET_KEY'),
        'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT', '5432'),
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD')
    }

class LambdaBase:
    """모든 Lambda 함수의 공통 베이스 클래스"""
    
    def __init__(self):
        self.env_vars = load_env_vars()
        self.logger = self._setup_logger()
        self.start_time = time.time()
        
        # DB 연결은 지연 초기화
        self._db_connection = None
        
    def _setup_logger(self) -> logging.Logger:
        """로거 설정"""
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # CloudWatch 포맷터
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        if logger.handlers:
            logger.handlers[0].setFormatter(formatter)
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    @contextmanager
    def get_db_connection(self):
        """DB 연결 컨텍스트 매니저 (재사용)"""
        if self._db_connection is None:
            try:
                import psycopg2
                self._db_connection = psycopg2.connect(
                    host=self.env_vars['DB_HOST'],
                    port=self.env_vars['DB_PORT'],
                    database=self.env_vars['DB_NAME'],
                    user=self.env_vars['DB_USER'],
                    password=self.env_vars['DB_PASSWORD']
                )
                self.logger.info("✅ DB 연결 성공")
            except Exception as e:
                self.logger.error(f"❌ DB 연결 실패: {e}")
                raise
        
        try:
            yield self._db_connection
        except Exception as e:
            self.logger.error(f"❌ DB 작업 중 오류: {e}")
            if self._db_connection:
                self._db_connection.rollback()
            raise
        finally:
            if self._db_connection:
                self._db_connection.commit()
    
    def execute_query(self, query: str, params: tuple = None, fetchone: bool = False):
        """안전한 쿼리 실행"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchone() if fetchone else cursor.fetchall()
                else:
                    return cursor.rowcount
    
    def create_response(self, status_code: int, body: Any, headers: Dict = None) -> Dict:
        """Lambda 응답 생성"""
        execution_time = time.time() - self.start_time
        
        response_body = {
            'data': body,
            'execution_time': round(execution_time, 3),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if headers is None:
            headers = {'Content-Type': 'application/json'}
        
        return {
            'statusCode': status_code,
            'headers': headers,
            'body': json.dumps(response_body, ensure_ascii=False, default=str)
        }
    
    def handle_error(self, error: Exception, context: str) -> Dict:
        """공통 에러 처리"""
        error_msg = f"{context} 중 오류: {str(error)}"
        self.logger.error(error_msg)
        
        import traceback
        self.logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return self.create_response(500, {
            'error': error_msg,
            'context': context,
            'type': type(error).__name__
        })
    
    def log_performance(self, operation: str, duration: float, details: Dict = None):
        """성능 로깅"""
        log_data = {
            'operation': operation,
            'duration_ms': round(duration * 1000, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if details:
            log_data.update(details)
            
        self.logger.info(f"📊 성능: {json.dumps(log_data, ensure_ascii=False)}")
    
    def validate_required_env_vars(self, required_vars: list) -> bool:
        """필수 환경변수 검증"""
        missing_vars = []
        
        for var in required_vars:
            if not self.env_vars.get(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.error(f"❌ 필수 환경변수 누락: {missing_vars}")
            return False
            
        return True
    
    def cleanup(self):
        """리소스 정리"""
        if self._db_connection:
            try:
                self._db_connection.close()
                self.logger.info("✅ DB 연결 해제 완료")
            except Exception as e:
                self.logger.warning(f"⚠️ DB 연결 해제 중 오류: {e}")

# 공통 유틸리티 함수들
def safe_float_convert(value, context: str = "", default: float = 0.0) -> float:
    """안전한 float 변환"""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_strftime(dt, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    """안전한 날짜 포맷팅"""
    try:
        if dt is None:
            return ""
        return dt.strftime(format_str)
    except (AttributeError, ValueError):
        return str(dt)

def load_blacklist() -> dict:
    """블랙리스트 로드 (Lambda용 간소화)"""
    # Lambda에서는 환경변수나 S3에서 로드
    blacklist_str = os.getenv('BLACKLIST_TICKERS', '{}')
    try:
        return json.loads(blacklist_str)
    except json.JSONDecodeError:
        return {}
