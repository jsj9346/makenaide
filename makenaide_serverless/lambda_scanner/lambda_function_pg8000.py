#!/usr/bin/env python3
"""
Lambda Scanner - Entry Point (pg8000 버전)
AWS Lambda 함수의 메인 핸들러 파일 (pg8000 Pure Python PostgreSQL 드라이버 사용)
"""

import json
import logging
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda 함수 진입점 (pg8000 버전)
    
    Args:
        event (dict): Lambda 이벤트 데이터
        context (LambdaContext): Lambda 실행 컨텍스트
        
    Returns:
        dict: HTTP 응답 형태의 결과
    """
    try:
        logger.info("🚀 Lambda Scanner (pg8000) 시작")
        logger.info(f"📥 이벤트 수신: {json.dumps(event, ensure_ascii=False, indent=2)}")
        
        # pg8000 기반 Scanner 사용
        from scanner_pg8000 import lambda_handler as pg8000_handler
        result = pg8000_handler(event, context)
        
        logger.info("✅ Lambda Scanner (pg8000) 완료")
        return result
        
    except ImportError as e:
        error_msg = f"모듈 import 실패: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': error_msg,
                'error_type': 'ImportError',
                'available_modules': check_available_modules(),
                'timestamp': datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        error_msg = f"Lambda 함수 실행 실패: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': error_msg,
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat()
            }
        }

def check_available_modules():
    """사용 가능한 모듈 확인"""
    modules = {}
    
    try:
        import psycopg2
        modules['psycopg2'] = True
    except ImportError:
        modules['psycopg2'] = False
    
    try:
        import pg8000
        modules['pg8000'] = True
    except ImportError:
        modules['pg8000'] = False
        
    try:
        import pyupbit
        modules['pyupbit'] = True
    except ImportError:
        modules['pyupbit'] = False
        
    return modules

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트용 이벤트
    test_event = {
        'action': 'full_scan',
        'force_update': True
    }
    
    # 로컬 테스트 실행
    print("🧪 로컬 테스트 실행...")
    result = lambda_handler(test_event, None)
    print("📋 테스트 결과:")
    print(json.dumps(result, ensure_ascii=False, indent=2))