#!/usr/bin/env python3
"""
Lambda DB Initializer - Entry Point
AWS Lambda 함수의 메인 핸들러 파일

이 파일은 AWS Lambda에서 기본적으로 찾는 lambda_function.py 규칙을 따릅니다.
실제 로직은 db_initializer.py 모듈에 구현되어 있습니다.
"""

import json
import logging
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda 함수 진입점
    
    Args:
        event (dict): Lambda 이벤트 데이터
        context (LambdaContext): Lambda 실행 컨텍스트
        
    Returns:
        dict: HTTP 응답 형태의 결과
    """
    try:
        logger.info("🚀 Lambda DB 초기화 시작")
        logger.info(f"📥 이벤트 수신: {json.dumps(event, ensure_ascii=False, indent=2)}")
        
        # 임시로 연결 테스트만 수행
        from test_connection_only import lambda_handler as test_handler
        result = test_handler(event, context)
        
        logger.info("✅ Lambda DB 초기화 완료")
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

# 로컬 테스트용 (Lambda 환경에서는 실행되지 않음)
if __name__ == "__main__":
    # 테스트용 이벤트
    test_event = {
        'operation_type': 'check_schema',
        'force_recreate': False,
        'include_backtest': True
    }
    
    # 로컬 테스트 실행
    print("🧪 로컬 테스트 실행...")
    result = lambda_handler(test_event, None)
    print("📋 테스트 결과:")
    print(json.dumps(result, ensure_ascii=False, indent=2))