#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide Basic RDB Controller
기능: RDS PostgreSQL 인스턴스 시작 (Integrated Orchestrator와 분리 운영)

🎯 주요 기능:
1. RDS 인스턴스 시작 (20분 전 스케줄링)
2. 상태 확인 및 연결 대기
3. Integrated Orchestrator를 위한 RDS 준비

📋 스케줄링:
- RDB Controller: 00:40, 04:40, 08:40, 12:40, 16:40, 20:40
- Integrated Orchestrator: 01:00, 05:00, 09:00, 13:00, 17:00, 21:00 (20분 후)

📋 환경변수:
- DB_IDENTIFIER: RDS 인스턴스 식별자 (makenaide)
- AWS_REGION: AWS 리전 (ap-northeast-2)
"""

import json
import boto3
import logging
import time
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
rds_client = boto3.client('rds', region_name='ap-northeast-2')

# RDS 설정
DB_IDENTIFIER = 'makenaide'
REGION = 'ap-northeast-2'
MAX_WAIT_TIME = 600  # 10분 최대 대기

def check_rds_status(db_identifier: str) -> dict:
    """RDS 인스턴스 상태 확인"""
    try:
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)
        db_instance = response['DBInstances'][0]
        
        return {
            'status': db_instance['DBInstanceStatus'],
            'endpoint': db_instance.get('Endpoint', {}).get('Address', ''),
            'port': db_instance.get('Endpoint', {}).get('Port', 5432),
            'engine': db_instance['Engine'],
            'allocated_storage': db_instance['AllocatedStorage'],
            'instance_class': db_instance['DBInstanceClass']
        }
    except Exception as e:
        logger.error(f"RDS 상태 확인 실패: {e}")
        raise

def start_rds_instance(db_identifier: str) -> dict:
    """RDS 인스턴스 시작"""
    try:
        # 현재 상태 확인
        current_status = check_rds_status(db_identifier)
        logger.info(f"현재 RDS 상태: {current_status['status']}")
        
        if current_status['status'] == 'available':
            logger.info("✅ RDS 인스턴스가 이미 실행 중입니다")
            return {
                'success': True,
                'message': 'RDS 인스턴스가 이미 실행 중',
                'status': current_status['status'],
                'endpoint': current_status['endpoint'],
                'action': 'already_running'
            }
        
        elif current_status['status'] == 'stopped':
            logger.info("🚀 RDS 인스턴스 시작 중...")
            
            # RDS 인스턴스 시작
            response = rds_client.start_db_instance(DBInstanceIdentifier=db_identifier)
            
            logger.info(f"RDS 시작 요청 완료: {response['DBInstance']['DBInstanceStatus']}")
            
            return {
                'success': True,
                'message': 'RDS 인스턴스 시작 요청 완료',
                'status': response['DBInstance']['DBInstanceStatus'],
                'endpoint': current_status['endpoint'],
                'action': 'start_requested'
            }
            
        elif current_status['status'] in ['starting', 'stopping', 'rebooting']:
            logger.info(f"⏳ RDS 인스턴스가 전환 중입니다: {current_status['status']}")
            return {
                'success': True,
                'message': f'RDS 인스턴스가 전환 중: {current_status["status"]}',
                'status': current_status['status'],
                'endpoint': current_status['endpoint'],
                'action': 'transitioning'
            }
            
        else:
            logger.warning(f"⚠️ 예상치 못한 RDS 상태: {current_status['status']}")
            return {
                'success': False,
                'message': f'예상치 못한 RDS 상태: {current_status["status"]}',
                'status': current_status['status'],
                'endpoint': current_status['endpoint'],
                'action': 'unexpected_status'
            }
            
    except Exception as e:
        logger.error(f"❌ RDS 시작 실패: {e}")
        return {
            'success': False,
            'message': f'RDS 시작 실패: {str(e)}',
            'status': 'error',
            'endpoint': '',
            'action': 'failed'
        }

def wait_for_rds_available(db_identifier: str, max_wait_seconds: int = 600) -> dict:
    """RDS 인스턴스가 사용 가능해질 때까지 대기"""
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        try:
            status_info = check_rds_status(db_identifier)
            current_status = status_info['status']
            
            logger.info(f"RDS 상태 확인: {current_status}")
            
            if current_status == 'available':
                logger.info("✅ RDS 인스턴스가 사용 가능합니다")
                return {
                    'success': True,
                    'message': 'RDS 인스턴스 시작 완료',
                    'status': current_status,
                    'endpoint': status_info['endpoint'],
                    'wait_time': round(time.time() - start_time, 2)
                }
            
            elif current_status in ['starting']:
                logger.info(f"⏳ RDS 시작 중... (경과 시간: {round(time.time() - start_time, 1)}초)")
                time.sleep(30)  # 30초 대기
                
            else:
                logger.warning(f"⚠️ 예상치 못한 상태로 전환: {current_status}")
                return {
                    'success': False,
                    'message': f'예상치 못한 상태: {current_status}',
                    'status': current_status,
                    'endpoint': status_info['endpoint'],
                    'wait_time': round(time.time() - start_time, 2)
                }
                
        except Exception as e:
            logger.error(f"RDS 상태 확인 중 오류: {e}")
            time.sleep(10)
    
    # 타임아웃 발생
    logger.error(f"❌ RDS 시작 대기 타임아웃 ({max_wait_seconds}초)")
    return {
        'success': False,
        'message': f'RDS 시작 대기 타임아웃 ({max_wait_seconds}초)',
        'status': 'timeout',
        'endpoint': '',
        'wait_time': max_wait_seconds
    }

def lambda_handler(event, context):
    """Lambda 메인 핸들러 (스케줄링 최적화)"""
    try:
        logger.info("🗄️ Makenaide Basic RDB Controller 시작 (스케줄링 최적화)")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        logger.info(f"📍 대상 RDS: {DB_IDENTIFIER}")
        logger.info("📋 Integrated Orchestrator 20분 전 RDS 준비 작업")
        
        # 현재 실행 시간 로깅 (스케줄링 확인용)
        current_time = datetime.now()
        logger.info(f"⏰ 현재 실행 시간: {current_time.strftime('%H:%M')} (예상: 00:40, 04:40, 08:40, 12:40, 16:40, 20:40)")
        
        # 이벤트에서 옵션 확인
        wait_for_available = event.get('wait_for_available', True)
        max_wait_time = event.get('max_wait_time', MAX_WAIT_TIME)
        
        # RDS 인스턴스 시작
        start_result = start_rds_instance(DB_IDENTIFIER)
        
        if not start_result['success']:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'message': start_result['message'],
                    'db_identifier': DB_IDENTIFIER,
                    'status': start_result['status'],
                    'timestamp': datetime.now().isoformat(),
                    'version': 'basic_rdb_controller_v2.0'
                })
            }
        
        # 사용 가능해질 때까지 대기 (옵션)
        if wait_for_available and start_result['action'] in ['start_requested', 'transitioning']:
            logger.info("⏳ RDS 인스턴스가 사용 가능해질 때까지 대기 중...")
            wait_result = wait_for_rds_available(DB_IDENTIFIER, max_wait_time)
            
            # 대기 결과를 시작 결과에 병합
            start_result.update(wait_result)
        
        # 최종 상태 확인
        final_status = check_rds_status(DB_IDENTIFIER)
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'success': start_result['success'],
                'message': start_result['message'],
                'db_identifier': DB_IDENTIFIER,
                'action': start_result['action'],
                'status': final_status['status'],
                'endpoint': final_status['endpoint'],
                'port': final_status['port'],
                'wait_time': start_result.get('wait_time', 0),
                'timestamp': datetime.now().isoformat(),
                'version': 'basic_rdb_controller_v2.0'
            }, indent=2)
        }
        
        logger.info("✅ RDB Controller 실행 완료")
        return response
        
    except Exception as e:
        logger.error(f"❌ RDB Controller 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'RDB Controller 실행 실패',
                'db_identifier': DB_IDENTIFIER,
                'timestamp': datetime.now().isoformat(),
                'version': 'basic_rdb_controller_v2.0'
            })
        }

# 테스트용 로컬 실행
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'wait_for_available': True,
        'max_wait_time': 300
    }
    
    # 테스트 컨텍스트 (더미)
    class TestContext:
        def __init__(self):
            self.function_name = 'test'
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = 'test'
            self.aws_request_id = 'test'
    
    result = lambda_handler(test_event, TestContext())
    print(json.dumps(result, indent=2))