#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide RDB Shutdown
기능: RDS PostgreSQL 인스턴스 종료

🎯 주요 기능:
1. RDS 인스턴스 종료
2. 상태 확인
3. 종료 완료 응답

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

def stop_rds_instance(db_identifier: str) -> dict:
    """RDS 인스턴스 종료"""
    try:
        # 현재 상태 확인
        current_status = check_rds_status(db_identifier)
        logger.info(f"현재 RDS 상태: {current_status['status']}")
        
        if current_status['status'] == 'stopped':
            logger.info("✅ RDS 인스턴스가 이미 종료되어 있습니다")
            return {
                'success': True,
                'message': 'RDS 인스턴스가 이미 종료됨',
                'status': current_status['status'],
                'endpoint': current_status['endpoint'],
                'action': 'already_stopped'
            }
        
        elif current_status['status'] == 'available':
            logger.info("🛑 RDS 인스턴스 종료 중...")
            
            # RDS 인스턴스 종료
            response = rds_client.stop_db_instance(DBInstanceIdentifier=db_identifier)
            
            logger.info(f"RDS 종료 요청 완료: {response['DBInstance']['DBInstanceStatus']}")
            
            return {
                'success': True,
                'message': 'RDS 인스턴스 종료 요청 완료',
                'status': response['DBInstance']['DBInstanceStatus'],
                'endpoint': current_status['endpoint'],
                'action': 'stop_requested'
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
        logger.error(f"❌ RDS 종료 실패: {e}")
        return {
            'success': False,
            'message': f'RDS 종료 실패: {str(e)}',
            'status': 'error',
            'endpoint': '',
            'action': 'failed'
        }

def wait_for_rds_stopped(db_identifier: str, max_wait_seconds: int = 600) -> dict:
    """RDS 인스턴스가 종료될 때까지 대기"""
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        try:
            status_info = check_rds_status(db_identifier)
            current_status = status_info['status']
            
            logger.info(f"RDS 상태 확인: {current_status}")
            
            if current_status == 'stopped':
                logger.info("✅ RDS 인스턴스가 성공적으로 종료되었습니다")
                return {
                    'success': True,
                    'message': 'RDS 인스턴스 종료 완료',
                    'status': current_status,
                    'endpoint': status_info['endpoint'],
                    'wait_time': round(time.time() - start_time, 2)
                }
            
            elif current_status in ['stopping']:
                logger.info(f"⏳ RDS 종료 중... (경과 시간: {round(time.time() - start_time, 1)}초)")
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
    logger.error(f"❌ RDS 종료 대기 타임아웃 ({max_wait_seconds}초)")
    return {
        'success': False,
        'message': f'RDS 종료 대기 타임아웃 ({max_wait_seconds}초)',
        'status': 'timeout',
        'endpoint': '',
        'wait_time': max_wait_seconds
    }

def get_db_cost_estimation(db_identifier: str) -> dict:
    """RDS 비용 절약 추정치 계산"""
    try:
        status_info = check_rds_status(db_identifier)
        instance_class = status_info['instance_class']
        allocated_storage = status_info['allocated_storage']
        
        # 대략적인 시간당 비용 (USD) - 실제 비용은 AWS 문서 참조
        instance_costs = {
            'db.t3.micro': 0.018,
            'db.t3.small': 0.036,
            'db.t3.medium': 0.072,
            'db.t3.large': 0.144,
            'db.t3.xlarge': 0.288,
            'db.t3.2xlarge': 0.576
        }
        
        hourly_compute_cost = instance_costs.get(instance_class, 0.072)  # 기본값: medium
        hourly_storage_cost = allocated_storage * 0.000138  # GP2 스토리지 시간당 비용
        total_hourly_cost = hourly_compute_cost + hourly_storage_cost
        
        # 4시간 주기로 계산 (1일 6회 실행, 각 실행당 평균 1시간 가동)
        daily_saved_cost = total_hourly_cost * 18  # 24시간 - 6시간 = 18시간 절약
        monthly_saved_cost = daily_saved_cost * 30
        
        return {
            'instance_class': instance_class,
            'allocated_storage_gb': allocated_storage,
            'hourly_cost_usd': round(total_hourly_cost, 4),
            'daily_saved_usd': round(daily_saved_cost, 2),
            'monthly_saved_usd': round(monthly_saved_cost, 2),
            'annual_saved_usd': round(monthly_saved_cost * 12, 2)
        }
        
    except Exception as e:
        logger.warning(f"비용 추정 계산 실패: {e}")
        return {}

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🛑 Makenaide RDB Shutdown 시작")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        logger.info(f"📍 대상 RDS: {DB_IDENTIFIER}")
        
        # 이벤트에서 옵션 확인
        wait_for_stopped = event.get('wait_for_stopped', True)
        max_wait_time = event.get('max_wait_time', MAX_WAIT_TIME)
        
        # 비용 절약 추정치 계산
        cost_estimation = get_db_cost_estimation(DB_IDENTIFIER)
        if cost_estimation:
            logger.info(f"💰 예상 절약 비용: ${cost_estimation['monthly_saved_usd']}/월")
        
        # RDS 인스턴스 종료
        stop_result = stop_rds_instance(DB_IDENTIFIER)
        
        if not stop_result['success']:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'message': stop_result['message'],
                    'db_identifier': DB_IDENTIFIER,
                    'status': stop_result['status'],
                    'cost_estimation': cost_estimation,
                    'timestamp': datetime.now().isoformat(),
                    'version': 'rdb_shutdown_v1.0'
                })
            }
        
        # 종료될 때까지 대기 (옵션)
        if wait_for_stopped and stop_result['action'] in ['stop_requested', 'transitioning']:
            logger.info("⏳ RDS 인스턴스가 종료될 때까지 대기 중...")
            wait_result = wait_for_rds_stopped(DB_IDENTIFIER, max_wait_time)
            
            # 대기 결과를 종료 결과에 병합
            stop_result.update(wait_result)
        
        # 최종 상태 확인
        try:
            final_status = check_rds_status(DB_IDENTIFIER)
        except Exception:
            # 종료된 경우 상태 확인이 실패할 수 있음
            final_status = {'status': 'stopped', 'endpoint': '', 'port': 5432}
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'success': stop_result['success'],
                'message': stop_result['message'],
                'db_identifier': DB_IDENTIFIER,
                'action': stop_result['action'],
                'status': final_status['status'],
                'endpoint': final_status['endpoint'],
                'port': final_status['port'],
                'wait_time': stop_result.get('wait_time', 0),
                'cost_estimation': cost_estimation,
                'timestamp': datetime.now().isoformat(),
                'version': 'rdb_shutdown_v1.0'
            }, indent=2)
        }
        
        logger.info("✅ RDB Shutdown 실행 완료")
        return response
        
    except Exception as e:
        logger.error(f"❌ RDB Shutdown 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'message': 'RDB Shutdown 실행 실패',
                'db_identifier': DB_IDENTIFIER,
                'cost_estimation': {},
                'timestamp': datetime.now().isoformat(),
                'version': 'rdb_shutdown_v1.0'
            })
        }

# 테스트용 로컬 실행
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'wait_for_stopped': True,
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