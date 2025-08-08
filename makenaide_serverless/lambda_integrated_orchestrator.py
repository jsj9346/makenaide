#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide Integrated Orchestrator (RDS 분리 버전)
기능: EC2 + makenaide 실행 전용 (RDS는 별도 관리)

🎯 분리된 플로우:
1. RDS 상태 확인 및 대기 (makenaide-basic-RDB-controller가 20분 전 시작)
2. EC2 시작 (직접 제어)
3. EC2-RDB 커넥션 체크
4. EC2에서 SSM을 통해 makenaide.py 실행
5. EC2 종료 (직접 제어)
6. RDS는 계속 실행 상태 유지 (별도 스케줄로 관리)

🚀 최적화 특징:
- Lambda 15분 제한 준수 (RDS 관리 분리)
- EC2 + makenaide 실행에 집중
- 상세한 실행 로그 및 메트릭
- makenaide-basic-RDB-controller와 연계 운영

📋 스케줄링:
- RDB Controller: 00:40, 04:40, 08:40, 12:40, 16:40, 20:40
- Integrated Orchestrator: 01:00, 05:00, 09:00, 13:00, 17:00, 21:00 (20분 후)
"""

import json
import boto3
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
rds_client = boto3.client('rds', region_name='ap-northeast-2')
ssm_client = boto3.client('ssm', region_name='ap-northeast-2')
cloudwatch = boto3.client('cloudwatch')

class MakenaideIntegratedOrchestrator:
    """Makenaide 통합 파이프라인 조정 클래스"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.execution_id = f"integrated_exec_{int(time.time())}"
        
        # AWS 리소스 설정
        self.db_identifier = 'makenaide'
        self.ec2_instance_id = 'i-082bf343089af62d3'
        self.region = 'ap-northeast-2'
        
        # 타임아웃 설정 (초) - Lambda 15분 제한 고려
        self.timeouts = {
            'rds_check': 120,         # 2분 (RDS 상태 확인 및 대기)
            'ec2_start': 180,         # 3분
            'connection_check': 60,   # 1분
            'makenaide_launch': 60,   # 1분 (SSM 명령 시작만)
            'ec2_shutdown_wait': 480, # 8분 (EC2 종료 대기)
            'total_pipeline': 900     # 15분 (Lambda 최대 실행 시간)
            # 예상 총 시간: 2분 + 3분 + 1분 + 8분 = 14분 ✅ Lambda 15분 제한 준수
        }
        
        
        # 실행 결과 추적
        self.results = {
            'pipeline_start': self.start_time.isoformat(),
            'execution_id': self.execution_id,
            'steps': {},
            'metrics': {},
            'errors': [],
            'cost_savings': {}
        }
    
    def log_step(self, step_name: str, status: str, details: Dict = None):
        """단계별 실행 로그 기록"""
        step_info = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'duration': (datetime.now() - self.start_time).total_seconds()
        }
        
        if details:
            step_info.update(details)
            
        self.results['steps'][step_name] = step_info
        
        logger.info(f"📋 {step_name}: {status}")
        if details:
            for key, value in details.items():
                logger.info(f"   - {key}: {value}")
    
    def send_custom_metric(self, metric_name: str, value: float, unit: str = 'Count'):
        """CloudWatch 커스텀 메트릭 전송"""
        try:
            cloudwatch.put_metric_data(
                Namespace='Makenaide/IntegratedPipeline',
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': datetime.now(),
                        'Dimensions': [
                            {
                                'Name': 'ExecutionId',
                                'Value': self.execution_id
                            }
                        ]
                    }
                ]
            )
        except Exception as e:
            logger.warning(f"⚠️ 메트릭 전송 실패: {e}")
    
    def check_rds_status(self) -> dict:
        """RDS 인스턴스 상태 확인"""
        try:
            response = rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
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
    
    def start_rds_instance(self) -> Dict:
        """RDS 인스턴스 시작"""
        try:
            # 현재 상태 확인
            current_status = self.check_rds_status()
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
            
            elif current_status['status'] in ['stopped', 'stopping']:
                if current_status['status'] == 'stopping':
                    logger.info("⏳ RDS 인스턴스가 종료 중입니다. 완전히 종료될 때까지 대기...")
                    # stopping 상태에서 stopped가 될 때까지 대기
                    if not self.wait_for_rds_stopped():
                        return {
                            'success': False,
                            'message': 'RDS 종료 대기 실패',
                            'status': current_status['status'],
                            'action': 'wait_stop_failed'
                        }
                
                logger.info("🚀 RDS 인스턴스 시작 중...")
                
                # RDS 인스턴스 시작
                response = rds_client.start_db_instance(DBInstanceIdentifier=self.db_identifier)
                
                logger.info(f"RDS 시작 요청 완룉: {response['DBInstance']['DBInstanceStatus']}")
                
                # 시작 완룉까지 대기
                return self.wait_for_rds_available()
                
            else:
                logger.warning(f"⚠️ 예상치 못한 RDS 상태: {current_status['status']}")
                return {
                    'success': False,
                    'message': f'예상치 못한 RDS 상태: {current_status["status"]}',
                    'status': current_status['status'],
                    'action': 'unexpected_status'
                }
                
        except Exception as e:
            logger.error(f"❌ RDS 시작 실패: {e}")
            return {
                'success': False,
                'message': f'RDS 시작 실패: {str(e)}',
                'status': 'error',
                'action': 'failed'
            }
    
    def wait_for_rds_available(self, max_wait_seconds: int = None) -> Dict:
        """RDS 인스턴스가 사용 가능해질 때까지 대기"""
        if max_wait_seconds is None:
            max_wait_seconds = self.timeouts['rds_start']
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                status_info = self.check_rds_status()
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
                
                elif current_status in [
                    'starting',
                    'configuring-enhanced-monitoring',  # Enhanced Monitoring 설정 중 (정상 상태)
                    'configuring-iam-database-auth',    # IAM 데이터베이스 인증 설정 중
                    'configuring-log-exports',          # CloudWatch Logs 내보내기 설정 중
                    'backing-up',                       # 백업 중
                    'modifying',                        # 수정 중
                    'rebooting'                         # 재시작 중
                ]:
                    logger.info(f"⏳ RDS 시작 중... (상태: {current_status}, 경과 시간: {round(time.time() - start_time, 1)}초)")
                    time.sleep(30)  # 30초 대기
                    
                else:
                    logger.warning(f"⚠️ 예상치 못한 상태로 전환: {current_status}")
                    return {
                        'success': False,
                        'message': f'예상치 못한 상태: {current_status}',
                        'status': current_status,
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
            'wait_time': max_wait_seconds
        }
    
    def wait_for_rds_stopped(self, max_wait_seconds: int = 300) -> bool:
        """RDS 인스턴스가 완전히 종료될 때까지 대기"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                status_info = self.check_rds_status()
                current_status = status_info['status']
                
                logger.info(f"RDS 종료 대기 상태: {current_status}")
                
                if current_status == 'stopped':
                    logger.info("✅ RDS 인스턴스가 완전히 종룉되었습니다")
                    return True
                    
                elif current_status == 'stopping':
                    logger.info(f"⏳ RDS 종룉 중... (경과 시간: {round(time.time() - start_time, 1)}초)")
                    time.sleep(15)  # 15초 대기
                    
                else:
                    logger.warning(f"⚠️ 예상치 못한 상태: {current_status}")
                    return False
                    
            except Exception as e:
                logger.error(f"RDS 상태 확인 중 오류: {e}")
                time.sleep(10)
        
        # 타임아웃 발생
        logger.error(f"❌ RDS 종료 대기 타임아웃 ({max_wait_seconds}초)")
        return False
    
    def wait_for_rds_ready(self, max_wait_seconds: int = None) -> Dict:
        """RDS가 사용 가능한 상태인지 확인 및 대기"""
        if max_wait_seconds is None:
            max_wait_seconds = self.timeouts['rds_check']
        
        start_time = time.time()
        
        logger.info("🔍 RDS 상태 확인 중...")
        
        while time.time() - start_time < max_wait_seconds:
            try:
                status_info = self.check_rds_status()
                current_status = status_info['status']
                
                logger.info(f"📊 현재 RDS 상태: {current_status}")
                
                if current_status == 'available':
                    logger.info("✅ RDS 인스턴스가 사용 가능합니다")
                    return {
                        'success': True,
                        'message': 'RDS 사용 가능 확인',
                        'status': current_status,
                        'endpoint': status_info['endpoint'],
                        'wait_time': round(time.time() - start_time, 2)
                    }
                
                elif current_status in [
                    'starting',
                    'configuring-enhanced-monitoring',
                    'configuring-iam-database-auth',
                    'configuring-log-exports',
                    'backing-up',
                    'modifying',
                    'rebooting'
                ]:
                    logger.info(f"⏳ RDS 준비 중... (상태: {current_status}, 경과 시간: {round(time.time() - start_time, 1)}초)")
                    time.sleep(30)  # 30초 대기
                    
                else:
                    logger.warning(f"⚠️ 예상치 못한 RDS 상태: {current_status}")
                    # RDS가 stopped 상태라면 basic-RDB-controller를 기다려야 함
                    if current_status in ['stopped', 'stopping']:
                        logger.info("📋 RDS가 아직 시작되지 않음. basic-RDB-controller가 시작할 때까지 대기...")
                        time.sleep(60)  # 1분 대기
                    else:
                        return {
                            'success': False,
                            'message': f'예상치 못한 RDS 상태: {current_status}',
                            'status': current_status,
                            'wait_time': round(time.time() - start_time, 2)
                        }
                    
            except Exception as e:
                logger.error(f"RDS 상태 확인 중 오류: {e}")
                time.sleep(10)
        
        # 타임아웃 발생
        logger.error(f"❌ RDS 준비 대기 타임아웃 ({max_wait_seconds}초)")
        return {
            'success': False,
            'message': f'RDS 준비 대기 타임아웃 ({max_wait_seconds}초)',
            'status': 'timeout',
            'wait_time': max_wait_seconds
        }
    
    def check_ec2_status(self) -> dict:
        """EC2 인스턴스 상태 확인"""
        try:
            response = ec2_client.describe_instances(InstanceIds=[self.ec2_instance_id])
            instance = response['Reservations'][0]['Instances'][0]
            
            return {
                'status': instance['State']['Name'],
                'public_ip': instance.get('PublicIpAddress', ''),
                'private_ip': instance.get('PrivateIpAddress', ''),
                'instance_type': instance['InstanceType']
            }
        except Exception as e:
            logger.error(f"EC2 상태 확인 실패: {e}")
            raise
    
    def start_ec2_instance(self) -> Dict:
        """EC2 인스턴스 시작"""
        try:
            # 현재 상태 확인
            current_status = self.check_ec2_status()
            logger.info(f"현재 EC2 상태: {current_status['status']}")
            
            if current_status['status'] == 'running':
                logger.info("✅ EC2 인스턴스가 이미 실행 중입니다")
                return {
                    'success': True,
                    'message': 'EC2 인스턴스가 이미 실행 중',
                    'status': current_status['status'],
                    'public_ip': current_status['public_ip'],
                    'action': 'already_running'
                }
            
            elif current_status['status'] == 'stopped':
                logger.info("🚀 EC2 인스턴스 시작 중...")
                
                # EC2 인스턴스 시작
                response = ec2_client.start_instances(InstanceIds=[self.ec2_instance_id])
                
                logger.info(f"EC2 시작 요청 완료")
                
                # 시작 완료까지 대기
                return self.wait_for_ec2_running()
                
            else:
                logger.warning(f"⚠️ 예상치 못한 EC2 상태: {current_status['status']}")
                return {
                    'success': False,
                    'message': f'예상치 못한 EC2 상태: {current_status["status"]}',
                    'status': current_status['status'],
                    'action': 'unexpected_status'
                }
                
        except Exception as e:
            logger.error(f"❌ EC2 시작 실패: {e}")
            return {
                'success': False,
                'message': f'EC2 시작 실패: {str(e)}',
                'status': 'error',
                'action': 'failed'
            }
    
    def wait_for_ec2_running(self, max_wait_seconds: int = 300) -> Dict:
        """EC2 인스턴스가 실행될 때까지 대기"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                status_info = self.check_ec2_status()
                current_status = status_info['status']
                
                logger.info(f"EC2 상태 확인: {current_status}")
                
                if current_status == 'running':
                    logger.info("✅ EC2 인스턴스가 실행 중입니다")
                    return {
                        'success': True,
                        'message': 'EC2 인스턴스 시작 완료',
                        'status': current_status,
                        'public_ip': status_info['public_ip'],
                        'wait_time': round(time.time() - start_time, 2)
                    }
                
                elif current_status in ['pending', 'stopping', 'stopped']:
                    logger.info(f"⏳ EC2 상태 변화 중... (현재: {current_status}, 경과 시간: {round(time.time() - start_time, 1)}초)")
                    time.sleep(20)  # 20초 대기
                    
                else:
                    logger.warning(f"⚠️ 예상치 못한 상태로 전환: {current_status}")
                    return {
                        'success': False,
                        'message': f'예상치 못한 상태: {current_status}',
                        'status': current_status,
                        'wait_time': round(time.time() - start_time, 2)
                    }
                    
            except Exception as e:
                logger.error(f"EC2 상태 확인 중 오류: {e}")
                time.sleep(10)
        
        # 타임아웃 발생
        logger.error(f"❌ EC2 시작 대기 타임아웃 ({max_wait_seconds}초)")
        return {
            'success': False,
            'message': f'EC2 시작 대기 타임아웃 ({max_wait_seconds}초)',
            'status': 'timeout',
            'wait_time': max_wait_seconds
        }
    
    def execute_makenaide_on_ec2(self) -> Dict:
        """EC2에서 SSM을 통해 makenaide.py 실행"""
        try:
            logger.info("🚀 EC2에서 makenaide.py 실행 시작")
            
            # SSM을 통해 makenaide.py 실행
            command = """
            cd /home/ec2-user/makenaide
            
            # 환경변수 설정 (RDS 연결)
            export PG_HOST="makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com"
            export PG_PORT="5432"
            export PG_DATABASE="makenaide"
            export PG_USER="bruce"
            export PG_PASSWORD="0asis314."
            
            # Python 경로 설정
            source venv/bin/activate 2>/dev/null || echo "Virtual env not found, using system python"
            export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
            
            # 필수 패키지 설치 확인
            echo "📦 필수 패키지 설치 확인..."
            pip3 install --user psycopg2-binary pandas numpy requests PyJWT pyupbit 2>/dev/null || echo "Package installation skipped"
            
            echo "Starting Makenaide pipeline at $(date)"
            python3 makenaide.py 2>&1
            echo "Makenaide pipeline completed at $(date)"
            """
            
            response = ssm_client.send_command(
                InstanceIds=[self.ec2_instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                TimeoutSeconds=self.timeouts['makenaide_execution']
            )
            
            command_id = response['Command']['CommandId']
            logger.info(f"📋 SSM 명령 ID: {command_id}")
            
            # 명령 실행 상태 모니터링
            start_time = time.time()
            while time.time() - start_time < self.timeouts['makenaide_execution']:
                try:
                    result = ssm_client.get_command_invocation(
                        CommandId=command_id,
                        InstanceId=self.ec2_instance_id
                    )
                    
                    status = result['Status']
                    logger.info(f"📊 Makenaide 실행 상태: {status}")
                    
                    if status == 'Success':
                        output = result['StandardOutputContent']
                        logger.info("✅ Makenaide.py 실행 성공")
                        return {
                            'success': True,
                            'output': output,
                            'command_id': command_id,
                            'execution_time': time.time() - start_time
                        }
                    elif status == 'Failed':
                        error_output = result.get('StandardErrorContent', 'Unknown error')
                        logger.error(f"❌ Makenaide.py 실행 실패: {error_output}")
                        return {
                            'success': False,
                            'error': error_output,
                            'command_id': command_id,
                            'execution_time': time.time() - start_time
                        }
                    elif status in ['InProgress', 'Pending']:
                        time.sleep(30)  # 30초 대기 후 재확인
                    else:
                        logger.warning(f"⚠️ 예상치 못한 상태: {status}")
                        time.sleep(10)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 상태 확인 중 오류: {e}")
                    time.sleep(10)
            
            # 타임아웃 발생
            logger.error("❌ Makenaide.py 실행 타임아웃")
            return {
                'success': False,
                'error': 'Execution timeout',
                'command_id': command_id,
                'execution_time': self.timeouts['makenaide_execution']
            }
            
        except Exception as e:
            logger.error(f"❌ Makenaide.py 실행 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def launch_makenaide_on_ec2(self) -> Dict:
        """EC2에서 SSM을 통해 makenaide.py 비동기 실행 시작"""
        try:
            logger.info("🚀 EC2에서 makenaide.py 비동기 실행 시작")
            
            # SSM을 통해 makenaide.py 실행 (자동 종료 포함)
            command = """
            cd /home/ec2-user/makenaide
            
            # 환경변수 설정 (RDS 연결)
            export PG_HOST="makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com"
            export PG_PORT="5432"
            export PG_DATABASE="makenaide"
            export PG_USER="bruce"
            export PG_PASSWORD="0asis314."
            
            # Python 경로 설정
            source venv/bin/activate 2>/dev/null || echo "Virtual env not found, using system python"
            export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
            
            # 필수 패키지 설치 확인
            echo "📦 필수 패키지 설치 확인..."
            pip3 install --user psycopg2-binary pandas numpy requests PyJWT pyupbit 2>/dev/null || echo "Package installation skipped"
            
            echo "Starting Makenaide pipeline at $(date)"
            python3 makenaide.py 2>&1
            echo "Makenaide pipeline completed at $(date)"
            
            # 작업 완료 후 EC2 자동 종료
            echo "🔄 makenaide 실행 완료. EC2 자동 종료 중..."
            sudo shutdown -h now
            """
            
            response = ssm_client.send_command(
                InstanceIds=[self.ec2_instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                TimeoutSeconds=3600  # 1시간 타임아웃 (충분한 시간)
            )
            
            command_id = response['Command']['CommandId']
            logger.info(f"📋 SSM 명령 ID: {command_id}")
            logger.info("✅ makenaide.py 비동기 실행 시작 성공")
            
            return {
                'success': True,
                'command_id': command_id,
                'message': 'makenaide.py 비동기 실행 시작됨'
            }
            
        except Exception as e:
            logger.error(f"❌ Makenaide.py 시작 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def wait_for_ec2_stopped(self, max_wait_seconds: int = 600) -> Dict:
        """EC2 인스턴스가 종료될 때까지 대기"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                status_info = self.check_ec2_status()
                current_status = status_info['status']
                
                logger.info(f"EC2 상태 확인: {current_status}")
                
                if current_status == 'stopped':
                    logger.info("✅ EC2 인스턴스가 종료되었습니다")
                    return {
                        'success': True,
                        'message': 'EC2 인스턴스 종료 확인',
                        'status': current_status,
                        'wait_time': round(time.time() - start_time, 2)
                    }
                
                elif current_status in ['stopping', 'running', 'pending']:
                    logger.info(f"⏳ EC2 종료 대기 중... (현재: {current_status}, 경과 시간: {round(time.time() - start_time, 1)}초)")
                    time.sleep(30)  # 30초 대기
                    
                else:
                    logger.warning(f"⚠️ 예상치 못한 상태: {current_status}")
                    return {
                        'success': False,
                        'message': f'예상치 못한 상태: {current_status}',
                        'status': current_status,
                        'wait_time': round(time.time() - start_time, 2)
                    }
                    
            except Exception as e:
                logger.error(f"EC2 상태 확인 중 오류: {e}")
                time.sleep(10)
        
        # 타임아웃 발생
        logger.error(f"❌ EC2 종료 대기 타임아웃 ({max_wait_seconds}초)")
        return {
            'success': False,
            'message': f'EC2 종료 대기 타임아웃 ({max_wait_seconds}초)',
            'status': 'timeout',
            'wait_time': max_wait_seconds
        }
    
    def stop_ec2_instance(self) -> Dict:
        """EC2 인스턴스 종료"""
        try:
            logger.info("🛑 EC2 인스턴스 종료 중...")
            
            # EC2 인스턴스 종료
            response = ec2_client.stop_instances(InstanceIds=[self.ec2_instance_id])
            
            logger.info("EC2 종료 요청 완료")
            return {
                'success': True,
                'message': 'EC2 인스턴스 종료 요청 완료'
            }
            
        except Exception as e:
            logger.error(f"❌ EC2 종료 실패: {e}")
            return {
                'success': False,
                'message': f'EC2 종료 실패: {str(e)}'
            }
    
    def stop_rds_instance(self) -> Dict:
        """RDS 인스턴스 종료"""
        try:
            # 현재 상태 확인
            current_status = self.check_rds_status()
            logger.info(f"현재 RDS 상태: {current_status['status']}")
            
            if current_status['status'] == 'stopped':
                logger.info("✅ RDS 인스턴스가 이미 종료되어 있습니다")
                return {
                    'success': True,
                    'message': 'RDS 인스턴스가 이미 종료됨',
                    'status': current_status['status'],
                    'action': 'already_stopped'
                }
            
            elif current_status['status'] == 'available':
                logger.info("🛑 RDS 인스턴스 종료 중...")
                
                # RDS 인스턴스 종료
                response = rds_client.stop_db_instance(DBInstanceIdentifier=self.db_identifier)
                
                logger.info(f"RDS 종료 요청 완료: {response['DBInstance']['DBInstanceStatus']}")
                
                return {
                    'success': True,
                    'message': 'RDS 인스턴스 종료 요청 완료',
                    'status': response['DBInstance']['DBInstanceStatus'],
                    'action': 'stop_requested'
                }
                
            else:
                logger.warning(f"⚠️ 예상치 못한 RDS 상태: {current_status['status']}")
                return {
                    'success': False,
                    'message': f'예상치 못한 RDS 상태: {current_status["status"]}',
                    'status': current_status['status'],
                    'action': 'unexpected_status'
                }
                
        except Exception as e:
            logger.error(f"❌ RDS 종료 실패: {e}")
            return {
                'success': False,
                'message': f'RDS 종료 실패: {str(e)}',
                'status': 'error',
                'action': 'failed'
            }
    
    def calculate_cost_savings(self) -> Dict:
        """비용 절약 추정치 계산"""
        try:
            total_execution_time = time.time() - self.start_time.timestamp()
            
            # 대략적인 시간당 비용 (USD)
            rds_hourly_cost = 0.072  # db.t3.medium 기준
            ec2_hourly_cost = 0.0464  # t3.medium 기준
            
            # 실제 사용 시간 (시간 단위)
            actual_usage_hours = total_execution_time / 3600
            
            # 기존 방식 (24시간 가동) vs 최적화 방식 비교
            daily_traditional_cost = (rds_hourly_cost + ec2_hourly_cost) * 24
            daily_optimized_cost = (rds_hourly_cost + ec2_hourly_cost) * (actual_usage_hours * 6)  # 4시간마다 실행
            
            daily_savings = daily_traditional_cost - daily_optimized_cost
            monthly_savings = daily_savings * 30
            
            return {
                'execution_time_minutes': round(total_execution_time / 60, 2),
                'traditional_daily_cost_usd': round(daily_traditional_cost, 2),
                'optimized_daily_cost_usd': round(daily_optimized_cost, 2),
                'daily_savings_usd': round(daily_savings, 2),
                'monthly_savings_usd': round(monthly_savings, 2),
                'annual_savings_usd': round(monthly_savings * 12, 2),
                'cost_reduction_percentage': round((daily_savings / daily_traditional_cost) * 100, 1)
            }
            
        except Exception as e:
            logger.warning(f"비용 계산 실패: {e}")
            return {}
    
    def execute_integrated_pipeline(self) -> Dict:
        """통합 파이프라인 실행 (RDS 별도 관리)"""
        try:
            logger.info("🎭 Makenaide Integrated Pipeline 시작 (RDS 별도 관리, Lambda 15분 제한 준수)")
            logger.info("📋 makenaide-basic-RDB-controller가 RDS를 20분 전에 시작했을 예정")
            
            # 현재 실행 시간 로깅 (스케줄링 확인용)
            current_time = datetime.now()
            logger.info(f"⏰ 현재 실행 시간: {current_time.strftime('%H:%M')} (예상: 01:00, 05:00, 09:00, 13:00, 17:00, 21:00)")
            
            # 1. RDS 상태 확인 및 대기
            self.log_step("rds_check", "시작")
            rds_ready = self.wait_for_rds_ready()
            
            if not rds_ready['success']:
                self.log_step("rds_check", "실패", {'error': rds_ready['message']})
                self.results['errors'].append(f"RDS 준비 확인 실패: {rds_ready['message']}")
                return self._create_error_response("RDS 준비 확인 실패")
            
            self.log_step("rds_check", "완료", {'result': 'RDS 사용 가능 확인'})
            
            # 2. EC2 시작
            self.log_step("ec2_start", "시작")
            ec2_result = self.start_ec2_instance()
            
            if not ec2_result['success']:
                self.log_step("ec2_start", "실패", {'error': ec2_result['message']})
                self.results['errors'].append(f"EC2 시작 실패: {ec2_result['message']}")
                return self._create_error_response("EC2 시작 실패")
            
            self.log_step("ec2_start", "완료", {'result': 'EC2 시작 성공'})
            
            # 3. EC2 부팅 대기
            logger.info("⏳ EC2 부팅 완료 대기 중...")
            time.sleep(60)  # 60초 대기
            
            # 4. Makenaide.py 실행 (비동기 시작)
            self.log_step("makenaide_launch", "시작")
            makenaide_launch = self.launch_makenaide_on_ec2()
            
            if not makenaide_launch['success']:
                self.log_step("makenaide_launch", "실패", makenaide_launch)
                self.results['errors'].append(f"Makenaide 시작 실패: {makenaide_launch['error']}")
            else:
                self.log_step("makenaide_launch", "완료", {
                    'command_id': makenaide_launch.get('command_id', ''),
                    'note': 'makenaide가 비동기로 시작됨. EC2에서 자동 종료될 예정.'
                })
                logger.info("📋 makenaide.py가 EC2에서 비동기로 실행 중입니다")
                logger.info("🔄 EC2는 makenaide 실행 완료 후 자동으로 종료됩니다")
            
            # 5. EC2 종료 대기 및 RDS 종료
            logger.info("⏳ EC2 종료 대기 중...")
            
            # EC2가 자동 종료될 때까지 모니터링
            ec2_stopped = self.wait_for_ec2_stopped(max_wait_seconds=self.timeouts['ec2_shutdown_wait'])
            
            if ec2_stopped['success']:
                logger.info("✅ EC2가 성공적으로 종료되었습니다")
                self.log_step("ec2_stop", "완료", {'result': 'EC2 자동 종료 확인'})
                
                # 6. EC2 종료 후 RDS도 종료
                logger.info("🛑 EC2 종료 확인. RDS도 종료합니다...")
                rds_stop_result = self.stop_rds_instance()
                
                if rds_stop_result['success']:
                    logger.info("✅ RDS 종료 요청 완료")
                    self.log_step("rds_stop", "완료", {'result': 'RDS 종료 요청 성공'})
                else:
                    logger.error(f"❌ RDS 종료 실패: {rds_stop_result['message']}")
                    self.log_step("rds_stop", "실패", {'error': rds_stop_result['message']})
                    self.results['errors'].append(f"RDS 종료 실패: {rds_stop_result['message']}")
            else:
                logger.warning("⚠️ EC2 종료 대기 타임아웃. RDS는 실행 상태로 유지됩니다.")
                self.log_step("ec2_stop", "타임아웃", {'result': 'EC2 종료 확인 실패'})
                # Lambda 15분 제한이 가까워지면 RDS는 실행 상태로 유지
            
            # 7. 비용 절약 계산
            cost_savings = self.calculate_cost_savings()
            self.results['cost_savings'] = cost_savings
            
            # 메트릭 전송
            self.send_custom_metric('PipelineExecutionTime', 
                                  (datetime.now() - self.start_time).total_seconds(), 'Seconds')
            self.send_custom_metric('ErrorCount', len(self.results['errors']), 'Count')
            
            if cost_savings:
                self.send_custom_metric('DailyCostSavings', 
                                      cost_savings.get('daily_savings_usd', 0), 'None')
            
            # 파이프라인 성공 여부 결정
            critical_failures = ['RDS 시작 실패', 'EC2 시작 실패']
            has_critical_failure = any(error.startswith(cf) for cf in critical_failures for error in self.results['errors'])
            
            pipeline_success = not has_critical_failure
            
            return {
                'success': pipeline_success,
                'execution_id': self.execution_id,
                'total_duration': (datetime.now() - self.start_time).total_seconds(),
                'steps': self.results['steps'],
                'errors': self.results['errors'],
                'cost_savings': cost_savings,
                'makenaide_success': makenaide_launch.get('success', False)
            }
            
        except Exception as e:
            logger.error(f"❌ Pipeline 실행 중 치명적 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return self._create_error_response(f"치명적 오류: {str(e)}")
    
    def _create_error_response(self, error_message: str) -> Dict:
        """오류 응답 생성"""
        return {
            'success': False,
            'execution_id': self.execution_id,
            'total_duration': (datetime.now() - self.start_time).total_seconds(),
            'steps': self.results['steps'],
            'errors': self.results['errors'] + [error_message],
            'cost_savings': {},
            'makenaide_success': False
        }

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🎭 Makenaide Integrated Orchestrator 시작")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        
        # 이벤트 정보 로깅
        if event:
            logger.info(f"📨 이벤트: {json.dumps(event, default=str)}")
        
        # 통합 오케스트레이터 실행
        orchestrator = MakenaideIntegratedOrchestrator()
        results = orchestrator.execute_integrated_pipeline()
        
        # 응답 생성
        response = {
            'statusCode': 200 if results['success'] else 500,
            'body': json.dumps({
                'message': 'Makenaide Integrated Pipeline 실행 완료',
                'success': results['success'],
                'execution_id': results['execution_id'],
                'total_duration': results['total_duration'],
                'steps_completed': len(results['steps']),
                'errors_count': len(results['errors']),
                'cost_savings': results['cost_savings'],
                'detailed_results': results,
                'timestamp': datetime.now().isoformat(),
                'version': 'integrated_orchestrator_v2.0_rds_separated'
            }, indent=2)
        }
        
        if results['success']:
            logger.info("🎉 Makenaide Integrated Orchestrator 성공 완료")
            if results['cost_savings']:
                savings = results['cost_savings']
                logger.info(f"💰 예상 월간 비용 절약: ${savings.get('monthly_savings_usd', 0)}")
        else:
            logger.error(f"❌ Makenaide Integrated Orchestrator 실패: {len(results['errors'])}개 오류")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Integrated Orchestrator 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Integrated Orchestrator 실행 실패',
                'timestamp': datetime.now().isoformat(),
                'version': 'integrated_orchestrator_v2.0_rds_separated'
            })
        }

# 테스트용 로컬 실행
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {}
    
    # 테스트 컨텍스트 (더미)
    class TestContext:
        def __init__(self):
            self.function_name = 'test'
            self.memory_limit_in_mb = 512
            self.invoked_function_arn = 'test'
            self.aws_request_id = 'test'
    
    result = lambda_handler(test_event, TestContext())
    print(json.dumps(result, indent=2))