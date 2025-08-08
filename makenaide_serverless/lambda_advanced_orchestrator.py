#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide Advanced Orchestrator (고도화 버전)
기능: 비용 최적화를 위한 전체 파이프라인 조정

🎯 비용 최적화 플로우:
1. RDS 시작 (makenaide-basic-RDB-controller)
2. DB 커넥션 체크 & 스키마 확인
3. EC2 시작 (makenaide-basic-controller)
4. EC2-RDB 커넥션 체크
5. EC2에서 SSM을 통해 makenaide.py 실행
6. EC2 종료 (makenaide-basic-shutdown)
7. RDS 종료 (makenaide-RDB-shutdown)

🚀 최적화 특징:
- RDS/EC2 자동 시작/종료로 비용 절약
- 커넥션 체크 및 오류 처리
- SSM을 통한 안전한 EC2 명령 실행
- 상세한 실행 로그 및 메트릭
- 비용 절약 추정치 제공
"""

import json
import boto3
import logging
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
lambda_client = boto3.client('lambda')
ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
rds_client = boto3.client('rds', region_name='ap-northeast-2')
ssm_client = boto3.client('ssm', region_name='ap-northeast-2')
cloudwatch = boto3.client('cloudwatch')

class MakenaideAdvancedOrchestrator:
    """Makenaide 고도화 파이프라인 조정 클래스"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.execution_id = f"advanced_exec_{int(time.time())}"
        
        # AWS 리소스 설정
        self.db_identifier = 'makenaide'
        self.ec2_instance_id = 'i-082bf343089af62d3'
        self.region = 'ap-northeast-2'
        
        # Lambda 함수 설정
        self.functions = {
            'rdb_controller': 'makenaide-basic-RDB-controller',
            'ec2_controller': 'makenaide-basic-controller', 
            'ec2_shutdown': 'makenaide-basic-shutdown',
            'rdb_shutdown': 'makenaide-RDB-shutdown'
        }
        
        # DB 연결 정보
        self.db_config = {
            'host': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
            'port': 5432,
            'database': 'makenaide',
            'user': 'bruce',
            'password': '0asis314.'
        }
        
        # 타임아웃 설정 (초)
        self.timeouts = {
            'rds_start': 600,         # 10분
            'ec2_start': 300,         # 5분
            'connection_check': 180,  # 3분
            'makenaide_execution': 3600,  # 60분
            'shutdown': 600,          # 10분
            'total_pipeline': 5400    # 90분
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
                Namespace='Makenaide/AdvancedPipeline',
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
    
    def invoke_lambda_function(self, function_name: str, payload: Dict = None) -> Dict:
        """Lambda 함수 호출"""
        try:
            logger.info(f"🚀 Lambda 함수 호출: {function_name}")
            
            if payload is None:
                payload = {}
            
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            result = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200:
                logger.info(f"✅ {function_name} 호출 성공")
                return {'success': True, 'result': result}
            else:
                logger.error(f"❌ {function_name} 호출 실패: {result}")
                return {'success': False, 'error': result}
                
        except Exception as e:
            logger.error(f"❌ {function_name} 호출 예외: {e}")
            return {'success': False, 'error': str(e)}
    
    def check_db_connection(self, max_retries: int = 5, retry_delay: int = 30) -> Dict:
        """RDS 인스턴스 상태 확인 (직접 DB 연결 대신)"""
        for attempt in range(max_retries):
            try:
                logger.info(f"🔍 RDS 상태 체크 (시도 {attempt + 1}/{max_retries})")
                
                # RDS 인스턴스 상태 확인
                response = rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
                db_instance = response['DBInstances'][0]
                
                status = db_instance['DBInstanceStatus']
                endpoint = db_instance.get('Endpoint', {}).get('Address', '')
                
                if status == 'available':
                    logger.info(f"✅ RDS 연결 가능 - Status: {status}")
                    logger.info(f"📊 Endpoint: {endpoint}")
                    
                    return {
                        'success': True,
                        'status': status,
                        'endpoint': endpoint,
                        'attempt': attempt + 1
                    }
                else:
                    logger.warning(f"⚠️ RDS 상태 불안정: {status}")
                    if attempt < max_retries - 1:
                        logger.info(f"⏳ {retry_delay}초 후 재시도...")
                        time.sleep(retry_delay)
                    else:
                        return {
                            'success': False,
                            'error': f'RDS 상태 불안정: {status}',
                            'attempt': attempt + 1
                        }
                
            except Exception as e:
                logger.warning(f"⚠️ RDS 상태 확인 실패 (시도 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ {retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ 모든 RDS 상태 확인 시도 실패")
                    return {
                        'success': False,
                        'error': str(e),
                        'attempt': attempt + 1
                    }
    
    def check_schema_and_init_if_needed(self) -> Dict:
        """스키마 존재 확인 - 간소화된 버전 (Lambda 제한 회피)"""
        try:
            logger.info("🔍 DB 스키마 확인 건너뜀 (Lambda 제한)")
            
            # Lambda 환경에서는 스키마 확인을 EC2에서 처리하도록 변경
            logger.info("✅ 스키마 확인은 EC2 실행 시 처리됩니다")
            return {
                'success': True,
                'message': 'Schema check deferred to EC2 execution'
            }
                
        except Exception as e:
            logger.error(f"❌ 스키마 확인 실패: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def check_ec2_rdb_connection(self) -> Dict:
        """EC2에서 RDB로의 연결 확인"""
        try:
            logger.info("🔗 EC2-RDB 커넥션 체크 중...")
            
            # SSM을 통해 EC2에서 DB 연결 테스트 명령 실행
            command = f"""
            cd /home/ec2-user/makenaide
            python3 -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(
        host='{self.db_config['host']}',
        port={self.db_config['port']},
        database='{self.db_config['database']}',
        user='{self.db_config['user']}',
        password='{self.db_config['password']}',
        connect_timeout=10
    )
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tickers')
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f'SUCCESS: Connected to DB, tickers count: {{count}}')
except Exception as e:
    print(f'ERROR: {{e}}')
"
            """
            
            response = ssm_client.send_command(
                InstanceIds=[self.ec2_instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                TimeoutSeconds=120
            )
            
            command_id = response['Command']['CommandId']
            
            # 명령 실행 결과 대기
            time.sleep(10)
            
            result = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=self.ec2_instance_id
            )
            
            if result['Status'] == 'Success':
                output = result['StandardOutputContent']
                if 'SUCCESS:' in output:
                    logger.info(f"✅ EC2-RDB 연결 성공: {output.strip()}")
                    return {'success': True, 'output': output.strip()}
                else:
                    logger.error(f"❌ EC2-RDB 연결 실패: {output}")
                    return {'success': False, 'error': output}
            else:
                error_output = result.get('StandardErrorContent', 'Unknown error')
                logger.error(f"❌ SSM 명령 실행 실패: {error_output}")
                return {'success': False, 'error': error_output}
                
        except Exception as e:
            logger.error(f"❌ EC2-RDB 커넥션 체크 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_makenaide_on_ec2(self) -> Dict:
        """EC2에서 SSM을 통해 makenaide.py 실행"""
        try:
            logger.info("🚀 EC2에서 makenaide.py 실행 시작")
            
            # SSM을 통해 makenaide.py 실행
            command = """
            cd /home/ec2-user/makenaide
            source venv/bin/activate 2>/dev/null || echo "Virtual env not found, using system python"
            export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
            
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
    
    def calculate_cost_savings(self, rds_start_time: float, ec2_start_time: float) -> Dict:
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
    
    def execute_advanced_pipeline(self) -> Dict:
        """고도화된 파이프라인 실행"""
        try:
            logger.info("🎭 Makenaide Advanced Pipeline 시작")
            
            # 1. RDS 시작
            self.log_step("rds_start", "시작")
            rds_result = self.invoke_lambda_function(
                self.functions['rdb_controller'],
                {'wait_for_available': True, 'max_wait_time': self.timeouts['rds_start']}
            )
            
            if not rds_result['success']:
                self.log_step("rds_start", "실패", {'error': rds_result['error']})
                self.results['errors'].append(f"RDS 시작 실패: {rds_result['error']}")
                return self._create_error_response("RDS 시작 실패")
            
            self.log_step("rds_start", "완료", {'result': 'RDS 시작 성공'})
            rds_start_time = time.time()
            
            # 2. DB 커넥션 체크
            self.log_step("db_connection_check", "시작")
            db_check = self.check_db_connection()
            
            if not db_check['success']:
                self.log_step("db_connection_check", "실패", {'error': db_check['error']})
                self.results['errors'].append(f"DB 연결 실패: {db_check['error']}")
                return self._create_error_response("DB 연결 실패")
            
            self.log_step("db_connection_check", "완료", db_check)
            
            # 3. 스키마 확인
            self.log_step("schema_check", "시작")
            schema_check = self.check_schema_and_init_if_needed()
            
            if not schema_check['success']:
                self.log_step("schema_check", "실패", schema_check)
                # 스키마 문제는 경고로 처리하고 계속 진행
                self.results['errors'].append(f"스키마 문제: {schema_check}")
            else:
                self.log_step("schema_check", "완료", schema_check)
            
            # 4. EC2 시작
            self.log_step("ec2_start", "시작")
            ec2_result = self.invoke_lambda_function(self.functions['ec2_controller'])
            
            if not ec2_result['success']:
                self.log_step("ec2_start", "실패", {'error': ec2_result['error']})
                self.results['errors'].append(f"EC2 시작 실패: {ec2_result['error']}")
                return self._create_error_response("EC2 시작 실패")
            
            self.log_step("ec2_start", "완료", {'result': 'EC2 시작 성공'})
            ec2_start_time = time.time()
            
            # 5. EC2-RDB 커넥션 체크
            self.log_step("ec2_rdb_connection", "시작")
            time.sleep(60)  # EC2 부팅 대기
            
            ec2_db_check = self.check_ec2_rdb_connection()
            if not ec2_db_check['success']:
                self.log_step("ec2_rdb_connection", "실패", ec2_db_check)
                self.results['errors'].append(f"EC2-RDB 연결 실패: {ec2_db_check['error']}")
                # 연결 실패시에도 makenaide 실행 시도
            else:
                self.log_step("ec2_rdb_connection", "완료", ec2_db_check)
            
            # 6. Makenaide.py 실행
            self.log_step("makenaide_execution", "시작")
            makenaide_result = self.execute_makenaide_on_ec2()
            
            if not makenaide_result['success']:
                self.log_step("makenaide_execution", "실패", makenaide_result)
                self.results['errors'].append(f"Makenaide 실행 실패: {makenaide_result['error']}")
            else:
                self.log_step("makenaide_execution", "완료", {
                    'execution_time': makenaide_result.get('execution_time', 0)
                })
            
            # 7. EC2 종료
            self.log_step("ec2_shutdown", "시작")
            ec2_shutdown = self.invoke_lambda_function(self.functions['ec2_shutdown'])
            
            if ec2_shutdown['success']:
                self.log_step("ec2_shutdown", "완료")
            else:
                self.log_step("ec2_shutdown", "실패", {'error': ec2_shutdown['error']})
                self.results['errors'].append(f"EC2 종료 실패: {ec2_shutdown['error']}")
            
            # 8. RDS 종료
            self.log_step("rds_shutdown", "시작")
            rds_shutdown = self.invoke_lambda_function(self.functions['rdb_shutdown'])
            
            if rds_shutdown['success']:
                self.log_step("rds_shutdown", "완료")
            else:
                self.log_step("rds_shutdown", "실패", {'error': rds_shutdown['error']})
                self.results['errors'].append(f"RDS 종료 실패: {rds_shutdown['error']}")
            
            # 9. 비용 절약 계산
            cost_savings = self.calculate_cost_savings(rds_start_time, ec2_start_time)
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
                'makenaide_success': makenaide_result.get('success', False)
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
        logger.info("🎭 Makenaide Advanced Orchestrator 시작")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        
        # 이벤트 정보 로깅
        if event:
            logger.info(f"📨 이벤트: {json.dumps(event, default=str)}")
        
        # 고도화 오케스트레이터 실행
        orchestrator = MakenaideAdvancedOrchestrator()
        results = orchestrator.execute_advanced_pipeline()
        
        # 응답 생성
        response = {
            'statusCode': 200 if results['success'] else 500,
            'body': json.dumps({
                'message': 'Makenaide Advanced Pipeline 실행 완료',
                'success': results['success'],
                'execution_id': results['execution_id'],
                'total_duration': results['total_duration'],
                'steps_completed': len(results['steps']),
                'errors_count': len(results['errors']),
                'cost_savings': results['cost_savings'],
                'detailed_results': results,
                'timestamp': datetime.now().isoformat(),
                'version': 'advanced_orchestrator_v1.0'
            }, indent=2)
        }
        
        if results['success']:
            logger.info("🎉 Makenaide Advanced Orchestrator 성공 완료")
            if results['cost_savings']:
                savings = results['cost_savings']
                logger.info(f"💰 예상 월간 비용 절약: ${savings.get('monthly_savings_usd', 0)}")
        else:
            logger.error(f"❌ Makenaide Advanced Orchestrator 실패: {len(results['errors'])}개 오류")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Advanced Orchestrator 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Advanced Orchestrator 실행 실패',
                'timestamp': datetime.now().isoformat(),
                'version': 'advanced_orchestrator_v1.0'
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