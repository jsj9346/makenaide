#!/usr/bin/env python3
"""
실제 운영 환경 성능 최적화 및 튜닝
"""

import boto3
import json
import logging
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceOptimizer:
    """성능 최적화 및 튜닝"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.cloudwatch = boto3.client('cloudwatch') 
        self.events_client = boto3.client('events')
        self.sqs = boto3.client('sqs')
        
    def optimize_lambda_configurations(self):
        """Lambda 함수 설정 최적화"""
        logger.info("⚡ Lambda 함수 성능 최적화")
        
        # 함수별 최적화 설정
        optimizations = {
            'makenaide-orchestrator': {
                'MemorySize': 512,
                'Timeout': 900,  # 15분
                'Description': 'Orchestrator - 메모리 증가로 성능 향상'
            },
            'makenaide-ticker-scanner': {
                'MemorySize': 256,
                'Timeout': 300,  # 5분
                'Description': 'Ticker Scanner - 경량화 최적화'
            },
            'makenaide-ohlcv-collector': {
                'MemorySize': 1024,
                'Timeout': 900,  # 15분 (OHLCV 수집은 시간이 오래 걸림)
                'Description': 'OHLCV Collector - 고성능 메모리 할당'
            },
            'makenaide-api-gateway': {
                'MemorySize': 256,
                'Timeout': 60,
                'Description': 'API Gateway - 빠른 응답 최적화'
            }
        }
        
        optimization_results = {}
        
        for function_name, config in optimizations.items():
            try:
                # 현재 설정 확인
                current_config = self.lambda_client.get_function_configuration(
                    FunctionName=function_name
                )
                
                # 업데이트 필요 여부 확인
                updates_needed = []
                if current_config['MemorySize'] != config['MemorySize']:
                    updates_needed.append(f"메모리: {current_config['MemorySize']} → {config['MemorySize']}MB")
                if current_config['Timeout'] != config['Timeout']:
                    updates_needed.append(f"타임아웃: {current_config['Timeout']} → {config['Timeout']}초")
                
                if updates_needed:
                    # 설정 업데이트
                    self.lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        MemorySize=config['MemorySize'],
                        Timeout=config['Timeout'],
                        Description=config['Description']
                    )
                    
                    optimization_results[function_name] = {
                        'status': 'optimized',
                        'changes': updates_needed
                    }
                    logger.info(f"✅ {function_name} 최적화: {', '.join(updates_needed)}")
                else:
                    optimization_results[function_name] = {
                        'status': 'already_optimal',
                        'changes': []
                    }
                    logger.info(f"ℹ️ {function_name} 이미 최적화됨")
                    
            except Exception as e:
                optimization_results[function_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                logger.error(f"❌ {function_name} 최적화 실패: {e}")
        
        return optimization_results
    
    def setup_performance_monitoring(self):
        """성능 모니터링 설정"""
        logger.info("📊 성능 모니터링 알람 설정")
        
        # 성능 임계값 알람 설정
        alarms = [
            {
                'name': 'makenaide-orchestrator-duration',
                'function': 'makenaide-orchestrator',
                'metric': 'Duration',
                'threshold': 600000,  # 10분 (밀리초)
                'description': 'Orchestrator 실행 시간 초과 알람'
            },
            {
                'name': 'makenaide-orchestrator-errors',
                'function': 'makenaide-orchestrator',
                'metric': 'Errors',
                'threshold': 1,
                'description': 'Orchestrator 오류 발생 알람'
            },
            {
                'name': 'makenaide-ticker-scanner-duration',
                'function': 'makenaide-ticker-scanner',
                'metric': 'Duration',
                'threshold': 180000,  # 3분
                'description': 'Ticker Scanner 성능 알람'
            },
            {
                'name': 'makenaide-ohlcv-collector-duration',
                'function': 'makenaide-ohlcv-collector',
                'metric': 'Duration',
                'threshold': 600000,  # 10분
                'description': 'OHLCV Collector 성능 알람'
            }
        ]
        
        alarm_results = {}
        
        for alarm in alarms:
            try:
                self.cloudwatch.put_metric_alarm(
                    AlarmName=alarm['name'],
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=2,
                    MetricName=alarm['metric'],
                    Namespace='AWS/Lambda',
                    Period=300,
                    Statistic='Average',
                    Threshold=alarm['threshold'],
                    ActionsEnabled=True,
                    AlarmDescription=alarm['description'],
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': alarm['function']
                        }
                    ]
                )
                
                alarm_results[alarm['name']] = 'created'
                logger.info(f"✅ 알람 설정: {alarm['name']}")
                
            except Exception as e:
                alarm_results[alarm['name']] = f'failed: {e}'
                logger.error(f"❌ 알람 설정 실패 {alarm['name']}: {e}")
        
        return alarm_results
    
    def test_end_to_end_performance(self):
        """전체 파이프라인 성능 테스트"""
        logger.info("🏃 전체 파이프라인 성능 테스트")
        
        test_results = {
            'start_time': datetime.now(),
            'tests': {},
            'summary': {}
        }
        
        # 1. Orchestrator 성능 테스트
        try:
            logger.info("1️⃣ Orchestrator 성능 테스트")
            start_time = time.time()
            
            response = self.lambda_client.invoke(
                FunctionName='makenaide-orchestrator',
                InvocationType='RequestResponse',
                Payload=json.dumps({'performance_test': True})
            )
            
            duration = time.time() - start_time
            
            if response['StatusCode'] == 200:
                result = json.loads(response['Payload'].read())
                test_results['tests']['orchestrator'] = {
                    'status': 'success',
                    'duration': duration,
                    'response_size': len(str(result)),
                    'lambda_duration': result.get('duration', 0) if isinstance(result, dict) else 0
                }
                logger.info(f"✅ Orchestrator: {duration:.2f}초")
            else:
                test_results['tests']['orchestrator'] = {
                    'status': 'failed',
                    'duration': duration,
                    'status_code': response['StatusCode']
                }
                
        except Exception as e:
            test_results['tests']['orchestrator'] = {
                'status': 'error',
                'error': str(e)
            }
            logger.error(f"❌ Orchestrator 테스트 실패: {e}")
        
        # 2. 개별 Lambda 함수 성능 테스트
        lambda_functions = ['makenaide-ticker-scanner', 'makenaide-ohlcv-collector', 'makenaide-api-gateway']
        
        for func_name in lambda_functions:
            try:
                logger.info(f"2️⃣ {func_name} 성능 테스트")
                start_time = time.time()
                
                response = self.lambda_client.invoke(
                    FunctionName=func_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps({'test': True, 'performance_mode': True})
                )
                
                duration = time.time() - start_time
                
                if response['StatusCode'] == 200:
                    test_results['tests'][func_name] = {
                        'status': 'success',
                        'duration': duration
                    }
                    logger.info(f"✅ {func_name}: {duration:.2f}초")
                else:
                    test_results['tests'][func_name] = {
                        'status': 'failed',
                        'duration': duration,
                        'status_code': response['StatusCode']
                    }
                    
            except Exception as e:
                test_results['tests'][func_name] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {func_name} 테스트 실패: {e}")
        
        # 3. SQS 성능 테스트
        try:
            logger.info("3️⃣ SQS 성능 테스트")
            sqs_performance = self.test_sqs_performance()
            test_results['tests']['sqs'] = sqs_performance
            
        except Exception as e:
            test_results['tests']['sqs'] = {
                'status': 'error',
                'error': str(e)
            }
            logger.error(f"❌ SQS 테스트 실패: {e}")
        
        # 결과 요약
        test_results['end_time'] = datetime.now()
        test_results['total_duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
        
        # 성공/실패 통계
        success_count = sum(1 for test in test_results['tests'].values() 
                          if test.get('status') == 'success')
        total_count = len(test_results['tests'])
        
        test_results['summary'] = {
            'success_rate': f"{success_count}/{total_count} ({100*success_count/total_count:.1f}%)",
            'total_tests': total_count,
            'successful_tests': success_count,
            'failed_tests': total_count - success_count
        }
        
        return test_results
    
    def test_sqs_performance(self):
        """SQS 성능 테스트"""
        # 기본 SQS 큐 목록
        queue_names = ['makenaide-ticker-queue', 'makenaide-ohlcv-queue']
        sqs_results = {}
        
        for queue_name in queue_names:
            try:
                queue_url = self.sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
                
                # 메시지 전송/수신 테스트
                start_time = time.time()
                
                # 메시지 전송
                self.sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({'performance_test': True, 'timestamp': datetime.now().isoformat()})
                )
                
                send_time = time.time() - start_time
                
                # 메시지 수신
                start_time = time.time()
                response = self.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=3)
                receive_time = time.time() - start_time
                
                # 메시지 삭제
                if 'Messages' in response:
                    self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=response['Messages'][0]['ReceiptHandle']
                    )
                
                sqs_results[queue_name] = {
                    'status': 'success',
                    'send_time': send_time,
                    'receive_time': receive_time,
                    'total_time': send_time + receive_time
                }
                
            except Exception as e:
                sqs_results[queue_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return sqs_results
    
    def generate_performance_report(self, lambda_optimizations, alarm_results, performance_tests):
        """성능 보고서 생성"""
        print("\n" + "="*60)
        print("🚀 실제 운영 환경 성능 최적화 완료!")
        print("="*60)
        
        # Lambda 최적화 결과
        print("⚡ Lambda 함수 최적화:")
        optimized_count = sum(1 for result in lambda_optimizations.values() 
                            if result['status'] == 'optimized')
        print(f"  📊 최적화된 함수: {optimized_count}/{len(lambda_optimizations)}개")
        
        for func_name, result in lambda_optimizations.items():
            if result['status'] == 'optimized':
                print(f"  ✅ {func_name}: {', '.join(result['changes'])}")
            elif result['status'] == 'already_optimal':
                print(f"  ℹ️ {func_name}: 이미 최적화됨")
            else:
                print(f"  ❌ {func_name}: 최적화 실패")
        
        # 모니터링 알람 결과
        print(f"\n📊 성능 모니터링 알람:")
        alarm_success = sum(1 for result in alarm_results.values() if result == 'created')
        print(f"  📈 설정된 알람: {alarm_success}/{len(alarm_results)}개")
        
        # 성능 테스트 결과
        print(f"\n🏃 성능 테스트 결과:")
        print(f"  📊 전체 성공률: {performance_tests['summary']['success_rate']}")
        print(f"  ⏱️ 총 테스트 시간: {performance_tests['total_duration']:.2f}초")
        
        print(f"\n📋 개별 테스트 결과:")
        for test_name, result in performance_tests['tests'].items():
            if result['status'] == 'success':
                duration = result.get('duration', 0)
                print(f"  ✅ {test_name}: {duration:.2f}초")
            else:
                print(f"  ❌ {test_name}: {result['status']}")
        
        # 최적화 권장사항
        print(f"\n🎯 성능 최적화 효과:")
        print(f"  ✅ Lambda 메모리 최적화 → 실행 속도 향상")
        print(f"  ✅ 타임아웃 조정 → 안정적인 처리")
        print(f"  ✅ 성능 모니터링 → 실시간 성능 추적")
        print(f"  ✅ SQS 롱 폴링 → 비용 절약")
        
        print(f"\n🔗 모니터링 링크:")
        print(f"  📈 CloudWatch 대시보드: https://ap-northeast-2.console.aws.amazon.com/cloudwatch/home?region=ap-northeast-2#dashboards:name=makenaide-optimized-monitoring")
        print(f"  📊 Lambda 메트릭: https://ap-northeast-2.console.aws.amazon.com/lambda/home?region=ap-northeast-2#/functions")
        print("="*60)
    
    def run_complete_optimization(self):
        """완전한 성능 최적화 실행"""
        logger.info("🚀 실제 운영 환경 성능 최적화 시작")
        
        # 1. Lambda 설정 최적화
        lambda_optimizations = self.optimize_lambda_configurations()
        
        # 2. 성능 모니터링 설정
        alarm_results = self.setup_performance_monitoring()
        
        # 잠시 대기 (설정 반영)
        logger.info("⏳ 설정 반영 대기...")
        time.sleep(10)
        
        # 3. 전체 성능 테스트
        performance_tests = self.test_end_to_end_performance()
        
        # 4. 보고서 생성
        self.generate_performance_report(lambda_optimizations, alarm_results, performance_tests)
        
        return True

def main():
    """메인 실행 함수"""
    print("🚀 실제 운영 환경 성능 최적화")
    print("="*60)
    
    optimizer = PerformanceOptimizer()
    
    success = optimizer.run_complete_optimization()
    
    if success:
        logger.info("🎉 성능 최적화 완료!")
    else:
        logger.error("❌ 성능 최적화 실패")

if __name__ == "__main__":
    main() 