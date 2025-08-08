#!/usr/bin/env python3
"""
분산 백테스팅 성능 테스트 시스템

Lambda 기반 분산 백테스팅 시스템의 성능을 측정하고 
순차 처리와 비교하여 성능 개선 효과를 검증합니다.

Author: Performance Testing System
Version: 1.0.0
"""

import json
import logging
import boto3
import time
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import uuid
import statistics
from pathlib import Path
import csv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DistributedBacktestingPerformanceTest:
    """분산 백테스팅 성능 테스트 클래스"""
    
    def __init__(self, region_name: str = 'ap-northeast-2'):
        self.region = region_name
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.sqs_client = boto3.client('sqs', region_name=region_name)
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
        
        self.resource_prefix = "makenaide-distributed-backtest"
        self.worker_function = "makenaide-distributed-backtest-worker"
        self.collector_function = "makenaide-backtest-result-collector"
        
        # 큐 URL 조회
        self.job_queue_url = self._get_queue_url(f"{self.resource_prefix}-job-queue")
        self.result_queue_url = self._get_queue_url(f"{self.resource_prefix}-result-queue")
        
        logger.info(f"🚀 분산 백테스팅 성능 테스트 초기화 (리전: {region_name})")
    
    def run_sequential_performance_test(self, test_jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """순차 처리 성능 테스트"""
        try:
            logger.info(f"🐌 순차 처리 성능 테스트 시작: {len(test_jobs)}개 작업")
            start_time = datetime.now()
            
            results = []
            processing_times = []
            
            for i, job in enumerate(test_jobs):
                job_start = time.time()
                
                # Lambda 직접 호출 (순차 처리)
                response = self.lambda_client.invoke(
                    FunctionName=self.worker_function,
                    InvocationType='RequestResponse',
                    Payload=json.dumps({'job_data': job})
                )
                
                job_end = time.time()
                job_duration = job_end - job_start
                processing_times.append(job_duration)
                
                # 응답 파싱
                result_payload = json.loads(response['Payload'].read())
                results.append(result_payload)
                
                if i % 10 == 0:  # 10개마다 진행 상황 로그
                    logger.info(f"   순차 처리 진행: {i+1}/{len(test_jobs)} ({job_duration:.2f}초)")
            
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            
            # 성능 메트릭 계산
            sequential_metrics = {
                'test_type': 'sequential',
                'total_jobs': len(test_jobs),
                'total_duration_seconds': total_duration,
                'average_job_duration': statistics.mean(processing_times),
                'min_job_duration': min(processing_times),
                'max_job_duration': max(processing_times),
                'median_job_duration': statistics.median(processing_times),
                'jobs_per_second': len(test_jobs) / total_duration,
                'successful_jobs': len([r for r in results if r.get('statusCode') == 200]),
                'failed_jobs': len([r for r in results if r.get('statusCode') != 200]),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"✅ 순차 처리 완료: {total_duration:.2f}초, {sequential_metrics['jobs_per_second']:.2f} jobs/sec")
            
            return sequential_metrics
            
        except Exception as e:
            logger.error(f"❌ 순차 처리 테스트 실패: {e}")
            return {'error': str(e)}
    
    def run_distributed_performance_test(self, test_jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """분산 처리 성능 테스트"""
        try:
            logger.info(f"⚡ 분산 처리 성능 테스트 시작: {len(test_jobs)}개 작업")
            start_time = datetime.now()
            
            # 1. 모든 작업을 SQS 큐에 전송
            job_submission_start = time.time()
            sent_jobs = self._send_jobs_to_queue(test_jobs)
            job_submission_end = time.time()
            submission_duration = job_submission_end - job_submission_start
            
            logger.info(f"📤 작업 전송 완료: {len(sent_jobs)}개 ({submission_duration:.2f}초)")
            
            # 2. 결과 모니터링
            monitoring_results = self._monitor_distributed_processing(len(sent_jobs))
            
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            
            # 성능 메트릭 계산
            distributed_metrics = {
                'test_type': 'distributed',
                'total_jobs': len(test_jobs),
                'sent_jobs': len(sent_jobs),
                'total_duration_seconds': total_duration,
                'job_submission_duration': submission_duration,
                'processing_duration': monitoring_results.get('processing_duration', 0),
                'jobs_per_second': len(sent_jobs) / total_duration,
                'successful_jobs': monitoring_results.get('successful_jobs', 0),
                'failed_jobs': monitoring_results.get('failed_jobs', 0),
                'concurrent_workers': monitoring_results.get('concurrent_workers', 0),
                'average_worker_duration': monitoring_results.get('average_worker_duration', 0),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'monitoring_details': monitoring_results
            }
            
            logger.info(f"✅ 분산 처리 완료: {total_duration:.2f}초, {distributed_metrics['jobs_per_second']:.2f} jobs/sec")
            
            return distributed_metrics
            
        except Exception as e:
            logger.error(f"❌ 분산 처리 테스트 실패: {e}")
            return {'error': str(e)}
    
    def run_scalability_test(self) -> Dict[str, Any]:
        """확장성 테스트 - 다양한 작업 크기로 성능 측정"""
        try:
            logger.info("📈 확장성 테스트 시작")
            
            # 테스트 크기 정의 (작업 개수)
            test_sizes = [5, 10, 25, 50, 100]
            scalability_results = []
            
            for size in test_sizes:
                logger.info(f"\n📊 확장성 테스트: {size}개 작업")
                
                # 테스트 작업 생성
                test_jobs = self._generate_test_jobs(size, job_type='SCALABILITY')
                
                # 분산 처리 테스트
                distributed_result = self.run_distributed_performance_test(test_jobs)
                
                # 결과 저장
                scalability_results.append({
                    'job_count': size,
                    'distributed_metrics': distributed_result
                })
                
                # 큐 클리어링 대기
                time.sleep(10)
            
            # 확장성 분석
            scalability_analysis = self._analyze_scalability(scalability_results)
            
            return {
                'test_type': 'scalability',
                'test_results': scalability_results,
                'scalability_analysis': scalability_analysis,
                'test_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 확장성 테스트 실패: {e}")
            return {'error': str(e)}
    
    def run_comprehensive_performance_comparison(self) -> Dict[str, Any]:
        """종합 성능 비교 테스트"""
        try:
            logger.info("🏁 종합 성능 비교 테스트 시작")
            test_start = datetime.now()
            
            # 테스트 설정
            test_job_count = 50  # 적당한 크기로 비교
            test_jobs = self._generate_test_jobs(test_job_count, job_type='COMPARISON')
            
            logger.info(f"🎯 테스트 작업 생성 완료: {test_job_count}개")
            
            # 1. 순차 처리 테스트
            logger.info("\n🐌 1단계: 순차 처리 성능 측정")
            sequential_results = self.run_sequential_performance_test(test_jobs.copy())
            
            # 시스템 정리 시간
            logger.info("⏳ 시스템 정리 대기 중... (30초)")
            time.sleep(30)
            
            # 2. 분산 처리 테스트
            logger.info("\n⚡ 2단계: 분산 처리 성능 측정")
            distributed_results = self.run_distributed_performance_test(test_jobs.copy())
            
            # 3. 성능 비교 분석
            logger.info("\n📊 3단계: 성능 비교 분석")
            comparison_analysis = self._analyze_performance_comparison(
                sequential_results, distributed_results
            )
            
            test_end = datetime.now()
            total_test_duration = (test_end - test_start).total_seconds()
            
            # 종합 결과
            comprehensive_results = {
                'test_type': 'comprehensive_comparison',
                'test_job_count': test_job_count,
                'total_test_duration_seconds': total_test_duration,
                'sequential_results': sequential_results,
                'distributed_results': distributed_results,
                'performance_comparison': comparison_analysis,
                'test_start_time': test_start.isoformat(),
                'test_end_time': test_end.isoformat()
            }
            
            # 결과 리포트
            self._generate_performance_report(comprehensive_results)
            
            return comprehensive_results
            
        except Exception as e:
            logger.error(f"❌ 종합 성능 비교 실패: {e}")
            return {'error': str(e)}
    
    def _generate_test_jobs(self, count: int, job_type: str = 'PERFORMANCE_TEST') -> List[Dict[str, Any]]:
        """테스트용 백테스트 작업 생성"""
        test_jobs = []
        
        strategies = ['Momentum_Strategy', 'Mean_Reversion', 'Breakout_Strategy', 'Volume_Strategy']
        
        for i in range(count):
            strategy = strategies[i % len(strategies)]
            
            job = {
                'job_id': f'{job_type.lower()}-{i+1}-{int(time.time())}-{str(uuid.uuid4())[:8]}',
                'job_type': 'SINGLE_STRATEGY',
                'strategy_name': strategy,
                'parameters': {
                    'position_size_method': 'percent',
                    'position_size_value': 0.05 + (i % 10) * 0.01,  # 0.05~0.14
                    'stop_loss_pct': 0.03 + (i % 5) * 0.01,  # 0.03~0.07
                    'take_profit_pct': 0.08 + (i % 8) * 0.01,  # 0.08~0.15
                    'max_positions': 5 + (i % 10)  # 5~14
                },
                'data_range': {
                    'start_date': '2024-01-01',
                    'end_date': '2024-02-01'  # 1개월 데이터
                },
                'test_metadata': {
                    'test_type': job_type,
                    'test_index': i + 1,
                    'created_at': datetime.now().isoformat()
                }
            }
            
            test_jobs.append(job)
        
        return test_jobs
    
    def _send_jobs_to_queue(self, jobs: List[Dict[str, Any]]) -> List[str]:
        """작업들을 SQS 큐에 전송"""
        sent_job_ids = []
        
        try:
            for job in jobs:
                response = self.sqs_client.send_message(
                    QueueUrl=self.job_queue_url,
                    MessageBody=json.dumps(job),
                    MessageAttributes={
                        'job_type': {
                            'StringValue': job.get('job_type', 'SINGLE_STRATEGY'),
                            'DataType': 'String'
                        },
                        'test': {
                            'StringValue': 'performance_test',
                            'DataType': 'String'
                        }
                    }
                )
                sent_job_ids.append(job['job_id'])
            
            return sent_job_ids
            
        except Exception as e:
            logger.error(f"❌ 작업 전송 실패: {e}")
            return sent_job_ids
    
    def _monitor_distributed_processing(self, expected_jobs: int, timeout: int = 300) -> Dict[str, Any]:
        """분산 처리 모니터링"""
        try:
            logger.info(f"👁️ 분산 처리 모니터링 시작: {expected_jobs}개 작업 대기")
            
            start_time = time.time()
            completed_jobs = 0
            failed_jobs = 0
            processing_times = []
            
            # CloudWatch 메트릭 수집 시작점
            monitoring_start = datetime.now()
            
            while time.time() - start_time < timeout:
                # 결과 큐에서 메시지 확인
                try:
                    response = self.sqs_client.receive_message(
                        QueueUrl=self.result_queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=5,
                        MessageAttributeNames=['All']
                    )
                    
                    messages = response.get('Messages', [])
                    
                    for message in messages:
                        try:
                            result_data = json.loads(message['Body'])
                            
                            if result_data.get('status') == 'COMPLETED':
                                completed_jobs += 1
                                processing_times.append(
                                    result_data.get('execution_time_seconds', 0)
                                )
                            else:
                                failed_jobs += 1
                            
                            # 메시지 삭제
                            self.sqs_client.delete_message(
                                QueueUrl=self.result_queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            
                        except Exception as e:
                            logger.warning(f"⚠️ 결과 메시지 처리 실패: {e}")
                    
                    # 진행 상황 로그
                    total_processed = completed_jobs + failed_jobs
                    if total_processed > 0 and total_processed % 10 == 0:
                        logger.info(f"   처리 진행: {total_processed}/{expected_jobs} "
                                  f"(성공: {completed_jobs}, 실패: {failed_jobs})")
                    
                    # 모든 작업 완료 확인
                    if total_processed >= expected_jobs:
                        logger.info("✅ 모든 분산 작업 완료")
                        break
                        
                except Exception as e:
                    logger.warning(f"⚠️ 결과 큐 폴링 실패: {e}")
                    time.sleep(2)
            
            end_time = time.time()
            processing_duration = end_time - start_time
            
            # CloudWatch에서 동시 실행 메트릭 조회
            concurrent_metrics = self._get_lambda_concurrent_metrics(monitoring_start)
            
            monitoring_results = {
                'processing_duration': processing_duration,
                'successful_jobs': completed_jobs,
                'failed_jobs': failed_jobs,
                'total_processed': completed_jobs + failed_jobs,
                'completion_rate': (completed_jobs + failed_jobs) / expected_jobs,
                'average_worker_duration': statistics.mean(processing_times) if processing_times else 0,
                'concurrent_workers': concurrent_metrics.get('max_concurrent_executions', 0),
                'timeout_reached': processing_duration >= timeout
            }
            
            return monitoring_results
            
        except Exception as e:
            logger.error(f"❌ 분산 처리 모니터링 실패: {e}")
            return {'error': str(e)}
    
    def _get_lambda_concurrent_metrics(self, start_time: datetime) -> Dict[str, Any]:
        """Lambda 동시 실행 메트릭 조회"""
        try:
            end_time = datetime.now()
            
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='ConcurrentExecutions',
                Dimensions=[
                    {
                        'Name': 'FunctionName',
                        'Value': self.worker_function
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,  # 1분 간격
                Statistics=['Maximum', 'Average']
            )
            
            datapoints = response.get('Datapoints', [])
            
            if datapoints:
                max_concurrent = max(dp['Maximum'] for dp in datapoints)
                avg_concurrent = statistics.mean(dp['Average'] for dp in datapoints)
                
                return {
                    'max_concurrent_executions': max_concurrent,
                    'average_concurrent_executions': avg_concurrent,
                    'datapoints_count': len(datapoints)
                }
            else:
                return {'max_concurrent_executions': 0, 'average_concurrent_executions': 0}
                
        except Exception as e:
            logger.warning(f"⚠️ 동시 실행 메트릭 조회 실패: {e}")
            return {'max_concurrent_executions': 0, 'average_concurrent_executions': 0}
    
    def _analyze_scalability(self, scalability_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """확장성 분석"""
        try:
            job_counts = []
            throughputs = []
            durations = []
            
            for result in scalability_results:
                if 'error' not in result['distributed_metrics']:
                    job_counts.append(result['job_count'])
                    throughputs.append(result['distributed_metrics']['jobs_per_second'])
                    durations.append(result['distributed_metrics']['total_duration_seconds'])
            
            if not job_counts:
                return {'error': '분석할 유효한 데이터가 없습니다'}
            
            # 선형 확장성 분석
            linear_efficiency = []
            for i, count in enumerate(job_counts):
                if i == 0:
                    linear_efficiency.append(1.0)  # 기준점
                else:
                    expected_duration = durations[0] * (count / job_counts[0])
                    actual_duration = durations[i]
                    efficiency = expected_duration / actual_duration
                    linear_efficiency.append(efficiency)
            
            analysis = {
                'job_count_range': f"{min(job_counts)}-{max(job_counts)}",
                'throughput_range': f"{min(throughputs):.2f}-{max(throughputs):.2f} jobs/sec",
                'average_throughput': statistics.mean(throughputs),
                'peak_throughput': max(throughputs),
                'linear_efficiency': {
                    'average': statistics.mean(linear_efficiency),
                    'minimum': min(linear_efficiency),
                    'efficiency_scores': dict(zip(job_counts, linear_efficiency))
                },
                'scalability_trend': 'increasing' if throughputs[-1] > throughputs[0] else 'decreasing'
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ 확장성 분석 실패: {e}")
            return {'error': str(e)}
    
    def _analyze_performance_comparison(self, sequential: Dict[str, Any], distributed: Dict[str, Any]) -> Dict[str, Any]:
        """순차 vs 분산 처리 성능 비교 분석"""
        try:
            if 'error' in sequential or 'error' in distributed:
                return {'error': '비교를 위한 유효한 데이터가 없습니다'}
            
            # 성능 개선 계산
            throughput_improvement = (
                distributed['jobs_per_second'] / sequential['jobs_per_second']
            ) if sequential['jobs_per_second'] > 0 else 0
            
            duration_improvement = (
                sequential['total_duration_seconds'] / distributed['total_duration_seconds']
            ) if distributed['total_duration_seconds'] > 0 else 0
            
            # 효율성 분석
            sequential_efficiency = sequential['total_jobs'] / sequential['total_duration_seconds']
            distributed_efficiency = distributed['total_jobs'] / distributed['total_duration_seconds']
            
            comparison = {
                'throughput_improvement_ratio': throughput_improvement,
                'throughput_improvement_percentage': (throughput_improvement - 1) * 100,
                'duration_improvement_ratio': duration_improvement,
                'duration_improvement_percentage': (duration_improvement - 1) * 100,
                'performance_summary': {
                    'sequential_throughput': sequential['jobs_per_second'],
                    'distributed_throughput': distributed['jobs_per_second'],
                    'sequential_duration': sequential['total_duration_seconds'],
                    'distributed_duration': distributed['total_duration_seconds'],
                    'concurrent_workers': distributed.get('concurrent_workers', 0)
                },
                'efficiency_analysis': {
                    'sequential_efficiency': sequential_efficiency,
                    'distributed_efficiency': distributed_efficiency,
                    'efficiency_gain': distributed_efficiency / sequential_efficiency if sequential_efficiency > 0 else 0
                },
                'success_rates': {
                    'sequential_success_rate': sequential['successful_jobs'] / sequential['total_jobs'] if sequential['total_jobs'] > 0 else 0,
                    'distributed_success_rate': distributed['successful_jobs'] / distributed['total_jobs'] if distributed['total_jobs'] > 0 else 0
                }
            }
            
            # 성능 등급 평가
            if throughput_improvement >= 2.0:
                performance_grade = 'A+ (Excellent)'
            elif throughput_improvement >= 1.5:
                performance_grade = 'A (Very Good)'
            elif throughput_improvement >= 1.2:
                performance_grade = 'B (Good)'
            elif throughput_improvement >= 1.0:
                performance_grade = 'C (Fair)'
            else:
                performance_grade = 'D (Poor)'
            
            comparison['performance_grade'] = performance_grade
            
            return comparison
            
        except Exception as e:
            logger.error(f"❌ 성능 비교 분석 실패: {e}")
            return {'error': str(e)}
    
    def _generate_performance_report(self, results: Dict[str, Any]):
        """성능 테스트 리포트 생성"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = Path(f'distributed_backtesting_performance_report_{timestamp}.json')
            
            # JSON 리포트 저장
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
            # CSV 요약 저장
            csv_file = Path(f'performance_summary_{timestamp}.csv')
            comparison = results.get('performance_comparison', {})
            
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Metric', 'Sequential', 'Distributed', 'Improvement'])
                writer.writerow([
                    'Throughput (jobs/sec)',
                    f"{results['sequential_results']['jobs_per_second']:.2f}",
                    f"{results['distributed_results']['jobs_per_second']:.2f}",
                    f"{comparison.get('throughput_improvement_percentage', 0):.1f}%"
                ])
                writer.writerow([
                    'Duration (seconds)',
                    f"{results['sequential_results']['total_duration_seconds']:.2f}",
                    f"{results['distributed_results']['total_duration_seconds']:.2f}",
                    f"{comparison.get('duration_improvement_percentage', 0):.1f}%"
                ])
                writer.writerow([
                    'Success Rate',
                    f"{comparison.get('success_rates', {}).get('sequential_success_rate', 0)*100:.1f}%",
                    f"{comparison.get('success_rates', {}).get('distributed_success_rate', 0)*100:.1f}%",
                    ""
                ])
            
            logger.info(f"📋 성능 리포트 생성 완료:")
            logger.info(f"   📄 상세 리포트: {report_file}")
            logger.info(f"   📊 요약 CSV: {csv_file}")
            
        except Exception as e:
            logger.error(f"❌ 성능 리포트 생성 실패: {e}")
    
    def _get_queue_url(self, queue_name: str) -> str:
        """큐 URL 조회"""
        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except Exception as e:
            logger.warning(f"⚠️ 큐 URL 조회 실패 ({queue_name}): {e}")
            return ""

def main():
    """메인 실행 함수"""
    print("🏁 분산 백테스팅 성능 테스트 시작")
    print("=" * 80)
    
    try:
        performance_tester = DistributedBacktestingPerformanceTest()
        
        # 종합 성능 비교 테스트 실행
        logger.info("🎯 종합 성능 비교 테스트 실행")
        results = performance_tester.run_comprehensive_performance_comparison()
        
        if 'error' not in results:
            comparison = results.get('performance_comparison', {})
            
            print(f"\n🎉 분산 백테스팅 성능 테스트 완료!")
            print("=" * 50)
            print(f"📊 테스트 결과 요약:")
            print(f"   📝 테스트 작업 수: {results.get('test_job_count', 0)}개")
            print(f"   ⏱️  전체 테스트 시간: {results.get('total_test_duration_seconds', 0):.1f}초")
            
            if comparison:
                print(f"\n⚡ 성능 개선 결과:")
                print(f"   🚀 처리량 개선: {comparison.get('throughput_improvement_percentage', 0):.1f}%")
                print(f"   ⏰ 처리 시간 단축: {comparison.get('duration_improvement_percentage', 0):.1f}%")
                print(f"   📈 성능 등급: {comparison.get('performance_grade', 'N/A')}")
                
                summary = comparison.get('performance_summary', {})
                print(f"\n📈 상세 메트릭:")
                print(f"   순차 처리: {summary.get('sequential_throughput', 0):.2f} jobs/sec")
                print(f"   분산 처리: {summary.get('distributed_throughput', 0):.2f} jobs/sec")
                print(f"   동시 워커: {summary.get('concurrent_workers', 0)}개")
                
            # 추가 확장성 테스트 제안
            print(f"\n🔍 추가 테스트:")
            print("   python distributed_backtesting_performance_test.py --scalability")
            
        else:
            print(f"❌ 성능 테스트 실패: {results['error']}")
    
    except Exception as e:
        print(f"❌ 전체 성능 테스트 실패: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--scalability':
        # 확장성 테스트 실행
        print("📈 확장성 테스트 실행")
        tester = DistributedBacktestingPerformanceTest()
        scalability_results = tester.run_scalability_test()
        
        if 'error' not in scalability_results:
            analysis = scalability_results.get('scalability_analysis', {})
            print(f"📊 확장성 테스트 완료:")
            print(f"   처리량 범위: {analysis.get('throughput_range', 'N/A')}")
            print(f"   평균 처리량: {analysis.get('average_throughput', 0):.2f} jobs/sec")
            print(f"   최대 처리량: {analysis.get('peak_throughput', 0):.2f} jobs/sec")
            
            efficiency = analysis.get('linear_efficiency', {})
            print(f"   평균 선형 효율성: {efficiency.get('average', 0):.2f}")
        else:
            print(f"❌ 확장성 테스트 실패: {scalability_results['error']}")
    else:
        main()