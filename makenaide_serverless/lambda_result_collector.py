#!/usr/bin/env python3
"""
백테스트 결과 수집 및 통합 Lambda 함수

SQS 결과 큐에서 분산 처리된 백테스트 결과를 수집하고 
DynamoDB와 S3에 통합 저장하는 Lambda 함수입니다.

Author: Result Collection System
Version: 1.0.0
"""

import json
import logging
import boto3
import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal
import traceback

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class BacktestResultCollector:
    """백테스트 결과 수집 및 통합 클래스"""
    
    def __init__(self):
        self.sqs_client = boto3.client('sqs')
        self.dynamodb = boto3.resource('dynamodb')
        self.s3_client = boto3.client('s3')
        
        # 환경 변수
        self.result_queue_url = os.environ.get('RESULT_QUEUE_URL')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-backtest-data')
        self.results_table_name = os.environ.get('RESULTS_TABLE', 'makenaide-distributed-backtest-results')
        self.jobs_table_name = os.environ.get('JOBS_TABLE', 'makenaide-distributed-backtest-jobs')
        
        # DynamoDB 테이블
        try:
            self.results_table = self.dynamodb.Table(self.results_table_name)
            self.jobs_table = self.dynamodb.Table(self.jobs_table_name)
            logger.info("✅ DynamoDB 테이블 연결 성공")
        except Exception as e:
            logger.error(f"❌ DynamoDB 테이블 연결 실패: {e}")
            self.results_table = None
            self.jobs_table = None
        
        self.collector_id = os.environ.get('AWS_LAMBDA_LOG_STREAM_NAME', str(uuid.uuid4()))
        
        logger.info(f"🔧 ResultCollector 초기화: {self.collector_id}")
    
    def process_result_messages(self, sqs_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """SQS 메시지에서 백테스트 결과 처리"""
        try:
            processed_count = 0
            aggregated_results = {}
            errors = []
            
            logger.info(f"📊 결과 메시지 처리 시작: {len(sqs_messages)}개")
            
            for message in sqs_messages:
                try:
                    # SQS 메시지 구조 파싱
                    message_body = message.get('body') or message.get('Body')
                    result_data = json.loads(message_body)
                    
                    job_id = result_data.get('job_id', 'unknown')
                    logger.info(f"📤 결과 처리 중: {job_id}")
                    
                    # 개별 결과 저장
                    self._store_individual_result(result_data)
                    
                    # 집계 데이터 누적
                    self._aggregate_result(result_data, aggregated_results)
                    
                    # 작업 상태 업데이트
                    if self.jobs_table:
                        self._update_job_status(job_id, result_data)
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ 개별 결과 처리 실패: {e}")
                    errors.append(str(e))
                    continue
            
            # 집계 결과 저장
            if aggregated_results:
                self._store_aggregated_results(aggregated_results)
            
            # S3에 배치 결과 저장
            if processed_count > 0:
                batch_summary = self._create_batch_summary(aggregated_results, processed_count, errors)
                self._store_batch_to_s3(batch_summary)
            
            result_summary = {
                'collector_id': self.collector_id,
                'processed_count': processed_count,
                'error_count': len(errors),
                'aggregated_metrics': self._calculate_summary_metrics(aggregated_results),
                'processing_timestamp': datetime.now().isoformat(),
                'errors': errors[:5]  # 최대 5개 오류만 반환
            }
            
            logger.info(f"✅ 결과 처리 완료: {processed_count}개 성공, {len(errors)}개 오류")
            return result_summary
            
        except Exception as e:
            logger.error(f"❌ 결과 메시지 처리 실패: {e}")
            logger.error(f"스택 트레이스: {traceback.format_exc()}")
            return {
                'collector_id': self.collector_id,
                'error': str(e),
                'error_traceback': traceback.format_exc(),
                'failed_at': datetime.now().isoformat()
            }
    
    def _store_individual_result(self, result_data: Dict[str, Any]):
        """개별 백테스트 결과를 DynamoDB에 저장"""
        try:
            if not self.results_table:
                logger.warning("⚠️ DynamoDB 테이블 없음, 저장 건너뜀")
                return
            
            job_id = result_data.get('job_id')
            worker_id = result_data.get('worker_id', 'unknown')
            
            # DynamoDB 아이템 준비
            item = {
                'job_id': job_id,
                'worker_id': worker_id,
                'status': result_data.get('status', 'UNKNOWN'),
                'completed_at': result_data.get('completed_at', datetime.now().isoformat()),
                'execution_time_seconds': Decimal(str(result_data.get('execution_time_seconds', 0))),
                'result_data': json.dumps(result_data.get('result_data', {}), default=str),
                'performance_metrics': json.dumps(result_data.get('performance_metrics', {}), default=str),
                'created_at': datetime.now().isoformat(),
                'ttl': int((datetime.now() + timedelta(days=90)).timestamp())  # 90일 후 자동 삭제
            }
            
            # 오류 정보 추가 (있는 경우)
            if result_data.get('error_message'):
                item['error_message'] = result_data['error_message']
                item['error_traceback'] = result_data.get('error_traceback', '')
            
            # DynamoDB에 저장
            self.results_table.put_item(Item=item)
            
            logger.info(f"📀 개별 결과 저장 완료: {job_id}")
            
        except Exception as e:
            logger.error(f"❌ 개별 결과 저장 실패: {e}")
    
    def _aggregate_result(self, result_data: Dict[str, Any], aggregated_results: Dict[str, Any]):
        """결과 데이터를 집계에 누적"""
        try:
            status = result_data.get('status', 'UNKNOWN')
            result_info = result_data.get('result_data', {})
            performance = result_data.get('performance_metrics', {})
            
            # 상태별 카운트
            if 'status_counts' not in aggregated_results:
                aggregated_results['status_counts'] = {}
            
            aggregated_results['status_counts'][status] = aggregated_results['status_counts'].get(status, 0) + 1
            
            # 성공한 결과만 집계
            if status == 'COMPLETED' and result_info:
                # 메트릭 누적
                metrics_to_aggregate = [
                    'win_rate', 'avg_return', 'total_trades', 'mdd', 
                    'sharpe_ratio', 'kelly_fraction', 'data_points'
                ]
                
                if 'aggregated_metrics' not in aggregated_results:
                    aggregated_results['aggregated_metrics'] = {}
                
                for metric in metrics_to_aggregate:
                    if metric in result_info and isinstance(result_info[metric], (int, float)):
                        if metric not in aggregated_results['aggregated_metrics']:
                            aggregated_results['aggregated_metrics'][metric] = []
                        aggregated_results['aggregated_metrics'][metric].append(result_info[metric])
                
                # 성능 메트릭 누적
                if 'performance_totals' not in aggregated_results:
                    aggregated_results['performance_totals'] = {
                        'total_execution_time': 0,
                        'total_memory_used': 0,
                        'total_data_points': 0,
                        'successful_jobs': 0
                    }
                
                aggregated_results['performance_totals']['total_execution_time'] += performance.get('processing_time', 0)
                aggregated_results['performance_totals']['total_memory_used'] += performance.get('memory_used_mb', 0)
                aggregated_results['performance_totals']['total_data_points'] += performance.get('data_points_processed', 0)
                aggregated_results['performance_totals']['successful_jobs'] += 1
                
        except Exception as e:
            logger.error(f"❌ 결과 집계 실패: {e}")
    
    def _store_aggregated_results(self, aggregated_results: Dict[str, Any]):
        """집계된 결과를 별도 테이블에 저장"""
        try:
            if not self.results_table:
                logger.warning("⚠️ DynamoDB 테이블 없음, 집계 저장 건너뜀")
                return
            
            batch_id = f"batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"
            
            # 집계 메트릭 계산
            summary_metrics = self._calculate_summary_metrics(aggregated_results)
            
            # DynamoDB 아이템 생성
            aggregation_item = {
                'job_id': f"AGGREGATION-{batch_id}",
                'worker_id': 'RESULT_COLLECTOR',
                'status': 'AGGREGATED',
                'completed_at': datetime.now().isoformat(),
                'aggregation_summary': json.dumps(summary_metrics, default=str),
                'raw_aggregation': json.dumps(aggregated_results, default=str),
                'created_at': datetime.now().isoformat(),
                'ttl': int((datetime.now() + timedelta(days=365)).timestamp())  # 1년 보관
            }
            
            # 저장
            self.results_table.put_item(Item=aggregation_item)
            
            logger.info(f"📊 집계 결과 저장 완료: {batch_id}")
            
        except Exception as e:
            logger.error(f"❌ 집계 결과 저장 실패: {e}")
    
    def _calculate_summary_metrics(self, aggregated_results: Dict[str, Any]) -> Dict[str, Any]:
        """집계 결과에서 요약 메트릭 계산"""
        try:
            metrics = aggregated_results.get('aggregated_metrics', {})
            performance = aggregated_results.get('performance_totals', {})
            status_counts = aggregated_results.get('status_counts', {})
            
            summary = {
                'total_jobs': sum(status_counts.values()),
                'successful_jobs': status_counts.get('COMPLETED', 0),
                'failed_jobs': status_counts.get('FAILED', 0),
                'success_rate': 0.0,
                'total_execution_time': performance.get('total_execution_time', 0),
                'average_execution_time': 0.0,
                'total_data_points_processed': performance.get('total_data_points', 0)
            }
            
            # 성공률 계산
            if summary['total_jobs'] > 0:
                summary['success_rate'] = summary['successful_jobs'] / summary['total_jobs']
            
            # 평균 실행 시간 계산
            if summary['successful_jobs'] > 0:
                summary['average_execution_time'] = summary['total_execution_time'] / summary['successful_jobs']
            
            # 백테스팅 메트릭 평균값 계산
            backtest_metrics = {}
            for metric_name, values in metrics.items():
                if values and len(values) > 0:
                    backtest_metrics[f"avg_{metric_name}"] = sum(values) / len(values)
                    backtest_metrics[f"min_{metric_name}"] = min(values)
                    backtest_metrics[f"max_{metric_name}"] = max(values)
                    backtest_metrics[f"count_{metric_name}"] = len(values)
            
            summary['backtest_metrics'] = backtest_metrics
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 요약 메트릭 계산 실패: {e}")
            return {}
    
    def _update_job_status(self, job_id: str, result_data: Dict[str, Any]):
        """작업 상태를 jobs 테이블에서 업데이트"""
        try:
            if not self.jobs_table:
                return
            
            update_expression = "SET job_status = :status, completed_at = :completed_at, result_summary = :result"
            expression_values = {
                ':status': result_data.get('status', 'UNKNOWN'),
                ':completed_at': result_data.get('completed_at', datetime.now().isoformat()),
                ':result': json.dumps(result_data.get('result_data', {}), default=str)
            }
            
            # 오류 정보 추가 (있는 경우)
            if result_data.get('error_message'):
                update_expression += ", error_message = :error_msg"
                expression_values[':error_msg'] = result_data['error_message']
            
            self.jobs_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values
            )
            
        except Exception as e:
            logger.warning(f"⚠️ 작업 상태 업데이트 실패: {e}")
    
    def _create_batch_summary(self, aggregated_results: Dict[str, Any], processed_count: int, errors: List[str]) -> Dict[str, Any]:
        """배치 처리 요약 생성"""
        return {
            'batch_id': f"batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}",
            'collector_id': self.collector_id,
            'processed_timestamp': datetime.now().isoformat(),
            'processed_count': processed_count,
            'error_count': len(errors),
            'summary_metrics': self._calculate_summary_metrics(aggregated_results),
            'errors': errors,
            'raw_aggregation': aggregated_results
        }
    
    def _store_batch_to_s3(self, batch_summary: Dict[str, Any]):
        """배치 요약을 S3에 저장"""
        try:
            batch_id = batch_summary['batch_id']
            key = f"backtest-results/batch-summaries/{datetime.now().strftime('%Y/%m/%d')}/{batch_id}.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(batch_summary, indent=2, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"☁️ S3 배치 요약 저장 완료: s3://{self.s3_bucket}/{key}")
            
        except Exception as e:
            logger.error(f"❌ S3 배치 저장 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info("🚀 백테스트 결과 수집기 Lambda 시작")
        logger.info(f"📥 이벤트: {json.dumps(event, default=str)}")
        
        collector = BacktestResultCollector()
        
        # SQS 메시지 처리
        if 'Records' in event:
            sqs_messages = []
            for record in event['Records']:
                if record.get('eventSource') == 'aws:sqs':
                    sqs_messages.append(record)
            
            if sqs_messages:
                result = collector.process_result_messages(sqs_messages)
            else:
                result = {'message': 'No SQS messages found', 'processed_count': 0}
        
        # 직접 호출 처리 (테스트용)
        elif 'test_results' in event:
            logger.info("🧪 테스트 모드: 직접 결과 처리")
            test_messages = event['test_results']
            result = collector.process_result_messages(test_messages)
        
        else:
            result = {'message': 'No supported event type found', 'processed_count': 0}
        
        logger.info(f"✅ 결과 수집기 Lambda 완료: {result}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Result collection completed successfully',
                'result': result
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda 핸들러 실패: {e}")
        logger.error(f"스택 트레이스: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Lambda execution failed',
                'message': str(e),
                'traceback': traceback.format_exc()
            })
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 로컬 테스트 이벤트
    test_event = {
        'test_results': [
            {
                'body': json.dumps({
                    'job_id': 'test-job-001',
                    'status': 'COMPLETED',
                    'worker_id': 'test-worker-001',
                    'execution_time_seconds': 2.5,
                    'completed_at': datetime.now().isoformat(),
                    'result_data': {
                        'strategy_name': 'Test_Strategy',
                        'win_rate': 0.65,
                        'avg_return': 0.08,
                        'total_trades': 150,
                        'mdd': -0.12,
                        'sharpe_ratio': 1.35,
                        'kelly_fraction': 0.15,
                        'data_points': 720
                    },
                    'performance_metrics': {
                        'processing_time': 2.5,
                        'memory_used_mb': 128,
                        'data_points_processed': 720
                    }
                })
            },
            {
                'body': json.dumps({
                    'job_id': 'test-job-002',
                    'status': 'FAILED',
                    'worker_id': 'test-worker-002',
                    'error_message': 'Test error',
                    'failed_at': datetime.now().isoformat()
                })
            }
        ]
    }
    
    # 환경 변수 설정 (로컬 테스트용)
    os.environ['RESULT_QUEUE_URL'] = 'https://sqs.ap-northeast-2.amazonaws.com/123456789/test-result-queue'
    os.environ['S3_BUCKET'] = 'makenaide-backtest-data'
    
    print("🧪 로컬 테스트 실행")
    result = lambda_handler(test_event, None)
    print(f"📊 테스트 결과: {json.dumps(result, indent=2, default=str)}")