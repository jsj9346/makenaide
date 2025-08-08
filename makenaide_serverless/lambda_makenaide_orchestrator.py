#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide Orchestrator (최적화 버전)
기능: 전체 데이터 파이프라인 조정 및 각 Lambda 함수 호출

🎯 파이프라인 순서:
1. 티커 스캔 (makenaide-ticker-scanner)
2. OHLCV 데이터 수집 대기 (SQS 큐 모니터링)
3. 결과 집계 및 리포트
4. 성능 모니터링 및 알림

🚀 최적화 특징:
- 비동기 Lambda 호출
- SQS 큐 상태 모니터링
- 오류 처리 및 재시도 로직
- 상세한 실행 로그
- 성능 메트릭 수집
"""

import json
import boto3
import logging
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
lambda_client = boto3.client('lambda')
sqs_client = boto3.client('sqs')
cloudwatch = boto3.client('cloudwatch')

class MakenaideOrchestrator:
    """Makenaide 파이프라인 조정 클래스"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.execution_id = f"exec_{int(time.time())}"
        
        # Lambda 함수 설정
        self.functions = {
            'ticker_scanner': 'makenaide-ticker-scanner',
            'ohlcv_collector': 'makenaide-ohlcv-collector',
            'api_gateway': 'makenaide-api-gateway'
        }
        
        # SQS 큐 URL
        self.sqs_queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/901361833359/makenaide-ohlcv-collection'
        
        # 타임아웃 설정 (초)
        self.timeouts = {
            'ticker_scan': 300,      # 5분
            'ohlcv_collection': 1800, # 30분
            'total_pipeline': 2400   # 40분
        }
        
        # 실행 결과 추적
        self.results = {
            'pipeline_start': self.start_time.isoformat(),
            'execution_id': self.execution_id,
            'steps': {},
            'metrics': {},
            'errors': []
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
                Namespace='Makenaide/Pipeline',
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
        """Lambda 함수 비동기 호출"""
        try:
            logger.info(f"🚀 {function_name} 호출 시작")
            
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload or {})
            )
            
            # 응답 파싱
            response_payload = json.loads(response['Payload'].read())
            
            # 실행 시간 계산
            execution_time = (datetime.now() - self.start_time).total_seconds()
            
            if response['StatusCode'] == 200:
                logger.info(f"✅ {function_name} 실행 완료 ({execution_time:.2f}초)")
                return {
                    'success': True,
                    'response': response_payload,
                    'execution_time': execution_time,
                    'status_code': response['StatusCode']
                }
            else:
                logger.error(f"❌ {function_name} 실행 실패: {response['StatusCode']}")
                return {
                    'success': False,
                    'error': f"HTTP {response['StatusCode']}",
                    'response': response_payload,
                    'execution_time': execution_time
                }
                
        except Exception as e:
            logger.error(f"❌ {function_name} 호출 오류: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': (datetime.now() - self.start_time).total_seconds()
            }
    
    def wait_for_sqs_processing(self, max_wait_minutes: int = 30) -> Dict:
        """SQS 큐 처리 완료까지 대기"""
        logger.info(f"⏳ SQS 큐 처리 대기 시작 (최대 {max_wait_minutes}분)")
        
        start_wait = datetime.now()
        max_wait_time = timedelta(minutes=max_wait_minutes)
        
        initial_messages = None
        stable_count = 0
        required_stable_checks = 3  # 3회 연속 안정 확인
        
        while datetime.now() - start_wait < max_wait_time:
            try:
                # 큐 속성 확인
                response = sqs_client.get_queue_attributes(
                    QueueUrl=self.sqs_queue_url,
                    AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
                )
                
                visible_messages = int(response['Attributes'].get('ApproximateNumberOfMessages', 0))
                processing_messages = int(response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
                total_messages = visible_messages + processing_messages
                
                if initial_messages is None:
                    initial_messages = total_messages
                    logger.info(f"📊 초기 메시지 수: {initial_messages}")
                
                logger.info(f"📈 큐 상태: 대기={visible_messages}, 처리중={processing_messages}, 총={total_messages}")
                
                # 큐가 비어있고 안정적인 상태인지 확인
                if total_messages == 0:
                    stable_count += 1
                    logger.info(f"✅ 큐 안정 상태 확인 {stable_count}/{required_stable_checks}")
                    
                    if stable_count >= required_stable_checks:
                        wait_time = (datetime.now() - start_wait).total_seconds()
                        logger.info(f"🎉 SQS 처리 완료 확인 ({wait_time:.2f}초)")
                        
                        return {
                            'success': True,
                            'wait_time': wait_time,
                            'initial_messages': initial_messages,
                            'final_messages': total_messages
                        }
                else:
                    stable_count = 0
                
                # 30초 대기
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ SQS 상태 확인 오류: {e}")
                time.sleep(30)
        
        # 타임아웃
        wait_time = (datetime.now() - start_wait).total_seconds()
        logger.warning(f"⚠️ SQS 처리 대기 타임아웃 ({wait_time:.2f}초)")
        
        return {
            'success': False,
            'error': 'timeout',
            'wait_time': wait_time,
            'initial_messages': initial_messages
        }
    
    def get_db_summary(self) -> Dict:
        """DB 상태 요약 조회 (API Gateway 경유)"""
        try:
            logger.info("📊 DB 상태 요약 조회")
            
            # API Gateway Lambda 호출로 DB 상태 조회
            result = self.invoke_lambda_function(
                self.functions['api_gateway'],
                {
                    'httpMethod': 'GET',
                    'path': '/db/summary',
                    'headers': {'Content-Type': 'application/json'}
                }
            )
            
            if result['success'] and 'body' in result['response']:
                summary_data = json.loads(result['response']['body'])
                logger.info("✅ DB 상태 요약 조회 완료")
                return {
                    'success': True,
                    'data': summary_data
                }
            else:
                logger.warning("⚠️ DB 상태 요약 조회 실패")
                return {
                    'success': False,
                    'error': 'DB summary query failed'
                }
                
        except Exception as e:
            logger.error(f"❌ DB 상태 조회 오류: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def execute_pipeline(self) -> Dict:
        """전체 파이프라인 실행"""
        try:
            logger.info("🚀 Makenaide 파이프라인 실행 시작")
            logger.info(f"📋 실행 ID: {self.execution_id}")
            
            # 1단계: 티커 스캔
            self.log_step("ticker_scan", "시작")
            step_start = datetime.now()
            
            ticker_result = self.invoke_lambda_function(
                self.functions['ticker_scanner']
            )
            
            step_duration = (datetime.now() - step_start).total_seconds()
            
            if ticker_result['success']:
                # 응답에서 티커 수집 정보 추출
                if 'body' in ticker_result['response']:
                    ticker_data = json.loads(ticker_result['response']['body'])
                    processed_tickers = ticker_data.get('volume_filtered', 0)
                    
                    self.log_step("ticker_scan", "완료", {
                        'processed_tickers': processed_tickers,
                        'duration': step_duration,
                        'update_result': ticker_data.get('update_result', {})
                    })
                    
                    # 메트릭 전송
                    self.send_custom_metric('TickersScanned', processed_tickers)
                    self.send_custom_metric('TickerScanDuration', step_duration, 'Seconds')
                    
                    if processed_tickers == 0:
                        logger.warning("⚠️ 처리할 티커가 없어 OHLCV 수집 단계를 건너뜁니다")
                        self.log_step("ohlcv_collection", "건너뜀", {
                            'reason': '처리할 티커 없음'
                        })
                    else:
                        # 2단계: OHLCV 데이터 수집 대기
                        self.log_step("ohlcv_collection", "대기")
                        step_start = datetime.now()
                        
                        sqs_result = self.wait_for_sqs_processing(max_wait_minutes=30)
                        step_duration = (datetime.now() - step_start).total_seconds()
                        
                        if sqs_result['success']:
                            self.log_step("ohlcv_collection", "완료", {
                                'wait_time': sqs_result['wait_time'],
                                'initial_messages': sqs_result['initial_messages'],
                                'duration': step_duration
                            })
                            
                            # 메트릭 전송
                            self.send_custom_metric('OHLCVCollectionTime', sqs_result['wait_time'], 'Seconds')
                            self.send_custom_metric('ProcessedMessages', sqs_result.get('initial_messages', 0))
                        else:
                            self.log_step("ohlcv_collection", "타임아웃", {
                                'wait_time': sqs_result['wait_time'],
                                'error': sqs_result.get('error', 'timeout'),
                                'duration': step_duration
                            })
                            self.results['errors'].append("OHLCV 수집 타임아웃")
                else:
                    self.log_step("ticker_scan", "실패", {
                        'error': '응답 파싱 실패',
                        'duration': step_duration
                    })
                    self.results['errors'].append("티커 스캔 응답 파싱 실패")
            else:
                self.log_step("ticker_scan", "실패", {
                    'error': ticker_result.get('error', '알 수 없는 오류'),
                    'duration': step_duration
                })
                self.results['errors'].append(f"티커 스캔 실패: {ticker_result.get('error')}")
            
            # 3단계: DB 상태 요약
            self.log_step("db_summary", "시작")
            step_start = datetime.now()
            
            db_result = self.get_db_summary()
            step_duration = (datetime.now() - step_start).total_seconds()
            
            if db_result['success']:
                self.log_step("db_summary", "완료", {
                    'duration': step_duration,
                    'summary': db_result.get('data', {})
                })
            else:
                self.log_step("db_summary", "실패", {
                    'error': db_result.get('error', '알 수 없는 오류'),
                    'duration': step_duration
                })
                self.results['errors'].append(f"DB 요약 실패: {db_result.get('error')}")
            
            # 4단계: 최종 결과 정리
            total_duration = (datetime.now() - self.start_time).total_seconds()
            
            self.results.update({
                'pipeline_end': datetime.now().isoformat(),
                'total_duration': total_duration,
                'success': len(self.results['errors']) == 0,
                'execution_summary': {
                    'ticker_scan_success': ticker_result['success'],
                    'ohlcv_collection_attempted': processed_tickers > 0 if 'processed_tickers' in locals() else False,
                    'db_summary_success': db_result['success'],
                    'total_errors': len(self.results['errors'])
                }
            })
            
            # 최종 메트릭 전송
            self.send_custom_metric('PipelineDuration', total_duration, 'Seconds')
            self.send_custom_metric('PipelineSuccess', 1 if self.results['success'] else 0)
            self.send_custom_metric('PipelineErrors', len(self.results['errors']))
            
            logger.info(f"🏁 파이프라인 실행 완료 ({total_duration:.2f}초)")
            logger.info(f"📊 성공: {self.results['success']}, 오류: {len(self.results['errors'])}개")
            
            return self.results
            
        except Exception as e:
            logger.error(f"❌ 파이프라인 실행 중 치명적 오류: {e}")
            
            total_duration = (datetime.now() - self.start_time).total_seconds()
            self.results.update({
                'pipeline_end': datetime.now().isoformat(),
                'total_duration': total_duration,
                'success': False,
                'fatal_error': str(e)
            })
            
            # 오류 메트릭 전송
            self.send_custom_metric('PipelineFatalError', 1)
            self.send_custom_metric('PipelineDuration', total_duration, 'Seconds')
            
            return self.results

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🎭 Makenaide Orchestrator 시작")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        
        # 이벤트 정보 로깅
        if event:
            logger.info(f"📨 이벤트: {json.dumps(event, default=str)}")
        
        # 오케스트레이터 실행
        orchestrator = MakenaideOrchestrator()
        results = orchestrator.execute_pipeline()
        
        # 응답 생성
        response = {
            'statusCode': 200 if results['success'] else 500,
            'body': json.dumps({
                'message': 'Makenaide 파이프라인 실행 완료',
                'success': results['success'],
                'execution_id': results['execution_id'],
                'total_duration': results['total_duration'],
                'steps_completed': len(results['steps']),
                'errors_count': len(results['errors']),
                'detailed_results': results,
                'timestamp': datetime.now().isoformat(),
                'version': 'orchestrator_v1.0'
            }, indent=2)
        }
        
        if results['success']:
            logger.info("🎉 Makenaide Orchestrator 성공 완료")
        else:
            logger.error(f"❌ Makenaide Orchestrator 실패: {len(results['errors'])}개 오류")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Orchestrator 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Orchestrator 실행 실패',
                'timestamp': datetime.now().isoformat(),
                'version': 'orchestrator_v1.0'
            })
        } 