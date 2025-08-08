#!/usr/bin/env python3
"""
백테스트 결과 수집기 Lambda 함수 배포 스크립트

SQS 결과 큐에서 결과를 수집하여 DynamoDB와 S3에 저장하는
ResultCollector Lambda 함수를 배포합니다.

Author: Result Collector Deployment
Version: 1.0.0
"""

import boto3
import json
import logging
import zipfile
import os
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ResultCollectorDeployer:
    """결과 수집기 Lambda 배포 클래스"""
    
    def __init__(self, region_name: str = 'ap-northeast-2'):
        self.region = region_name
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.sqs_client = boto3.client('sqs', region_name=region_name)
        self.iam_client = boto3.client('iam', region_name=region_name)
        
        self.function_name = "makenaide-backtest-result-collector"
        self.resource_prefix = "makenaide-distributed-backtest"
        
        logger.info(f"🚀 결과 수집기 Lambda 배포 준비 (리전: {region_name})")
    
    def create_result_collector_function(self) -> str:
        """결과 수집기 Lambda 함수 생성"""
        try:
            logger.info("📊 결과 수집기 Lambda 함수 생성 시작")
            
            # Lambda 함수 코드 압축
            function_zip_path = f"{self.function_name}.zip"
            with zipfile.ZipFile(function_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write("lambda_result_collector.py", "lambda_function.py")
            
            # 함수 코드 읽기
            with open(function_zip_path, 'rb') as f:
                function_code = f.read()
            
            # 큐 URL 조회
            result_queue_url = self._get_queue_url(f"{self.resource_prefix}-result-queue")
            
            # 환경 변수
            environment = {
                'RESULT_QUEUE_URL': result_queue_url,
                'S3_BUCKET': 'makenaide-backtest-data',
                'RESULTS_TABLE': f'{self.resource_prefix}-results',
                'JOBS_TABLE': f'{self.resource_prefix}-jobs',
                'LOG_LEVEL': 'INFO'
            }
            
            # IAM 역할 ARN
            role_arn = f"arn:aws:iam::901361833359:role/{self.resource_prefix}-lambda-role"
            
            try:
                response = self.lambda_client.create_function(
                    FunctionName=self.function_name,
                    Runtime='python3.9',
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Code={
                        'ZipFile': function_code
                    },
                    Description='Makenaide Backtest Result Collector',
                    Timeout=900,  # 15분
                    MemorySize=512,
                    Environment={
                        'Variables': environment
                    },
                    DeadLetterConfig={
                        'TargetArn': f"arn:aws:sqs:{self.region}:901361833359:{self.resource_prefix}-dlq"
                    }
                )
                
                function_arn = response['FunctionArn']
                logger.info(f"✅ 결과 수집기 Lambda 생성 완료: {function_arn}")
                
                # 정리
                os.remove(function_zip_path)
                
                return function_arn
                
            except Exception as e:
                if "already exists" in str(e) or "ResourceConflictException" in str(e):
                    # 기존 함수 업데이트
                    logger.info("🔄 기존 결과 수집기 Lambda 업데이트")
                    
                    # 코드 업데이트
                    self.lambda_client.update_function_code(
                        FunctionName=self.function_name,
                        ZipFile=function_code
                    )
                    
                    # 설정 업데이트
                    self.lambda_client.update_function_configuration(
                        FunctionName=self.function_name,
                        Environment={'Variables': environment}
                    )
                    
                    function_arn = f"arn:aws:lambda:{self.region}:901361833359:function:{self.function_name}"
                    logger.info(f"✅ 결과 수집기 Lambda 업데이트 완료: {function_arn}")
                    
                    os.remove(function_zip_path)
                    return function_arn
                else:
                    raise e
                
        except Exception as e:
            logger.error(f"❌ 결과 수집기 Lambda 생성 실패: {e}")
            return ""
    
    def setup_result_queue_trigger(self, function_arn: str) -> bool:
        """결과 큐 SQS 트리거 설정"""
        try:
            logger.info("🔗 결과 큐 SQS 트리거 설정 시작")
            
            result_queue_url = self._get_queue_url(f"{self.resource_prefix}-result-queue")
            if not result_queue_url:
                logger.error("❌ 결과 큐 URL을 찾을 수 없음")
                return False
            
            # 큐 ARN 생성
            queue_arn = f"arn:aws:sqs:{self.region}:901361833359:{self.resource_prefix}-result-queue"
            
            # 기존 이벤트 소스 매핑 확인
            try:
                existing_mappings = self.lambda_client.list_event_source_mappings(
                    FunctionName=self.function_name
                )
                
                for mapping in existing_mappings.get('EventSourceMappings', []):
                    if queue_arn in mapping['EventSourceArn']:
                        logger.info(f"🔄 기존 결과 큐 트리거 발견: {mapping['UUID']}")
                        
                        if mapping['State'] == 'Enabled':
                            logger.info("✅ 기존 결과 큐 트리거가 이미 활성화됨")
                            return True
                        else:
                            # 비활성화된 매핑 활성화
                            logger.info("🔧 기존 결과 큐 트리거 활성화 중...")
                            self.lambda_client.update_event_source_mapping(
                                UUID=mapping['UUID'],
                                Enabled=True
                            )
                            logger.info("✅ 결과 큐 트리거 활성화 완료")
                            return True
                            
            except Exception as e:
                logger.warning(f"⚠️ 기존 매핑 확인 실패: {e}")
            
            # 새 이벤트 소스 매핑 생성
            try:
                response = self.lambda_client.create_event_source_mapping(
                    EventSourceArn=queue_arn,
                    FunctionName=self.function_name,
                    BatchSize=10,  # 배치로 처리
                    MaximumBatchingWindowInSeconds=30,  # 30초 대기
                    Enabled=True
                )
                
                logger.info(f"✅ 결과 큐 트리거 설정 완료: {response['UUID']}")
                return True
                
            except Exception as e:
                if "already exists" in str(e) or "ResourceConflictException" in str(e):
                    logger.info("🔄 기존 결과 큐 트리거 사용")
                    return True
                else:
                    raise e
                
        except Exception as e:
            logger.error(f"❌ 결과 큐 트리거 설정 실패: {e}")
            return False
    
    def test_result_collector(self, function_arn: str) -> bool:
        """결과 수집기 테스트"""
        try:
            logger.info("🧪 결과 수집기 테스트 시작")
            
            # 함수가 준비될 때까지 대기
            if not self._wait_for_lambda_ready(self.function_name):
                logger.error("❌ Lambda 함수가 준비되지 않아 테스트를 건너뜁니다")
                return False
            
            # 테스트 페이로드
            test_payload = {
                'test_results': [
                    {
                        'body': json.dumps({
                            'job_id': f'test-result-{int(time.time())}',
                            'status': 'COMPLETED',
                            'worker_id': 'test-worker',
                            'execution_time_seconds': 1.5,
                            'completed_at': datetime.now().isoformat(),
                            'result_data': {
                                'strategy_name': 'Test_Collection_Strategy',
                                'win_rate': 0.72,
                                'avg_return': 0.095,
                                'total_trades': 85,
                                'mdd': -0.08,
                                'sharpe_ratio': 1.8,
                                'kelly_fraction': 0.12,
                                'data_points': 500
                            },
                            'performance_metrics': {
                                'processing_time': 1.5,
                                'memory_used_mb': 96,
                                'data_points_processed': 500
                            }
                        })
                    }
                ]
            }
            
            # Lambda 호출
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_payload)
            )
            
            # 결과 확인
            result_payload = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200 and result_payload['statusCode'] == 200:
                logger.info("✅ 결과 수집기 테스트 성공")
                
                # 결과 상세 출력
                result_body = json.loads(result_payload['body'])
                result_data = result_body.get('result', {})
                logger.info(f"   처리된 결과 수: {result_data.get('processed_count', 0)}")
                logger.info(f"   오류 수: {result_data.get('error_count', 0)}")
                
                return True
            else:
                logger.error(f"❌ 결과 수집기 테스트 실패: {result_payload}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 결과 수집기 테스트 실패: {e}")
            return False
    
    def send_test_result_to_queue(self) -> bool:
        """테스트용 결과를 결과 큐에 전송"""
        try:
            logger.info("📤 테스트 결과를 결과 큐에 전송")
            
            result_queue_url = self._get_queue_url(f"{self.resource_prefix}-result-queue")
            if not result_queue_url:
                logger.error("❌ 결과 큐 URL을 찾을 수 없음")
                return False
            
            # 테스트 결과 메시지
            test_result = {
                'job_id': f'integration-test-result-{int(time.time())}',
                'status': 'COMPLETED',
                'worker_id': 'integration-test-worker',
                'execution_time_seconds': 3.2,
                'completed_at': datetime.now().isoformat(),
                'result_data': {
                    'strategy_name': 'Integration_Test_Strategy',
                    'win_rate': 0.68,
                    'avg_return': 0.12,
                    'total_trades': 120,
                    'mdd': -0.15,
                    'sharpe_ratio': 1.45,
                    'kelly_fraction': 0.18,
                    'data_points': 800
                },
                'performance_metrics': {
                    'processing_time': 3.2,
                    'memory_used_mb': 145,
                    'data_points_processed': 800
                }
            }
            
            # SQS에 메시지 전송
            response = self.sqs_client.send_message(
                QueueUrl=result_queue_url,
                MessageBody=json.dumps(test_result),
                MessageAttributes={
                    'job_id': {
                        'StringValue': test_result['job_id'],
                        'DataType': 'String'
                    },
                    'status': {
                        'StringValue': test_result['status'],
                        'DataType': 'String'
                    },
                    'test': {
                        'StringValue': 'true',
                        'DataType': 'String'
                    }
                }
            )
            
            message_id = response['MessageId']
            logger.info(f"✅ 테스트 결과 전송 완료: {message_id}")
            
            logger.info("⏳ 결과 수집기 처리 대기 중... (15초)")
            time.sleep(15)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 테스트 결과 전송 실패: {e}")
            return False
    
    def _get_queue_url(self, queue_name: str) -> str:
        """큐 URL 조회"""
        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except Exception as e:
            logger.warning(f"⚠️ 큐 URL 조회 실패 ({queue_name}): {e}")
            return ""
    
    def _wait_for_lambda_ready(self, function_name: str, timeout: int = 60) -> bool:
        """Lambda 함수가 준비될 때까지 대기"""
        try:
            logger.info(f"⏳ Lambda 함수 준비 대기: {function_name}")
            
            for i in range(timeout):
                try:
                    response = self.lambda_client.get_function(FunctionName=function_name)
                    state = response['Configuration']['State']
                    
                    if state == 'Active':
                        logger.info(f"✅ Lambda 함수 준비 완료: {function_name}")
                        return True
                    elif state == 'Failed':
                        logger.error(f"❌ Lambda 함수 실패 상태: {function_name}")
                        return False
                    
                    if i % 10 == 0:  # 10초마다 상태 로그
                        logger.info(f"   상태: {state} - 대기 중... ({i}/{timeout}초)")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    if i < timeout - 1:  # 마지막 시도가 아니면 계속
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            logger.error(f"❌ Lambda 함수 준비 시간 초과: {function_name}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 상태 확인 실패: {e}")
            return False
    
    def deploy_complete_system(self) -> Dict[str, Any]:
        """전체 결과 수집 시스템 배포"""
        try:
            logger.info("🏗️ 백테스트 결과 수집 시스템 배포 시작")
            deployment_start = datetime.now()
            
            results = {
                "deployment_timestamp": deployment_start.isoformat(),
                "function_name": self.function_name
            }
            
            # 1. 결과 수집기 Lambda 생성
            logger.info("\n📊 1단계: 결과 수집기 Lambda 생성")
            function_arn = self.create_result_collector_function()
            results["function_arn"] = function_arn
            
            if not function_arn:
                raise Exception("결과 수집기 Lambda 생성 실패")
            
            # 2. 결과 큐 트리거 설정
            logger.info("\n🔗 2단계: 결과 큐 SQS 트리거 설정")
            trigger_success = self.setup_result_queue_trigger(function_arn)
            results["sqs_trigger_configured"] = trigger_success
            
            # 3. 함수 테스트
            logger.info("\n🧪 3단계: 결과 수집기 테스트")
            test_success = self.test_result_collector(function_arn)
            results["test_success"] = test_success
            
            # 4. 통합 테스트
            logger.info("\n🔄 4단계: SQS 통합 테스트")
            integration_success = self.send_test_result_to_queue()
            results["integration_test_success"] = integration_success
            
            # 배포 완료 시간
            deployment_end = datetime.now()
            deployment_duration = (deployment_end - deployment_start).total_seconds()
            results["deployment_duration_seconds"] = deployment_duration
            
            # 배포 요약
            logger.info(f"\n🎉 백테스트 결과 수집 시스템 배포 완료!")
            logger.info(f"   ⏱️  배포 소요 시간: {deployment_duration:.2f}초")
            logger.info(f"   📊 결과 수집기: {'생성됨' if function_arn else '실패'}")
            logger.info(f"   🔗 SQS 트리거: {'설정됨' if trigger_success else '실패'}")
            logger.info(f"   🧪 기능 테스트: {'성공' if test_success else '실패'}")
            logger.info(f"   🔄 통합 테스트: {'성공' if integration_success else '실패'}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 결과 수집 시스템 배포 실패: {e}")
            return {"error": str(e)}

def main():
    """메인 실행 함수"""
    print("📊 백테스트 결과 수집 및 통합 시스템 배포 시작")
    print("=" * 80)
    
    try:
        deployer = ResultCollectorDeployer()
        results = deployer.deploy_complete_system()
        
        if "error" not in results:
            print("\n✅ 백테스트 결과 수집 및 통합 시스템 배포 완료!")
            print("📋 배포 결과:")
            print(f"   📊 Function ARN: {results.get('function_arn', 'N/A')}")
            print(f"   🔗 SQS 트리거: {'설정됨' if results.get('sqs_trigger_configured') else '실패'}")
            print(f"   🧪 테스트: {'성공' if results.get('test_success') else '실패'}")
            print(f"   🔄 통합 테스트: {'성공' if results.get('integration_test_success') else '실패'}")
            
            if results.get('integration_test_success'):
                print(f"\n🎯 결과 수집 시스템이 성공적으로 배포되었습니다!")
                print("📊 CloudWatch 로그에서 결과 수집 로그를 확인하세요:")
                print("   aws logs tail /aws/lambda/makenaide-backtest-result-collector --follow")
            else:
                print(f"\n⚠️ 통합 테스트 실패 - 설정을 확인해주세요")
                
        else:
            print(f"❌ 배포 실패: {results['error']}")
    
    except Exception as e:
        print(f"❌ 전체 배포 실패: {e}")

if __name__ == "__main__":
    main()