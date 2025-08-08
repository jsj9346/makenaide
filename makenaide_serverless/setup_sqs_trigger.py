#!/usr/bin/env python3
"""
SQS 트리거 설정 스크립트

Lambda 함수와 SQS 큐 간의 이벤트 소스 매핑을 설정합니다.
"""

import boto3
import logging
import json
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def setup_sqs_trigger():
    """SQS 트리거 설정"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        
        function_name = "makenaide-distributed-backtest-worker"
        queue_name = "makenaide-distributed-backtest-job-queue"
        
        logger.info(f"🔗 SQS 트리거 설정: {function_name} <- {queue_name}")
        
        # 큐 URL 및 ARN 조회
        try:
            response = sqs_client.get_queue_url(QueueName=queue_name)
            queue_url = response['QueueUrl']
            
            # 큐 ARN 생성
            queue_arn = f"arn:aws:sqs:ap-northeast-2:901361833359:{queue_name}"
            
            logger.info(f"📡 큐 URL: {queue_url}")
            logger.info(f"📡 큐 ARN: {queue_arn}")
            
        except Exception as e:
            logger.error(f"❌ SQS 큐를 찾을 수 없습니다: {e}")
            return False
        
        # 기존 이벤트 소스 매핑 확인
        try:
            existing_mappings = lambda_client.list_event_source_mappings(
                FunctionName=function_name
            )
            
            for mapping in existing_mappings.get('EventSourceMappings', []):
                if queue_arn in mapping['EventSourceArn']:
                    logger.info(f"🔄 기존 SQS 트리거 발견: {mapping['UUID']}")
                    
                    # 기존 매핑의 상태 확인
                    if mapping['State'] == 'Enabled':
                        logger.info("✅ 기존 SQS 트리거가 이미 활성화됨")
                        return True
                    else:
                        # 비활성화된 매핑 활성화
                        logger.info("🔧 기존 SQS 트리거 활성화 중...")
                        lambda_client.update_event_source_mapping(
                            UUID=mapping['UUID'],
                            Enabled=True
                        )
                        logger.info("✅ SQS 트리거 활성화 완료")
                        return True
                        
        except Exception as e:
            logger.warning(f"⚠️ 기존 매핑 확인 실패: {e}")
        
        # 새 이벤트 소스 매핑 생성
        logger.info("🆕 새 SQS 트리거 생성 중...")
        
        try:
            response = lambda_client.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=function_name,
                BatchSize=1,  # 한 번에 하나씩 처리
                MaximumBatchingWindowInSeconds=10,
                Enabled=True
            )
            
            uuid = response['UUID']
            logger.info(f"✅ SQS 트리거 생성 완료: {uuid}")
            
            # 트리거 상태 확인
            time.sleep(5)  # 설정이 적용될 시간 대기
            
            mapping_info = lambda_client.get_event_source_mapping(UUID=uuid)
            state = mapping_info['State']
            
            logger.info(f"📊 트리거 상태: {state}")
            
            if state == 'Enabled':
                logger.info("🎉 SQS 트리거 설정 및 활성화 완료!")
                return True
            else:
                logger.warning(f"⚠️ 트리거가 예상과 다른 상태입니다: {state}")
                return False
                
        except Exception as e:
            if "already exists" in str(e).lower() or "resourceconflictexception" in str(e).lower():
                logger.info("🔄 SQS 트리거가 이미 존재합니다")
                return True
            else:
                logger.error(f"❌ SQS 트리거 생성 실패: {e}")
                return False
                
    except Exception as e:
        logger.error(f"❌ SQS 트리거 설정 실패: {e}")
        return False

def test_sqs_integration():
    """SQS 통합 테스트"""
    try:
        sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        queue_name = "makenaide-distributed-backtest-job-queue"
        
        # 큐 URL 조회
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        # 테스트 메시지 전송
        test_message = {
            'job_id': f'integration-test-{int(time.time())}',
            'job_type': 'SINGLE_STRATEGY',
            'strategy_name': 'Integration_Test',
            'parameters': {
                'position_size_method': 'percent',
                'position_size_value': 0.05,
                'stop_loss_pct': 0.03
            },
            'data_range': {
                'start_date': '2024-01-01',
                'end_date': '2024-01-07'
            }
        }
        
        logger.info("📤 SQS 테스트 메시지 전송...")
        
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(test_message),
            MessageAttributes={
                'job_type': {
                    'StringValue': 'SINGLE_STRATEGY',
                    'DataType': 'String'
                },
                'test': {
                    'StringValue': 'true',
                    'DataType': 'String'  
                }
            }
        )
        
        message_id = response['MessageId']
        logger.info(f"✅ 테스트 메시지 전송 완료: {message_id}")
        
        logger.info("⏳ Lambda 함수 처리 대기 중... (30초)")
        time.sleep(30)
        
        logger.info("📊 CloudWatch 로그에서 처리 결과를 확인하세요:")
        logger.info("   aws logs tail /aws/lambda/makenaide-distributed-backtest-worker --follow")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SQS 통합 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🔗 SQS 트리거 설정 및 통합 테스트")
    print("=" * 60)
    
    # 1. SQS 트리거 설정
    logger.info("1️⃣ SQS 트리거 설정")
    trigger_success = setup_sqs_trigger()
    
    if trigger_success:
        logger.info("✅ SQS 트리거 설정 완료")
        
        # 2. 통합 테스트
        logger.info("\n2️⃣ SQS 통합 테스트")
        test_success = test_sqs_integration()
        
        if test_success:
            logger.info("🎉 분산 백테스팅 시스템 SQS 통합 완료!")
        else:
            logger.warning("⚠️ 통합 테스트에서 문제 발생")
    else:
        logger.error("❌ SQS 트리거 설정 실패")

if __name__ == "__main__":
    main()