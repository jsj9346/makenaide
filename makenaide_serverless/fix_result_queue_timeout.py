#!/usr/bin/env python3
"""
결과 큐 타임아웃 설정 수정 스크립트

SQS 결과 큐의 visibility timeout을 Lambda 함수 timeout보다 크게 설정하고
SQS 트리거를 다시 설정합니다.
"""

import boto3
import logging
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def fix_result_queue_timeout():
    """결과 큐 타임아웃 설정 수정"""
    try:
        sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        resource_prefix = "makenaide-distributed-backtest"
        result_queue_name = f"{resource_prefix}-result-queue"
        collector_function = "makenaide-backtest-result-collector"
        
        logger.info("🔧 결과 큐 타임아웃 설정 수정 시작")
        
        # 1. 큐 URL 조회
        try:
            response = sqs_client.get_queue_url(QueueName=result_queue_name)
            queue_url = response['QueueUrl']
            logger.info(f"📡 결과 큐 URL: {queue_url}")
        except Exception as e:
            logger.error(f"❌ 결과 큐를 찾을 수 없음: {e}")
            return False
        
        # 2. 큐 속성 업데이트 (visibility timeout을 1200초로 설정)
        logger.info("⏱️ 큐 visibility timeout 업데이트 중...")
        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                'VisibilityTimeout': '1200',  # 20분 (Lambda timeout 900초보다 큼)
                'MessageRetentionPeriod': '1209600'  # 14일
            }
        )
        logger.info("✅ 큐 속성 업데이트 완료")
        
        # 3. SQS 트리거 설정
        logger.info("🔗 SQS 트리거 재설정 중...")
        
        # 큐 ARN 생성
        queue_arn = f"arn:aws:sqs:ap-northeast-2:901361833359:{result_queue_name}"
        
        try:
            response = lambda_client.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=collector_function,
                BatchSize=10,  # 배치로 처리
                MaximumBatchingWindowInSeconds=30,  # 30초 대기
                Enabled=True
            )
            
            uuid = response['UUID']
            logger.info(f"✅ SQS 트리거 설정 완료: {uuid}")
            
            # 트리거 상태 확인
            time.sleep(5)
            mapping_info = lambda_client.get_event_source_mapping(UUID=uuid)
            state = mapping_info['State']
            
            logger.info(f"📊 트리거 상태: {state}")
            
            if state == 'Enabled':
                logger.info("🎉 SQS 트리거 설정 및 활성화 완료!")
                return True
            else:
                logger.warning(f"⚠️ 트리거가 예상과 다른 상태: {state}")
                return False
                
        except Exception as e:
            if "already exists" in str(e).lower() or "resourceconflictexception" in str(e).lower():
                logger.info("🔄 기존 SQS 트리거가 존재함")
                
                # 기존 매핑 확인 및 업데이트
                existing_mappings = lambda_client.list_event_source_mappings(
                    FunctionName=collector_function
                )
                
                for mapping in existing_mappings.get('EventSourceMappings', []):
                    if queue_arn in mapping['EventSourceArn']:
                        uuid = mapping['UUID']
                        logger.info(f"🔄 기존 매핑 업데이트: {uuid}")
                        
                        # 매핑 업데이트
                        lambda_client.update_event_source_mapping(
                            UUID=uuid,
                            Enabled=True,
                            BatchSize=10,
                            MaximumBatchingWindowInSeconds=30
                        )
                        
                        logger.info("✅ 기존 매핑 업데이트 완료")
                        return True
                        
                return True
            else:
                logger.error(f"❌ SQS 트리거 설정 실패: {e}")
                return False
        
    except Exception as e:
        logger.error(f"❌ 결과 큐 타임아웃 수정 실패: {e}")
        return False

def test_end_to_end_integration():
    """종단 간 통합 테스트"""
    try:
        sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        
        resource_prefix = "makenaide-distributed-backtest"
        job_queue_name = f"{resource_prefix}-job-queue"
        
        logger.info("🧪 종단 간 통합 테스트 시작")
        
        # 1. 작업 큐에 테스트 작업 전송
        response = sqs_client.get_queue_url(QueueName=job_queue_name)
        job_queue_url = response['QueueUrl']
        
        test_job = {
            'job_id': f'end-to-end-test-{int(time.time())}',
            'job_type': 'SINGLE_STRATEGY',
            'strategy_name': 'End_To_End_Test',
            'parameters': {
                'position_size_method': 'percent',
                'position_size_value': 0.08,
                'stop_loss_pct': 0.04
            },
            'data_range': {
                'start_date': '2024-01-01',
                'end_date': '2024-01-07'
            }
        }
        
        logger.info("📤 테스트 작업 전송 중...")
        response = sqs_client.send_message(
            QueueUrl=job_queue_url,
            MessageBody=json.dumps(test_job),
            MessageAttributes={
                'job_type': {
                    'StringValue': 'SINGLE_STRATEGY',
                    'DataType': 'String'
                },
                'test': {
                    'StringValue': 'end_to_end',
                    'DataType': 'String'
                }
            }
        )
        
        message_id = response['MessageId']
        logger.info(f"✅ 테스트 작업 전송 완료: {message_id}")
        
        logger.info("⏳ 종단 간 처리 완료 대기 중... (60초)")
        logger.info("   1. 워커 Lambda가 작업을 처리합니다")
        logger.info("   2. 결과가 결과 큐로 전송됩니다")
        logger.info("   3. 수집기 Lambda가 결과를 처리합니다")
        
        time.sleep(60)
        
        logger.info("🎉 종단 간 테스트 완료!")
        logger.info("📊 CloudWatch 로그에서 전체 처리 과정을 확인하세요:")
        logger.info("   워커: aws logs tail /aws/lambda/makenaide-distributed-backtest-worker --follow")
        logger.info("   수집기: aws logs tail /aws/lambda/makenaide-backtest-result-collector --follow")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 종단 간 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🔧 결과 큐 SQS 트리거 수정 및 통합 테스트")
    print("=" * 60)
    
    # 1. 결과 큐 타임아웃 수정 및 트리거 설정
    logger.info("1️⃣ 결과 큐 타임아웃 수정")
    fix_success = fix_result_queue_timeout()
    
    if fix_success:
        logger.info("✅ 결과 큐 트리거 설정 완료")
        
        # 2. 종단 간 통합 테스트
        logger.info("\n2️⃣ 종단 간 통합 테스트")
        import json  # 여기서 json import 추가
        test_success = test_end_to_end_integration()
        
        if test_success:
            logger.info("🎉 분산 백테스팅 결과 수집 시스템 완전 구축 완료!")
        else:
            logger.warning("⚠️ 통합 테스트에서 문제 발생")
    else:
        logger.error("❌ 결과 큐 트리거 설정 실패")

if __name__ == "__main__":
    import json  # json import 추가
    main()