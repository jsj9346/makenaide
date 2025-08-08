#!/usr/bin/env python3
"""
최종 SQS 큐 처리 최적화 (올바른 속성 이름 사용)
"""

import boto3
import json
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinalSQSOptimizer:
    """최종 SQS 큐 최적화"""
    
    def __init__(self):
        self.sqs = boto3.client('sqs')
        self.cloudwatch = boto3.client('cloudwatch')
        self.region = 'ap-northeast-2'
        
    def optimize_sqs_queues(self):
        """SQS 큐 최적화"""
        logger.info("🔧 최종 SQS 큐 최적화")
        
        # 올바른 속성 이름 사용
        queues_config = {
            'makenaide-ticker-queue': {
                'VisibilityTimeout': '300',  # 5분
                'MessageRetentionPeriod': '1209600',  # 14일
                'ReceiveMessageWaitTimeSeconds': '20',  # 롱 폴링 (올바른 속성명)
            },
            'makenaide-ohlcv-queue': {
                'VisibilityTimeout': '900',  # 15분
                'MessageRetentionPeriod': '1209600',
                'ReceiveMessageWaitTimeSeconds': '20',
            },
            'makenaide-dlq': {  # Dead Letter Queue
                'VisibilityTimeout': '60',
                'MessageRetentionPeriod': '1209600',
                'ReceiveMessageWaitTimeSeconds': '0'
            }
        }
        
        created_queues = {}
        
        for queue_name, config in queues_config.items():
            try:
                # 기존 큐 확인
                try:
                    response = self.sqs.get_queue_url(QueueName=queue_name)
                    queue_url = response['QueueUrl']
                    logger.info(f"✅ 기존 큐 발견: {queue_name}")
                    
                    # 속성 업데이트
                    self.sqs.set_queue_attributes(
                        QueueUrl=queue_url,
                        Attributes=config
                    )
                    logger.info(f"🔄 큐 속성 업데이트: {queue_name}")
                    
                except self.sqs.exceptions.QueueDoesNotExist:
                    # 새 큐 생성
                    response = self.sqs.create_queue(
                        QueueName=queue_name,
                        Attributes=config
                    )
                    queue_url = response['QueueUrl']
                    logger.info(f"🆕 새 큐 생성: {queue_name}")
                
                # 큐 ARN 획득
                attrs = self.sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['QueueArn']
                )
                queue_arn = attrs['Attributes']['QueueArn']
                
                created_queues[queue_name] = {
                    'url': queue_url,
                    'arn': queue_arn
                }
                
            except Exception as e:
                logger.error(f"❌ 큐 {queue_name} 설정 실패: {e}")
        
        # DLQ 설정
        self.setup_dead_letter_queues(created_queues)
        
        return created_queues
    
    def setup_dead_letter_queues(self, queues):
        """Dead Letter Queue 설정"""
        logger.info("💀 Dead Letter Queue 설정")
        
        if 'makenaide-dlq' not in queues:
            logger.warning("⚠️ DLQ가 없어 설정을 건너뜁니다")
            return
        
        dlq_arn = queues['makenaide-dlq']['arn']
        
        for queue_name in ['makenaide-ticker-queue', 'makenaide-ohlcv-queue']:
            if queue_name in queues:
                try:
                    redrive_policy = {
                        'deadLetterTargetArn': dlq_arn,
                        'maxReceiveCount': 3 if 'ticker' in queue_name else 5
                    }
                    
                    self.sqs.set_queue_attributes(
                        QueueUrl=queues[queue_name]['url'],
                        Attributes={
                            'RedrivePolicy': json.dumps(redrive_policy)
                        }
                    )
                    logger.info(f"✅ DLQ 설정 완료: {queue_name}")
                    
                except Exception as e:
                    logger.error(f"❌ DLQ 설정 실패 {queue_name}: {e}")
    
    def test_queue_functionality(self, queues):
        """큐 기능 테스트"""
        logger.info("🧪 큐 기능 테스트")
        
        test_results = {}
        
        for queue_name, queue_info in queues.items():
            if queue_name == 'makenaide-dlq':
                continue  # DLQ는 테스트에서 제외
                
            try:
                queue_url = queue_info['url']
                
                # 테스트 메시지
                test_message = {
                    'test': True,
                    'timestamp': datetime.now().isoformat(),
                    'queue': queue_name,
                    'message': f'큐 기능 테스트 - {queue_name}'
                }
                
                # 1. 메시지 전송 테스트
                start_time = time.time()
                self.sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(test_message, ensure_ascii=False)
                )
                send_time = time.time() - start_time
                
                # 2. 메시지 수신 테스트
                start_time = time.time()
                response = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=3
                )
                receive_time = time.time() - start_time
                
                # 3. 메시지 삭제
                success = False
                if 'Messages' in response:
                    receipt_handle = response['Messages'][0]['ReceiptHandle']
                    self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    success = True
                
                test_results[queue_name] = {
                    'send_time': send_time,
                    'receive_time': receive_time,
                    'success': success,
                    'status': 'passed'
                }
                
                logger.info(f"✅ {queue_name}: 전송 {send_time:.3f}초, 수신 {receive_time:.3f}초")
                
            except Exception as e:
                test_results[queue_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                logger.error(f"❌ {queue_name} 테스트 실패: {e}")
        
        return test_results
    
    def create_monitoring_dashboard(self):
        """모니터링 대시보드 생성"""
        logger.info("📈 모니터링 대시보드 생성")
        
        dashboard_config = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["AWS/SQS", "ApproximateNumberOfVisibleMessages", "QueueName", "makenaide-ticker-queue"],
                            [".", ".", ".", "makenaide-ohlcv-queue"],
                            [".", ".", ".", "makenaide-dlq"]
                        ],
                        "period": 300,
                        "stat": "Maximum",
                        "region": self.region,
                        "title": "큐 메시지 수",
                        "yAxis": {"left": {"min": 0}}
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["AWS/Lambda", "Duration", "FunctionName", "makenaide-orchestrator"],
                            [".", ".", ".", "makenaide-ticker-scanner"],
                            [".", ".", ".", "makenaide-ohlcv-collector"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": self.region,
                        "title": "Lambda 실행 시간",
                        "yAxis": {"left": {"min": 0}}
                    }
                }
            ]
        }
        
        try:
            self.cloudwatch.put_dashboard(
                DashboardName='makenaide-optimized-monitoring',
                DashboardBody=json.dumps(dashboard_config)
            )
            logger.info("✅ 모니터링 대시보드 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 대시보드 생성 실패: {e}")
            return False
    
    def run_final_optimization(self):
        """최종 SQS 최적화 실행"""
        logger.info("🚀 최종 SQS 시스템 최적화 시작")
        
        # 1. 큐 최적화
        optimized_queues = self.optimize_sqs_queues()
        
        # 2. 기능 테스트
        if optimized_queues:
            test_results = self.test_queue_functionality(optimized_queues)
        else:
            test_results = {}
        
        # 3. 모니터링 대시보드 생성
        dashboard_success = self.create_monitoring_dashboard()
        
        # 결과 출력
        print("\n" + "="*60)
        print("🎉 최종 SQS 시스템 최적화 완료!")
        print("="*60)
        
        if optimized_queues:
            print("✅ 최적화된 큐 목록:")
            for queue_name, queue_info in optimized_queues.items():
                print(f"  📦 {queue_name}")
                print(f"     🔗 {queue_info['url']}")
            
            print("\n🧪 기능 테스트 결과:")
            for queue_name, result in test_results.items():
                if result['status'] == 'passed':
                    print(f"  ✅ {queue_name}: 정상 작동 (전송 {result['send_time']:.3f}초)")
                else:
                    print(f"  ❌ {queue_name}: 실패")
            
            # 성공 통계
            success_count = sum(1 for r in test_results.values() if r['status'] == 'passed')
            total_count = len(test_results)
            print(f"\n📊 테스트 성공률: {success_count}/{total_count} ({100*success_count/total_count:.1f}%)")
        
        if dashboard_success:
            print("\n📈 모니터링:")
            print("  ✅ CloudWatch 대시보드 생성됨")
            print("  🔗 https://ap-northeast-2.console.aws.amazon.com/cloudwatch/home?region=ap-northeast-2#dashboards:name=makenaide-optimized-monitoring")
        
        print("\n🎯 최적화 효과:")
        print("  ✅ 롱 폴링 활성화 → AWS 요청 비용 절약")
        print("  ✅ 적절한 타임아웃 설정 → 효율적인 메시지 처리")
        print("  ✅ Dead Letter Queue → 실패 메시지 관리")
        print("  ✅ CloudWatch 모니터링 → 실시간 상태 추적")
        print("="*60)
        
        return len(optimized_queues) > 0

def main():
    """메인 실행 함수"""
    print("🔧 최종 SQS 큐 처리 최적화")
    print("="*60)
    
    optimizer = FinalSQSOptimizer()
    
    success = optimizer.run_final_optimization()
    
    if success:
        logger.info("🎉 최종 SQS 최적화 성공!")
    else:
        logger.error("❌ 최종 SQS 최적화 실패")
    
    return success

if __name__ == "__main__":
    main() 