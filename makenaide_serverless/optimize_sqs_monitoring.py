#!/usr/bin/env python3
"""
SQS 큐 처리 최적화 및 모니터링 개선
"""

import boto3
import json
import logging
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQSOptimizer:
    """SQS 큐 최적화 및 모니터링"""
    
    def __init__(self):
        self.sqs = boto3.client('sqs')
        self.cloudwatch = boto3.client('cloudwatch')
        self.region = 'ap-northeast-2'
        
    def get_or_create_optimized_queues(self):
        """최적화된 SQS 큐 생성 또는 업데이트"""
        logger.info("🔧 SQS 큐 최적화 설정")
        
        queues_config = {
            'makenaide-ticker-queue': {
                'VisibilityTimeoutSeconds': '300',  # 5분
                'MessageRetentionPeriod': '1209600',  # 14일
                'ReceiveMessageWaitTimeSeconds': '20',  # 롱 폴링
                'MaxReceiveCount': '3',  # DLQ로 이동 전 최대 재시도
                'RedrivePolicy': {
                    'deadLetterTargetArn': None,  # 나중에 설정
                    'maxReceiveCount': 3
                }
            },
            'makenaide-ohlcv-queue': {
                'VisibilityTimeoutSeconds': '900',  # 15분 (OHLCV 수집이 오래 걸림)
                'MessageRetentionPeriod': '1209600',
                'ReceiveMessageWaitTimeSeconds': '20',
                'MaxReceiveCount': '5',
                'RedrivePolicy': {
                    'deadLetterTargetArn': None,
                    'maxReceiveCount': 5
                }
            },
            'makenaide-dlq': {  # Dead Letter Queue
                'VisibilityTimeoutSeconds': '60',
                'MessageRetentionPeriod': '1209600',
                'ReceiveMessageWaitTimeSeconds': '0'
            }
        }
        
        created_queues = {}
        
        for queue_name, config in queues_config.items():
            try:
                # 기존 큐 URL 확인
                try:
                    response = self.sqs.get_queue_url(QueueName=queue_name)
                    queue_url = response['QueueUrl']
                    logger.info(f"✅ 기존 큐 발견: {queue_name}")
                    
                    # 속성 업데이트
                    attributes = {k: v for k, v in config.items() 
                                if k != 'RedrivePolicy' and v is not None}
                    
                    self.sqs.set_queue_attributes(
                        QueueUrl=queue_url,
                        Attributes=attributes
                    )
                    logger.info(f"🔄 큐 속성 업데이트: {queue_name}")
                    
                except self.sqs.exceptions.QueueDoesNotExist:
                    # 새 큐 생성
                    attributes = {k: v for k, v in config.items() 
                                if k != 'RedrivePolicy' and v is not None}
                    
                    response = self.sqs.create_queue(
                        QueueName=queue_name,
                        Attributes=attributes
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
        
        # DLQ 설정 (DLQ가 생성된 후)
        if 'makenaide-dlq' in created_queues:
            dlq_arn = created_queues['makenaide-dlq']['arn']
            
            for queue_name in ['makenaide-ticker-queue', 'makenaide-ohlcv-queue']:
                if queue_name in created_queues:
                    try:
                        redrive_policy = {
                            'deadLetterTargetArn': dlq_arn,
                            'maxReceiveCount': queues_config[queue_name]['MaxReceiveCount']
                        }
                        
                        self.sqs.set_queue_attributes(
                            QueueUrl=created_queues[queue_name]['url'],
                            Attributes={
                                'RedrivePolicy': json.dumps(redrive_policy)
                            }
                        )
                        logger.info(f"✅ DLQ 설정 완료: {queue_name}")
                        
                    except Exception as e:
                        logger.error(f"❌ DLQ 설정 실패 {queue_name}: {e}")
        
        return created_queues
    
    def setup_cloudwatch_alarms(self, queues):
        """CloudWatch 알람 설정"""
        logger.info("📊 CloudWatch 알람 설정")
        
        alarms_config = [
            {
                'name': 'makenaide-ticker-queue-depth',
                'queue_name': 'makenaide-ticker-queue',
                'metric': 'ApproximateNumberOfVisibleMessages',
                'threshold': 100,
                'description': '티커 큐 메시지 적체 알람'
            },
            {
                'name': 'makenaide-ohlcv-queue-depth',
                'queue_name': 'makenaide-ohlcv-queue', 
                'metric': 'ApproximateNumberOfVisibleMessages',
                'threshold': 500,
                'description': 'OHLCV 큐 메시지 적체 알람'
            },
            {
                'name': 'makenaide-dlq-messages',
                'queue_name': 'makenaide-dlq',
                'metric': 'ApproximateNumberOfVisibleMessages',
                'threshold': 1,
                'description': 'Dead Letter Queue 메시지 알람'
            }
        ]
        
        for alarm_config in alarms_config:
            try:
                queue_name = alarm_config['queue_name']
                if queue_name not in queues:
                    continue
                
                self.cloudwatch.put_metric_alarm(
                    AlarmName=alarm_config['name'],
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=2,
                    MetricName=alarm_config['metric'],
                    Namespace='AWS/SQS',
                    Period=300,
                    Statistic='Average',
                    Threshold=alarm_config['threshold'],
                    ActionsEnabled=True,
                    AlarmDescription=alarm_config['description'],
                    Dimensions=[
                        {
                            'Name': 'QueueName',
                            'Value': queue_name
                        }
                    ]
                )
                
                logger.info(f"✅ 알람 설정: {alarm_config['name']}")
                
            except Exception as e:
                logger.error(f"❌ 알람 설정 실패 {alarm_config['name']}: {e}")
    
    def create_sqs_monitoring_dashboard(self):
        """SQS 모니터링 대시보드 생성"""
        logger.info("📈 CloudWatch 대시보드 생성")
        
        dashboard_body = {
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
                        "stat": "Average",
                        "region": self.region,
                        "title": "SQS 큐 메시지 수",
                        "yAxis": {
                            "left": {
                                "min": 0
                            }
                        }
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
                            ["AWS/SQS", "NumberOfMessagesSent", "QueueName", "makenaide-ticker-queue"],
                            [".", "NumberOfMessagesReceived", ".", "."],
                            [".", "NumberOfMessagesDeleted", ".", "."]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": self.region,
                        "title": "SQS 메시지 처리량",
                        "yAxis": {
                            "left": {
                                "min": 0
                            }
                        }
                    }
                },
                {
                    "type": "metric",
                    "x": 0,
                    "y": 6,
                    "width": 24,
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
                        "title": "Lambda 함수 실행 시간",
                        "yAxis": {
                            "left": {
                                "min": 0
                            }
                        }
                    }
                }
            ]
        }
        
        try:
            self.cloudwatch.put_dashboard(
                DashboardName='makenaide-sqs-monitoring',
                DashboardBody=json.dumps(dashboard_body)
            )
            
            logger.info("✅ CloudWatch 대시보드 생성 완료")
            logger.info("🔗 대시보드 URL: https://ap-northeast-2.console.aws.amazon.com/cloudwatch/home?region=ap-northeast-2#dashboards:name=makenaide-sqs-monitoring")
            
        except Exception as e:
            logger.error(f"❌ 대시보드 생성 실패: {e}")
    
    def test_queue_performance(self, queues):
        """큐 성능 테스트"""
        logger.info("🧪 SQS 큐 성능 테스트")
        
        test_results = {}
        
        for queue_name, queue_info in queues.items():
            if queue_name == 'makenaide-dlq':
                continue  # DLQ는 테스트에서 제외
                
            try:
                queue_url = queue_info['url']
                start_time = time.time()
                
                # 테스트 메시지 전송
                test_message = {
                    'test': True,
                    'timestamp': datetime.now().isoformat(),
                    'queue': queue_name
                }
                
                self.sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(test_message),
                    MessageAttributes={
                        'test': {
                            'StringValue': 'performance_test',
                            'DataType': 'String'
                        }
                    }
                )
                
                send_time = time.time() - start_time
                
                # 메시지 수신 테스트
                start_time = time.time()
                response = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=1
                )
                
                receive_time = time.time() - start_time
                
                # 메시지 삭제
                if 'Messages' in response:
                    receipt_handle = response['Messages'][0]['ReceiptHandle']
                    self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                
                test_results[queue_name] = {
                    'send_time': send_time,
                    'receive_time': receive_time,
                    'status': 'success'
                }
                
                logger.info(f"✅ {queue_name} 테스트 성공 (전송: {send_time:.3f}초, 수신: {receive_time:.3f}초)")
                
            except Exception as e:
                test_results[queue_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                logger.error(f"❌ {queue_name} 테스트 실패: {e}")
        
        return test_results
    
    def optimize_complete_sqs_system(self):
        """완전한 SQS 시스템 최적화"""
        logger.info("🚀 완전한 SQS 시스템 최적화 시작")
        
        # 1단계: 큐 최적화
        queues = self.get_or_create_optimized_queues()
        
        # 2단계: CloudWatch 알람 설정
        self.setup_cloudwatch_alarms(queues)
        
        # 3단계: 모니터링 대시보드 생성
        self.create_sqs_monitoring_dashboard()
        
        # 4단계: 성능 테스트
        test_results = self.test_queue_performance(queues)
        
        # 결과 요약
        print("\n" + "="*60)
        print("📊 SQS 시스템 최적화 완료!")
        print("="*60)
        
        print("✅ 생성/최적화된 큐:")
        for queue_name, queue_info in queues.items():
            print(f"  - {queue_name}")
            print(f"    URL: {queue_info['url']}")
        
        print("\n📈 성능 테스트 결과:")
        for queue_name, result in test_results.items():
            if result['status'] == 'success':
                print(f"  ✅ {queue_name}: 전송 {result['send_time']:.3f}초, 수신 {result['receive_time']:.3f}초")
            else:
                print(f"  ❌ {queue_name}: {result['error']}")
        
        print("\n🔗 모니터링 링크:")
        print("  - CloudWatch 대시보드: https://ap-northeast-2.console.aws.amazon.com/cloudwatch/home?region=ap-northeast-2#dashboards:name=makenaide-sqs-monitoring")
        print("  - SQS 콘솔: https://ap-northeast-2.console.aws.amazon.com/sqs/v2/home?region=ap-northeast-2")
        
        print("\n🎯 최적화 효과:")
        print("  - 롱 폴링으로 비용 절약 (20초 대기)")
        print("  - Dead Letter Queue로 실패 메시지 처리")
        print("  - CloudWatch 알람으로 실시간 모니터링")
        print("  - 큐별 최적화된 타임아웃 설정")
        print("="*60)
        
        return len(queues) > 0

def main():
    """메인 실행 함수"""
    print("🔧 SQS 큐 처리 최적화 및 모니터링 개선")
    print("="*60)
    
    optimizer = SQSOptimizer()
    
    if optimizer.optimize_complete_sqs_system():
        logger.info("🎉 SQS 최적화 완료!")
    else:
        logger.error("❌ SQS 최적화 실패")

if __name__ == "__main__":
    main() 