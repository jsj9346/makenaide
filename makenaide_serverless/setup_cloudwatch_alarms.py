#!/usr/bin/env python3
"""
🚨 CloudWatch Alarms 설정 스크립트
- Makenaide 자동매매 시스템 핵심 알람 설정
- Lambda 실행 실패, 비정상 거래 패턴, 비용 초과 감지
- SNS 통합으로 즉시 알림 발송
"""

import boto3
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MakenaideCriticalAlarms:
    """Makenaide 핵심 알람 설정 클래스"""
    
    def __init__(self):
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.region = 'ap-northeast-2'
        self.account_id = '901361833359'
        
        # SNS 토픽 ARN
        self.sns_topic_arn = f"arn:aws:sns:{self.region}:{self.account_id}:makenaide-alerts"
        
        # Lambda 함수 목록
        self.lambda_functions = [
            'makenaide-ticker-scanner-phase0',
            'makenaide-selective-data-collection-phase1', 
            'makenaide-comprehensive-filtering-phase2',
            'makenaide-gpt-analysis-phase3',
            'makenaide-4h-analysis-phase4',
            'makenaide-condition-check-phase5',
            'makenaide-trade-execution-phase6',
            'makenaide-market-sentiment-check'
        ]

    def create_lambda_failure_alarms(self):
        """Lambda 함수 실행 실패 알람 생성"""
        try:
            logger.info("Lambda 실행 실패 알람 설정 중...")
            
            for func_name in self.lambda_functions:
                # 개별 Lambda 오류 알람
                alarm_name = f"Makenaide-{func_name}-Errors"
                
                self.cloudwatch_client.put_metric_alarm(
                    AlarmName=alarm_name,
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=1,
                    MetricName='Errors',
                    Namespace='AWS/Lambda',
                    Period=300,  # 5분
                    Statistic='Sum',
                    Threshold=1.0,
                    ActionsEnabled=True,
                    AlarmActions=[self.sns_topic_arn],
                    AlarmDescription=f'{func_name} Lambda 함수 실행 오류 감지',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': func_name
                        }
                    ],
                    Unit='Count',
                    TreatMissingData='notBreaching'
                )
                
                # Lambda 실행 시간 초과 알람 (Phase별로 다른 임계값)
                if 'phase6' in func_name or 'sentiment' in func_name:
                    timeout_threshold = 30000  # 30초
                elif 'phase3' in func_name or 'phase4' in func_name:
                    timeout_threshold = 45000  # 45초 (GPT 분석)
                else:
                    timeout_threshold = 15000  # 15초
                
                duration_alarm_name = f"Makenaide-{func_name}-Duration"
                
                self.cloudwatch_client.put_metric_alarm(
                    AlarmName=duration_alarm_name,
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=2,
                    MetricName='Duration',
                    Namespace='AWS/Lambda',
                    Period=300,
                    Statistic='Average',
                    Threshold=timeout_threshold,
                    ActionsEnabled=True,
                    AlarmActions=[self.sns_topic_arn],
                    AlarmDescription=f'{func_name} Lambda 함수 실행 시간 초과',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': func_name
                        }
                    ],
                    Unit='Milliseconds',
                    TreatMissingData='notBreaching'
                )
                
            logger.info(f"✅ {len(self.lambda_functions)}개 Lambda 함수 알람 설정 완료")
            
        except Exception as e:
            logger.error(f"Lambda 실행 실패 알람 설정 실패: {e}")

    def create_trading_safety_alarms(self):
        """거래 안전성 관련 알람"""
        try:
            logger.info("거래 안전성 알람 설정 중...")
            
            # 1. Phase 6 연속 실패 알람
            phase6_failure_alarm = "Makenaide-Phase6-Consecutive-Failures"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=phase6_failure_alarm,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=3,  # 3회 연속
                MetricName='Errors',
                Namespace='AWS/Lambda',
                Period=300,
                Statistic='Sum',
                Threshold=0.0,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='Phase 6 거래 실행 연속 실패 - 즉시 점검 필요',
                Dimensions=[
                    {
                        'Name': 'FunctionName',
                        'Value': 'makenaide-trade-execution-phase6'
                    }
                ],
                Unit='Count',
                TreatMissingData='breaching'  # 데이터 없음도 장애로 간주
            )
            
            # 2. 시장 상황 체크 실패 알람
            market_check_alarm = "Makenaide-Market-Sentiment-Failure"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=market_check_alarm,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='Errors',
                Namespace='AWS/Lambda',
                Period=300,
                Statistic='Sum',
                Threshold=0.0,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='시장 상황 분석 실패 - 거래 중단 가능',
                Dimensions=[
                    {
                        'Name': 'FunctionName',
                        'Value': 'makenaide-market-sentiment-check'
                    }
                ],
                Unit='Count',
                TreatMissingData='breaching'
            )
            
            logger.info("✅ 거래 안전성 알람 설정 완료")
            
        except Exception as e:
            logger.error(f"거래 안전성 알람 설정 실패: {e}")

    def create_cost_monitoring_alarms(self):
        """비용 모니터링 알람"""
        try:
            logger.info("비용 모니터링 알람 설정 중...")
            
            # 1. Lambda 일일 비용 초과 알람 (복합 메트릭)
            cost_alarm_name = "Makenaide-Daily-Cost-Exceeded"
            
            # Lambda 전체 호출량 초과 알람 (간접적 비용 측정)
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=cost_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='Invocations',
                Namespace='AWS/Lambda',
                Period=86400,  # 1일
                Statistic='Sum',
                Threshold=500.0,  # 일일 500회 초과시 (약 $0.1 = 예상 최대 비용)
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='일일 Lambda 호출량 초과 - 비용 점검 필요',
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            # 2. Lambda 동시 실행 수 초과 알람
            concurrent_alarm_name = "Makenaide-Concurrent-Executions-High"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=concurrent_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='ConcurrentExecutions',
                Namespace='AWS/Lambda',
                Period=300,
                Statistic='Maximum',
                Threshold=10.0,  # 동시 10개 이상
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='Lambda 동시 실행 수 초과 - 무한 루프 가능성',
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            # 3. Lambda 스로틀링 발생 알람
            throttle_alarm_name = "Makenaide-Lambda-Throttles"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=throttle_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='Throttles',
                Namespace='AWS/Lambda',
                Period=300,
                Statistic='Sum',
                Threshold=0.0,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='Lambda 스로틀링 발생 - 동시 실행 한도 초과',
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            logger.info("✅ 비용 모니터링 알람 설정 완료")
            
        except Exception as e:
            logger.error(f"비용 모니터링 알람 설정 실패: {e}")

    def create_system_health_alarms(self):
        """시스템 건강성 알람"""
        try:
            logger.info("시스템 건강성 알람 설정 중...")
            
            # 1. SNS 메시지 발송 실패 알람
            sns_failure_alarm = "Makenaide-SNS-Delivery-Failures"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=sns_failure_alarm,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='NumberOfNotificationsFailed',
                Namespace='AWS/SNS',
                Period=300,
                Statistic='Sum',
                Threshold=0.0,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='SNS 알림 발송 실패 - 알림 시스템 점검 필요',
                Dimensions=[
                    {
                        'Name': 'TopicName',
                        'Value': 'makenaide-alerts'
                    }
                ],
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            # 2. EventBridge 규칙 실행 실패 알람
            eventbridge_failure_alarm = "Makenaide-EventBridge-Failed-Invocations"
            
            # Market sentiment 규칙 실패
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=eventbridge_failure_alarm,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='FailedInvocations',
                Namespace='AWS/Events',
                Period=300,
                Statistic='Sum',
                Threshold=0.0,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='EventBridge 규칙 실행 실패 - 파이프라인 중단 가능',
                Dimensions=[
                    {
                        'Name': 'RuleName',
                        'Value': 'makenaide-market-sentiment-daily'
                    }
                ],
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            logger.info("✅ 시스템 건강성 알람 설정 완료")
            
        except Exception as e:
            logger.error(f"시스템 건강성 알람 설정 실패: {e}")

    def create_composite_alarms(self):
        """복합 알람 생성 - 시스템 전체 상태"""
        try:
            logger.info("복합 알람 설정 중...")
            
            # Phase 0-5 파이프라인 전체 실패 복합 알람
            pipeline_alarm_name = "Makenaide-Pipeline-System-Failure"
            
            # 기본 알람들이 생성된 후에 복합 알람 생성
            pipeline_alarm_rule = (
                f"ALARM(\"Makenaide-makenaide-ticker-scanner-phase0-Errors\") OR "
                f"ALARM(\"Makenaide-makenaide-selective-data-collection-phase1-Errors\") OR "
                f"ALARM(\"Makenaide-makenaide-comprehensive-filtering-phase2-Errors\") OR "
                f"ALARM(\"Makenaide-makenaide-gpt-analysis-phase3-Errors\") OR "
                f"ALARM(\"Makenaide-makenaide-4h-analysis-phase4-Errors\") OR "
                f"ALARM(\"Makenaide-makenaide-condition-check-phase5-Errors\")"
            )
            
            self.cloudwatch_client.put_composite_alarm(
                AlarmName=pipeline_alarm_name,
                AlarmRule=pipeline_alarm_rule,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='Makenaide 파이프라인 시스템 장애 - 즉시 점검 필요',
                InsufficientDataActions=[self.sns_topic_arn]
            )
            
            logger.info("✅ 복합 알람 설정 완료")
            
        except Exception as e:
            logger.error(f"복합 알람 설정 실패: {e}")
            # 복합 알람은 실패해도 계속 진행

    def test_alarm_notification(self):
        """알람 테스트 - 테스트 메트릭 발송"""
        try:
            logger.info("알람 테스트 메트릭 발송...")
            
            # 테스트용 커스텀 메트릭 발송
            self.cloudwatch_client.put_metric_data(
                Namespace='Makenaide/Test',
                MetricData=[
                    {
                        'MetricName': 'AlarmTest',
                        'Value': 1.0,
                        'Unit': 'Count',
                        'Timestamp': datetime.utcnow()
                    }
                ]
            )
            
            # 테스트 알람 생성
            test_alarm_name = "Makenaide-Alarm-System-Test"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=test_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='AlarmTest',
                Namespace='Makenaide/Test',
                Period=60,
                Statistic='Sum',
                Threshold=0.5,
                ActionsEnabled=True,
                AlarmActions=[self.sns_topic_arn],
                AlarmDescription='알람 시스템 테스트 - 정상 동작 확인용',
                Unit='Count',
                TreatMissingData='notBreaching'
            )
            
            logger.info("✅ 테스트 알람 생성 완료 - 1-2분 후 SNS 알림 수신 예정")
            
        except Exception as e:
            logger.error(f"알람 테스트 실패: {e}")

    def list_created_alarms(self):
        """생성된 알람 목록 조회"""
        try:
            logger.info("생성된 알람 목록 조회...")
            
            response = self.cloudwatch_client.describe_alarms(
                AlarmNamePrefix='Makenaide-'
            )
            
            alarms = response.get('MetricAlarms', []) + response.get('CompositeAlarms', [])
            
            logger.info(f"\n{'='*80}")
            logger.info(f"🚨 Makenaide CloudWatch 알람 목록 ({len(alarms)}개)")
            logger.info(f"{'='*80}")
            
            # 카테고리별로 분류
            categories = {
                'Lambda 오류': [],
                'Lambda 성능': [],
                '거래 안전': [],
                '비용 관리': [],
                '시스템 건강': [],
                '복합 알람': [],
                '테스트': []
            }
            
            for alarm in alarms:
                alarm_name = alarm['AlarmName']
                
                if 'Errors' in alarm_name and any(func in alarm_name for func in self.lambda_functions):
                    categories['Lambda 오류'].append(alarm_name)
                elif 'Duration' in alarm_name:
                    categories['Lambda 성능'].append(alarm_name)
                elif 'Phase6' in alarm_name or 'Market-Sentiment' in alarm_name:
                    categories['거래 안전'].append(alarm_name)
                elif 'Cost' in alarm_name or 'Concurrent' in alarm_name or 'Throttles' in alarm_name:
                    categories['비용 관리'].append(alarm_name)
                elif 'SNS' in alarm_name or 'EventBridge' in alarm_name:
                    categories['시스템 건강'].append(alarm_name)
                elif 'Pipeline-System' in alarm_name:
                    categories['복합 알람'].append(alarm_name)
                elif 'Test' in alarm_name:
                    categories['테스트'].append(alarm_name)
                    
            for category, alarm_list in categories.items():
                if alarm_list:
                    logger.info(f"\n📊 {category}:")
                    for alarm_name in sorted(alarm_list):
                        logger.info(f"   • {alarm_name}")
            
            logger.info(f"\n{'='*80}")
            logger.info(f"🎯 알람 설정 완료 요약:")
            logger.info(f"   • Lambda 함수별 오류/성능 알람: {len(self.lambda_functions) * 2}개")
            logger.info(f"   • 거래 안전성 알람: 2개")
            logger.info(f"   • 비용 모니터링 알람: 3개")
            logger.info(f"   • 시스템 건강성 알람: 2개")
            logger.info(f"   • 복합 알람: 1개")
            logger.info(f"   • 테스트 알람: 1개")
            logger.info(f"   📧 모든 알람 → SNS 토픽: makenaide-alerts")
            logger.info(f"{'='*80}")
            
        except Exception as e:
            logger.error(f"알람 목록 조회 실패: {e}")

def main():
    """메인 실행"""
    try:
        logger.info("🚨 Makenaide CloudWatch 알람 설정 시작")
        logger.info("="*80)
        
        alarm_manager = MakenaideCriticalAlarms()
        
        # 1. Lambda 실행 실패 알람
        alarm_manager.create_lambda_failure_alarms()
        
        # 2. 거래 안전성 알람
        alarm_manager.create_trading_safety_alarms()
        
        # 3. 비용 모니터링 알람
        alarm_manager.create_cost_monitoring_alarms()
        
        # 4. 시스템 건강성 알람
        alarm_manager.create_system_health_alarms()
        
        # 5. 복합 알람 (기본 알람 생성 후)
        alarm_manager.create_composite_alarms()
        
        # 6. 알람 테스트
        alarm_manager.test_alarm_notification()
        
        # 7. 생성된 알람 목록 확인
        alarm_manager.list_created_alarms()
        
        logger.info("\n🎉 CloudWatch 알람 설정 완료!")
        
        print(f"""

✅ CloudWatch 알람 설정 완료!

🚨 설정된 알람 카테고리:
   1. Lambda 함수별 오류 감지 ({len(alarm_manager.lambda_functions)}개)
   2. Lambda 실행 시간 초과 ({len(alarm_manager.lambda_functions)}개)
   3. 거래 안전성 - Phase 6 연속 실패, 시장 분석 실패
   4. 비용 관리 - 일일 호출량 초과, 동시 실행, 스로틀링
   5. 시스템 건강 - SNS 실패, EventBridge 실패
   6. 파이프라인 전체 장애 복합 알람

📧 알림 방식:
   • SNS 토픽: makenaide-alerts
   • 즉시 알림: 이메일, SMS (설정된 구독자)
   • 중요도별 차등 알림 (오류 즉시, 성능 2회 연속)

⚠️  다음 단계:
   1. SNS 토픽 구독 설정 (이메일/SMS)
   2. 1-2분 후 테스트 알람 확인
   3. 실제 운영 시 알람 임계값 조정

🔧 AWS 콘솔 확인:
   CloudWatch → 알람 → "Makenaide-" 접두사 알람들
        """)
        
        return True
        
    except Exception as e:
        logger.error(f"CloudWatch 알람 설정 실패: {e}")
        return False

if __name__ == '__main__':
    main()