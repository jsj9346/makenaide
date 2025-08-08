#!/usr/bin/env python3
"""
📧 Makenaide SNS 이메일 알림 시스템 (SMS 제외)
비용 효율적인 이메일 알림만 설정
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MakenaideSNSEmailSetup:
    """Makenaide SNS 이메일 알림 설정"""
    
    def __init__(self, dry_run=True):
        self.region = 'ap-northeast-2'
        self.sns_client = boto3.client('sns', region_name=self.region)
        self.events_client = boto3.client('events', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.dry_run = dry_run
        self.mode = "DRY RUN" if dry_run else "LIVE"
        
        logger.info(f"📧 Makenaide SNS 이메일 설정 시작 - {self.mode} 모드")
    
    def check_existing_topics(self):
        """기존 SNS 토픽 확인"""
        try:
            logger.info("🔍 기존 SNS 토픽 확인 중...")
            
            response = self.sns_client.list_topics()
            existing_topics = []
            
            for topic in response['Topics']:
                topic_arn = topic['TopicArn']
                if 'makenaide' in topic_arn:
                    existing_topics.append(topic_arn)
                    logger.info(f"발견된 토픽: {topic_arn}")
            
            if not existing_topics:
                logger.info("기존 Makenaide SNS 토픽이 없습니다. 새로 생성합니다.")
            
            return existing_topics
            
        except Exception as e:
            logger.error(f"❌ 토픽 확인 실패: {e}")
            return []
    
    def create_sns_topics(self):
        """필요한 SNS 토픽 생성"""
        topics_to_create = {
            'makenaide-daily-reports': '일일 거래 성과 리포트',
            'makenaide-trading-alerts': '거래 신호 및 체결 알림',
            'makenaide-system-alerts': '시스템 상태 알림',
            'makenaide-critical-alerts': '긴급 시스템 문제'
        }
        
        created_topics = {}
        
        for topic_name, description in topics_to_create.items():
            try:
                if not self.dry_run:
                    response = self.sns_client.create_topic(
                        Name=topic_name,
                        Attributes={
                            'DisplayName': f'Makenaide {description}',
                            'DeliveryPolicy': json.dumps({
                                'http': {
                                    'defaultHealthyRetryPolicy': {
                                        'numRetries': 3,
                                        'minDelayTarget': 20,
                                        'maxDelayTarget': 20
                                    }
                                }
                            })
                        }
                    )
                    topic_arn = response['TopicArn']
                    created_topics[topic_name] = topic_arn
                    logger.info(f"✅ 토픽 생성: {topic_name}")
                else:
                    logger.info(f"🔍 DRY RUN: {topic_name} 토픽이 생성될 예정")
                    created_topics[topic_name] = f"arn:aws:sns:ap-northeast-2:901361833359:{topic_name}"
                    
            except Exception as e:
                if 'already exists' in str(e):
                    logger.info(f"✅ 토픽 이미 존재: {topic_name}")
                    created_topics[topic_name] = f"arn:aws:sns:ap-northeast-2:901361833359:{topic_name}"
                else:
                    logger.error(f"❌ 토픽 생성 실패 {topic_name}: {e}")
        
        return created_topics
    
    def setup_email_subscription(self, email: str, topics: Dict[str, str]):
        """이메일 구독 설정 (SMS 제외)"""
        try:
            logger.info(f"📧 이메일 구독 설정: {email}")
            
            # 주요 토픽에만 이메일 구독 (비용 효율화)
            important_topics = [
                ('makenaide-daily-reports', '일일 리포트'),
                ('makenaide-trading-alerts', '거래 알림'),
                ('makenaide-critical-alerts', '긴급 알림')
            ]
            
            subscriptions = []
            
            for topic_name, description in important_topics:
                if topic_name in topics:
                    if not self.dry_run:
                        response = self.sns_client.subscribe(
                            TopicArn=topics[topic_name],
                            Protocol='email',
                            Endpoint=email
                        )
                        subscription_arn = response['SubscriptionArn']
                        subscriptions.append(subscription_arn)
                        logger.info(f"✅ 이메일 구독 생성: {description}")
                    else:
                        logger.info(f"🔍 DRY RUN: {description} 이메일 구독이 생성될 예정")
            
            logger.info(f"\n📧 이메일 알림 설정 완료:")
            logger.info(f"- 이메일: {email}")
            logger.info(f"- 구독 토픽: 3개 (일일리포트, 거래알림, 긴급알림)")
            logger.info(f"- 예상 메시지: ~30개/월")
            logger.info(f"- 예상 비용: $0.00 (무료 티어 내)")
            
            if not self.dry_run:
                print(f"\n✅ 이메일 알림 설정이 완료되었습니다!")
                print(f"📧 {email}로 구독 확인 이메일이 발송됩니다.")
                print(f"📬 이메일을 확인하고 구독을 승인해 주세요.")
            
            return subscriptions
            
        except Exception as e:
            logger.error(f"❌ 이메일 구독 설정 실패: {e}")
            return []
    
    def integrate_with_lambda_functions(self, topics: Dict[str, str]):
        """Lambda 함수와 SNS 통합"""
        try:
            logger.info("🔗 Lambda 함수와 SNS 통합 중...")
            
            # 주요 Lambda 함수들에 SNS 알림 기능 추가
            lambda_sns_mapping = {
                'makenaide-trade-execution-phase6': 'makenaide-trading-alerts',
                'makenaide-market-sentiment-check': 'makenaide-system-alerts',
                'makenaide-integrated-orchestrator-v2': 'makenaide-daily-reports'
            }
            
            for lambda_name, topic_name in lambda_sns_mapping.items():
                if not self.dry_run:
                    try:
                        # Lambda 함수 환경 변수에 SNS 토픽 ARN 추가
                        current_config = self.lambda_client.get_function_configuration(
                            FunctionName=lambda_name
                        )
                        
                        env_vars = current_config.get('Environment', {}).get('Variables', {})
                        env_vars['SNS_TOPIC_ARN'] = topics.get(topic_name, '')
                        
                        self.lambda_client.update_function_configuration(
                            FunctionName=lambda_name,
                            Environment={'Variables': env_vars}
                        )
                        
                        logger.info(f"✅ {lambda_name} SNS 통합 완료")
                    except Exception as e:
                        logger.warning(f"⚠️ {lambda_name} SNS 통합 실패: {e}")
                else:
                    logger.info(f"🔍 DRY RUN: {lambda_name}이 {topic_name}과 통합될 예정")
                    
        except Exception as e:
            logger.error(f"❌ Lambda 통합 실패: {e}")
    
    def create_test_message(self, topics: Dict[str, str]):
        """테스트 메시지 발송"""
        try:
            if not self.dry_run:
                test_topic = topics.get('makenaide-system-alerts')
                if test_topic:
                    self.sns_client.publish(
                        TopicArn=test_topic,
                        Subject='🎉 Makenaide SNS 알림 시스템 활성화',
                        Message=f"""
Makenaide SNS 이메일 알림 시스템이 성공적으로 활성화되었습니다!

📊 설정 정보:
- 활성화 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S KST')}
- 구독 토픽: 3개 (일일리포트, 거래알림, 긴급알림)
- 예상 메시지: ~30개/월
- 비용: 무료 (AWS 무료 티어)

🚀 이제 다음과 같은 알림을 받으실 수 있습니다:
✅ 일일 거래 성과 리포트
✅ 거래 신호 및 체결 알림  
✅ 시스템 상태 및 긴급 알림

Best regards,
Makenaide Auto-Trading System
                        """
                    )
                    logger.info("✅ 테스트 메시지 발송 완료")
            else:
                logger.info("🔍 DRY RUN: 테스트 메시지가 발송될 예정")
                
        except Exception as e:
            logger.error(f"❌ 테스트 메시지 발송 실패: {e}")
    
    def setup_sns_system(self, email: str):
        """전체 SNS 시스템 설정"""
        # 기존 토픽 확인
        existing_topics = self.check_existing_topics()
        
        # 필요한 토픽 생성
        topics = self.create_sns_topics()
        
        # 이메일 구독 설정
        subscriptions = self.setup_email_subscription(email, topics)
        
        # Lambda 함수 통합
        self.integrate_with_lambda_functions(topics)
        
        # 테스트 메시지 발송
        self.create_test_message(topics)
        
        # 결과 요약
        logger.info("\n" + "="*50)
        logger.info("📧 SNS 이메일 알림 시스템 설정 완료")
        logger.info(f"- 생성된 토픽: {len(topics)}개")
        logger.info(f"- 이메일 구독: {len(subscriptions)}개")
        logger.info(f"- 예상 월간 비용: $0.00 (무료)")
        logger.info("="*50)
        
        return {
            'topics_created': len(topics),
            'email_subscriptions': len(subscriptions),
            'monthly_cost': 0.00
        }

if __name__ == "__main__":
    import sys
    
    # DRY RUN 모드 체크
    dry_run = '--execute' not in sys.argv
    
    if dry_run:
        print("\n⚠️ DRY RUN 모드로 실행됩니다.")
        print("실제 설정을 원하시면 '--execute' 옵션과 이메일 주소를 추가하세요.")
        print("예: python setup_sns_email_only.py --execute your@email.com\n")
    else:
        if len(sys.argv) < 3:
            print("❌ 이메일 주소를 입력해주세요.")
            print("예: python setup_sns_email_only.py --execute your@email.com")
            sys.exit(1)
        
        email = sys.argv[2]
        print(f"\n📧 이메일 알림 설정을 시작합니다: {email}")
    
    # 기본 이메일 (DRY RUN용)
    test_email = sys.argv[2] if len(sys.argv) > 2 else "test@example.com"
    
    setup = MakenaideSNSEmailSetup(dry_run=dry_run)
    result = setup.setup_sns_system(test_email)
    
    if dry_run:
        print(f"\n💡 실제 실행: python {sys.argv[0]} --execute your@email.com")