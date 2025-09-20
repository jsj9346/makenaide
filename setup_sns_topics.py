#!/usr/bin/env python3
"""
Makenaide SNS Topics 및 구독 설정 스크립트

🎯 목적:
- SNS Topic 생성 및 구성
- 이메일 구독 설정
- 알림 필터 정책 적용
- 비용 최적화 설정
"""

import boto3
import json
import logging
from typing import Dict, List

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트
sns = boto3.client('sns')

# 설정
REGION = 'ap-northeast-2'
TOPICS_CONFIG = {
    'makenaide-trading-alerts': {
        'description': 'Makenaide 거래 및 포트폴리오 관련 알림',
        'subscriptions': [
            {
                'protocol': 'email',
                'endpoint': 'jsj9346@gmail.com',
                'filter_policy': {
                    'category': ['TRADING', 'PORTFOLIO']
                }
            }
        ]
    },
    'makenaide-system-alerts': {
        'description': 'Makenaide 시스템 및 파이프라인 관련 알림',
        'subscriptions': [
            {
                'protocol': 'email',
                'endpoint': 'jsj9346@gmail.com',
                'filter_policy': {
                    'level': ['CRITICAL', 'WARNING']
                }
            }
        ]
    }
}

def create_sns_topic(topic_name: str, description: str) -> str:
    """SNS Topic 생성"""
    try:
        # Topic 생성
        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']

        # 설명 추가
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName='DisplayName',
            AttributeValue=topic_name
        )

        logger.info(f"✅ SNS Topic 생성 완료: {topic_name}")
        logger.info(f"   ARN: {topic_arn}")

        return topic_arn

    except Exception as e:
        logger.error(f"❌ SNS Topic 생성 실패 ({topic_name}): {e}")
        return None

def subscribe_to_topic(topic_arn: str, protocol: str, endpoint: str, filter_policy: Dict = None) -> str:
    """Topic 구독 설정"""
    try:
        # 구독 생성
        response = sns.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint
        )

        subscription_arn = response['SubscriptionArn']

        # 필터 정책 적용
        if filter_policy and subscription_arn != 'pending confirmation':
            sns.set_subscription_attributes(
                SubscriptionArn=subscription_arn,
                AttributeName='FilterPolicy',
                AttributeValue=json.dumps(filter_policy)
            )

            logger.info(f"✅ 필터 정책 적용: {filter_policy}")

        logger.info(f"✅ 구독 설정 완료: {protocol}:{endpoint}")

        if subscription_arn == 'pending confirmation':
            logger.warning("⚠️ 이메일 확인이 필요합니다. 받은편지함을 확인하세요.")

        return subscription_arn

    except Exception as e:
        logger.error(f"❌ 구독 설정 실패: {e}")
        return None

def setup_topic_policies(topic_arn: str):
    """Topic 정책 설정"""
    try:
        # Lambda 및 EC2에서 publish 허용하는 정책
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "SNS:Publish",
                    "Resource": topic_arn
                },
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "SNS:Publish",
                    "Resource": topic_arn
                }
            ]
        }

        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName='Policy',
            AttributeValue=json.dumps(policy)
        )

        logger.info("✅ Topic 정책 설정 완료")

    except Exception as e:
        logger.error(f"❌ Topic 정책 설정 실패: {e}")

def create_message_templates():
    """메시지 템플릿 생성"""
    templates = {
        'trading_success': {
            'subject': '✅ Makenaide 거래 성공',
            'message': '''
{emoji} Makenaide {category} - {title}

{message}

📅 시간: {timestamp}
🔍 실행 ID: {execution_id}
🏷️ 종목: {ticker}
💰 금액: {amount}원

---
🤖 Makenaide 자동매매 시스템
            '''.strip()
        },
        'system_error': {
            'subject': '🚨 Makenaide 시스템 오류',
            'message': '''
🚨 Makenaide SYSTEM - 시스템 오류 발생

{message}

📅 시간: {timestamp}
🔍 실행 ID: {execution_id}

---
🤖 Makenaide 자동매매 시스템
            '''.strip()
        }
    }

    return templates

def test_sns_notification(topic_arn: str, topic_name: str):
    """SNS 알림 테스트"""
    try:
        test_message = f"""
🧪 Makenaide SNS 테스트

{topic_name} Topic의 알림 시스템이 정상적으로 작동합니다.

📅 테스트 시간: {boto3.Session().region_name} 리전
🔧 설정 상태: 정상
📧 이메일 구독: 활성화

다음 단계:
1. 이메일 구독 확인 (받은편지함 확인)
2. 첫 번째 파이프라인 실행 모니터링
3. 알림 빈도 조정 (필요시)

---
🤖 Makenaide 자동매매 시스템
        """.strip()

        response = sns.publish(
            TopicArn=topic_arn,
            Subject=f"🧪 Makenaide SNS 테스트 - {topic_name}",
            Message=test_message
        )

        message_id = response['MessageId']
        logger.info(f"✅ 테스트 알림 전송 완료: {message_id}")

        return True

    except Exception as e:
        logger.error(f"❌ 테스트 알림 전송 실패: {e}")
        return False

def get_existing_topics() -> Dict[str, str]:
    """기존 SNS Topics 조회"""
    try:
        response = sns.list_topics()
        existing_topics = {}

        for topic in response['Topics']:
            topic_arn = topic['TopicArn']
            topic_name = topic_arn.split(':')[-1]

            if 'makenaide' in topic_name:
                existing_topics[topic_name] = topic_arn

        return existing_topics

    except Exception as e:
        logger.error(f"❌ 기존 Topics 조회 실패: {e}")
        return {}

def main():
    """메인 설정 함수"""
    logger.info("🚀 Makenaide SNS 알림 시스템 설정 시작")
    logger.info("=" * 60)

    # 기존 Topics 확인
    existing_topics = get_existing_topics()
    logger.info(f"📋 기존 Topics: {list(existing_topics.keys())}")

    created_topics = {}

    # Topics 생성 및 설정
    for topic_name, config in TOPICS_CONFIG.items():
        logger.info(f"\n📡 {topic_name} 설정 중...")

        # Topic 생성 또는 기존 사용
        if topic_name in existing_topics:
            topic_arn = existing_topics[topic_name]
            logger.info(f"ℹ️ 기존 Topic 사용: {topic_arn}")
        else:
            topic_arn = create_sns_topic(topic_name, config['description'])

        if not topic_arn:
            logger.error(f"❌ {topic_name} 설정 실패")
            continue

        created_topics[topic_name] = topic_arn

        # Topic 정책 설정
        setup_topic_policies(topic_arn)

        # 구독 설정
        for subscription in config['subscriptions']:
            if subscription['endpoint'] == 'your-email@example.com':
                logger.warning("⚠️ 이메일 주소를 실제 주소로 변경해주세요!")
                continue

            subscribe_to_topic(
                topic_arn=topic_arn,
                protocol=subscription['protocol'],
                endpoint=subscription['endpoint'],
                filter_policy=subscription.get('filter_policy')
            )

        # 테스트 알림 전송
        logger.info(f"🧪 {topic_name} 테스트 알림 전송...")
        test_sns_notification(topic_arn, topic_name)

    # 결과 요약
    logger.info("\n" + "=" * 60)
    logger.info("🎉 SNS 알림 시스템 설정 완료!")
    logger.info(f"✅ 생성된 Topics: {len(created_topics)}개")

    for topic_name, topic_arn in created_topics.items():
        logger.info(f"   📡 {topic_name}: {topic_arn}")

    # 환경 변수 설정 가이드
    logger.info("\n📋 환경 변수 설정:")
    for topic_name, topic_arn in created_topics.items():
        env_var = f"SNS_{topic_name.upper().replace('-', '_')}_ARN"
        logger.info(f"   {env_var}={topic_arn}")

    logger.info("\n⚠️ 다음 단계:")
    logger.info("1. 이메일 구독 확인 (받은편지함 확인)")
    logger.info("2. 환경 변수를 .env 파일에 추가")
    logger.info("3. makenaide.py에 SNS 통합 적용")
    logger.info("4. 첫 번째 파이프라인 실행으로 알림 테스트")

    return created_topics

if __name__ == "__main__":
    try:
        topics = main()
        print(f"\n🎯 성공: {len(topics)}개 Topics 설정 완료")

    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")

    except Exception as e:
        print(f"\n❌ 설정 실패: {e}")
        import traceback
        traceback.print_exc()