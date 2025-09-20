#!/usr/bin/env python3
"""
Makenaide EventBridge 자동 스케줄링 설정
EventBridge Schedule → Lambda → EC2 Start → Makenaide 실행

🎯 메인 거래 파이프라인 실행시간 (KST → UTC 변환):
- 02:00 KST = 17:00 UTC (전날) - 아시아 심야 + 유럽 저녁
- 09:00 KST = 00:00 UTC - 한국/일본 장 시작 + 미국 동부 밤
- 15:00 KST = 06:00 UTC - 아시아 오후 + 유럽 오전 시작
- 18:00 KST = 09:00 UTC - 한국 퇴근시간 + 유럽 점심 활성화
- 21:00 KST = 12:00 UTC - 아시아 저녁 골든타임 + 유럽 오후
- 23:00 KST = 14:00 UTC - 아시아 밤 + 미국 동부 오전 시작
"""

import boto3
import json
import logging
from typing import Dict, List

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트
events = boto3.client('events')

# 설정
LAMBDA_FUNCTION_ARN = "arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-ec2-starter"
REGION = 'ap-northeast-2'

# 메인 거래 파이프라인 스케줄 설정
TRADING_SCHEDULES = [
    {
        'name': 'makenaide-trading-02-00-kst',
        'description': 'Makenaide 거래 파이프라인 (02:00 KST - 아시아 심야 + 유럽 저녁)',
        'schedule_expression': 'cron(0 17 * * ? *)',  # 17:00 UTC = 02:00 KST (next day)
        'timezone': 'Asia/Seoul'
    },
    {
        'name': 'makenaide-trading-09-00-kst',
        'description': 'Makenaide 거래 파이프라인 (09:00 KST - 한국/일본 장 시작)',
        'schedule_expression': 'cron(0 0 * * ? *)',   # 00:00 UTC = 09:00 KST
        'timezone': 'Asia/Seoul'
    },
    {
        'name': 'makenaide-trading-15-00-kst',
        'description': 'Makenaide 거래 파이프라인 (15:00 KST - 아시아 오후 + 유럽 오전)',
        'schedule_expression': 'cron(0 6 * * ? *)',   # 06:00 UTC = 15:00 KST
        'timezone': 'Asia/Seoul'
    },
    {
        'name': 'makenaide-trading-18-00-kst',
        'description': 'Makenaide 거래 파이프라인 (18:00 KST - 한국 퇴근시간 + 유럽 점심)',
        'schedule_expression': 'cron(0 9 * * ? *)',   # 09:00 UTC = 18:00 KST
        'timezone': 'Asia/Seoul'
    },
    {
        'name': 'makenaide-trading-21-00-kst',
        'description': 'Makenaide 거래 파이프라인 (21:00 KST - 아시아 골든타임 + 유럽 오후)',
        'schedule_expression': 'cron(0 12 * * ? *)',  # 12:00 UTC = 21:00 KST
        'timezone': 'Asia/Seoul'
    },
    {
        'name': 'makenaide-trading-23-00-kst',
        'description': 'Makenaide 거래 파이프라인 (23:00 KST - 아시아 밤 + 미국 동부 오전)',
        'schedule_expression': 'cron(0 14 * * ? *)',  # 14:00 UTC = 23:00 KST
        'timezone': 'Asia/Seoul'
    }
]

def create_eventbridge_rule(rule_config: Dict) -> bool:
    """EventBridge 스케줄 규칙 생성"""
    try:
        rule_name = rule_config['name']

        # EventBridge 규칙 생성
        response = events.put_rule(
            Name=rule_name,
            ScheduleExpression=rule_config['schedule_expression'],
            Description=rule_config['description'],
            State='ENABLED'
        )

        rule_arn = response['RuleArn']
        logger.info(f"✅ EventBridge 규칙 생성 완료: {rule_name}")
        logger.info(f"   ARN: {rule_arn}")
        logger.info(f"   스케줄: {rule_config['schedule_expression']}")

        # Lambda 함수를 대상으로 추가
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': LAMBDA_FUNCTION_ARN,
                    'Input': json.dumps({
                        'pipeline_type': 'main_trading',
                        'schedule_name': rule_name,
                        'kst_time': rule_config['description'].split('(')[1].split(' ')[0],
                        'market_timing': rule_config['description'].split(' - ')[1].rstrip(')')
                    })
                }
            ]
        )

        logger.info(f"✅ Lambda 대상 연결 완료: {rule_name} → {LAMBDA_FUNCTION_ARN}")
        return True

    except Exception as e:
        logger.error(f"❌ EventBridge 규칙 생성 실패 ({rule_name}): {e}")
        return False

def add_lambda_permission(rule_name: str) -> bool:
    """Lambda 함수에 EventBridge 호출 권한 추가"""
    try:
        lambda_client = boto3.client('lambda')

        # Lambda 함수명 추출
        function_name = LAMBDA_FUNCTION_ARN.split(':')[-1]

        # EventBridge에서 Lambda 호출 권한 추가
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=f'AllowExecutionFromEventBridge-{rule_name}',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=f'arn:aws:events:{REGION}:901361833359:rule/{rule_name}'
        )

        logger.info(f"✅ Lambda 권한 추가 완료: {rule_name}")
        return True

    except Exception as e:
        if 'ResourceConflictException' in str(e):
            logger.info(f"ℹ️ Lambda 권한 이미 존재: {rule_name}")
            return True
        else:
            logger.error(f"❌ Lambda 권한 추가 실패 ({rule_name}): {e}")
            return False

def get_existing_rules() -> List[str]:
    """기존 Makenaide EventBridge 규칙 조회"""
    try:
        response = events.list_rules()
        existing_rules = []

        for rule in response['Rules']:
            rule_name = rule['Name']
            if 'makenaide' in rule_name.lower():
                existing_rules.append(rule_name)

        return existing_rules

    except Exception as e:
        logger.error(f"❌ 기존 규칙 조회 실패: {e}")
        return []

def main():
    """메인 실행 함수"""
    logger.info("🚀 Makenaide EventBridge 자동 스케줄링 설정 시작")
    logger.info("=" * 70)

    # 기존 규칙 확인
    existing_rules = get_existing_rules()
    logger.info(f"📋 기존 Makenaide 규칙: {existing_rules}")

    # 각 거래 시간대별 스케줄 생성
    success_count = 0
    total_rules = len(TRADING_SCHEDULES)

    for rule_config in TRADING_SCHEDULES:
        rule_name = rule_config['name']

        logger.info(f"\n📡 {rule_name} 설정 중...")

        # 기존 규칙이 있으면 삭제 후 재생성
        if rule_name in existing_rules:
            try:
                # 기존 대상 제거
                events.remove_targets(Rule=rule_name, Ids=['1'])
                # 기존 규칙 삭제
                events.delete_rule(Name=rule_name)
                logger.info(f"🗑️ 기존 규칙 삭제: {rule_name}")
            except Exception as e:
                logger.warning(f"⚠️ 기존 규칙 삭제 실패: {e}")

        # 새 규칙 생성
        if create_eventbridge_rule(rule_config):
            # Lambda 권한 추가
            if add_lambda_permission(rule_name):
                success_count += 1
                logger.info(f"🎯 {rule_name} 설정 완료!")
            else:
                logger.warning(f"⚠️ {rule_name} 권한 설정 실패")
        else:
            logger.error(f"❌ {rule_name} 규칙 생성 실패")

    # 결과 요약
    logger.info("\n" + "=" * 70)
    logger.info("🎉 EventBridge 자동 스케줄링 설정 완료!")
    logger.info(f"✅ 성공: {success_count}/{total_rules}개 규칙")

    if success_count == total_rules:
        logger.info("\n📅 설정된 거래 파이프라인 스케줄 (KST):")
        for rule_config in TRADING_SCHEDULES:
            kst_time = rule_config['description'].split('(')[1].split(' ')[0]
            market_timing = rule_config['description'].split(' - ')[1].rstrip(')')
            logger.info(f"   🕒 {kst_time} - {market_timing}")

        logger.info(f"\n🎯 다음 단계:")
        logger.info("1. Lambda 함수 'makenaide-ec2-starter' 생성 필요")
        logger.info("2. EC2 자동 시작/실행/종료 로직 구현")
        logger.info("3. 첫 번째 스케줄 테스트 실행")

    else:
        logger.warning(f"\n⚠️ 일부 규칙 설정 실패. 로그를 확인하세요.")

    return success_count == total_rules

if __name__ == "__main__":
    try:
        success = main()
        print(f"\n🎯 설정 {'성공' if success else '실패'}")

    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")

    except Exception as e:
        print(f"\n❌ 설정 실패: {e}")
        import traceback
        traceback.print_exc()