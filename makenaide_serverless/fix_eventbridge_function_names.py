#!/usr/bin/env python3
"""
🔧 EventBridge 함수명 수정
- 실제 생성된 Lambda 함수명으로 스케줄 업데이트
"""

import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_eventbridge_with_correct_function_names():
    """실제 함수명으로 EventBridge 타겟 업데이트"""
    
    events_client = boto3.client('events')
    
    # 실제 함수명 매핑
    function_mapping = {
        # 기존 스케줄에서 사용한 이름 → 실제 함수명
        'makenaide-ticker-scanner-phase0': 'makenaide-scanner',
        'makenaide-selective-data-collection-phase1': 'makenaide-data-collector', 
        'makenaide-comprehensive-filtering-phase2': 'makenaide-comprehensive-filter-phase2',
        'makenaide-gpt-analysis-phase3': 'makenaide-gpt-analysis-phase3',
        'makenaide-4h-analysis-phase4': 'makenaide-4h-analysis-phase4',
        'makenaide-condition-check-phase5': 'makenaide-condition-check-phase5',
        'makenaide-trade-execution-phase6': 'makenaide-trade-execution-phase6'
    }
    
    # Phase 전환 규칙 업데이트
    phase_transitions = [
        {
            'rule_name': 'makenaide-phase0-to-phase1',
            'target_function': 'makenaide-data-collector'
        },
        {
            'rule_name': 'makenaide-phase1-to-phase2',
            'target_function': 'makenaide-comprehensive-filter-phase2'
        },
        {
            'rule_name': 'makenaide-phase2-to-phase3',
            'target_function': 'makenaide-gpt-analysis-phase3'
        },
        {
            'rule_name': 'makenaide-phase3-to-phase4',
            'target_function': 'makenaide-4h-analysis-phase4'
        },
        {
            'rule_name': 'makenaide-phase4-to-phase5',
            'target_function': 'makenaide-condition-check-phase5'
        },
        {
            'rule_name': 'makenaide-phase5-to-phase6',
            'target_function': 'makenaide-trade-execution-phase6'
        }
    ]
    
    # 거래 스케줄 규칙 업데이트
    trading_schedules = [
        'makenaide-trading-02-asian-night-european-prime',
        'makenaide-trading-09-asian-morning-prime', 
        'makenaide-trading-15-asian-afternoon',
        'makenaide-trading-18-asian-evening-european-morning',
        'makenaide-trading-21-asian-prime-european-afternoon',
        'makenaide-trading-23-asian-night-us-morning'
    ]
    
    try:
        logger.info("EventBridge 규칙의 Lambda 함수명 수정 중...")
        
        # 1. Phase 전환 규칙 타겟 업데이트
        for transition in phase_transitions:
            try:
                # 기존 타겟 제거
                events_client.remove_targets(
                    Rule=transition['rule_name'],
                    Ids=['1']
                )
                
                # 새로운 타겟 추가
                events_client.put_targets(
                    Rule=transition['rule_name'],
                    Targets=[
                        {
                            'Id': '1',
                            'Arn': f'arn:aws:lambda:ap-northeast-2:901361833359:function:{transition["target_function"]}'
                        }
                    ]
                )
                
                logger.info(f"✅ Phase 전환 타겟 업데이트: {transition['rule_name']} → {transition['target_function']}")
                
            except Exception as e:
                logger.error(f"Phase 전환 규칙 {transition['rule_name']} 업데이트 실패: {e}")
        
        # 2. 거래 스케줄 타겟 업데이트 (Phase 0 시작점)
        for schedule_name in trading_schedules:
            try:
                # 기존 타겟 제거
                events_client.remove_targets(
                    Rule=schedule_name,
                    Ids=['1']
                )
                
                # 정확한 Phase 0 함수로 타겟 추가
                events_client.put_targets(
                    Rule=schedule_name,
                    Targets=[
                        {
                            'Id': '1', 
                            'Arn': 'arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-scanner',
                            'Input': json.dumps({
                                'source': 'eventbridge-schedule',
                                'schedule_name': schedule_name,
                                'trading_session': schedule_name.split('-')[2],
                                'global_optimization': True
                            })
                        }
                    ]
                )
                
                logger.info(f"✅ 거래 스케줄 타겟 업데이트: {schedule_name} → makenaide-scanner")
                
            except Exception as e:
                logger.error(f"거래 스케줄 {schedule_name} 업데이트 실패: {e}")
        
        # 3. Lambda 권한 추가
        logger.info("Lambda 함수 EventBridge 권한 추가 중...")
        
        lambda_client = boto3.client('lambda')
        functions_to_grant = [
            'makenaide-scanner',
            'makenaide-data-collector',
            'makenaide-comprehensive-filter-phase2',
            'makenaide-gpt-analysis-phase3',
            'makenaide-4h-analysis-phase4', 
            'makenaide-condition-check-phase5',
            'makenaide-trade-execution-phase6',
            'makenaide-market-sentiment-check'
        ]
        
        for function_name in functions_to_grant:
            try:
                lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=f'eventbridge-invoke-{function_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com'
                )
                logger.info(f"✅ EventBridge 권한 추가: {function_name}")
                
            except lambda_client.exceptions.ResourceConflictException:
                logger.info(f"권한 이미 존재: {function_name}")
            except Exception as e:
                logger.warning(f"권한 추가 실패 {function_name}: {e}")
        
        logger.info("✅ EventBridge 함수명 수정 완료!")
        
        return True
        
    except Exception as e:
        logger.error(f"EventBridge 함수명 수정 실패: {e}")
        return False

def verify_updated_schedules():
    """수정된 스케줄 검증"""
    
    events_client = boto3.client('events')
    
    try:
        logger.info("수정된 EventBridge 스케줄 검증 중...")
        
        # 거래 스케줄 확인
        trading_rules = [
            'makenaide-trading-02-asian-night-european-prime',
            'makenaide-trading-09-asian-morning-prime',
            'makenaide-trading-15-asian-afternoon',
            'makenaide-trading-18-asian-evening-european-morning', 
            'makenaide-trading-21-asian-prime-european-afternoon',
            'makenaide-trading-23-asian-night-us-morning'
        ]
        
        logger.info("🕐 거래 스케줄 타겟 확인:")
        for rule_name in trading_rules:
            targets = events_client.list_targets_by_rule(Rule=rule_name)
            for target in targets.get('Targets', []):
                function_name = target['Arn'].split(':')[-1]
                logger.info(f"   {rule_name} → {function_name}")
        
        # Phase 전환 규칙 확인
        phase_rules = [
            'makenaide-phase0-to-phase1',
            'makenaide-phase1-to-phase2', 
            'makenaide-phase2-to-phase3',
            'makenaide-phase3-to-phase4',
            'makenaide-phase4-to-phase5',
            'makenaide-phase5-to-phase6'
        ]
        
        logger.info("\n🔄 Phase 전환 규칙 타겟 확인:")
        for rule_name in phase_rules:
            targets = events_client.list_targets_by_rule(Rule=rule_name)
            for target in targets.get('Targets', []):
                function_name = target['Arn'].split(':')[-1]
                logger.info(f"   {rule_name} → {function_name}")
        
        logger.info("✅ EventBridge 스케줄 검증 완료!")
        
    except Exception as e:
        logger.error(f"스케줄 검증 실패: {e}")

def main():
    """메인 실행"""
    
    logger.info("🔧 EventBridge Lambda 함수명 수정 시작")
    logger.info("=" * 60)
    
    if update_eventbridge_with_correct_function_names():
        verify_updated_schedules()
        
        print(f"""

✅ EventBridge Lambda 함수명 수정 완료!

🔧 수정된 함수 매핑:
   Phase 0: makenaide-ticker-scanner-phase0 → makenaide-scanner
   Phase 1: makenaide-selective-data-collection-phase1 → makenaide-data-collector
   Phase 2: makenaide-comprehensive-filtering-phase2 → makenaide-comprehensive-filter-phase2
   Phase 3-6: 기존 이름 유지 (정확함)

🔄 업데이트된 스케줄:
   • 6개 거래 스케줄 → makenaide-scanner 타겟
   • 6개 Phase 전환 규칙 → 정확한 Phase 함수 타겟
   • 모든 Lambda 함수 EventBridge 권한 추가

🎯 다음 단계:
   • 스케줄 테스트 실행 가능
   • 실시간 파이프라인 동작 확인
        """)
        
        return True
    else:
        logger.error("❌ 함수명 수정 실패")
        return False

if __name__ == '__main__':
    main()