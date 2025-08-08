#!/usr/bin/env python3
"""
🧪 시간대 전략 통합 검증 스크립트
- 모든 업데이트된 Lambda 함수들의 시간대 전략 적용 상태 확인
- S3에 저장되는 전략 설정 파일들 검증
- 실시간 시간대 분석 데이터 확인
- EventBridge 스케줄과의 연동 테스트
"""

import boto3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimezoneStrategyValidator:
    """시간대 전략 통합 검증 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.cloudwatch_client = boto3.client('cloudwatch')
        
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.region = 'ap-northeast-2'
        
        # 검증할 Lambda 함수 목록
        self.target_functions = [
            'makenaide-market-sentiment-check',       # 시간대 분석 생성
            'makenaide-scanner',                      # Phase 0
            'makenaide-data-collector',               # Phase 1
            'makenaide-comprehensive-filter-phase2',  # Phase 2 
            'makenaide-gpt-analysis-phase3',          # Phase 3
            'makenaide-4h-analysis-phase4',           # Phase 4
            'makenaide-condition-check-phase5',       # Phase 5
            'makenaide-trade-execution-phase6'        # Phase 6
        ]
    
    def check_s3_timezone_analysis_data(self) -> Dict:
        """S3에 저장된 시간대 분석 데이터 확인"""
        logger.info("📊 S3 시간대 분석 데이터 확인 중...")
        
        results = {
            'current_sentiment': False,
            'sentiment_history': False,
            'strategy_configs': [],
            'latest_analysis': None
        }
        
        try:
            # 1. 현재 시장 상황 파일 확인
            try:
                response = self.s3_client.get_object(
                    Bucket=self.s3_bucket,
                    Key='market_sentiment/current_sentiment.json'
                )
                current_data = json.loads(response['Body'].read().decode('utf-8'))
                
                # 시간대 분석 데이터 확인
                if 'timezone_analysis' in current_data:
                    results['current_sentiment'] = True
                    results['latest_analysis'] = current_data['timezone_analysis']
                    
                    logger.info("✅ 현재 시장 상황 파일 확인 완료")
                    logger.info(f"   글로벌 활성도: {current_data['timezone_analysis']['global_activity_score']:.1f}%")
                    logger.info(f"   주도 지역: {current_data['timezone_analysis']['dominant_regions'][0]['timezone']}")
                    logger.info(f"   거래 스타일: {current_data['timezone_analysis']['trading_style']['primary_style']}")
                else:
                    logger.warning("⚠️ 시간대 분석 데이터가 누락됨")
                    
            except Exception as e:
                logger.error(f"❌ 현재 시장 상황 파일 확인 실패: {e}")
            
            # 2. 시간대 분석 히스토리 확인
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix='market_sentiment/history/'
                )
                
                if response.get('Contents'):
                    results['sentiment_history'] = True
                    history_count = len(response['Contents'])
                    logger.info(f"✅ 시간대 분석 히스토리: {history_count}개 파일")
                else:
                    logger.warning("⚠️ 시간대 분석 히스토리 없음")
                    
            except Exception as e:
                logger.warning(f"시간대 분석 히스토리 확인 실패: {e}")
            
            # 3. 전략 설정 파일들 확인
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix='trading_strategy/'
                )
                
                strategy_files = []
                if response.get('Contents'):
                    for obj in response['Contents']:
                        key = obj['Key']
                        if 'current_' in key and key.endswith('.json'):
                            strategy_files.append(key)
                
                results['strategy_configs'] = strategy_files
                logger.info(f"📋 전략 설정 파일: {len(strategy_files)}개")
                for config_file in strategy_files:
                    logger.info(f"   - {config_file}")
                    
            except Exception as e:
                logger.warning(f"전략 설정 파일 확인 실패: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"S3 데이터 확인 실패: {e}")
            return results
    
    def test_lambda_timezone_integration(self, function_name: str) -> Dict:
        """개별 Lambda 함수의 시간대 전략 통합 테스트"""
        logger.info(f"🧪 {function_name} 시간대 전략 테스트 중...")
        
        result = {
            'function_name': function_name,
            'timezone_strategy_loaded': False,
            'strategy_applied': False,
            'execution_time': 0,
            'error': None,
            'logs_sample': []
        }
        
        try:
            # 테스트 이벤트
            test_event = {
                'test': True,
                'timezone_strategy_test': True,
                'source': 'validation_test',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            start_time = time.time()
            
            # Lambda 함수 호출
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            execution_time = (time.time() - start_time) * 1000  # ms
            result['execution_time'] = execution_time
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read())
                
                # 응답 분석
                body_str = payload.get('body', '{}')
                if isinstance(body_str, str):
                    try:
                        body = json.loads(body_str)
                        response_text = json.dumps(body, ensure_ascii=False)
                        
                        # 시간대 전략 관련 키워드 확인
                        strategy_keywords = ['timezone', 'strategy', 'global_activity', 'position_size', 'dominant_region']
                        
                        for keyword in strategy_keywords:
                            if keyword in response_text.lower():
                                result['timezone_strategy_loaded'] = True
                                break
                        
                        if 'strategy_enhanced' in response_text or 'strategy_applied' in response_text:
                            result['strategy_applied'] = True
                            
                    except json.JSONDecodeError:
                        pass
                
                # CloudWatch 로그 확인 (최근 5분)
                try:
                    end_time = datetime.utcnow()
                    start_time = end_time - timedelta(minutes=5)
                    
                    log_response = self.cloudwatch_client.filter_log_events(
                        logGroupName=f'/aws/lambda/{function_name}',
                        startTime=int(start_time.timestamp() * 1000),
                        endTime=int(end_time.timestamp() * 1000),
                        filterPattern='시간대 전략 OR timezone OR strategy',
                        limit=5
                    )
                    
                    if log_response.get('events'):
                        result['logs_sample'] = [
                            event['message'].strip() 
                            for event in log_response['events'][-3:]  # 최근 3개
                        ]
                        
                        # 로그에서 전략 적용 여부 확인
                        for log_msg in result['logs_sample']:
                            if '시간대 전략' in log_msg or 'timezone strategy' in log_msg.lower():
                                result['timezone_strategy_loaded'] = True
                            if '포지션 크기' in log_msg or 'position_size' in log_msg.lower():
                                result['strategy_applied'] = True
                
                except Exception as log_error:
                    logger.warning(f"CloudWatch 로그 확인 실패 ({function_name}): {log_error}")
                
                # 결과 로깅
                if result['timezone_strategy_loaded'] and result['strategy_applied']:
                    logger.info(f"✅ {function_name}: 시간대 전략 완전 적용 ({execution_time:.0f}ms)")
                elif result['timezone_strategy_loaded']:
                    logger.info(f"🟡 {function_name}: 시간대 전략 로드됨, 적용 부분적 ({execution_time:.0f}ms)")
                else:
                    logger.warning(f"🟠 {function_name}: 시간대 전략 미확인 ({execution_time:.0f}ms)")
                
            else:
                result['error'] = f"HTTP {response['StatusCode']}"
                logger.error(f"❌ {function_name}: 실행 실패 - {result['error']}")
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ {function_name}: 테스트 실패 - {e}")
        
        return result
    
    def test_eventbridge_integration(self) -> Dict:
        """EventBridge 스케줄과 시간대 전략 연동 테스트"""
        logger.info("⏰ EventBridge 연동 테스트 중...")
        
        result = {
            'rules_checked': 0,
            'active_rules': 0,
            'market_sentiment_rule': False,
            'phase_rules': [],
            'next_execution_times': []
        }
        
        try:
            # EventBridge 규칙 확인
            response = self.events_client.list_rules(NamePrefix='makenaide-')
            
            for rule in response.get('Rules', []):
                rule_name = rule['Name']
                rule_state = rule['State']
                
                result['rules_checked'] += 1
                
                if rule_state == 'ENABLED':
                    result['active_rules'] += 1
                    
                    if 'market-sentiment' in rule_name:
                        result['market_sentiment_rule'] = True
                    elif 'phase' in rule_name or any(phase in rule_name for phase in ['scanner', 'collector', 'filter', 'analysis', 'condition', 'execution']):
                        result['phase_rules'].append(rule_name)
                    
                    # 다음 실행 시간 계산 (cron 표현식 기반)
                    schedule_expr = rule.get('ScheduleExpression', '')
                    if schedule_expr:
                        # 간단한 다음 실행 시간 추정
                        if 'cron(' in schedule_expr:
                            result['next_execution_times'].append({
                                'rule': rule_name,
                                'schedule': schedule_expr,
                                'estimated_next': '다음 정시 실행'
                            })
            
            logger.info(f"✅ EventBridge 규칙 확인: {result['active_rules']}/{result['rules_checked']} 활성")
            logger.info(f"   시장 상황 확인 규칙: {'✅' if result['market_sentiment_rule'] else '❌'}")
            logger.info(f"   Phase 규칙: {len(result['phase_rules'])}개")
            
        except Exception as e:
            logger.error(f"EventBridge 연동 테스트 실패: {e}")
        
        return result
    
    def generate_comprehensive_validation_report(self) -> Dict:
        """종합 검증 보고서 생성"""
        logger.info("📋 시간대 전략 통합 종합 검증 시작")
        logger.info("=" * 80)
        
        report = {
            'validation_timestamp': datetime.utcnow().isoformat(),
            's3_data_check': {},
            'lambda_tests': [],
            'eventbridge_check': {},
            'overall_status': 'UNKNOWN',
            'recommendations': []
        }
        
        try:
            # 1. S3 데이터 확인
            logger.info("\n📊 1. S3 시간대 분석 데이터 검증")
            report['s3_data_check'] = self.check_s3_timezone_analysis_data()
            
            # 2. Lambda 함수들 테스트
            logger.info("\n🧪 2. Lambda 함수 시간대 전략 통합 테스트")
            for function_name in self.target_functions:
                test_result = self.test_lambda_timezone_integration(function_name)
                report['lambda_tests'].append(test_result)
                time.sleep(1)  # API 제한 방지
            
            # 3. EventBridge 연동 확인
            logger.info("\n⏰ 3. EventBridge 스케줄 연동 확인")
            report['eventbridge_check'] = self.test_eventbridge_integration()
            
            # 4. 종합 평가
            logger.info("\n📈 4. 종합 평가")
            
            # 성공률 계산
            s3_success = report['s3_data_check']['current_sentiment']
            lambda_success_count = sum(1 for test in report['lambda_tests'] if test['timezone_strategy_loaded'])
            lambda_total = len(report['lambda_tests'])
            eventbridge_success = report['eventbridge_check']['market_sentiment_rule']
            
            success_rate = (
                (1 if s3_success else 0) +
                (lambda_success_count / lambda_total) +
                (1 if eventbridge_success else 0)
            ) / 3
            
            if success_rate >= 0.8:
                report['overall_status'] = 'SUCCESS'
            elif success_rate >= 0.6:
                report['overall_status'] = 'PARTIAL'
            else:
                report['overall_status'] = 'FAILED'
            
            # 권장사항 생성
            recommendations = []
            
            if not s3_success:
                recommendations.append("시장 상황 분석 함수를 실행하여 시간대 분석 데이터 생성 필요")
            
            failed_lambdas = [test['function_name'] for test in report['lambda_tests'] if not test['timezone_strategy_loaded']]
            if failed_lambdas:
                recommendations.append(f"시간대 전략 미적용 함수 재배포 필요: {', '.join(failed_lambdas)}")
            
            if not eventbridge_success:
                recommendations.append("EventBridge 시장 상황 확인 규칙 활성화 필요")
            
            if not recommendations:
                recommendations.append("모든 시스템이 정상 작동 중 - 실시간 모니터링 지속")
            
            report['recommendations'] = recommendations
            
            # 결과 출력
            logger.info("=" * 80)
            logger.info(f"🎯 종합 검증 결과: {report['overall_status']}")
            logger.info(f"📊 성공률: {success_rate*100:.1f}%")
            logger.info(f"✅ S3 데이터: {'정상' if s3_success else '미확인'}")
            logger.info(f"🧪 Lambda 통합: {lambda_success_count}/{lambda_total}개 함수")
            logger.info(f"⏰ EventBridge: {'연동됨' if eventbridge_success else '미연동'}")
            
            print(f"""
🌏 시간대 전략 통합 검증 완료!

📋 검증 결과 요약:
   • 전체 상태: {report['overall_status']} ({success_rate*100:.1f}%)
   • S3 시간대 데이터: {'✅ 정상' if s3_success else '❌ 미확인'}
   • Lambda 함수 통합: ✅ {lambda_success_count}/{lambda_total}개
   • EventBridge 연동: {'✅ 연동' if eventbridge_success else '❌ 미연동'}

📊 시간대 분석 데이터:
{f'   • 글로벌 활성도: {report["s3_data_check"]["latest_analysis"]["global_activity_score"]:.1f}%' if report["s3_data_check"]["latest_analysis"] else '   • 데이터 없음'}
{f'   • 주도 지역: {report["s3_data_check"]["latest_analysis"]["dominant_regions"][0]["timezone"]}' if report["s3_data_check"]["latest_analysis"] else ''}
{f'   • 거래 스타일: {report["s3_data_check"]["latest_analysis"]["trading_style"]["primary_style"]}' if report["s3_data_check"]["latest_analysis"] else ''}

🔧 Lambda 함수 상태:
{chr(10).join(f'   • {test["function_name"]}: {"✅" if test["timezone_strategy_loaded"] else "❌"} ({test["execution_time"]:.0f}ms)' for test in report["lambda_tests"])}

💡 권장사항:
{chr(10).join(f'   • {rec}' for rec in recommendations)}

🎯 다음 단계:
   • 실시간 EventBridge 스케줄 실행 모니터링
   • CloudWatch 로그에서 시간대 전략 적용 확인
   • 거래 시나리오 시뮬레이션 및 성과 검증
            """)
            
            return report
            
        except Exception as e:
            logger.error(f"종합 검증 실패: {e}")
            report['overall_status'] = 'ERROR'
            report['error'] = str(e)
            return report

def main():
    """메인 실행"""
    validator = TimezoneStrategyValidator()
    report = validator.generate_comprehensive_validation_report()
    
    if report['overall_status'] in ['SUCCESS', 'PARTIAL']:
        print("🎉 시간대 전략 통합 검증 완료!")
        exit(0)
    else:
        print("❌ 시간대 전략 통합 검증 실패!")
        exit(1)

if __name__ == '__main__':
    main()