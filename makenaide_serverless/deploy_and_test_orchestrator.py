#!/usr/bin/env python3
"""
Makenaide Orchestrator 배포 및 통합 검증 스크립트

🎯 기능:
1. makenaide-orchestrator Lambda 함수 배포
2. 전체 파이프라인 통합 테스트
3. DB 티커 업데이트 정상 동작 검증
4. 성능 모니터링 및 리포트

🔧 검증 항목:
- Lambda 함수들 간 통신
- SQS 큐 처리 상태
- DB 데이터 업데이트 확인
- 오류 처리 및 복구
- CloudWatch 메트릭 수집
"""

import boto3
import json
import zipfile
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import psycopg2

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrchestratorDeploymentTester:
    """Orchestrator 배포 및 검증 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        self.cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')
        
        # 함수 및 리소스 설정
        self.orchestrator_function = 'makenaide-orchestrator'
        self.test_results = {
            'deployment': {},
            'integration_test': {},
            'db_verification': {},
            'performance_metrics': {},
            'summary': {}
        }
        
        # DB 연결 정보
        self.db_config = {
            'host': os.getenv('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
            'port': int(os.getenv('PG_PORT', '5432')),
            'database': os.getenv('PG_DATABASE', 'makenaide'),
            'user': os.getenv('PG_USER', 'bruce'),
            'password': os.getenv('PG_PASSWORD')
        }
    
    def deploy_orchestrator_function(self) -> bool:
        """Orchestrator Lambda 함수 배포"""
        try:
            logger.info("🚀 Orchestrator Lambda 함수 배포 시작")
            
            # Lambda 함수 ZIP 생성
            function_zip = "makenaide_orchestrator.zip"
            with zipfile.ZipFile(function_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write('lambda_makenaide_orchestrator.py', 'lambda_function.py')
            
            # IAM 역할 ARN (기존 사용)
            role_arn = 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role'
            
            try:
                # 함수 생성 시도
                with open(function_zip, 'rb') as f:
                    function_code = f.read()
                
                response = self.lambda_client.create_function(
                    FunctionName=self.orchestrator_function,
                    Runtime='python3.11',
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Code={'ZipFile': function_code},
                    Description='Makenaide 파이프라인 조정자 - 전체 워크플로우 관리',
                    Timeout=900,  # 15분
                    MemorySize=512,  # 충분한 메모리
                    Environment={
                        'Variables': {
                            'REGION': 'ap-northeast-2',
                            'PIPELINE_VERSION': 'v1.0'
                        }
                    }
                )
                
                logger.info(f"✅ Orchestrator 함수 생성 완료: {response['FunctionArn']}")
                
            except self.lambda_client.exceptions.ResourceConflictException:
                # 함수가 이미 존재하면 업데이트
                logger.info("🔄 기존 Orchestrator 함수 업데이트")
                
                with open(function_zip, 'rb') as f:
                    function_code = f.read()
                
                self.lambda_client.update_function_code(
                    FunctionName=self.orchestrator_function,
                    ZipFile=function_code
                )
                
                # 설정 업데이트
                self.lambda_client.update_function_configuration(
                    FunctionName=self.orchestrator_function,
                    Runtime='python3.11',
                    Handler='lambda_function.lambda_handler',
                    Timeout=900,
                    MemorySize=512,
                    Environment={
                        'Variables': {
                            'REGION': 'ap-northeast-2',
                            'PIPELINE_VERSION': 'v1.0'
                        }
                    }
                )
                
                logger.info("✅ Orchestrator 함수 업데이트 완료")
            
            # 파일 정리
            os.remove(function_zip)
            
            # 함수 준비 대기
            logger.info("⏳ 함수 준비 대기 중...")
            time.sleep(10)
            
            self.test_results['deployment'] = {
                'success': True,
                'function_name': self.orchestrator_function,
                'timestamp': datetime.now().isoformat()
            }
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Orchestrator 배포 실패: {e}")
            self.test_results['deployment'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            return False
    
    def get_db_connection(self):
        """DB 연결 생성"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            return None
    
    def check_db_state_before(self) -> Dict:
        """테스트 전 DB 상태 확인"""
        try:
            logger.info("📊 테스트 전 DB 상태 확인")
            
            conn = self.get_db_connection()
            if not conn:
                return {'success': False, 'error': 'DB 연결 실패'}
            
            cursor = conn.cursor()
            
            # 티커 테이블 상태
            cursor.execute("SELECT COUNT(*) FROM tickers")
            ticker_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
            active_ticker_count = cursor.fetchone()[0]
            
            # OHLCV 테이블 상태 
            cursor.execute("SELECT COUNT(*) FROM ohlcv WHERE date >= CURRENT_DATE - INTERVAL '1 day'")
            recent_ohlcv_count = cursor.fetchone()[0]
            
            # Static indicators 테이블 상태
            cursor.execute("SELECT COUNT(*) FROM static_indicators WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'")
            recent_indicators_count = cursor.fetchone()[0]
            
            # 최근 업데이트된 티커들
            cursor.execute("""
                SELECT ticker, updated_at 
                FROM tickers 
                WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
                ORDER BY updated_at DESC 
                LIMIT 5
            """)
            recent_updates = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            db_state = {
                'success': True,
                'ticker_count': ticker_count,
                'active_ticker_count': active_ticker_count,
                'recent_ohlcv_count': recent_ohlcv_count,
                'recent_indicators_count': recent_indicators_count,
                'recent_updates': [
                    {'ticker': ticker, 'updated_at': updated_at.isoformat()}
                    for ticker, updated_at in recent_updates
                ],
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"📈 DB 상태 - 티커: {ticker_count}개 (활성: {active_ticker_count}개)")
            logger.info(f"📊 최근 OHLCV: {recent_ohlcv_count}개, 지표: {recent_indicators_count}개")
            
            return db_state
            
        except Exception as e:
            logger.error(f"❌ DB 상태 확인 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def run_orchestrator_test(self) -> Dict:
        """Orchestrator 통합 테스트 실행"""
        try:
            logger.info("🧪 Orchestrator 통합 테스트 시작")
            
            # Orchestrator 호출
            start_time = time.time()
            
            response = self.lambda_client.invoke(
                FunctionName=self.orchestrator_function,
                InvocationType='RequestResponse',
                Payload=json.dumps({
                    'test_mode': True,
                    'test_timestamp': datetime.now().isoformat()
                })
            )
            
            execution_time = time.time() - start_time
            
            # 응답 파싱
            response_payload = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200:
                logger.info(f"✅ Orchestrator 테스트 성공 ({execution_time:.2f}초)")
                
                # 응답 내용 분석
                if 'body' in response_payload:
                    body = json.loads(response_payload['body'])
                    
                    test_result = {
                        'success': True,
                        'execution_time': execution_time,
                        'response_status': response['StatusCode'],
                        'pipeline_success': body.get('success', False),
                        'steps_completed': body.get('steps_completed', 0),
                        'errors_count': body.get('errors_count', 0),
                        'detailed_results': body.get('detailed_results', {}),
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    logger.info(f"📊 파이프라인 성공: {body.get('success')}")
                    logger.info(f"📋 완료 단계: {body.get('steps_completed')}개")
                    logger.info(f"⚠️ 오류 수: {body.get('errors_count')}개")
                    
                    return test_result
                else:
                    return {
                        'success': False,
                        'error': '응답 본문 파싱 실패',
                        'execution_time': execution_time
                    }
            else:
                logger.error(f"❌ Orchestrator 테스트 실패: {response['StatusCode']}")
                return {
                    'success': False,
                    'error': f"HTTP {response['StatusCode']}",
                    'response': response_payload,
                    'execution_time': execution_time
                }
                
        except Exception as e:
            logger.error(f"❌ Orchestrator 테스트 오류: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_db_state_after(self) -> Dict:
        """테스트 후 DB 상태 확인 및 비교"""
        try:
            logger.info("📊 테스트 후 DB 상태 확인")
            
            # 현재 상태 조회 (check_db_state_before와 동일한 로직)
            current_state = self.check_db_state_before()
            
            if not current_state['success']:
                return current_state
            
            # 이전 상태와 비교
            before_state = self.test_results.get('db_before', {})
            
            if before_state:
                changes = {
                    'ticker_count_change': current_state['ticker_count'] - before_state.get('ticker_count', 0),
                    'active_ticker_change': current_state['active_ticker_count'] - before_state.get('active_ticker_count', 0),
                    'ohlcv_data_added': current_state['recent_ohlcv_count'] - before_state.get('recent_ohlcv_count', 0),
                    'indicators_updated': current_state['recent_indicators_count'] - before_state.get('recent_indicators_count', 0)
                }
                
                current_state['changes'] = changes
                
                logger.info("📈 DB 변화량:")
                logger.info(f"   - 티커 수 변화: {changes['ticker_count_change']}")
                logger.info(f"   - 활성 티커 변화: {changes['active_ticker_change']}")
                logger.info(f"   - OHLCV 데이터 추가: {changes['ohlcv_data_added']}")
                logger.info(f"   - 지표 업데이트: {changes['indicators_updated']}")
                
                # 데이터 업데이트 성공 여부 판단
                data_updated = (
                    changes['ticker_count_change'] >= 0 and
                    changes['ohlcv_data_added'] > 0 and
                    changes['indicators_updated'] > 0
                )
                
                current_state['data_update_success'] = data_updated
                
                if data_updated:
                    logger.info("✅ DB 데이터 정상 업데이트 확인")
                else:
                    logger.warning("⚠️ DB 데이터 업데이트 미확인")
            
            return current_state
            
        except Exception as e:
            logger.error(f"❌ 테스트 후 DB 상태 확인 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def collect_performance_metrics(self) -> Dict:
        """성능 메트릭 수집"""
        try:
            logger.info("📊 성능 메트릭 수집")
            
            # CloudWatch 메트릭 조회
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=60)  # 최근 1시간
            
            metrics_to_collect = [
                ('AWS/Lambda', 'Duration', 'FunctionName'),
                ('AWS/Lambda', 'Errors', 'FunctionName'),
                ('AWS/Lambda', 'Invocations', 'FunctionName'),
                ('Makenaide/Pipeline', 'PipelineDuration', 'ExecutionId'),
                ('Makenaide/Pipeline', 'TickersScanned', 'ExecutionId')
            ]
            
            collected_metrics = {}
            
            for namespace, metric_name, dimension_name in metrics_to_collect:
                try:
                    response = self.cloudwatch.get_metric_statistics(
                        Namespace=namespace,
                        MetricName=metric_name,
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5분 간격
                        Statistics=['Average', 'Sum', 'Maximum']
                    )
                    
                    if response['Datapoints']:
                        latest_datapoint = max(response['Datapoints'], key=lambda x: x['Timestamp'])
                        collected_metrics[f"{namespace}_{metric_name}"] = {
                            'average': latest_datapoint.get('Average'),
                            'sum': latest_datapoint.get('Sum'),
                            'maximum': latest_datapoint.get('Maximum'),
                            'timestamp': latest_datapoint['Timestamp'].isoformat()
                        }
                    
                except Exception as e:
                    logger.warning(f"⚠️ 메트릭 {metric_name} 수집 실패: {e}")
            
            performance_summary = {
                'success': True,
                'collected_at': datetime.now().isoformat(),
                'metrics': collected_metrics,
                'total_metrics_collected': len(collected_metrics)
            }
            
            logger.info(f"📈 성능 메트릭 {len(collected_metrics)}개 수집 완료")
            
            return performance_summary
            
        except Exception as e:
            logger.error(f"❌ 성능 메트릭 수집 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def generate_test_report(self) -> Dict:
        """통합 테스트 리포트 생성"""
        try:
            logger.info("📋 통합 테스트 리포트 생성")
            
            # 전체 테스트 성공 여부 판단
            deployment_success = self.test_results.get('deployment', {}).get('success', False)
            integration_success = self.test_results.get('integration_test', {}).get('success', False)
            db_verification_success = self.test_results.get('db_verification', {}).get('data_update_success', False)
            
            overall_success = deployment_success and integration_success and db_verification_success
            
            summary = {
                'overall_success': overall_success,
                'test_timestamp': datetime.now().isoformat(),
                'components_tested': {
                    'orchestrator_deployment': deployment_success,
                    'pipeline_integration': integration_success,
                    'db_data_update': db_verification_success,
                    'performance_metrics': self.test_results.get('performance_metrics', {}).get('success', False)
                },
                'recommendations': []
            }
            
            # 권장사항 생성
            if not deployment_success:
                summary['recommendations'].append("Orchestrator 배포 문제 해결 필요")
            
            if not integration_success:
                summary['recommendations'].append("Lambda 함수 간 통신 점검 필요")
            
            if not db_verification_success:
                summary['recommendations'].append("DB 연결 및 데이터 업데이트 로직 점검 필요")
            
            if overall_success:
                summary['recommendations'].append("모든 테스트 통과 - 프로덕션 배포 준비 완료")
            
            self.test_results['summary'] = summary
            
            # 리포트 파일 저장
            report_filename = f"orchestrator_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(self.test_results, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📊 테스트 리포트 저장: {report_filename}")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 테스트 리포트 생성 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def run_full_test_suite(self):
        """전체 테스트 수트 실행"""
        logger.info("🚀 Makenaide Orchestrator 통합 테스트 시작")
        logger.info(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1단계: 테스트 전 DB 상태 확인
            logger.info("=" * 60)
            logger.info("📊 1단계: 테스트 전 DB 상태 확인")
            self.test_results['db_before'] = self.check_db_state_before()
            
            # 2단계: Orchestrator 배포
            logger.info("=" * 60)
            logger.info("🚀 2단계: Orchestrator 배포")
            if not self.deploy_orchestrator_function():
                logger.error("❌ Orchestrator 배포 실패 - 테스트 중단")
                return False
            
            # 3단계: 통합 테스트 실행
            logger.info("=" * 60)
            logger.info("🧪 3단계: 통합 테스트 실행")
            self.test_results['integration_test'] = self.run_orchestrator_test()
            
            # 4단계: 테스트 후 DB 상태 확인
            logger.info("=" * 60)
            logger.info("📊 4단계: 테스트 후 DB 상태 확인")
            self.test_results['db_verification'] = self.check_db_state_after()
            
            # 5단계: 성능 메트릭 수집
            logger.info("=" * 60)
            logger.info("📈 5단계: 성능 메트릭 수집")
            self.test_results['performance_metrics'] = self.collect_performance_metrics()
            
            # 6단계: 최종 리포트 생성
            logger.info("=" * 60)
            logger.info("📋 6단계: 최종 리포트 생성")
            final_summary = self.generate_test_report()
            
            # 결과 출력
            logger.info("=" * 60)
            logger.info("🎉 통합 테스트 완료!")
            logger.info("=" * 60)
            
            if final_summary.get('overall_success'):
                logger.info("✅ 모든 테스트 성공 - makenaide-orchestrator 정상 동작 확인")
                logger.info("🚀 프로덕션 환경 배포 준비 완료")
            else:
                logger.warning("⚠️ 일부 테스트 실패 - 문제 해결 후 재테스트 권장")
            
            logger.info("📊 테스트 결과 요약:")
            for component, status in final_summary['components_tested'].items():
                status_emoji = "✅" if status else "❌"
                logger.info(f"   {status_emoji} {component}: {'성공' if status else '실패'}")
            
            if final_summary.get('recommendations'):
                logger.info("💡 권장사항:")
                for rec in final_summary['recommendations']:
                    logger.info(f"   - {rec}")
            
            return final_summary.get('overall_success', False)
            
        except Exception as e:
            logger.error(f"❌ 통합 테스트 중 치명적 오류: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🎭 Makenaide Orchestrator 배포 및 통합 검증")
    print("=" * 60)
    
    # 환경변수 확인
    if not os.getenv('PG_PASSWORD'):
        print("❌ DB 패스워드 환경변수(PG_PASSWORD)가 설정되지 않았습니다.")
        print("💡 .env 파일을 확인하거나 환경변수를 설정해주세요.")
        return False
    
    # 테스트 실행
    tester = OrchestratorDeploymentTester()
    success = tester.run_full_test_suite()
    
    if success:
        print("\n🎉 성공: Makenaide Orchestrator 통합 검증 완료!")
        print("🚀 이제 EventBridge 스케줄러를 설정하여 자동 실행할 수 있습니다.")
    else:
        print("\n⚠️ 실패: 일부 문제가 발견되었습니다.")
        print("🔧 문제 해결 후 다시 테스트해주세요.")
    
    return success

if __name__ == "__main__":
    main() 