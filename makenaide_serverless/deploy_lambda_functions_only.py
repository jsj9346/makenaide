#!/usr/bin/env python3
"""
🚀 Makenaide Lambda Functions Only Deployment
- Deploy Phase 2-6 Lambda functions with existing infrastructure
- Use existing role and minimal permissions
"""

import boto3
import json
import os
import sys
import zipfile
import time
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MakenaideSimpleDeployer:
    """Makenaide Lambda 함수 간단 배포 클래스"""
    
    def __init__(self):
        self.session = boto3.Session()
        self.lambda_client = self.session.client('lambda')
        self.s3_client = self.session.client('s3')
        
        # 기존 리소스 사용
        self.config = {
            'region': 'ap-northeast-2',
            's3_bucket': 'makenaide-serverless-data',
            'existing_role_arn': 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            'function_timeout': 900,
            'memory_size': 1024,
            'python_runtime': 'python3.11',
            'environment_variables': {
                'S3_BUCKET': 'makenaide-serverless-data',
                'REGION': 'ap-northeast-2',
                'LOOKBACK_DAYS': '30',
                'VOLUME_MULTIPLIER': '1.5',
                'MA_SLOPE_THRESHOLD': '0.5',
                'ADX_THRESHOLD': '20',
                'RSI_LOWER': '40',
                'RSI_UPPER': '70',
                'OPENAI_SECRET_NAME': 'makenaide/openai-api-key',
                'UPBIT_SECRET_NAME': 'makenaide/upbit-api-keys',
                'MAX_POSITION_PCT': '10.0',
                'MAX_TOTAL_EXPOSURE': '50.0',
                'MAX_DAILY_TRADES': '5',
                'STOP_LOSS_PCT': '8.0',
                'TAKE_PROFIT_PCT': '25.0'
            }
        }
        
        # Lambda 함수 정의
        self.lambda_functions = {
            'makenaide-phase2-comprehensive-filter': {
                'file': 'lambda_comprehensive_filter_phase2_adaptive.py',
                'handler': 'lambda_handler',
                'description': 'Phase 2: Comprehensive filtering with market condition detection'
            },
            'makenaide-phase3-gpt-analysis': {
                'file': 'lambda_gpt_analysis_phase3_v2.py',
                'handler': 'lambda_handler',
                'description': 'Phase 3: GPT-4 analysis with chart generation'
            },
            'makenaide-phase4-4h-analysis': {
                'file': 'lambda_phase4_4h_analysis.py',
                'handler': 'lambda_handler',
                'description': 'Phase 4: 4-hour technical analysis and timing'
            },
            'makenaide-phase5-condition-check': {
                'file': 'lambda_phase5_condition_check.py',
                'handler': 'lambda_handler',
                'description': 'Phase 5: Final condition validation and risk management'
            },
            'makenaide-phase6-trade-execution': {
                'file': 'lambda_phase6_trade_execution.py',
                'handler': 'lambda_handler',
                'description': 'Phase 6: Trade execution and position management'
            }
        }

    def create_deployment_package(self, function_file: str) -> str:
        """배포 패키지 생성"""
        try:
            logger.info(f"📦 배포 패키지 생성 중: {function_file}")
            
            zip_filename = f"{function_file.replace('.py', '')}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Lambda 함수 파일 추가
                if os.path.exists(function_file):
                    zipf.write(function_file, 'lambda_function.py')
                else:
                    logger.error(f"❌ 함수 파일을 찾을 수 없습니다: {function_file}")
                    return None
                
                # requirements.txt가 있다면 추가
                if os.path.exists('requirements.txt'):
                    zipf.write('requirements.txt')
            
            logger.info(f"✅ 배포 패키지 생성 완료: {zip_filename}")
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 배포 패키지 생성 실패: {e}")
            return None

    def deploy_lambda_function(self, function_name: str, function_config: dict) -> bool:
        """Lambda 함수 배포"""
        try:
            logger.info(f"🚀 Lambda 함수 배포 중: {function_name}")
            
            # 배포 패키지 생성
            zip_file = self.create_deployment_package(function_config['file'])
            if not zip_file:
                return False
            
            # 함수 코드 읽기
            with open(zip_file, 'rb') as f:
                zip_content = f.read()
            
            try:
                # 함수 생성
                response = self.lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime=self.config['python_runtime'],
                    Role=self.config['existing_role_arn'],
                    Handler=f"lambda_function.{function_config['handler']}",
                    Code={'ZipFile': zip_content},
                    Description=function_config['description'],
                    Timeout=self.config['function_timeout'],
                    MemorySize=self.config['memory_size'],
                    Environment={'Variables': self.config['environment_variables']}
                )
                logger.info(f"✅ Lambda 함수 생성 완료: {function_name}")
                
            except self.lambda_client.exceptions.ResourceConflictException:
                # 함수가 이미 존재하면 업데이트
                logger.info(f"ℹ️ 기존 함수 업데이트: {function_name}")
                
                try:
                    # 함수 코드 업데이트
                    self.lambda_client.update_function_code(
                        FunctionName=function_name,
                        ZipFile=zip_content
                    )
                    
                    # 함수 설정 업데이트
                    self.lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Runtime=self.config['python_runtime'],
                        Role=self.config['existing_role_arn'],
                        Handler=f"lambda_function.{function_config['handler']}",
                        Description=function_config['description'],
                        Timeout=self.config['function_timeout'],
                        MemorySize=self.config['memory_size'],
                        Environment={'Variables': self.config['environment_variables']}
                    )
                    logger.info(f"✅ Lambda 함수 업데이트 완료: {function_name}")
                    
                except Exception as update_error:
                    logger.error(f"❌ Lambda 함수 업데이트 실패 {function_name}: {update_error}")
                    return False
            
            # 배포 패키지 파일 삭제
            if os.path.exists(zip_file):
                os.remove(zip_file)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 배포 실패 {function_name}: {e}")
            return False

    def test_function_invocation(self, function_name: str) -> bool:
        """함수 호출 테스트"""
        try:
            logger.info(f"🧪 함수 호출 테스트: {function_name}")
            
            test_event = {
                'test': True,
                'source': 'deployment_test',
                'timestamp': datetime.now().isoformat()
            }
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read().decode('utf-8'))
                logger.info(f"✅ {function_name} 테스트 호출 성공")
                return True
            else:
                logger.warning(f"⚠️ {function_name} 테스트 호출 실패: {response['StatusCode']}")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️ {function_name} 테스트 호출 에러: {e}")
            return False

    def deploy_all_functions(self) -> dict:
        """모든 Lambda 함수 배포"""
        try:
            logger.info("🚀 Makenaide Lambda 함수 배포 시작")
            
            deployment_results = {
                'total_functions': len(self.lambda_functions),
                'deployed_successfully': [],
                'deployment_failed': [],
                'test_passed': [],
                'test_failed': []
            }
            
            # Lambda 함수들 배포
            for function_name, function_config in self.lambda_functions.items():
                logger.info(f"📦 배포 중: {function_name}")
                
                if self.deploy_lambda_function(function_name, function_config):
                    deployment_results['deployed_successfully'].append(function_name)
                    logger.info(f"✅ {function_name} 배포 성공")
                    
                    # 배포 후 테스트
                    time.sleep(2)  # 함수 활성화 대기
                    if self.test_function_invocation(function_name):
                        deployment_results['test_passed'].append(function_name)
                    else:
                        deployment_results['test_failed'].append(function_name)
                        
                else:
                    deployment_results['deployment_failed'].append(function_name)
                    logger.error(f"❌ {function_name} 배포 실패")
            
            # 결과 요약
            logger.info("📊 배포 결과 요약:")
            logger.info(f"   총 함수: {deployment_results['total_functions']}")
            logger.info(f"   배포 성공: {len(deployment_results['deployed_successfully'])}")
            logger.info(f"   배포 실패: {len(deployment_results['deployment_failed'])}")
            logger.info(f"   테스트 성공: {len(deployment_results['test_passed'])}")
            logger.info(f"   테스트 실패: {len(deployment_results['test_failed'])}")
            
            return deployment_results
            
        except Exception as e:
            logger.error(f"❌ 전체 배포 실패: {e}")
            return {'error': str(e)}

def main():
    """메인 실행 함수"""
    try:
        print("🚀 Makenaide Lambda Functions Only Deployment")
        print("=" * 60)
        
        deployer = MakenaideSimpleDeployer()
        
        # 모든 함수 배포
        results = deployer.deploy_all_functions()
        
        if 'error' in results:
            print(f"\n❌ 배포 실패: {results['error']}")
            return False
        
        # 성공 여부 판정
        total_functions = results['total_functions']
        deployed_successfully = len(results['deployed_successfully'])
        
        if deployed_successfully == total_functions:
            print(f"\n✅ 모든 Lambda 함수 배포 성공! ({deployed_successfully}/{total_functions})")
            print(f"   테스트 통과: {len(results['test_passed'])}개")
            print(f"   테스트 실패: {len(results['test_failed'])}개")
            print("\n📋 다음 단계:")
            print("1. AWS Console에서 함수 로그 확인")
            print("2. 테스트 데이터로 전체 워크플로우 테스트")
            print("3. API 키 설정 (Secrets Manager)")
            return True
        else:
            print(f"\n⚠️ 일부 배포 실패: {deployed_successfully}/{total_functions}")
            if results['deployment_failed']:
                print(f"   실패한 함수: {', '.join(results['deployment_failed'])}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)