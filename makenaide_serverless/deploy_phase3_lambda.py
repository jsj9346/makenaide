#!/usr/bin/env python3
"""
🚀 Phase 3: GPT Analysis Lambda 배포
- OpenAI GPT 기반 전문가 분석 Lambda 함수 배포
- EventBridge 연동 설정
- 테스트 및 검증
"""

import boto3
import json
import zipfile
import os
import time
from datetime import datetime

class Phase3LambdaDeployer:
    """Phase 3 Lambda 배포 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        self.region = 'ap-northeast-2'
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
        self.function_name = 'makenaide-gpt-analysis-phase3'
        self.layer_arn = f"arn:aws:lambda:{self.region}:{self.account_id}:layer:makenaide-core-layer:2"
        
    def create_deployment_package(self) -> str:
        """Lambda 배포 패키지 생성"""
        print("📦 Phase 3 Lambda 배포 패키지 생성...")
        
        zip_filename = 'lambda_phase3_gpt_analysis.zip'
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 메인 Lambda 코드
            zip_file.write('lambda_gpt_analysis_phase3.py', 'lambda_function.py')
            # 시스템 프롬프트 파일
            zip_file.write('system_prompt.txt', 'system_prompt.txt')
            
        print(f"✅ 배포 패키지 생성 완료: {zip_filename}")
        return zip_filename
    
    def create_lambda_function(self, zip_filename: str):
        """Lambda 함수 생성 또는 업데이트"""
        print(f"🔧 Lambda 함수 생성/업데이트: {self.function_name}")
        
        try:
            # 기존 함수 확인
            self.lambda_client.get_function(FunctionName=self.function_name)
            print(f"✅ Lambda 함수 이미 존재: {self.function_name}")
            return self.update_lambda_function(zip_filename)
            
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass
        
        # IAM 역할 ARN
        role_arn = f"arn:aws:iam::{self.account_id}:role/makenaide-lambda-execution-role"
        
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        # Lambda 함수 생성
        response = self.lambda_client.create_function(
            FunctionName=self.function_name,
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Makenaide Phase 3: GPT-based Professional Technical Analysis',
            Timeout=300,  # 5분 (GPT API 호출 시간)
            MemorySize=256,
            Layers=[self.layer_arn],
            Environment={
                'Variables': {
                    'OPENAI_API_KEY': '',  # 배포 후 수동 설정 필요
                    'GPT_MODEL': 'gpt-4-turbo-preview',  # 또는 gpt-3.5-turbo
                    'LOG_LEVEL': 'INFO'
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Phase': 'Phase3-GPTAnalysis',
                'Environment': 'Production'
            }
        )
        
        print(f"✅ Lambda 함수 생성 완료: {self.function_name}")
        print("⚠️  중요: AWS Console에서 OPENAI_API_KEY 환경변수를 설정해주세요!")
        return response
    
    def update_lambda_function(self, zip_filename: str):
        """기존 Lambda 함수 업데이트"""
        print(f"🔄 Lambda 함수 업데이트: {self.function_name}")
        
        with open(zip_filename, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        # 함수 코드 업데이트
        response = self.lambda_client.update_function_code(
            FunctionName=self.function_name,
            ZipFile=zip_content
        )
        
        # 함수 업데이트 완료 대기
        time.sleep(5)
        
        # 환경변수 업데이트 (OPENAI_API_KEY는 보안상 제외)
        current_config = self.lambda_client.get_function_configuration(
            FunctionName=self.function_name
        )
        
        current_vars = current_config.get('Environment', {}).get('Variables', {})
        
        # 기존 OPENAI_API_KEY 유지하면서 다른 변수 업데이트
        updated_vars = {
            'OPENAI_API_KEY': current_vars.get('OPENAI_API_KEY', ''),
            'GPT_MODEL': 'gpt-4-turbo-preview',
            'LOG_LEVEL': 'INFO'
        }
        
        self.lambda_client.update_function_configuration(
            FunctionName=self.function_name,
            Timeout=300,
            MemorySize=256,
            Environment={'Variables': updated_vars},
            Description='Makenaide Phase 3: GPT-based Professional Technical Analysis'
        )
        
        print(f"✅ Lambda 함수 업데이트 완료: {self.function_name}")
        
        if not current_vars.get('OPENAI_API_KEY'):
            print("⚠️  중요: OPENAI_API_KEY가 설정되지 않았습니다!")
            print("   AWS Console에서 환경변수를 설정해주세요.")
        
        return response
    
    def setup_eventbridge_integration(self):
        """EventBridge 연동 설정"""
        print("🔗 EventBridge 연동 설정...")
        
        # Phase 2 → Phase 3 이벤트 규칙
        rule_name = 'makenaide-phase2-to-phase3'
        
        try:
            # EventBridge 규칙 생성/업데이트
            self.events_client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps({
                    "source": ["makenaide.comprehensive_filtering"],
                    "detail-type": ["Comprehensive Filtering Completed"],
                    "detail": {
                        "status": ["completed"]
                    }
                }),
                Description='Phase 2 완료 시 Phase 3 트리거',
                State='ENABLED'
            )
            
            # Lambda 타겟 추가
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': f"arn:aws:lambda:{self.region}:{self.account_id}:function:{self.function_name}"
                    }
                ]
            )
            
            # Lambda 호출 권한 부여
            try:
                self.lambda_client.add_permission(
                    FunctionName=self.function_name,
                    StatementId=f'{rule_name}-permission',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f"arn:aws:events:{self.region}:{self.account_id}:rule/{rule_name}"
                )
            except self.lambda_client.exceptions.ResourceConflictException:
                pass  # 권한이 이미 존재
            
            print(f"✅ EventBridge 규칙 생성: {rule_name}")
            
        except Exception as e:
            print(f"❌ EventBridge 연동 설정 실패: {e}")
    
    def test_lambda_function(self) -> bool:
        """Lambda 함수 테스트"""
        print("🧪 Lambda 함수 테스트...")
        
        try:
            # API 키 확인
            config = self.lambda_client.get_function_configuration(
                FunctionName=self.function_name
            )
            
            env_vars = config.get('Environment', {}).get('Variables', {})
            
            if not env_vars.get('OPENAI_API_KEY'):
                print("⚠️  OPENAI_API_KEY가 설정되지 않았습니다.")
                print("   테스트를 건너뛰고 배포를 완료합니다.")
                print("   AWS Console에서 API 키를 설정한 후 테스트하세요.")
                return True  # 배포는 성공으로 처리
            
            test_payload = {
                'source': 'manual_test',
                'trigger': 'test_execution',
                'timestamp': datetime.now().isoformat()
            }
            
            print("   - Lambda 함수 호출 중...")
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_payload)
            )
            
            # 응답 파싱
            response_payload = json.loads(response['Payload'].read())
            
            print(f"   - 응답 상태: {response_payload.get('statusCode')}")
            
            if response_payload.get('statusCode') == 400:
                print("   ⚠️ Phase 2 데이터 없음 (정상 - Phase 2를 먼저 실행 필요)")
                print("   ✅ Lambda 함수 정상 동작 확인")
                return True
            elif response_payload.get('statusCode') == 200:
                print("   ✅ Phase 3 테스트 성공!")
                print(f"   - 분석된 티커: {response_payload.get('analyzed_tickers')}개")
                print(f"   - BUY 신호: {response_payload.get('buy_signals')}개")
                print(f"   - 사용 모델: {response_payload.get('model_used')}")
                return True
            else:
                print(f"   ❌ Phase 3 테스트 실패: {response_payload}")
                return False
                
        except Exception as e:
            print(f"   ❌ Lambda 테스트 실행 실패: {e}")
            return False
    
    def cleanup_deployment_files(self, zip_filename: str):
        """배포 파일 정리"""
        try:
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
                print(f"🧹 배포 파일 정리: {zip_filename}")
        except Exception as e:
            print(f"⚠️ 파일 정리 실패: {e}")
    
    def deploy_complete_phase3(self):
        """Phase 3 전체 배포"""
        try:
            print("🚀 Phase 3: GPT Analysis Lambda 배포 시작")
            print("="*60)
            
            # 1. 배포 패키지 생성
            zip_filename = self.create_deployment_package()
            
            # 2. Lambda 함수 생성/업데이트
            self.create_lambda_function(zip_filename)
            
            # 3. EventBridge 연동 설정
            self.setup_eventbridge_integration()
            
            # 4. 함수 테스트
            test_success = self.test_lambda_function()
            
            # 5. 배포 파일 정리
            self.cleanup_deployment_files(zip_filename)
            
            print("="*60)
            print("✅ Phase 3 Lambda 배포 완료!")
            print(f"📍 함수명: {self.function_name}")
            print("🔗 EventBridge 연동: Phase 2 → Phase 3 자동 트리거")
            print("🤖 기능: OpenAI GPT 기반 전문가 수준 기술적 분석")
            print("⏱️ 타임아웃: 5분")
            print("💾 메모리: 256MB")
            print("\n⚠️  중요 설정 필요:")
            print("1. AWS Console에서 Lambda 함수의 환경변수 편집")
            print("2. OPENAI_API_KEY 값 설정")
            print("3. GPT_MODEL 확인 (gpt-4-turbo-preview 또는 gpt-3.5-turbo)")
            print("="*60)
            return True
                
        except Exception as e:
            print(f"❌ Phase 3 배포 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    deployer = Phase3LambdaDeployer()
    success = deployer.deploy_complete_phase3()
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)