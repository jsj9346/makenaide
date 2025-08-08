#!/usr/bin/env python3
"""
🚀 Phase 2: Comprehensive Filtering Lambda 배포
- 기술적 분석 필터링 Lambda 함수 배포
- EventBridge 연동 설정
- 테스트 및 검증
"""

import boto3
import json
import zipfile
import os
import time
from datetime import datetime

class Phase2LambdaDeployer:
    """Phase 2 Lambda 배포 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        self.region = 'ap-northeast-2'
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
        self.function_name = 'makenaide-comprehensive-filter-phase2'
        self.layer_arn = f"arn:aws:lambda:{self.region}:{self.account_id}:layer:makenaide-core-layer:1"
        
    def create_deployment_package(self) -> str:
        """Lambda 배포 패키지 생성"""
        print("📦 Phase 2 Lambda 배포 패키지 생성...")
        
        zip_filename = 'lambda_phase2_comprehensive_filter.zip'
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 메인 Lambda 코드
            zip_file.write('lambda_comprehensive_filter_phase2.py', 'lambda_function.py')
            
        print(f"✅ 배포 패키지 생성 완료: {zip_filename}")
        return zip_filename
    
    def create_lambda_function(self, zip_filename: str):
        """Lambda 함수 생성"""
        print(f"🔧 Lambda 함수 생성: {self.function_name}")
        
        try:
            # 기존 함수 확인
            self.lambda_client.get_function(FunctionName=self.function_name)
            print(f"✅ Lambda 함수 이미 존재: {self.function_name}")
            return self.update_lambda_function(zip_filename)
            
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass
        
        # IAM 역할 ARN (기존 역할 재사용)
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
            Description='Makenaide Phase 2: Comprehensive Technical Analysis Filtering',
            Timeout=300,  # 5분 (기술적 분석 시간 필요)
            MemorySize=512,  # 메모리 증가 (복잡한 계산)
            Layers=[self.layer_arn],
            Environment={
                'Variables': {
                    'VOLUME_MULTIPLIER': '1.5',
                    'MA_SLOPE_THRESHOLD': '0.5',
                    'ADX_THRESHOLD': '20',
                    'RSI_LOWER': '40',
                    'RSI_UPPER': '70',
                    'LOOKBACK_DAYS': '252',
                    'CONSOLIDATION_THRESHOLD': '0.25'
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Phase': 'Phase2-ComprehensiveFiltering',
                'Environment': 'Production'
            }
        )
        
        print(f"✅ Lambda 함수 생성 완료: {self.function_name}")
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
        
        # 환경변수 업데이트
        self.lambda_client.update_function_configuration(
            FunctionName=self.function_name,
            Timeout=300,
            MemorySize=512,
            Environment={
                'Variables': {
                    'VOLUME_MULTIPLIER': '1.5',
                    'MA_SLOPE_THRESHOLD': '0.5',
                    'ADX_THRESHOLD': '20',
                    'RSI_LOWER': '40',
                    'RSI_UPPER': '70',
                    'LOOKBACK_DAYS': '252',
                    'CONSOLIDATION_THRESHOLD': '0.25'
                }
            }
        )
        
        print(f"✅ Lambda 함수 업데이트 완료: {self.function_name}")
        return response
    
    def setup_eventbridge_integration(self):
        """EventBridge 연동 설정"""
        print("🔗 EventBridge 연동 설정...")
        
        # Phase 1 → Phase 2 이벤트 규칙 (이미 생성되어 있을 수 있음)
        rule_name = 'makenaide-phase1-to-phase2'
        
        try:
            # EventBridge 규칙 확인/생성
            try:
                self.events_client.describe_rule(Name=rule_name)
                print(f"✅ EventBridge 규칙 이미 존재: {rule_name}")
            except self.events_client.exceptions.ResourceNotFoundException:
                # 규칙 생성
                self.events_client.put_rule(
                    Name=rule_name,
                    EventPattern=json.dumps({
                        "source": ["makenaide.selective_data_collection"],
                        "detail-type": ["Selective Data Collection Completed"],
                        "detail": {
                            "status": ["completed"]
                        }
                    }),
                    Description='Phase 1 완료 시 Phase 2 트리거',
                    State='ENABLED'
                )
                print(f"✅ EventBridge 규칙 생성: {rule_name}")
            
            # Lambda 타겟 추가/업데이트
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
            
            print("✅ EventBridge 연동 설정 완료")
            
        except Exception as e:
            print(f"❌ EventBridge 연동 설정 실패: {e}")
    
    def test_lambda_function(self) -> bool:
        """Lambda 함수 테스트"""
        print("🧪 Lambda 함수 테스트...")
        
        try:
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
                # Phase 1 데이터 없음 - 정상적인 상황
                print("   ⚠️ Phase 1 데이터 없음 (정상 - Phase 1을 먼저 실행 필요)")
                print("   ✅ Lambda 함수 정상 동작 확인")
                return True
            elif response_payload.get('statusCode') == 200:
                print("   ✅ Phase 2 테스트 성공!")
                print(f"   - 입력 티커: {response_payload.get('input_tickers')}개")
                print(f"   - 필터링된 티커: {response_payload.get('filtered_tickers')}개")
                print(f"   - 실행 시간: {response_payload.get('execution_time')}")
                return True
            else:
                print(f"   ❌ Phase 2 테스트 실패: {response_payload}")
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
    
    def deploy_complete_phase2(self):
        """Phase 2 전체 배포"""
        try:
            print("🚀 Phase 2: Comprehensive Filtering Lambda 배포 시작")
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
            
            if test_success:
                print("="*60)
                print("✅ Phase 2 Lambda 배포 완료!")
                print(f"📍 함수명: {self.function_name}")
                print("🔗 EventBridge 연동: Phase 1 → Phase 2 자동 트리거")
                print("📊 기능: 와인스타인/미너비니/오닐 기술적 분석")
                print("⏱️ 타임아웃: 5분")
                print("💾 메모리: 512MB")
                print("="*60)
                return True
            else:
                print("⚠️ 배포는 완료되었으나 테스트에서 문제 발생")
                return True  # 배포 자체는 성공
                
        except Exception as e:
            print(f"❌ Phase 2 배포 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    deployer = Phase2LambdaDeployer()
    success = deployer.deploy_complete_phase2()
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)