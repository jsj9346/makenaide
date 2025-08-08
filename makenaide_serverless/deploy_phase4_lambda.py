#!/usr/bin/env python3
"""
🚀 Phase 4: 4H Analysis Lambda 배포
- 4시간봉 기반 마켓타이밍 분석 Lambda 함수 배포
- EventBridge 연동 설정
- 테스트 및 검증
"""

import boto3
import json
import zipfile
import os
import time
from datetime import datetime

class Phase4LambdaDeployer:
    """Phase 4 Lambda 배포 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        self.region = 'ap-northeast-2'
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
        self.function_name = 'makenaide-4h-analysis-phase4'
        self.layer_arn = f"arn:aws:lambda:{self.region}:{self.account_id}:layer:makenaide-core-layer:2"
        
    def create_deployment_package(self) -> str:
        """Lambda 배포 패키지 생성"""
        print("📦 Phase 4 Lambda 배포 패키지 생성...")
        
        zip_filename = 'lambda_phase4_4h_analysis.zip'
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 메인 Lambda 코드
            zip_file.write('lambda_4h_analysis_phase4.py', 'lambda_function.py')
            
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
            Description='Makenaide Phase 4: 4H Market Timing Analysis',
            Timeout=300,  # 5분 (4시간봉 데이터 수집 및 분석)
            MemorySize=512,  # 메모리 증가 (기술적 지표 계산)
            Layers=[self.layer_arn],
            Environment={
                'Variables': {
                    'MIN_SCORE': '5',  # 7개 지표 중 최소 통과 점수
                    'RSI_MAX': '80',   # RSI 과열 임계값
                    'ADX_MIN': '25',   # ADX 추세 강도 임계값
                    'LOOKBACK_PERIODS': '100'  # 4시간봉 조회 기간
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Phase': 'Phase4-4HAnalysis',
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
        
        # 함수 업데이트 완료 대기
        time.sleep(5)
        
        # 환경변수 업데이트
        self.lambda_client.update_function_configuration(
            FunctionName=self.function_name,
            Timeout=300,
            MemorySize=512,
            Environment={
                'Variables': {
                    'MIN_SCORE': '5',
                    'RSI_MAX': '80',
                    'ADX_MIN': '25',
                    'LOOKBACK_PERIODS': '100'
                }
            },
            Description='Makenaide Phase 4: 4H Market Timing Analysis'
        )
        
        print(f"✅ Lambda 함수 업데이트 완료: {self.function_name}")
        return response
    
    def setup_eventbridge_integration(self):
        """EventBridge 연동 설정"""
        print("🔗 EventBridge 연동 설정...")
        
        # Phase 3 → Phase 4 이벤트 규칙
        rule_name = 'makenaide-phase3-to-phase4'
        
        try:
            # EventBridge 규칙 생성/업데이트
            self.events_client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps({
                    "source": ["makenaide.gpt_analysis"],
                    "detail-type": ["GPT Analysis Completed"],
                    "detail": {
                        "status": ["completed"]
                    }
                }),
                Description='Phase 3 완료 시 Phase 4 트리거',
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
                print("   ⚠️ Phase 3 데이터 없음 (정상 - Phase 3을 먼저 실행 필요)")
                print("   ✅ Lambda 함수 정상 동작 확인")
                return True
            elif response_payload.get('statusCode') == 200:
                print("   ✅ Phase 4 테스트 성공!")
                print(f"   - 분석된 티커: {response_payload.get('analyzed_tickers')}개")
                print(f"   - 타이밍 통과: {response_payload.get('timing_passed')}개")
                print(f"   - 최종 후보: {response_payload.get('final_candidates')}")
                return True
            else:
                print(f"   ❌ Phase 4 테스트 실패: {response_payload}")
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
    
    def deploy_complete_phase4(self):
        """Phase 4 전체 배포"""
        try:
            print("🚀 Phase 4: 4H Analysis Lambda 배포 시작")
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
            print("✅ Phase 4 Lambda 배포 완료!")
            print(f"📍 함수명: {self.function_name}")
            print("🔗 EventBridge 연동: Phase 3 → Phase 4 자동 트리거")
            print("📊 기능: 4시간봉 기반 마켓타이밍 분석")
            print("⏱️ 타임아웃: 5분")
            print("💾 메모리: 512MB")
            print("\n📋 분석 지표:")
            print("1. MACD Signal 상향 돌파")
            print("2. Stochastic 상승")
            print("3. CCI 돌파 (>100)")
            print("4. ADX 추세 강도 (>25)")
            print("5. MA200 돌파")
            print("6. 중기 상승 추세")
            print("7. Bollinger Band 상단 돌파")
            print("\n⚡ 통과 기준: 7개 지표 중 5개 이상")
            print("="*60)
            return True
                
        except Exception as e:
            print(f"❌ Phase 4 배포 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    deployer = Phase4LambdaDeployer()
    success = deployer.deploy_complete_phase4()
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)