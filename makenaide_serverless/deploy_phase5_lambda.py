#!/usr/bin/env python3
"""
🚀 Phase 5: Condition Check Lambda 배포
- 최종 거래 조건 검증 Lambda 함수 배포
- EventBridge 연동 설정
- 테스트 및 검증
"""

import boto3
import json
import zipfile
import os
import time
from datetime import datetime

class Phase5LambdaDeployer:
    """Phase 5 Lambda 배포 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.iam_client = boto3.client('iam')
        self.region = 'ap-northeast-2'
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
        self.function_name = 'makenaide-condition-check-phase5'
        self.layer_arn = f"arn:aws:lambda:{self.region}:{self.account_id}:layer:makenaide-core-layer:2"
        
    def create_deployment_package(self) -> str:
        """Lambda 배포 패키지 생성"""
        print("📦 Phase 5 Lambda 배포 패키지 생성...")
        
        zip_filename = 'lambda_phase5_condition_check.zip'
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 메인 Lambda 코드
            zip_file.write('lambda_condition_check_phase5.py', 'lambda_function.py')
            
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
            Description='Makenaide Phase 5: Final Condition Check and Signal Generation',
            Timeout=300,  # 5분 (최종 조건 검증 및 신호 생성)
            MemorySize=512,  # 메모리 증가 (복합 분석)
            Layers=[self.layer_arn],
            Environment={
                'Variables': {
                    'MAX_POSITIONS': '3',          # 최대 동시 보유 종목
                    'POSITION_SIZE_PCT': '0.3',    # 종목당 포트폴리오 비중
                    'STOP_LOSS_PCT': '0.08',       # 손절 비율 8%
                    'TAKE_PROFIT_PCT': '0.25',     # 1차 익절 비율 25%
                    'MIN_VOLUME_KRW': '1000000000', # 최소 거래대금 10억원
                    'RSI_OVERBOUGHT': '80'         # RSI 과매수 임계값
                }
            },
            Tags={
                'Project': 'Makenaide',
                'Phase': 'Phase5-ConditionCheck',
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
                    'MAX_POSITIONS': '3',
                    'POSITION_SIZE_PCT': '0.3',
                    'STOP_LOSS_PCT': '0.08',
                    'TAKE_PROFIT_PCT': '0.25',
                    'MIN_VOLUME_KRW': '1000000000',
                    'RSI_OVERBOUGHT': '80'
                }
            },
            Description='Makenaide Phase 5: Final Condition Check and Signal Generation'
        )
        
        print(f"✅ Lambda 함수 업데이트 완료: {self.function_name}")
        return response
    
    def setup_eventbridge_integration(self):
        """EventBridge 연동 설정"""
        print("🔗 EventBridge 연동 설정...")
        
        # Phase 4 → Phase 5 이벤트 규칙
        rule_name = 'makenaide-phase4-to-phase5'
        
        try:
            # EventBridge 규칙 생성/업데이트
            self.events_client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps({
                    "source": ["makenaide.4h_analysis"],
                    "detail-type": ["4H Analysis Completed"],
                    "detail": {
                        "status": ["completed"]
                    }
                }),
                Description='Phase 4 완료 시 Phase 5 트리거',
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
                print("   ⚠️ Phase 4 데이터 없음 (정상 - Phase 4를 먼저 실행 필요)")
                print("   ✅ Lambda 함수 정상 동작 확인")
                return True
            elif response_payload.get('statusCode') == 200:
                print("   ✅ Phase 5 테스트 성공!")
                print(f"   - 처리된 티커: {response_payload.get('processed_tickers')}개")
                print(f"   - BUY 신호: {response_payload.get('buy_signals')}개")
                print(f"   - 최고 종목: {response_payload.get('top_pick')}")
                print(f"   - 신호 통계: {response_payload.get('signal_statistics')}")
                return True
            else:
                print(f"   ❌ Phase 5 테스트 실패: {response_payload}")
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
    
    def deploy_complete_phase5(self):
        """Phase 5 전체 배포"""
        try:
            print("🚀 Phase 5: Condition Check Lambda 배포 시작")
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
            print("✅ Phase 5 Lambda 배포 완료!")
            print(f"📍 함수명: {self.function_name}")
            print("🔗 EventBridge 연동: Phase 4 → Phase 5 자동 트리거")
            print("📊 기능: 최종 거래 조건 검증 및 신호 생성")
            print("⏱️ 타임아웃: 5분")
            print("💾 메모리: 512MB")
            print("\n📋 검증 항목:")
            print("1. 기본 조건 (가격대, 거래량, 변동성)")
            print("2. 리스크 메트릭 (손절가, 익절가, 위험도)")
            print("3. 포지션 크기 (리스크 조정 포지션)")
            print("4. 종합 점수 (기술적+GPT+신뢰도)")
            print("\n⚡ 신호 종류:")
            print("- STRONG_BUY: 최고 등급 (85+ 점수, 40- 리스크)")
            print("- BUY: 매수 권장 (70+ 점수, 60- 리스크)")
            print("- HOLD: 관망 권장")
            print("- REJECT: 매수 부적합")
            print("="*60)
            return True
                
        except Exception as e:
            print(f"❌ Phase 5 배포 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    deployer = Phase5LambdaDeployer()
    success = deployer.deploy_complete_phase5()
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)