#!/usr/bin/env python3
"""
🌏 시간대 분석 기능을 추가한 Lambda 함수 배포
- market-sentiment-check 함수를 V2로 업데이트
- 시간대 분석 Lambda 레이어 생성 및 연결
"""

import boto3
import json
import logging
import zipfile
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimezoneEnhancedLambdaDeployer:
    """시간대 분석 강화 Lambda 배포기"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.region = 'ap-northeast-2'
        self.account_id = '901361833359'
        
        self.function_name = 'makenaide-market-sentiment-check'
        self.layer_name = 'makenaide-timezone-analyzer'
        
    def create_lambda_layer(self) -> str:
        """시간대 분석 Lambda 레이어 생성"""
        try:
            logger.info("시간대 분석 Lambda 레이어 생성 중...")
            
            # 레이어 ZIP 파일 읽기
            layer_zip_path = 'timezone_analyzer_layer.zip'
            
            if not os.path.exists(layer_zip_path):
                logger.error(f"레이어 파일이 없습니다: {layer_zip_path}")
                return None
            
            with open(layer_zip_path, 'rb') as f:
                layer_zip_content = f.read()
            
            # 기존 레이어 버전 확인
            try:
                response = self.lambda_client.list_layer_versions(LayerName=self.layer_name)
                existing_versions = len(response.get('LayerVersions', []))
                logger.info(f"기존 레이어 버전: {existing_versions}개")
            except:
                existing_versions = 0
            
            # 새 레이어 버전 생성
            response = self.lambda_client.publish_layer_version(
                LayerName=self.layer_name,
                Description=f'Timezone Market Analyzer Layer - includes TimezoneMarketAnalyzer class and pytz',
                Content={
                    'ZipFile': layer_zip_content
                },
                CompatibleRuntimes=['python3.9', 'python3.10', 'python3.11'],
                CompatibleArchitectures=['x86_64']
            )
            
            layer_arn = response['LayerVersionArn']
            layer_version = response['Version']
            
            logger.info(f"✅ 레이어 생성 완료: {self.layer_name} v{layer_version}")
            logger.info(f"레이어 ARN: {layer_arn}")
            
            return layer_arn
            
        except Exception as e:
            logger.error(f"레이어 생성 실패: {e}")
            return None
    
    def create_lambda_deployment_package(self) -> str:
        """Lambda 함수 배포 패키지 생성"""
        try:
            logger.info("Lambda 배포 패키지 생성 중...")
            
            # 배포용 ZIP 파일 생성
            deployment_zip = 'makenaide_sentiment_v2_deployment.zip'
            
            with zipfile.ZipFile(deployment_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 메인 Lambda 함수 파일
                zipf.write('lambda_market_sentiment_check_v2.py', 'lambda_function.py')
                
                logger.info("✅ Lambda 배포 패키지 생성 완료")
            
            return deployment_zip
            
        except Exception as e:
            logger.error(f"배포 패키지 생성 실패: {e}")
            return None
    
    def update_lambda_function(self, layer_arn: str) -> bool:
        """Lambda 함수 코드 및 설정 업데이트"""
        try:
            logger.info("Lambda 함수 업데이트 중...")
            
            # 배포 패키지 생성
            deployment_zip = self.create_lambda_deployment_package()
            if not deployment_zip:
                return False
            
            # ZIP 파일 읽기
            with open(deployment_zip, 'rb') as f:
                zip_content = f.read()
            
            # 함수 코드 업데이트
            logger.info("함수 코드 업데이트 중...")
            code_response = self.lambda_client.update_function_code(
                FunctionName=self.function_name,
                ZipFile=zip_content,
                Publish=True
            )
            
            logger.info(f"✅ 함수 코드 업데이트 완료: {code_response['Version']}")
            
            # 함수 설정 업데이트 (레이어 연결)
            logger.info("함수 설정 업데이트 중...")
            config_response = self.lambda_client.update_function_configuration(
                FunctionName=self.function_name,
                Description='Market Sentiment Check V2 with Timezone Analysis - 시장 상황 분석 및 시간대별 글로벌 거래량 고려',
                Layers=[layer_arn],  # 시간대 분석 레이어 추가
                Environment={
                    'Variables': {
                        'EC2_INSTANCE_IDS': '',  # 환경에 맞게 설정
                        'RDS_INSTANCE_ID': '',   # 환경에 맞게 설정
                        'SNS_TOPIC_ARN': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-alerts'
                    }
                }
            )
            
            logger.info(f"✅ 함수 설정 업데이트 완료")
            
            # 임시 파일 정리
            os.remove(deployment_zip)
            
            return True
            
        except Exception as e:
            logger.error(f"Lambda 함수 업데이트 실패: {e}")
            return False
    
    def test_updated_function(self) -> bool:
        """업데이트된 함수 테스트"""
        try:
            logger.info("업데이트된 함수 테스트 중...")
            
            # 테스트 이벤트
            test_event = {
                'test': True,
                'source': 'deployment_test',
                'timestamp': '2025-08-06T05:30:00Z'
            }
            
            # 함수 호출
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            # 응답 확인
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read())
                
                logger.info("✅ 함수 테스트 성공!")
                logger.info(f"응답: {json.dumps(payload, indent=2, ensure_ascii=False)}")
                
                return True
            else:
                logger.error(f"함수 테스트 실패: {response['StatusCode']}")
                return False
                
        except Exception as e:
            logger.error(f"함수 테스트 중 오류: {e}")
            return False
    
    def deploy_complete_system(self) -> bool:
        """전체 시스템 배포"""
        logger.info("🚀 시간대 분석 강화 Lambda 시스템 배포 시작")
        logger.info("=" * 80)
        
        try:
            # 1. Lambda 레이어 생성
            layer_arn = self.create_lambda_layer()
            if not layer_arn:
                logger.error("❌ 레이어 생성 실패")
                return False
            
            # 2. Lambda 함수 업데이트
            if not self.update_lambda_function(layer_arn):
                logger.error("❌ Lambda 함수 업데이트 실패")
                return False
            
            # 3. 함수 테스트
            if not self.test_updated_function():
                logger.error("❌ 함수 테스트 실패")
                return False
            
            logger.info("=" * 80)
            logger.info("🎉 시간대 분석 강화 Lambda 시스템 배포 완료!")
            
            print(f"""
✅ 배포 완료!

🌏 시간대 분석 기능 추가:
   • TimezoneMarketAnalyzer 레이어: {self.layer_name}
   • 글로벌 거래 활성도 분석
   • 시간대별 최적 전략 제안
   • 지역별 거래 특성 고려

🔧 업데이트된 기능:
   • 기존 BTC 기반 시장 분석 + 시간대 분석
   • 포지션 크기 자동 조정
   • 리스크 파라미터 동적 설정
   • 주도 지역별 전략 맞춤화

📊 분석 정보 확장:
   • 글로벌 활성도 점수
   • 주도 거래 지역 식별
   • 거래 스타일 최적화
   • 시간대별 권장사항

🎯 다음 단계:
   • EventBridge 스케줄에서 자동 실행
   • S3에 시간대 분석 결과 저장
   • 후속 Phase들이 분석 결과 활용
            """)
            
            return True
            
        except Exception as e:
            logger.error(f"배포 실패: {e}")
            return False

def main():
    """메인 실행"""
    deployer = TimezoneEnhancedLambdaDeployer()
    success = deployer.deploy_complete_system()
    
    if success:
        print("🎉 배포 성공!")
        exit(0)
    else:
        print("❌ 배포 실패!")
        exit(1)

if __name__ == '__main__':
    main()