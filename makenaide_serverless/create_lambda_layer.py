#!/usr/bin/env python3
"""
📦 Lambda Layer Creation for Makenaide Dependencies
- Create Lambda Layer with pandas, pyupbit, and other required libraries
- Deploy layer and update all Lambda functions to use it
"""

import boto3
import os
import sys
import subprocess
import zipfile
import tempfile
import shutil
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MakenaideLambdaLayerCreator:
    """Makenaide Lambda Layer 생성 및 배포 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.layer_name = 'makenaide-dependencies-layer'
        self.description = 'Makenaide Python dependencies: pandas, pyupbit, numpy, etc.'
        self.compatible_runtimes = ['python3.11']
        
        # Lambda 함수 목록
        self.lambda_functions = [
            'makenaide-phase2-comprehensive-filter',
            'makenaide-phase3-gpt-analysis', 
            'makenaide-phase4-4h-analysis',
            'makenaide-phase5-condition-check',
            'makenaide-phase6-trade-execution'
        ]

    def create_layer_package(self) -> str:
        """Lambda Layer 패키지 생성"""
        try:
            logger.info("📦 Lambda Layer 패키지 생성 시작...")
            
            # 임시 디렉토리 생성
            temp_dir = tempfile.mkdtemp()
            python_dir = os.path.join(temp_dir, 'python')
            os.makedirs(python_dir)
            
            logger.info(f"임시 디렉토리: {temp_dir}")
            
            # 필수 라이브러리만 선별하여 설치
            essential_packages = [
                'pandas==2.0.3',
                'numpy==1.24.3', 
                'pyupbit==0.2.34',
                'boto3==1.28.44',
                'pytz',
                'requests',
                'Pillow',
                'matplotlib==3.7.2'
            ]
            
            logger.info("필수 패키지 설치 중...")
            for package in essential_packages:
                try:
                    logger.info(f"  설치 중: {package}")
                    subprocess.run([
                        sys.executable, '-m', 'pip', 'install', 
                        package,
                        '--target', python_dir,
                        '--no-deps'  # 의존성 충돌 방지
                    ], check=True, capture_output=True, text=True)
                except subprocess.CalledProcessError as e:
                    logger.warning(f"  {package} 설치 실패, 계속 진행: {e}")
            
            # ZIP 파일 생성
            zip_filename = f'makenaide-layer-{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            
            logger.info(f"ZIP 파일 생성 중: {zip_filename}")
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(python_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zipf.write(file_path, arcname)
            
            # 임시 디렉토리 정리
            shutil.rmtree(temp_dir)
            
            # 파일 크기 확인
            file_size = os.path.getsize(zip_filename)
            logger.info(f"✅ Layer 패키지 생성 완료: {zip_filename} ({file_size/1024/1024:.1f}MB)")
            
            if file_size > 250 * 1024 * 1024:  # 250MB 제한
                logger.warning("⚠️ Layer 크기가 250MB를 초과했습니다. S3 업로드 방식을 고려하세요.")
            
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ Layer 패키지 생성 실패: {e}")
            return None

    def create_lightweight_layer(self) -> str:
        """최소한의 경량화된 Layer 생성"""
        try:
            logger.info("📦 경량화된 Lambda Layer 생성 중...")
            
            # 임시 디렉토리 생성
            temp_dir = tempfile.mkdtemp()
            python_dir = os.path.join(temp_dir, 'python')
            os.makedirs(python_dir)
            
            # 가장 필수적인 패키지만 설치
            core_packages = [
                'pyupbit==0.2.34',
                'pytz',
                'requests'
            ]
            
            logger.info("핵심 패키지만 설치...")
            for package in core_packages:
                try:
                    logger.info(f"  설치 중: {package}")
                    result = subprocess.run([
                        sys.executable, '-m', 'pip', 'install', 
                        package,
                        '--target', python_dir,
                        '--no-cache-dir',
                        '--no-deps'
                    ], capture_output=True, text=True)
                    
                    if result.returncode != 0:
                        logger.warning(f"  {package} 설치 실패: {result.stderr}")
                    else:
                        logger.info(f"  ✅ {package} 설치 완료")
                        
                except Exception as e:
                    logger.warning(f"  {package} 설치 에러: {e}")
            
            # ZIP 파일 생성
            zip_filename = f'makenaide-lightweight-layer-{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            
            logger.info(f"ZIP 파일 생성: {zip_filename}")
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(python_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zipf.write(file_path, arcname)
            
            # 정리
            shutil.rmtree(temp_dir)
            
            file_size = os.path.getsize(zip_filename)
            logger.info(f"✅ 경량 Layer 생성 완료: {zip_filename} ({file_size/1024/1024:.1f}MB)")
            
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 경량 Layer 생성 실패: {e}")
            return None

    def deploy_layer(self, zip_filename: str) -> str:
        """Layer AWS에 배포"""
        try:
            logger.info(f"🚀 Lambda Layer 배포 중: {zip_filename}")
            
            # ZIP 파일 읽기
            with open(zip_filename, 'rb') as f:
                zip_content = f.read()
            
            # Layer 생성
            response = self.lambda_client.publish_layer_version(
                LayerName=self.layer_name,
                Description=self.description,
                Content={'ZipFile': zip_content},
                CompatibleRuntimes=self.compatible_runtimes
            )
            
            layer_arn = response['LayerArn']
            layer_version_arn = response['LayerVersionArn']
            
            logger.info(f"✅ Layer 배포 완료")
            logger.info(f"   Layer ARN: {layer_arn}")
            logger.info(f"   Version ARN: {layer_version_arn}")
            
            # 파일 정리
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
            
            return layer_version_arn
            
        except Exception as e:
            logger.error(f"❌ Layer 배포 실패: {e}")
            return None

    def update_lambda_functions_with_layer(self, layer_version_arn: str) -> dict:
        """모든 Lambda 함수에 Layer 적용"""
        try:
            logger.info("🔄 Lambda 함수들에 Layer 적용 중...")
            
            results = {
                'updated': [],
                'failed': []
            }
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  업데이트 중: {function_name}")
                    
                    # 현재 함수 설정 조회
                    current_config = self.lambda_client.get_function_configuration(
                        FunctionName=function_name
                    )
                    
                    # 기존 Layer들 유지하고 새 Layer 추가
                    current_layers = current_config.get('Layers', [])
                    layer_arns = [layer['Arn'] for layer in current_layers]
                    
                    # 새 Layer ARN 추가 (중복 방지)
                    if layer_version_arn not in layer_arns:
                        layer_arns.append(layer_version_arn)
                    
                    # 함수 설정 업데이트
                    self.lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Layers=layer_arns
                    )
                    
                    results['updated'].append(function_name)
                    logger.info(f"  ✅ {function_name} 업데이트 완료")
                    
                except Exception as e:
                    logger.error(f"  ❌ {function_name} 업데이트 실패: {e}")
                    results['failed'].append(function_name)
            
            logger.info(f"📊 Layer 적용 결과:")
            logger.info(f"   성공: {len(results['updated'])}개")
            logger.info(f"   실패: {len(results['failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 업데이트 실패: {e}")
            return {'updated': [], 'failed': self.lambda_functions}

    def test_functions_after_layer(self) -> dict:
        """Layer 적용 후 함수 테스트"""
        try:
            logger.info("🧪 Layer 적용 후 함수 테스트...")
            
            results = {
                'passed': [],
                'failed': []
            }
            
            test_event = {
                'test': True,
                'source': 'layer_test',
                'timestamp': datetime.now().isoformat()
            }
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  테스트 중: {function_name}")
                    
                    response = self.lambda_client.invoke(
                        FunctionName=function_name,
                        InvocationType='RequestResponse',
                        Payload=str.encode(str(test_event))
                    )
                    
                    if response['StatusCode'] == 200:
                        results['passed'].append(function_name)
                        logger.info(f"  ✅ {function_name} 테스트 통과")
                    else:
                        results['failed'].append(function_name)
                        logger.warning(f"  ⚠️ {function_name} 테스트 실패: {response['StatusCode']}")
                        
                except Exception as e:
                    logger.warning(f"  ⚠️ {function_name} 테스트 에러: {e}")
                    results['failed'].append(function_name)
            
            logger.info(f"📊 테스트 결과:")
            logger.info(f"   통과: {len(results['passed'])}개")
            logger.info(f"   실패: {len(results['failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 함수 테스트 실패: {e}")
            return {'passed': [], 'failed': self.lambda_functions}

    def create_and_deploy_layer(self) -> bool:
        """Layer 생성 및 배포 전체 프로세스"""
        try:
            logger.info("🚀 Lambda Layer 생성 및 배포 시작")
            
            # 1. 경량화된 Layer 먼저 시도
            zip_file = self.create_lightweight_layer()
            if not zip_file:
                logger.error("❌ Layer 패키지 생성 실패")
                return False
            
            # 2. Layer 배포
            layer_version_arn = self.deploy_layer(zip_file)
            if not layer_version_arn:
                logger.error("❌ Layer 배포 실패")
                return False
            
            # 3. Lambda 함수들에 Layer 적용
            update_results = self.update_lambda_functions_with_layer(layer_version_arn)
            
            if not update_results['updated']:
                logger.error("❌ 모든 Lambda 함수 업데이트 실패")
                return False
            
            # 4. 함수들 테스트
            test_results = self.test_functions_after_layer()
            
            logger.info("🎉 Lambda Layer 배포 및 적용 완료!")
            logger.info(f"   Layer ARN: {layer_version_arn}")
            logger.info(f"   업데이트된 함수: {len(update_results['updated'])}개")
            logger.info(f"   테스트 통과 함수: {len(test_results['passed'])}개")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 전체 프로세스 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("📦 Makenaide Lambda Layer Creation & Deployment")
        print("=" * 60)
        
        creator = MakenaideLambdaLayerCreator()
        
        if creator.create_and_deploy_layer():
            print("\n✅ Layer 생성 및 배포 성공!")
            print("\n📋 다음 단계:")
            print("1. 함수 로그 확인")
            print("2. 실제 데이터로 테스트")
            print("3. 필요시 추가 라이브러리 설치")
            return True
        else:
            print("\n❌ Layer 배포 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)