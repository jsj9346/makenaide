#!/usr/bin/env python3
"""
📦 Create Comprehensive Lambda Layer with Pandas & Scientific Libraries
- Use AWS Lambda Pandas layer + custom dependencies
- Deploy comprehensive layer for all required libraries
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

class ComprehensiveLambdaLayerCreator:
    """포괄적 Lambda Layer 생성 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.layer_name = 'makenaide-comprehensive-layer'
        self.description = 'Comprehensive layer: pandas, numpy, pyupbit, talib, etc.'
        self.compatible_runtimes = ['python3.11']
        
        # AWS 공식 Pandas Layer (Pandas + NumPy 포함)
        self.aws_pandas_layer = 'arn:aws:lambda:ap-northeast-2:336392948345:layer:AWSSDKPandas-Python311:9'
        
        # Lambda 함수 목록
        self.lambda_functions = [
            'makenaide-phase2-comprehensive-filter',
            'makenaide-phase3-gpt-analysis', 
            'makenaide-phase4-4h-analysis',
            'makenaide-phase5-condition-check',
            'makenaide-phase6-trade-execution'
        ]

    def create_custom_layer_package(self) -> str:
        """커스텀 라이브러리 Layer 패키지 생성"""
        try:
            logger.info("📦 커스텀 라이브러리 Layer 패키지 생성 중...")
            
            # 임시 디렉토리 생성
            temp_dir = tempfile.mkdtemp()
            python_dir = os.path.join(temp_dir, 'python')
            os.makedirs(python_dir)
            
            logger.info(f"임시 디렉토리: {temp_dir}")
            
            # AWS Pandas Layer에 없는 추가 패키지들
            additional_packages = [
                'pyupbit==0.2.34',
                'pytz',
                'requests',
                'Pillow==9.5.0',
                'matplotlib==3.7.2', 
                'yfinance==0.2.18',
                'openai==1.3.5',
                'ta==0.10.2'  # TA-Lib 대신 ta 라이브러리 사용
            ]
            
            logger.info("추가 패키지 설치 중...")
            for package in additional_packages:
                try:
                    logger.info(f"  설치 중: {package}")
                    result = subprocess.run([
                        sys.executable, '-m', 'pip', 'install', 
                        package,
                        '--target', python_dir,
                        '--no-cache-dir',
                        '--no-deps',
                        '--quiet'
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        logger.info(f"  ✅ {package} 설치 완료")
                    else:
                        logger.warning(f"  ⚠️ {package} 설치 실패: {result.stderr}")
                        # 의존성 포함하여 재시도
                        result2 = subprocess.run([
                            sys.executable, '-m', 'pip', 'install', 
                            package,
                            '--target', python_dir,
                            '--no-cache-dir',
                            '--quiet'
                        ], capture_output=True, text=True)
                        if result2.returncode == 0:
                            logger.info(f"  ✅ {package} 의존성 포함 설치 완료")
                        else:
                            logger.warning(f"  ❌ {package} 완전 실패")
                        
                except Exception as e:
                    logger.warning(f"  ❌ {package} 설치 예외: {e}")
            
            # 불필요한 파일 정리 (용량 절약)
            logger.info("불필요한 파일 정리 중...")
            cleanup_patterns = ['__pycache__', '*.pyc', '*.pyo', 'tests', 'test', '*.egg-info']
            
            for root, dirs, files in os.walk(python_dir):
                # 디렉토리 정리
                dirs_to_remove = [d for d in dirs if any(pattern in d for pattern in cleanup_patterns)]
                for d in dirs_to_remove:
                    shutil.rmtree(os.path.join(root, d), ignore_errors=True)
                
                # 파일 정리
                for file in files:
                    if any(pattern.replace('*', '') in file for pattern in cleanup_patterns if '*' in pattern):
                        try:
                            os.remove(os.path.join(root, file))
                        except:
                            pass
            
            # ZIP 파일 생성
            zip_filename = f'makenaide-comprehensive-layer-{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            
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
            logger.info(f"✅ 커스텀 Layer 생성 완료: {zip_filename} ({file_size/1024/1024:.1f}MB)")
            
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 커스텀 Layer 생성 실패: {e}")
            return None

    def deploy_custom_layer(self, zip_filename: str) -> str:
        """커스텀 Layer AWS에 배포"""
        try:
            logger.info(f"🚀 커스텀 Lambda Layer 배포 중: {zip_filename}")
            
            # ZIP 파일 읽기
            with open(zip_filename, 'rb') as f:
                zip_content = f.read()
            
            # 기존 Layer 버전 삭제 시도 (새 버전 생성을 위해)
            try:
                # Layer 버전들 조회
                versions_response = self.lambda_client.list_layer_versions(LayerName=self.layer_name)
                for version_info in versions_response.get('LayerVersions', []):
                    version_number = version_info['Version']
                    try:
                        self.lambda_client.delete_layer_version(
                            LayerName=self.layer_name,
                            VersionNumber=version_number
                        )
                        logger.info(f"  기존 버전 {version_number} 삭제")
                    except:
                        pass
            except:
                pass  # Layer가 없을 수도 있음
            
            # Layer 생성
            response = self.lambda_client.publish_layer_version(
                LayerName=self.layer_name,
                Description=self.description,
                Content={'ZipFile': zip_content},
                CompatibleRuntimes=self.compatible_runtimes
            )
            
            layer_version_arn = response['LayerVersionArn']
            
            logger.info(f"✅ 커스텀 Layer 배포 완료")
            logger.info(f"   Version ARN: {layer_version_arn}")
            
            # 파일 정리
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
            
            return layer_version_arn
            
        except Exception as e:
            logger.error(f"❌ 커스텀 Layer 배포 실패: {e}")
            return None

    def update_lambda_functions_with_comprehensive_layers(self, custom_layer_arn: str) -> dict:
        """모든 Lambda 함수에 포괄적 Layer들 적용 (AWS Pandas + 커스텀)"""
        try:
            logger.info("🔄 Lambda 함수들에 포괄적 Layer 적용 중...")
            
            results = {
                'updated': [],
                'failed': []
            }
            
            # 사용할 Layer ARN 목록 (AWS Pandas + 우리 커스텀)
            layer_arns = [
                self.aws_pandas_layer,  # AWS 공식 Pandas Layer (pandas, numpy 포함)
                custom_layer_arn        # 우리 커스텀 Layer (pyupbit, openai 등)
            ]
            
            logger.info(f"적용할 Layer들:")
            logger.info(f"  AWS Pandas Layer: {self.aws_pandas_layer}")
            logger.info(f"  Custom Layer: {custom_layer_arn}")
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  업데이트 중: {function_name}")
                    
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
            
            logger.info(f"📊 포괄적 Layer 적용 결과:")
            logger.info(f"   성공: {len(results['updated'])}개")
            logger.info(f"   실패: {len(results['failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 업데이트 실패: {e}")
            return {'updated': [], 'failed': self.lambda_functions}

    def test_functions_after_layers(self) -> dict:
        """Layer 적용 후 함수 테스트"""
        try:
            logger.info("🧪 포괄적 Layer 적용 후 함수 테스트...")
            
            results = {
                'import_test_passed': [],
                'import_test_failed': [],
                'execution_passed': [],
                'execution_failed': []
            }
            
            # pandas import 테스트 이벤트
            import_test_event = {
                'test_type': 'import_test',
                'source': 'comprehensive_layer_test',
                'timestamp': datetime.now().isoformat()
            }
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  테스트 중: {function_name}")
                    
                    response = self.lambda_client.invoke(
                        FunctionName=function_name,
                        InvocationType='RequestResponse',
                        Payload=json.dumps(import_test_event)
                    )
                    
                    if response['StatusCode'] == 200:
                        payload = json.loads(response['Payload'].read().decode('utf-8'))
                        
                        if 'errorMessage' in payload:
                            error_msg = payload['errorMessage']
                            if 'No module named' in error_msg:
                                results['import_test_failed'].append(function_name)
                                logger.warning(f"  ⚠️ {function_name} import 실패: {error_msg}")
                            else:
                                results['execution_failed'].append(function_name)
                                logger.warning(f"  ⚠️ {function_name} 실행 오류: {error_msg}")
                        else:
                            results['import_test_passed'].append(function_name)
                            results['execution_passed'].append(function_name)
                            logger.info(f"  ✅ {function_name} 테스트 통과")
                    else:
                        results['execution_failed'].append(function_name)
                        logger.warning(f"  ⚠️ {function_name} HTTP 오류: {response['StatusCode']}")
                        
                except Exception as e:
                    logger.warning(f"  ⚠️ {function_name} 테스트 예외: {e}")
                    results['execution_failed'].append(function_name)
            
            logger.info(f"📊 테스트 결과:")
            logger.info(f"   Import 성공: {len(results['import_test_passed'])}개")
            logger.info(f"   Import 실패: {len(results['import_test_failed'])}개")  
            logger.info(f"   실행 성공: {len(results['execution_passed'])}개")
            logger.info(f"   실행 실패: {len(results['execution_failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 함수 테스트 실패: {e}")
            return {'import_test_passed': [], 'import_test_failed': self.lambda_functions, 'execution_passed': [], 'execution_failed': []}

    def create_and_deploy_comprehensive_layer(self) -> bool:
        """포괄적 Layer 생성 및 배포 전체 프로세스"""
        try:
            logger.info("🚀 포괄적 Lambda Layer 생성 및 배포 시작")
            
            # 1. 커스텀 Layer 생성
            zip_file = self.create_custom_layer_package()
            if not zip_file:
                logger.error("❌ 커스텀 Layer 패키지 생성 실패")
                return False
            
            # 2. 커스텀 Layer 배포
            custom_layer_arn = self.deploy_custom_layer(zip_file)
            if not custom_layer_arn:
                logger.error("❌ 커스텀 Layer 배포 실패")
                return False
            
            # 3. Lambda 함수들에 포괄적 Layer 적용 (AWS Pandas + 커스텀)
            update_results = self.update_lambda_functions_with_comprehensive_layers(custom_layer_arn)
            
            if not update_results['updated']:
                logger.error("❌ 모든 Lambda 함수 업데이트 실패")
                return False
            
            # 4. 함수들 테스트
            test_results = self.test_functions_after_layers()
            
            logger.info("🎉 포괄적 Lambda Layer 배포 및 적용 완료!")
            logger.info(f"   AWS Pandas Layer: {self.aws_pandas_layer}")
            logger.info(f"   Custom Layer: {custom_layer_arn}")
            logger.info(f"   업데이트된 함수: {len(update_results['updated'])}개")
            logger.info(f"   Import 테스트 통과: {len(test_results['import_test_passed'])}개")
            
            return len(test_results['import_test_passed']) > 0
            
        except Exception as e:
            logger.error(f"❌ 전체 프로세스 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("📦 Makenaide Comprehensive Lambda Layer Creation")
        print("=" * 60)
        
        creator = ComprehensiveLambdaLayerCreator()
        
        if creator.create_and_deploy_comprehensive_layer():
            print("\n✅ 포괄적 Layer 생성 및 배포 성공!")
            print("\n📋 다음 단계:")
            print("1. Phase 2 실제 데이터 테스트")
            print("2. 전체 워크플로우 검증")
            print("3. API 키 설정")
            return True
        else:
            print("\n❌ 포괄적 Layer 배포 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    import json
    success = main()
    sys.exit(0 if success else 1)