#!/usr/bin/env python3
"""
🔧 Create Minimal JWT Layer for Lambda Functions
- Create lightweight layer with just essential missing dependencies
- Focus on jwt, openai, and core packages only
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
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MinimalJWTLayerCreator:
    """최소 JWT Layer 생성 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.layer_name = 'makenaide-jwt-layer'
        self.description = 'Minimal layer: jwt, openai, essential packages only'
        self.compatible_runtimes = ['python3.11']
        
        # Lambda 함수 목록 (Phase 2는 제외 - 이미 동작중)
        self.lambda_functions = [
            'makenaide-phase3-gpt-analysis', 
            'makenaide-phase4-4h-analysis',
            'makenaide-phase5-condition-check',
            'makenaide-phase6-trade-execution'
        ]

    def create_minimal_jwt_layer_package(self) -> str:
        """최소 JWT Layer 패키지 생성"""
        try:
            logger.info("📦 최소 JWT Layer 패키지 생성 중...")
            
            # 임시 디렉토리 생성
            temp_dir = tempfile.mkdtemp()
            python_dir = os.path.join(temp_dir, 'python')
            os.makedirs(python_dir)
            
            logger.info(f"임시 디렉토리: {temp_dir}")
            
            # 최소한의 필수 패키지들만
            minimal_packages = [
                'PyJWT==2.8.0',  # jwt 패키지
                'openai==1.3.5',
                'pytz',
                'pyupbit==0.2.34'
            ]
            
            logger.info("최소 필수 패키지 설치 중...")
            for package in minimal_packages:
                try:
                    logger.info(f"  설치 중: {package}")
                    result = subprocess.run([
                        sys.executable, '-m', 'pip', 'install', 
                        package,
                        '--target', python_dir,
                        '--no-cache-dir',
                        '--no-deps',  # 의존성 제외로 크기 최소화
                        '--quiet'
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        logger.info(f"  ✅ {package} 설치 완료")
                    else:
                        logger.warning(f"  ⚠️ {package} 설치 실패, 의존성 포함 재시도...")
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
                            logger.error(f"  ❌ {package} 완전 실패")
                        
                except Exception as e:
                    logger.warning(f"  ❌ {package} 설치 예외: {e}")
            
            # 더 공격적인 파일 정리
            logger.info("공격적인 파일 정리 중...")
            cleanup_patterns = [
                '__pycache__', '*.pyc', '*.pyo', 'tests', 'test', '*.egg-info', 
                'dist-info', 'docs', 'doc', 'examples', 'example', 'bin'
            ]
            
            for root, dirs, files in os.walk(python_dir):
                # 디렉토리 정리
                dirs_to_remove = [d for d in dirs[:] if any(pattern.replace('*', '') in d.lower() for pattern in cleanup_patterns)]
                for d in dirs_to_remove:
                    dirs.remove(d)
                    shutil.rmtree(os.path.join(root, d), ignore_errors=True)
                
                # 파일 정리
                files_to_remove = []
                for file in files:
                    file_lower = file.lower()
                    if any(pattern.replace('*', '') in file_lower for pattern in cleanup_patterns if '*' in pattern):
                        files_to_remove.append(file)
                    elif file.endswith(('.pyc', '.pyo', '.md', '.txt', '.rst')):
                        files_to_remove.append(file)
                
                for file in files_to_remove:
                    try:
                        os.remove(os.path.join(root, file))
                    except:
                        pass
            
            # ZIP 파일 생성
            zip_filename = f'makenaide-jwt-layer-{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            
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
            logger.info(f"✅ 최소 JWT Layer 생성 완료: {zip_filename} ({file_size/1024/1024:.1f}MB)")
            
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 최소 JWT Layer 생성 실패: {e}")
            return None

    def deploy_jwt_layer(self, zip_filename: str) -> str:
        """JWT Layer AWS에 배포"""
        try:
            logger.info(f"🚀 JWT Lambda Layer 배포 중: {zip_filename}")
            
            # ZIP 파일 읽기
            with open(zip_filename, 'rb') as f:
                zip_content = f.read()
            
            # 기존 Layer 버전 삭제 시도
            try:
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
                pass
            
            # Layer 생성
            response = self.lambda_client.publish_layer_version(
                LayerName=self.layer_name,
                Description=self.description,
                Content={'ZipFile': zip_content},
                CompatibleRuntimes=self.compatible_runtimes
            )
            
            layer_version_arn = response['LayerVersionArn']
            
            logger.info(f"✅ JWT Layer 배포 완료")
            logger.info(f"   Version ARN: {layer_version_arn}")
            
            # 파일 정리
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
            
            return layer_version_arn
            
        except Exception as e:
            logger.error(f"❌ JWT Layer 배포 실패: {e}")
            return None

    def update_lambda_functions_with_jwt_layer(self, jwt_layer_arn: str) -> dict:
        """Lambda 함수들에 JWT Layer만 적용 (Phase 2 제외)"""
        try:
            logger.info("🔄 Lambda 함수들에 JWT Layer 적용 중...")
            
            results = {
                'updated': [],
                'failed': []
            }
            
            # JWT Layer만 적용 (크기 제한 회피)
            layer_arns = [jwt_layer_arn]
            
            logger.info(f"적용할 Layer: {jwt_layer_arn}")
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  업데이트 중: {function_name}")
                    
                    # 함수 설정 업데이트 (JWT Layer만)
                    self.lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Layers=layer_arns
                    )
                    
                    results['updated'].append(function_name)
                    logger.info(f"  ✅ {function_name} 업데이트 완료")
                    
                    # 각 함수 업데이트 후 잠시 대기
                    import time
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"  ❌ {function_name} 업데이트 실패: {e}")
                    results['failed'].append(function_name)
            
            logger.info(f"📊 JWT Layer 적용 결과:")
            logger.info(f"   성공: {len(results['updated'])}개")
            logger.info(f"   실패: {len(results['failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 업데이트 실패: {e}")
            return {'updated': [], 'failed': self.lambda_functions}

    def test_jwt_functions(self) -> dict:
        """JWT Layer 적용 후 함수 테스트"""
        try:
            logger.info("🧪 JWT Layer 적용 후 함수 테스트...")
            
            results = {
                'import_test_passed': [],
                'import_test_failed': [],
                'execution_passed': [],
                'execution_failed': []
            }
            
            # import 테스트 이벤트
            import_test_event = {
                'test_type': 'jwt_import_test',
                'source': 'jwt_layer_test',
                'timestamp': datetime.now().isoformat()
            }
            
            import time
            
            for function_name in self.lambda_functions:
                try:
                    logger.info(f"  테스트 중: {function_name}")
                    
                    # Layer 전파 대기
                    time.sleep(3)
                    
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
            
            logger.info(f"📊 JWT Layer 테스트 결과:")
            logger.info(f"   Import 성공: {len(results['import_test_passed'])}개")
            logger.info(f"   Import 실패: {len(results['import_test_failed'])}개")
            logger.info(f"   실행 성공: {len(results['execution_passed'])}개")
            logger.info(f"   실행 실패: {len(results['execution_failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 함수 테스트 실패: {e}")
            return {'import_test_passed': [], 'import_test_failed': self.lambda_functions, 'execution_passed': [], 'execution_failed': []}

    def create_and_deploy_jwt_layer(self) -> bool:
        """JWT Layer 생성 및 배포 전체 프로세스"""
        try:
            logger.info("🚀 최소 JWT Layer 생성 및 배포 시작")
            
            # 1. JWT Layer 생성
            zip_file = self.create_minimal_jwt_layer_package()
            if not zip_file:
                logger.error("❌ JWT Layer 패키지 생성 실패")
                return False
            
            # 2. JWT Layer 배포
            jwt_layer_arn = self.deploy_jwt_layer(zip_file)
            if not jwt_layer_arn:
                logger.error("❌ JWT Layer 배포 실패")
                return False
            
            # 3. Lambda 함수들에 JWT Layer 적용
            update_results = self.update_lambda_functions_with_jwt_layer(jwt_layer_arn)
            
            if not update_results['updated']:
                logger.error("❌ 모든 Lambda 함수 업데이트 실패")
                return False
            
            # 4. 함수들 테스트
            test_results = self.test_jwt_functions()
            
            logger.info("🎉 JWT Layer 배포 및 적용 완료!")
            logger.info(f"   JWT Layer: {jwt_layer_arn}")
            logger.info(f"   업데이트된 함수: {len(update_results['updated'])}개")
            logger.info(f"   Import 테스트 통과: {len(test_results['import_test_passed'])}개")
            
            return len(test_results['import_test_passed']) > 0
            
        except Exception as e:
            logger.error(f"❌ 전체 프로세스 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("🔧 Minimal JWT Layer Creation")
        print("=" * 60)
        
        creator = MinimalJWTLayerCreator()
        
        if creator.create_and_deploy_jwt_layer():
            print("\n✅ JWT Layer 생성 및 배포 성공!")
            print("\n📋 다음 단계:")
            print("1. 전체 워크플로우 재테스트")
            print("2. Phase별 기능 검증")
            print("3. 인프라 설정 완료")
            return True
        else:
            print("\n❌ JWT Layer 배포 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)