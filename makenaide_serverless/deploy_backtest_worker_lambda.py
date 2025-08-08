#!/usr/bin/env python3
"""
분산 백테스팅 워커 Lambda 함수 배포 스크립트

백테스트 작업을 병렬로 처리하는 Lambda 함수를 배포합니다.
기존 백테스팅 모듈을 Lambda Layer로 패키징하고 배포합니다.

Author: Distributed Backtesting Deployment
Version: 1.0.0
"""

import boto3
import json
import logging
import zipfile
import os
from pathlib import Path
import time
import shutil
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BacktestWorkerLambdaDeployer:
    """백테스트 워커 Lambda 배포 클래스"""
    
    def __init__(self, region_name: str = 'ap-northeast-2'):
        self.region = region_name
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.sqs_client = boto3.client('sqs', region_name=region_name)
        
        self.function_name = "makenaide-distributed-backtest-worker"
        self.layer_name = "makenaide-backtesting-modules-layer"
        self.resource_prefix = "makenaide-distributed-backtest"
        
        logger.info(f"🚀 백테스트 워커 Lambda 배포 준비 (리전: {region_name})")
    
    def create_lambda_layer(self) -> str:
        """백테스팅 모듈 Lambda Layer 생성"""
        try:
            logger.info("📦 백테스팅 모듈 Lambda Layer 생성 시작")
            
            # Layer 디렉토리 구조 생성
            layer_dir = Path("lambda_layer_build")
            python_dir = layer_dir / "python"
            
            # 기존 빌드 디렉토리 삭제
            if layer_dir.exists():
                shutil.rmtree(layer_dir)
            
            python_dir.mkdir(parents=True, exist_ok=True)
            
            # 백테스팅 모듈 복사
            backtesting_modules_dir = Path("backtesting_modules")
            if backtesting_modules_dir.exists():
                shutil.copytree(backtesting_modules_dir, python_dir / "backtesting_modules")
                logger.info("✅ 백테스팅 모듈 복사 완료")
            
            # 필요한 추가 파일들 복사
            additional_files = [
                "timezone_market_analyzer.py",
                "utils.py",
                "config.py"
            ]
            
            for file_name in additional_files:
                if Path(file_name).exists():
                    shutil.copy(file_name, python_dir)
                    logger.info(f"✅ {file_name} 복사 완료")
            
            # requirements.txt 생성 및 의존성 설치
            requirements = [
                "pandas>=1.5.0",
                "numpy>=1.21.0", 
                "pytz>=2022.1",
                "psutil>=5.8.0",
                "boto3>=1.26.0"
            ]
            
            requirements_file = python_dir / "requirements.txt"
            with open(requirements_file, "w") as f:
                f.write("\n".join(requirements))
                
            # pip install로 의존성을 Layer에 직접 설치
            logger.info("📦 Python 패키지 설치 중...")
            import subprocess
            import sys
            
            try:
                # Lambda 호환 패키지 설치
                subprocess.run([
                    sys.executable, "-m", "pip", "install", 
                    "-r", str(requirements_file),
                    "-t", str(python_dir),
                    "--platform", "linux_x86_64",
                    "--only-binary=:all:",
                    "--upgrade"
                ], check=True)
                logger.info("✅ Python 패키지 설치 완료")
            except subprocess.CalledProcessError as e:
                logger.warning(f"⚠️ 패키지 설치 실패, 기본 설치로 진행: {e}")
                # 로컬 패키지로라도 설치 시도
                subprocess.run([
                    sys.executable, "-m", "pip", "install",
                    "-r", str(requirements_file),
                    "-t", str(python_dir)
                ], check=False)
            
            # Layer 패키지 압축
            layer_zip_path = f"{self.layer_name}.zip"
            with zipfile.ZipFile(layer_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(layer_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, layer_dir)
                        zipf.write(file_path, arcname)
            
            # Layer 업로드
            with open(layer_zip_path, 'rb') as f:
                layer_content = f.read()
            
            try:
                response = self.lambda_client.publish_layer_version(
                    LayerName=self.layer_name,
                    Description="Makenaide Backtesting Modules Layer",
                    Content={
                        'ZipFile': layer_content
                    },
                    CompatibleRuntimes=['python3.9', 'python3.10'],
                    CompatibleArchitectures=['x86_64']
                )
                
                layer_arn = response['LayerVersionArn']
                logger.info(f"✅ Lambda Layer 생성 완료: {layer_arn}")
                
                # 정리
                os.remove(layer_zip_path)
                shutil.rmtree(layer_dir)
                
                return layer_arn
                
            except Exception as e:
                if "already exists" in str(e):
                    # 기존 레이어 사용
                    layers = self.lambda_client.list_layer_versions(LayerName=self.layer_name)
                    if layers['LayerVersions']:
                        layer_arn = layers['LayerVersions'][0]['LayerVersionArn']
                        logger.info(f"🔄 기존 Layer 사용: {layer_arn}")
                        return layer_arn
                raise e
                
        except Exception as e:
            logger.error(f"❌ Lambda Layer 생성 실패: {e}")
            return ""
    
    def create_lambda_function(self, layer_arn: str) -> str:
        """Lambda 함수 생성"""
        try:
            logger.info("⚡ Lambda 함수 생성 시작")
            
            # Lambda 함수 코드 압축
            function_zip_path = f"{self.function_name}.zip"
            with zipfile.ZipFile(function_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write("lambda_backtest_worker.py", "lambda_function.py")
            
            # 함수 코드 읽기
            with open(function_zip_path, 'rb') as f:
                function_code = f.read()
            
            # 큐 URL 조회
            job_queue_url = self._get_queue_url(f"{self.resource_prefix}-job-queue")
            result_queue_url = self._get_queue_url(f"{self.resource_prefix}-result-queue")
            
            # 환경 변수
            environment = {
                'JOB_QUEUE_URL': job_queue_url,
                'RESULT_QUEUE_URL': result_queue_url,
                'S3_BUCKET': 'makenaide-backtest-data',
                'LOG_LEVEL': 'INFO'
            }
            
            # IAM 역할 ARN
            role_arn = f"arn:aws:iam::901361833359:role/{self.resource_prefix}-lambda-role"
            
            try:
                response = self.lambda_client.create_function(
                    FunctionName=self.function_name,
                    Runtime='python3.9',
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Code={
                        'ZipFile': function_code
                    },
                    Description='Makenaide Distributed Backtesting Worker',
                    Timeout=900,  # 15분
                    MemorySize=1024,
                    Environment={
                        'Variables': environment
                    },
                    Layers=[layer_arn] if layer_arn else [],
                    DeadLetterConfig={
                        'TargetArn': f"arn:aws:sqs:{self.region}:901361833359:{self.resource_prefix}-dlq"
                    }
                )
                
                function_arn = response['FunctionArn']
                logger.info(f"✅ Lambda 함수 생성 완료: {function_arn}")
                
                # 정리
                os.remove(function_zip_path)
                
                return function_arn
                
            except Exception as e:
                if "already exists" in str(e) or "ResourceConflictException" in str(e):
                    # 기존 함수 업데이트
                    logger.info("🔄 기존 Lambda 함수 업데이트")
                    
                    # 코드 업데이트
                    self.lambda_client.update_function_code(
                        FunctionName=self.function_name,
                        ZipFile=function_code
                    )
                    
                    # 설정 업데이트
                    self.lambda_client.update_function_configuration(
                        FunctionName=self.function_name,
                        Environment={'Variables': environment},
                        Layers=[layer_arn] if layer_arn else []
                    )
                    
                    function_arn = f"arn:aws:lambda:{self.region}:901361833359:function:{self.function_name}"
                    logger.info(f"✅ Lambda 함수 업데이트 완료: {function_arn}")
                    
                    os.remove(function_zip_path)
                    return function_arn
                else:
                    raise e
                
        except Exception as e:
            logger.error(f"❌ Lambda 함수 생성 실패: {e}")
            return ""
    
    def setup_sqs_trigger(self, function_arn: str) -> bool:
        """SQS 트리거 설정"""
        try:
            logger.info("🔗 SQS 트리거 설정 시작")
            
            job_queue_url = self._get_queue_url(f"{self.resource_prefix}-job-queue")
            if not job_queue_url:
                logger.error("❌ 작업 큐 URL을 찾을 수 없음")
                return False
            
            # 큐 ARN 생성
            queue_arn = f"arn:aws:sqs:{self.region}:901361833359:{self.resource_prefix}-job-queue"
            
            try:
                # 이벤트 소스 매핑 생성
                response = self.lambda_client.create_event_source_mapping(
                    EventSourceArn=queue_arn,
                    FunctionName=self.function_name,
                    BatchSize=1,  # 한 번에 하나씩 처리
                    MaximumBatchingWindowInSeconds=10
                )
                
                logger.info(f"✅ SQS 트리거 설정 완료: {response['UUID']}")
                return True
                
            except Exception as e:
                if "already exists" in str(e) or "ResourceConflictException" in str(e):
                    logger.info("🔄 기존 SQS 트리거 사용")
                    return True
                else:
                    raise e
                
        except Exception as e:
            logger.error(f"❌ SQS 트리거 설정 실패: {e}")
            return False
    
    def _get_queue_url(self, queue_name: str) -> str:
        """큐 URL 조회"""
        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except Exception as e:
            logger.warning(f"⚠️ 큐 URL 조회 실패 ({queue_name}): {e}")
            return ""
    
    def wait_for_lambda_ready(self, function_name: str, timeout: int = 60) -> bool:
        """Lambda 함수가 준비될 때까지 대기"""
        try:
            logger.info(f"⏳ Lambda 함수 준비 대기: {function_name}")
            
            for i in range(timeout):
                try:
                    response = self.lambda_client.get_function(FunctionName=function_name)
                    state = response['Configuration']['State']
                    
                    if state == 'Active':
                        logger.info(f"✅ Lambda 함수 준비 완료: {function_name}")
                        return True
                    elif state == 'Failed':
                        logger.error(f"❌ Lambda 함수 실패 상태: {function_name}")
                        return False
                    
                    if i % 10 == 0:  # 10초마다 상태 로그
                        logger.info(f"   상태: {state} - 대기 중... ({i}/{timeout}초)")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    if i < timeout - 1:  # 마지막 시도가 아니면 계속
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            logger.error(f"❌ Lambda 함수 준비 시간 초과: {function_name}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Lambda 함수 상태 확인 실패: {e}")
            return False
    
    def test_lambda_function(self, function_arn: str) -> bool:
        """Lambda 함수 테스트"""
        try:
            logger.info("🧪 Lambda 함수 테스트 시작")
            
            # 함수가 준비될 때까지 대기
            if not self.wait_for_lambda_ready(self.function_name):
                logger.error("❌ Lambda 함수가 준비되지 않아 테스트를 건너뜁니다")
                return False
            
            # 테스트 페이로드
            test_payload = {
                'job_data': {
                    'job_id': f'test-{int(time.time())}',
                    'job_type': 'SINGLE_STRATEGY',
                    'strategy_name': 'Test_Strategy',
                    'parameters': {
                        'position_size_method': 'percent',
                        'position_size_value': 0.1,
                        'stop_loss_pct': 0.05
                    },
                    'data_range': {
                        'start_date': '2024-01-01',
                        'end_date': '2024-01-31'
                    }
                }
            }
            
            # Lambda 호출
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_payload)
            )
            
            # 결과 확인
            result_payload = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200 and result_payload['statusCode'] == 200:
                logger.info("✅ Lambda 함수 테스트 성공")
                
                # 결과 상세 출력
                result_body = json.loads(result_payload['body'])
                logger.info(f"   처리된 작업 수: {result_body.get('processed_jobs', 0)}")
                
                return True
            else:
                logger.error(f"❌ Lambda 함수 테스트 실패: {result_payload}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Lambda 함수 테스트 실패: {e}")
            return False
    
    def deploy_complete_system(self) -> Dict[str, Any]:
        """전체 시스템 배포"""
        try:
            logger.info("🏗️ 분산 백테스팅 워커 Lambda 전체 배포 시작")
            deployment_start = datetime.now()
            
            results = {
                "deployment_timestamp": deployment_start.isoformat(),
                "function_name": self.function_name,
                "layer_name": self.layer_name
            }
            
            # 1. Lambda Layer 생성
            logger.info("\n📦 1단계: Lambda Layer 생성")
            layer_arn = self.create_lambda_layer()
            results["layer_arn"] = layer_arn
            
            if not layer_arn:
                raise Exception("Lambda Layer 생성 실패")
            
            # 2. Lambda 함수 생성
            logger.info("\n⚡ 2단계: Lambda 함수 생성")
            function_arn = self.create_lambda_function(layer_arn)
            results["function_arn"] = function_arn
            
            if not function_arn:
                raise Exception("Lambda 함수 생성 실패")
            
            # 3. SQS 트리거 설정
            logger.info("\n🔗 3단계: SQS 트리거 설정")
            trigger_success = self.setup_sqs_trigger(function_arn)
            results["sqs_trigger_configured"] = trigger_success
            
            # 4. Lambda 함수 테스트
            logger.info("\n🧪 4단계: Lambda 함수 테스트")
            test_success = self.test_lambda_function(function_arn)
            results["test_success"] = test_success
            
            # 배포 완료 시간
            deployment_end = datetime.now()
            deployment_duration = (deployment_end - deployment_start).total_seconds()
            results["deployment_duration_seconds"] = deployment_duration
            
            # 배포 요약
            logger.info(f"\n🎉 분산 백테스팅 워커 Lambda 배포 완료!")
            logger.info(f"   ⏱️  배포 소요 시간: {deployment_duration:.2f}초")
            logger.info(f"   📦 Lambda Layer: {'생성됨' if layer_arn else '실패'}")
            logger.info(f"   ⚡ Lambda 함수: {'생성됨' if function_arn else '실패'}")
            logger.info(f"   🔗 SQS 트리거: {'설정됨' if trigger_success else '실패'}")
            logger.info(f"   🧪 기능 테스트: {'성공' if test_success else '실패'}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 전체 배포 실패: {e}")
            return {"error": str(e)}

def main():
    """메인 실행 함수"""
    print("⚡ 분산 백테스팅 워커 Lambda 배포 시작")
    print("=" * 80)
    
    try:
        deployer = BacktestWorkerLambdaDeployer()
        results = deployer.deploy_complete_system()
        
        if "error" not in results:
            print("\n✅ Phase 2: 분산 처리 엔진 구축 완료!")
            print("📋 배포 결과:")
            print(f"   📦 Layer ARN: {results.get('layer_arn', 'N/A')}")
            print(f"   ⚡ Function ARN: {results.get('function_arn', 'N/A')}")
            print(f"   🔗 SQS 트리거: {'설정됨' if results.get('sqs_trigger_configured') else '실패'}")
            print(f"   🧪 테스트: {'성공' if results.get('test_success') else '실패'}")
            
            if results.get('test_success'):
                print(f"\n🎯 다음 단계: Phase 3 - 백테스트 결과 수집 및 통합 시스템")
            else:
                print(f"\n⚠️ 테스트 실패 - 설정을 확인해주세요")
                
        else:
            print(f"❌ 배포 실패: {results['error']}")
    
    except Exception as e:
        print(f"❌ 전체 배포 실패: {e}")

if __name__ == "__main__":
    main()