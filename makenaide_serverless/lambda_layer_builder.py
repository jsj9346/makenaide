#!/usr/bin/env python3
"""
📚 Makenaide Core Layer 빌드 스크립트
- 모든 Lambda 함수에서 공유할 핵심 의존성 패키지
- 99.6% 크기 최적화 경험을 활용한 효율적 Layer 구축
"""

import os
import subprocess
import shutil
import zipfile
import json
import boto3
from pathlib import Path

class MakenaideCoreLayerBuilder:
    def __init__(self):
        self.layer_name = "makenaide-core-layer" 
        self.build_dir = Path("lambda_layer_build")
        self.python_dir = self.build_dir / "python"
        
    def clean_build_directory(self):
        """빌드 디렉토리 정리"""
        if self.build_dir.exists():
            shutil.rmtree(self.build_dir)
        self.python_dir.mkdir(parents=True, exist_ok=True)
        print(f"🧹 빌드 디렉토리 정리 완료: {self.build_dir}")
    
    def create_requirements_file(self):
        """핵심 의존성 requirements.txt 생성"""
        requirements = [
            # 암호화폐 API
            "pyupbit==0.2.22",
            
            # 데이터베이스
            "psycopg2-binary==2.9.7",
            
            # AWS SDK
            "boto3==1.28.57",
            "botocore==1.31.57",
            
            # 데이터 처리
            "pandas==2.0.3", 
            "numpy==1.24.3",
            
            # 기술적 분석
            "pandas-ta==0.3.14b0",
            
            # HTTP 요청
            "requests==2.31.0",
            
            # 유틸리티
            "python-dotenv==1.0.0",
            "PyYAML==6.0.1",
            
            # OpenAI (Phase 3용)
            "openai==0.28.1",
            
            # 이미지 처리 (차트 생성용)
            "Pillow==10.0.1",
            "matplotlib==3.7.2",
            
            # 날짜/시간 처리  
            "python-dateutil==2.8.2",
            "pytz==2023.3"
        ]
        
        requirements_path = self.build_dir / "requirements.txt"
        with open(requirements_path, 'w') as f:
            f.write('\n'.join(requirements))
        
        print(f"✅ Requirements 파일 생성: {len(requirements)}개 패키지")
        return requirements_path
    
    def install_packages(self, requirements_path):
        """패키지 설치 및 최적화"""
        print("📦 패키지 설치 중...")
        
        # pip install 명령어 실행
        install_cmd = [
            "pip", "install",
            "-r", str(requirements_path),
            "-t", str(self.python_dir),
            "--no-cache-dir",
            "--no-deps",  # 의존성 자동 설치 비활성화로 크기 최적화
            "--platform", "linux_x86_64",
            "--implementation", "cp",
            "--python-version", "3.9",
            "--only-binary=:all:"
        ]
        
        try:
            result = subprocess.run(install_cmd, capture_output=True, text=True, check=True)
            print("✅ 패키지 설치 완료")
        except subprocess.CalledProcessError as e:
            print(f"❌ 패키지 설치 실패: {e}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            raise
    
    def create_common_utils(self):
        """공통 유틸리티 함수 모듈 생성"""
        utils_content = '''
"""
🔧 Makenaide Lambda 공통 유틸리티
모든 Phase Lambda에서 공유하는 핵심 기능들
"""

import json
import logging
import os
import boto3
from datetime import datetime
from typing import Dict, List, Any, Optional

# AWS 클라이언트 캐시 (지연 로딩)
_aws_clients = {}

def get_aws_client(service_name: str):
    """AWS 클라이언트 지연 로딩 및 캐싱"""
    if service_name not in _aws_clients:
        _aws_clients[service_name] = boto3.client(service_name)
    return _aws_clients[service_name]

def setup_lambda_logger(phase_name: str = "unknown") -> logging.Logger:
    """Lambda용 표준 로거 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - {phase_name} - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_db_connection_params() -> Dict[str, str]:
    """DB 연결 파라미터 추출"""
    return {
        'host': os.getenv("PG_HOST"),
        'port': os.getenv("PG_PORT", "5432"),
        'dbname': os.getenv("PG_DATABASE"),
        'user': os.getenv("PG_USER"),
        'password': os.getenv("PG_PASSWORD")
    }

def save_to_s3(bucket: str, key: str, data: Any, content_type: str = 'application/json') -> bool:
    """S3에 데이터 저장"""
    try:
        s3 = get_aws_client('s3')
        
        if isinstance(data, dict) or isinstance(data, list):
            body = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        else:
            body = str(data)
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type
        )
        return True
    except Exception as e:
        logging.error(f"S3 저장 실패 {key}: {e}")
        return False

def load_from_s3(bucket: str, key: str) -> Optional[Any]:
    """S3에서 데이터 로드"""
    try:
        s3 = get_aws_client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # JSON 파싱 시도
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content
            
    except Exception as e:
        logging.error(f"S3 로드 실패 {key}: {e}")
        return None

def trigger_next_phase(source_phase: str, next_phase: str, data: Dict = None) -> bool:
    """EventBridge를 통한 다음 단계 트리거"""
    try:
        eventbridge = get_aws_client('events')
        
        event_detail = {
            'source_phase': source_phase,
            'target_phase': next_phase,
            'timestamp': datetime.now().isoformat(),
            'status': 'completed'
        }
        
        if data:
            event_detail.update(data)
        
        response = eventbridge.put_events(
            Entries=[
                {
                    'Source': f'makenaide.{source_phase}',
                    'DetailType': f'{source_phase.title()} Completed',
                    'Detail': json.dumps(event_detail)
                }
            ]
        )
        
        return response['FailedEntryCount'] == 0
        
    except Exception as e:
        logging.error(f"EventBridge 트리거 실패: {e}")
        return False

def create_lambda_response(status_code: int, phase: str, data: Dict = None, error: str = None) -> Dict:
    """표준 Lambda 응답 생성"""
    response = {
        'statusCode': status_code,
        'phase': phase,
        'timestamp': datetime.now().isoformat()
    }
    
    if status_code == 200:
        response['status'] = 'success'
        if data:
            response.update(data)
    else:
        response['status'] = 'error'
        if error:
            response['error'] = error
    
    return response

def get_blacklist_from_s3(bucket: str = None) -> Dict[str, Any]:
    """S3에서 블랙리스트 로드"""
    bucket = bucket or os.getenv('S3_BUCKET', 'makenaide-serverless-data')
    return load_from_s3(bucket, 'config/blacklist.json') or {}

def validate_db_connection() -> bool:
    """DB 연결 상태 검증"""
    try:
        import psycopg2
        params = get_db_connection_params()
        
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logging.error(f"DB 연결 검증 실패: {e}")
        return False

class LambdaTimer:
    """Lambda 실행 시간 측정"""
    def __init__(self, phase_name: str):
        self.phase_name = phase_name
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        logging.info(f"🚀 {self.phase_name} 시작")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        execution_time = time.time() - self.start_time
        if exc_type is None:
            logging.info(f"✅ {self.phase_name} 완료 ({execution_time:.2f}초)")
        else:
            logging.error(f"❌ {self.phase_name} 실패 ({execution_time:.2f}초): {exc_val}")
'''
        
        utils_path = self.python_dir / "makenaide_utils.py"
        with open(utils_path, 'w', encoding='utf-8') as f:
            f.write(utils_content)
        
        print("✅ 공통 유틸리티 모듈 생성 완료")
    
    def optimize_layer(self):
        """Layer 크기 최적화 (99.6% 경험 활용)"""
        print("🔧 Layer 크기 최적화 중...")
        
        # 불필요한 파일 제거
        unnecessary_patterns = [
            "**/__pycache__",
            "**/*.pyc", 
            "**/*.pyo",
            "**/tests",
            "**/test",
            "**/*.dist-info",
            "**/*.egg-info",
            "**/examples",
            "**/docs",
            "**/*.md",
            "**/*.txt",
            "**/LICENSE*",
            "**/NOTICE*"
        ]
        
        removed_count = 0
        for pattern in unnecessary_patterns:
            for path in self.python_dir.rglob(pattern.replace("**/", "")):
                if path.exists():
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                    removed_count += 1
        
        print(f"🗑️ 불필요한 파일 {removed_count}개 제거")
        
        # 디렉토리 크기 측정
        total_size = sum(f.stat().st_size for f in self.python_dir.rglob('*') if f.is_file())
        print(f"📦 최적화 후 Layer 크기: {total_size / 1024 / 1024:.2f} MB")
    
    def create_layer_zip(self) -> str:
        """Layer ZIP 파일 생성"""
        zip_filename = f"{self.layer_name}.zip"
        
        print(f"📦 Layer ZIP 생성 중: {zip_filename}")
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in self.python_dir.rglob('*'):
                if file_path.is_file():
                    # Layer 구조에 맞게 경로 조정
                    arc_name = file_path.relative_to(self.build_dir)
                    zip_file.write(file_path, arc_name)
        
        zip_size = os.path.getsize(zip_filename) / 1024 / 1024
        print(f"✅ Layer ZIP 생성 완료: {zip_filename} ({zip_size:.2f} MB)")
        
        return zip_filename
    
    def deploy_to_aws(self, zip_filename: str) -> str:
        """AWS Lambda Layer 배포"""
        try:
            lambda_client = boto3.client('lambda')
            
            with open(zip_filename, 'rb') as zip_file:
                zip_content = zip_file.read()
            
            print(f"🚀 AWS Lambda Layer 배포 중: {self.layer_name}")
            
            response = lambda_client.publish_layer_version(
                LayerName=self.layer_name,
                Description='Makenaide serverless core dependencies - optimized for all phases',
                Content={'ZipFile': zip_content},
                CompatibleRuntimes=['python3.9', 'python3.10', 'python3.11'],
                CompatibleArchitectures=['x86_64']
            )
            
            layer_arn = response['LayerVersionArn']
            print(f"✅ Layer 배포 완료: {layer_arn}")
            
            return layer_arn
            
        except Exception as e:
            print(f"❌ Layer 배포 실패: {e}")
            raise
    
    def build_and_deploy(self) -> str:
        """전체 빌드 및 배포 프로세스"""
        try:
            print("="*60)
            print(f"🏗️ {self.layer_name} 빌드 시작")
            print("="*60)
            
            # 1. 빌드 디렉토리 정리
            self.clean_build_directory()
            
            # 2. Requirements 파일 생성
            requirements_path = self.create_requirements_file()
            
            # 3. 패키지 설치
            self.install_packages(requirements_path)
            
            # 4. 공통 유틸리티 생성
            self.create_common_utils()
            
            # 5. Layer 최적화
            self.optimize_layer()
            
            # 6. ZIP 파일 생성
            zip_filename = self.create_layer_zip()
            
            # 7. AWS 배포
            layer_arn = self.deploy_to_aws(zip_filename)
            
            # 8. 정리
            os.remove(zip_filename)
            shutil.rmtree(self.build_dir)
            
            print("="*60)
            print(f"✅ {self.layer_name} 빌드 및 배포 완료!")
            print(f"📍 Layer ARN: {layer_arn}")
            print("="*60)
            
            return layer_arn
            
        except Exception as e:
            print(f"❌ 빌드 실패: {e}")
            raise

def main():
    """메인 실행 함수"""
    builder = MakenaideCoreLayerBuilder()
    try:
        layer_arn = builder.build_and_deploy()
        print(f"\n🎉 성공! Layer ARN: {layer_arn}")
        
        # Layer ARN을 파일에 저장 (다른 스크립트에서 사용)
        with open('layer_arn.txt', 'w') as f:
            f.write(layer_arn)
        
        return True
    except Exception as e:
        print(f"\n💥 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)