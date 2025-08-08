#!/usr/bin/env python3
"""
EC2 인스턴스의 .env 파일에서 환경변수를 읽어와 Lambda 배포에 사용
"""

import boto3
import json
import logging
import subprocess
import os
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_ec2_env_vars_from_file():
    """EC2 인스턴스의 실제 .env 파일에서 환경변수 직접 추출"""
    
    logger.info("🔍 EC2 인스턴스에서 실제 환경변수 추출 중...")
    
    # EC2 연결 정보
    EC2_HOST = "52.78.186.226"  # Elastic IP
    EC2_USER = "ec2-user"
    PEM_KEY_PATH = "/Users/13ruce/aws/makenaide-key.pem"
    REMOTE_ENV_PATH = "/home/ec2-user/makenaide/.env"
    
    try:
        # SSH를 통해 EC2의 .env 파일 내용 가져오기
        ssh_command = [
            "ssh", "-i", PEM_KEY_PATH,
            "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=no",
            f"{EC2_USER}@{EC2_HOST}",
            f"cat {REMOTE_ENV_PATH}"
        ]
        
        logger.info(f"📡 SSH 연결 시도: {EC2_HOST}")
        result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            logger.error(f"❌ SSH 연결 실패: {result.stderr}")
            return get_fallback_env_vars()
        
        # .env 파일 내용 파싱
        env_content = result.stdout.strip()
        env_vars = {}
        
        for line in env_content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                # 따옴표 제거
                value = value.strip('"\'')
                env_vars[key.strip()] = value
        
        # 필수 환경변수 확인
        required_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD',
                        'UPBIT_ACCESS_KEY', 'UPBIT_SECRET_KEY', 'OPENAI_API_KEY']
        
        missing_vars = [var for var in required_vars if not env_vars.get(var)]
        
        if missing_vars:
            logger.warning(f"⚠️ 누락된 환경변수: {missing_vars}")
        
        # SQS URL 추가 (Lambda에서 필요)
        env_vars['OHLCV_QUEUE_URL'] = 'https://sqs.ap-northeast-2.amazonaws.com/901361833359/makenaide-ohlcv-collection'
        
        logger.info(f"✅ EC2에서 환경변수 추출 완료: {len(env_vars)}개")
        
        # 민감한 정보는 마스킹하여 로그 출력
        for key, value in env_vars.items():
            if any(sensitive in key.upper() for sensitive in ['PASSWORD', 'SECRET', 'KEY']):
                logger.info(f"   {key}: {'*' * min(len(value), 8)}")
            else:
                logger.info(f"   {key}: {value}")
        
        return env_vars
        
    except subprocess.TimeoutExpired:
        logger.error("❌ SSH 연결 타임아웃")
        return get_fallback_env_vars()
    except Exception as e:
        logger.error(f"❌ EC2 환경변수 추출 실패: {e}")
        return get_fallback_env_vars()

def get_fallback_env_vars():
    """EC2 연결 실패시 대체 환경변수 (로컬 .env 사용)"""
    logger.info("🔄 로컬 .env 파일을 대체 소스로 사용")
    
    # 로컬 .env 파일 로드
    if os.path.exists('.env'):
        load_dotenv('.env')
    else:
        logger.error("❌ 로컬 .env 파일도 없음")
        return get_default_env_vars()
    
    env_vars = {
        'PG_HOST': os.getenv('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
        'PG_PORT': os.getenv('PG_PORT', '5432'),
        'PG_DATABASE': os.getenv('PG_DATABASE', 'makenaide'),
        'PG_USER': os.getenv('PG_USER', 'bruce'),
        'PG_PASSWORD': os.getenv('PG_PASSWORD'),
        'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
        'UPBIT_ACCESS_KEY': os.getenv('UPBIT_ACCESS_KEY'),
        'UPBIT_SECRET_KEY': os.getenv('UPBIT_SECRET_KEY'),
        'OHLCV_QUEUE_URL': 'https://sqs.ap-northeast-2.amazonaws.com/901361833359/makenaide-ohlcv-collection'
    }
    
    # None 값 제거
    env_vars = {k: v for k, v in env_vars.items() if v is not None}
    
    logger.info(f"✅ 로컬 환경변수 사용: {len(env_vars)}개")
    return env_vars

def get_default_env_vars():
    """최종 대체 환경변수 (기본값들)"""
    logger.warning("⚠️ 기본 환경변수 사용 (실제 값 설정 필요)")
    
    return {
        'PG_HOST': 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com',
        'PG_PORT': '5432',
        'PG_DATABASE': 'makenaide',
        'PG_USER': 'bruce',
        'PG_PASSWORD': 'REPLACE_WITH_ACTUAL_PASSWORD',
        'OPENAI_API_KEY': 'REPLACE_WITH_ACTUAL_KEY',
        'UPBIT_ACCESS_KEY': 'REPLACE_WITH_ACTUAL_KEY',
        'UPBIT_SECRET_KEY': 'REPLACE_WITH_ACTUAL_KEY',
        'OHLCV_QUEUE_URL': 'https://sqs.ap-northeast-2.amazonaws.com/901361833359/makenaide-ohlcv-collection'
    }

def get_ec2_env_vars():
    """메인 함수: EC2 환경변수 추출 (우선순위: EC2 실제 파일 > 로컬 .env > 기본값)"""
    
    logger.info("🚀 환경변수 추출 시작")
    
    # 1차 시도: EC2 실제 .env 파일
    env_vars = get_ec2_env_vars_from_file()
    
    # 검증: 실제 값이 있는지 확인
    critical_vars = ['PG_PASSWORD', 'OPENAI_API_KEY', 'UPBIT_ACCESS_KEY']
    has_real_values = all(
        env_vars.get(var) and 
        not any(placeholder in env_vars.get(var, '') for placeholder in ['REPLACE', 'your_', 'user_set'])
        for var in critical_vars
    )
    
    if has_real_values:
        logger.info("✅ 실제 환경변수 값 확인됨")
        return env_vars
    else:
        logger.warning("⚠️ 플레이스홀더 값 감지됨, 다시 확인 필요")
        return env_vars

if __name__ == "__main__":
    env_vars = get_ec2_env_vars()
    print(json.dumps(env_vars, indent=2)) 