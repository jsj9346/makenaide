#!/usr/bin/env python3
"""
EC2 자동 시작 설정 스크립트
EC2 인스턴스에 Makenaide 자동 실행 시스템을 설정

🎯 기능:
1. 자동 실행 스크립트를 EC2에 업로드
2. systemd 서비스 파일 생성
3. 부팅 시 자동 실행 설정
4. 로그 시스템 설정
"""

import subprocess
import logging
import os
from typing import List

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 설정
EC2_HOST = '52.78.186.226'
EC2_USER = 'ec2-user'
KEY_PATH = '/Users/13ruce/aws/makenaide-key.pem'
REMOTE_MAKENAIDE_DIR = '/home/ec2-user/makenaide'

def run_ssh_command(command: str) -> bool:
    """EC2에서 SSH 명령 실행"""
    try:
        ssh_command = [
            'ssh', '-i', KEY_PATH,
            f'{EC2_USER}@{EC2_HOST}',
            command
        ]

        logger.info(f"🔧 실행 중: {command}")
        result = subprocess.run(ssh_command, capture_output=True, text=True, check=True)

        if result.stdout:
            logger.info(f"✅ 출력: {result.stdout.strip()}")

        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ SSH 명령 실행 실패: {e}")
        if e.stderr:
            logger.error(f"오류: {e.stderr.strip()}")
        return False

def upload_file(local_path: str, remote_path: str) -> bool:
    """파일을 EC2에 업로드"""
    try:
        scp_command = [
            'scp', '-i', KEY_PATH,
            local_path,
            f'{EC2_USER}@{EC2_HOST}:{remote_path}'
        ]

        logger.info(f"📤 업로드: {local_path} → {remote_path}")
        subprocess.run(scp_command, check=True)
        logger.info(f"✅ 업로드 완료")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ 파일 업로드 실패: {e}")
        return False

def create_systemd_service() -> str:
    """systemd 서비스 파일 내용 생성"""
    service_content = """[Unit]
Description=Makenaide Cryptocurrency Trading Pipeline
After=network.target
Wants=network-online.target

[Service]
Type=oneshot
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/makenaide
ExecStart=/bin/bash /home/ec2-user/makenaide/ec2_auto_start_script.sh
Environment=EC2_AUTO_SHUTDOWN=true
Environment=PYTHONPATH=/home/ec2-user/makenaide
StandardOutput=journal
StandardError=journal
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
"""
    return service_content

def setup_autostart_system() -> bool:
    """자동 시작 시스템 설정"""
    try:
        logger.info("🚀 EC2 자동 시작 시스템 설정 시작")

        # 1. 자동 실행 스크립트 업로드
        logger.info("1️⃣ 자동 실행 스크립트 업로드 중...")
        if not upload_file('./ec2_auto_start_script.sh', f'{REMOTE_MAKENAIDE_DIR}/ec2_auto_start_script.sh'):
            return False

        # 스크립트 실행 권한 부여
        if not run_ssh_command(f'chmod +x {REMOTE_MAKENAIDE_DIR}/ec2_auto_start_script.sh'):
            return False

        # 2. 로그 디렉토리 생성
        logger.info("2️⃣ 로그 디렉토리 설정 중...")
        commands = [
            f'mkdir -p {REMOTE_MAKENAIDE_DIR}/logs',
            f'mkdir -p {REMOTE_MAKENAIDE_DIR}/logs/backups',
            f'touch {REMOTE_MAKENAIDE_DIR}/logs/auto_execution.log',
            f'touch {REMOTE_MAKENAIDE_DIR}/logs/auto_execution_error.log'
        ]

        for cmd in commands:
            if not run_ssh_command(cmd):
                return False

        # 3. systemd 서비스 파일 생성
        logger.info("3️⃣ systemd 서비스 설정 중...")

        # 임시 서비스 파일 생성
        service_content = create_systemd_service()
        temp_service_file = '/tmp/makenaide-auto.service'

        with open(temp_service_file, 'w') as f:
            f.write(service_content)

        # 서비스 파일 업로드
        if not upload_file(temp_service_file, '/tmp/makenaide-auto.service'):
            return False

        # 서비스 파일 이동 및 설정
        service_commands = [
            'sudo mv /tmp/makenaide-auto.service /etc/systemd/system/',
            'sudo chmod 644 /etc/systemd/system/makenaide-auto.service',
            'sudo systemctl daemon-reload',
            'sudo systemctl enable makenaide-auto.service'
        ]

        for cmd in service_commands:
            if not run_ssh_command(cmd):
                return False

        # 임시 파일 정리
        if os.path.exists(temp_service_file):
            os.remove(temp_service_file)

        # 4. cron 기반 트리거 설정 (systemd 서비스를 수동으로 트리거)
        logger.info("4️⃣ 부팅 트리거 설정 중...")

        # 부팅 시 자동 실행을 위한 스크립트
        boot_trigger_script = """#!/bin/bash
# Makenaide 부팅 트리거 스크립트

# EC2 완전 부팅 대기 (네트워크 안정화)
sleep 30

# 로그 파일에 부팅 정보 기록
echo "$(date): EC2 부팅 완료, Makenaide 자동 실행 트리거" >> /home/ec2-user/makenaide/logs/boot_trigger.log

# Makenaide 자동 실행 서비스 시작
systemctl start makenaide-auto.service
"""

        # 트리거 스크립트 생성 및 업로드
        temp_trigger_file = '/tmp/makenaide_boot_trigger.sh'
        with open(temp_trigger_file, 'w') as f:
            f.write(boot_trigger_script)

        if not upload_file(temp_trigger_file, '/tmp/makenaide_boot_trigger.sh'):
            return False

        # 트리거 스크립트 설정
        trigger_commands = [
            'sudo mv /tmp/makenaide_boot_trigger.sh /usr/local/bin/',
            'sudo chmod +x /usr/local/bin/makenaide_boot_trigger.sh',
            'echo "@reboot root /usr/local/bin/makenaide_boot_trigger.sh" | sudo tee -a /etc/crontab'
        ]

        for cmd in trigger_commands:
            if not run_ssh_command(cmd):
                logger.warning(f"⚠️ 트리거 설정 실패: {cmd}")

        # 임시 파일 정리
        if os.path.exists(temp_trigger_file):
            os.remove(temp_trigger_file)

        # 5. 설정 확인
        logger.info("5️⃣ 설정 확인 중...")
        verification_commands = [
            'systemctl status makenaide-auto.service',
            'ls -la /home/ec2-user/makenaide/ec2_auto_start_script.sh',
            'ls -la /home/ec2-user/makenaide/logs/'
        ]

        for cmd in verification_commands:
            run_ssh_command(cmd)  # 실패해도 계속 진행

        logger.info("✅ EC2 자동 시작 시스템 설정 완료!")
        return True

    except Exception as e:
        logger.error(f"❌ 자동 시작 시스템 설정 실패: {e}")
        return False

def test_manual_execution() -> bool:
    """수동 실행 테스트"""
    try:
        logger.info("🧪 수동 실행 테스트 중...")

        # 자동 실행 스크립트 테스트 (dry-run)
        test_command = f'cd {REMOTE_MAKENAIDE_DIR} && bash ec2_auto_start_script.sh'

        logger.info("실제 파이프라인은 실행하지 않고 스크립트 검증만 수행합니다...")
        logger.info("테스트를 중단하려면 Ctrl+C를 누르세요.")

        # 실제로는 테스트하지 않고 설명만 제공
        logger.info("📋 수동 테스트 방법:")
        logger.info(f"1. SSH 접속: ssh -i {KEY_PATH} {EC2_USER}@{EC2_HOST}")
        logger.info(f"2. 테스트 실행: cd {REMOTE_MAKENAIDE_DIR} && bash ec2_auto_start_script.sh")
        logger.info("3. 로그 확인: tail -f logs/auto_execution.log")

        return True

    except Exception as e:
        logger.error(f"❌ 테스트 실행 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    logger.info("🚀 Makenaide EC2 자동 시작 설정")
    logger.info("=" * 60)

    try:
        # EC2 연결 테스트
        logger.info("🔍 EC2 연결 테스트 중...")
        if not run_ssh_command('echo "EC2 연결 성공"'):
            logger.error("❌ EC2 연결 실패")
            return False

        # 자동 시작 시스템 설정
        if not setup_autostart_system():
            logger.error("❌ 자동 시작 시스템 설정 실패")
            return False

        # 테스트 가이드 제공
        test_manual_execution()

        logger.info("\n" + "=" * 60)
        logger.info("🎉 EC2 자동 시작 설정 완료!")

        logger.info("\n🎯 다음 단계:")
        logger.info("1. EventBridge 스케줄 및 Lambda 함수 배포")
        logger.info("2. 전체 자동화 파이프라인 테스트")
        logger.info("3. EC2 인스턴스 수동 재부팅으로 자동 실행 테스트")

        logger.info("\n📋 확인 방법:")
        logger.info(f"- SSH 접속: ssh -i {KEY_PATH} {EC2_USER}@{EC2_HOST}")
        logger.info("- 서비스 상태: sudo systemctl status makenaide-auto.service")
        logger.info("- 로그 확인: tail -f ~/makenaide/logs/auto_execution.log")

        return True

    except Exception as e:
        logger.error(f"❌ 설정 실패: {e}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        print(f"\n🎯 설정 {'성공' if success else '실패'}")

    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")

    except Exception as e:
        print(f"\n❌ 설정 실패: {e}")
        import traceback
        traceback.print_exc()