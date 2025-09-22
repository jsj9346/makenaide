#!/usr/bin/env python3
"""
Smart Shutdown System for makenaide EC2 Instance
AWS CLI 기반 안전한 자동 종료 시스템

기존 shutdown 명령어 대신 AWS CLI를 사용하여:
1. 안전한 데이터 정리
2. SQLite DB 백업
3. 로그 동기화
4. SNS 알림 발송
5. AWS API를 통한 정상 종료
"""

import json
import logging
import subprocess
import time
import boto3
import requests
from datetime import datetime
from pathlib import Path

class SmartShutdown:
    def __init__(self):
        self.logger = self._setup_logger()
        self.instance_id = self._get_instance_id()
        self.region = 'ap-northeast-2'  # Seoul region
        self.ec2 = boto3.client('ec2', region_name=self.region)
        self.sns = boto3.client('sns', region_name=self.region)
        self.shutdown_reason = "파이프라인 완료"

    def _setup_logger(self):
        """로거 설정"""
        logger = logging.getLogger('smart_shutdown')
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def _get_instance_id(self):
        """EC2 Instance ID 획득 (IMDSv2 사용)"""
        try:
            # IMDSv2 토큰 획득
            token_response = requests.put(
                'http://169.254.169.254/latest/api/token',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
                timeout=5
            )
            token = token_response.text.strip()

            # Instance ID 획득
            response = requests.get(
                'http://169.254.169.254/latest/meta-data/instance-id',
                headers={'X-aws-ec2-metadata-token': token},
                timeout=5
            )
            return response.text.strip()
        except Exception as e:
            self.logger.error(f"❌ Instance ID 획득 실패: {e}")
            return None

    def cleanup_database(self):
        """SQLite 데이터베이스 정리 및 백업"""
        try:
            self.logger.info("🗃️ SQLite 데이터베이스 정리 시작")

            # DB 파일 경로
            db_path = Path("makenaide_local.db")
            if not db_path.exists():
                self.logger.warning("⚠️ SQLite DB 파일을 찾을 수 없음")
                return False

            # VACUUM 최적화
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            conn.execute("VACUUM;")
            conn.close()

            # 백업 생성
            backup_name = f"makenaide_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            backup_path = Path("backups") / backup_name
            backup_path.parent.mkdir(exist_ok=True)

            subprocess.run(["cp", str(db_path), str(backup_path)], check=True)

            self.logger.info(f"✅ DB 정리 및 백업 완료: {backup_name}")
            return True

        except Exception as e:
            self.logger.error(f"❌ DB 정리 실패: {e}")
            return False

    def sync_logs(self):
        """로그 파일 동기화"""
        try:
            self.logger.info("📝 로그 파일 동기화 시작")

            # 로그 디렉토리 확인
            log_files = list(Path(".").glob("*.log"))
            if log_files:
                # 로그 아카이브 생성
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_name = f"logs_archive_{timestamp}.tar.gz"

                subprocess.run([
                    "tar", "-czf", archive_name
                ] + [str(f) for f in log_files], check=True)

                self.logger.info(f"✅ 로그 아카이브 생성: {archive_name}")

            # 시스템 로그 동기화
            subprocess.run(["sync"], check=True)
            time.sleep(2)  # 동기화 완료 대기

            return True

        except Exception as e:
            self.logger.error(f"❌ 로그 동기화 실패: {e}")
            return False

    def send_shutdown_notification(self, success_stats=None):
        """종료 전 SNS 알림 발송"""
        try:
            self.logger.info("📡 종료 알림 발송 시작")

            # 종료 통계 수집
            stats = success_stats or {}

            message = {
                "event_type": "SMART_SHUTDOWN",
                "instance_id": self.instance_id,
                "shutdown_time": datetime.now().isoformat(),
                "reason": self.shutdown_reason,
                "statistics": stats,
                "status": "SUCCESS"
            }

            # SNS 주제들
            topics = [
                "arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts"
            ]

            for topic_arn in topics:
                try:
                    self.sns.publish(
                        TopicArn=topic_arn,
                        Subject=f"🔄 Makenaide 파이프라인 완료 - 인스턴스 종료",
                        Message=json.dumps(message, indent=2, ensure_ascii=False)
                    )
                    self.logger.info(f"✅ SNS 알림 발송 완료: {topic_arn}")
                except Exception as e:
                    self.logger.error(f"❌ SNS 알림 발송 실패: {e}")

            return True

        except Exception as e:
            self.logger.error(f"❌ 알림 발송 실패: {e}")
            return False

    def stop_instance(self):
        """AWS CLI를 통한 EC2 인스턴스 안전 종료"""
        try:
            if not self.instance_id:
                self.logger.error("❌ Instance ID가 없어 종료할 수 없음")
                return False

            self.logger.info(f"🛑 EC2 인스턴스 종료 시작: {self.instance_id}")

            # AWS CLI를 통한 안전한 종료
            response = self.ec2.stop_instances(
                InstanceIds=[self.instance_id],
                Hibernate=False,
                Force=False
            )

            self.logger.info(f"✅ 종료 명령 성공: {response['StoppingInstances'][0]['CurrentState']['Name']}")
            return True

        except Exception as e:
            self.logger.error(f"❌ 인스턴스 종료 실패: {e}")
            return False

    def execute_smart_shutdown(self, reason="파이프라인 완료", stats=None):
        """전체 Smart Shutdown 프로세스 실행"""
        self.shutdown_reason = reason
        self.logger.info("🚀 Smart Shutdown 프로세스 시작")

        # 1단계: 데이터베이스 정리
        db_success = self.cleanup_database()

        # 2단계: 로그 동기화
        log_success = self.sync_logs()

        # 3단계: 알림 발송
        notification_success = self.send_shutdown_notification(stats)

        # 4단계: 인스턴스 종료
        if db_success and log_success:
            self.logger.info("✅ 모든 정리 작업 완료 - 인스턴스 종료 진행")
            return self.stop_instance()
        else:
            self.logger.warning("⚠️ 일부 정리 작업 실패 - 강제 종료는 하지 않음")
            return False

def main():
    """Smart Shutdown 스크립트 직접 실행"""
    import sys

    shutdown = SmartShutdown()

    reason = sys.argv[1] if len(sys.argv) > 1 else "수동 종료"
    success = shutdown.execute_smart_shutdown(reason)

    if success:
        print("✅ Smart Shutdown 완료")
    else:
        print("❌ Smart Shutdown 실패")
        sys.exit(1)

if __name__ == "__main__":
    main()