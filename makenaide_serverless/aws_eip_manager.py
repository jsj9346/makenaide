#!/usr/bin/env python3
"""
AWS Elastic IP 관리 스크립트

🎯 목적:
- EC2 인스턴스 재시작 시 IP 변동 문제 해결
- Elastic IP 자동 할당 및 관리
- IP 변동 모니터링 및 알림
- 자동화된 EIP 연결/해제

🔧 사용법:
python aws_eip_manager.py [--allocate] [--associate] [--check-status] [--monitor]
"""

import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import os
from dotenv import load_dotenv

class ElasticIPManager:
    """AWS Elastic IP 관리 클래스"""
    
    def __init__(self):
        load_dotenv('env.aws')
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # AWS 클라이언트 초기화
        try:
            self.ec2 = boto3.client('ec2')
            self.ec2_resource = boto3.resource('ec2')
            
            self.logger.info("✅ AWS EC2 클라이언트 초기화 완료")
        except Exception as e:
            self.logger.error(f"❌ AWS 클라이언트 초기화 실패: {e}")
            raise
        
        # 설정값
        self.instance_name = 'makenaide-ec2'
        self.eip_tag_name = 'makenaide-eip'
        
    def get_instance_info(self) -> Optional[Dict[str, Any]]:
        """makenaide EC2 인스턴스 정보 조회"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [self.instance_name]
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': ['running', 'stopped', 'stopping', 'pending']
                    }
                ]
            )
            
            if response['Reservations'] and response['Reservations'][0]['Instances']:
                instance = response['Reservations'][0]['Instances'][0]
                
                instance_info = {
                    'instance_id': instance['InstanceId'],
                    'state': instance['State']['Name'],
                    'public_ip': instance.get('PublicIpAddress'),
                    'private_ip': instance.get('PrivateIpAddress'),
                    'launch_time': instance.get('LaunchTime'),
                    'instance_type': instance.get('InstanceType'),
                    'vpc_id': instance.get('VpcId'),
                    'subnet_id': instance.get('SubnetId')
                }
                
                self.logger.info(f"✅ EC2 인스턴스 발견: {instance_info['instance_id']}")
                self.logger.info(f"   상태: {instance_info['state']}")
                self.logger.info(f"   퍼블릭 IP: {instance_info['public_ip']}")
                
                return instance_info
            else:
                self.logger.warning(f"⚠️ '{self.instance_name}' EC2 인스턴스를 찾을 수 없습니다")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ EC2 인스턴스 정보 조회 실패: {e}")
            return None
    
    def get_elastic_ip_info(self) -> Optional[Dict[str, Any]]:
        """makenaide Elastic IP 정보 조회"""
        try:
            response = self.ec2.describe_addresses(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [self.eip_tag_name]
                    }
                ]
            )
            
            if response['Addresses']:
                eip = response['Addresses'][0]
                
                eip_info = {
                    'allocation_id': eip['AllocationId'],
                    'public_ip': eip['PublicIp'],
                    'association_id': eip.get('AssociationId'),
                    'instance_id': eip.get('InstanceId'),
                    'domain': eip.get('Domain'),
                    'network_interface_id': eip.get('NetworkInterfaceId')
                }
                
                self.logger.info(f"✅ Elastic IP 발견: {eip_info['public_ip']}")
                if eip_info['instance_id']:
                    self.logger.info(f"   연결된 인스턴스: {eip_info['instance_id']}")
                else:
                    self.logger.info("   ⚠️ 연결된 인스턴스 없음")
                
                return eip_info
            else:
                self.logger.warning(f"⚠️ '{self.eip_tag_name}' Elastic IP를 찾을 수 없습니다")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Elastic IP 정보 조회 실패: {e}")
            return None
    
    def allocate_elastic_ip(self) -> Optional[str]:
        """새로운 Elastic IP 할당"""
        try:
            self.logger.info("🔧 새로운 Elastic IP 할당 중...")
            
            response = self.ec2.allocate_address(Domain='vpc')
            
            allocation_id = response['AllocationId']
            public_ip = response['PublicIp']
            
            # 태그 추가
            self.ec2.create_tags(
                Resources=[allocation_id],
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': self.eip_tag_name
                    },
                    {
                        'Key': 'Project',
                        'Value': 'makenaide'
                    },
                    {
                        'Key': 'Purpose',
                        'Value': 'trading-bot-static-ip'
                    }
                ]
            )
            
            self.logger.info(f"✅ Elastic IP 할당 완료:")
            self.logger.info(f"   IP 주소: {public_ip}")
            self.logger.info(f"   할당 ID: {allocation_id}")
            
            return allocation_id
            
        except Exception as e:
            self.logger.error(f"❌ Elastic IP 할당 실패: {e}")
            return None
    
    def associate_elastic_ip(self, instance_id: str, allocation_id: str) -> bool:
        """Elastic IP를 EC2 인스턴스에 연결"""
        try:
            self.logger.info(f"🔗 Elastic IP 연결 중: {allocation_id} → {instance_id}")
            
            response = self.ec2.associate_address(
                InstanceId=instance_id,
                AllocationId=allocation_id
            )
            
            association_id = response.get('AssociationId')
            
            self.logger.info(f"✅ Elastic IP 연결 완료")
            self.logger.info(f"   연결 ID: {association_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Elastic IP 연결 실패: {e}")
            return False
    
    def disassociate_elastic_ip(self, association_id: str) -> bool:
        """Elastic IP 연결 해제"""
        try:
            self.logger.info(f"🔓 Elastic IP 연결 해제 중: {association_id}")
            
            self.ec2.disassociate_address(AssociationId=association_id)
            
            self.logger.info("✅ Elastic IP 연결 해제 완료")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Elastic IP 연결 해제 실패: {e}")
            return False
    
    def ensure_elastic_ip_connected(self) -> Tuple[bool, Optional[str]]:
        """EC2 인스턴스에 Elastic IP가 연결되어 있는지 확인하고 필요시 연결"""
        try:
            # 1. EC2 인스턴스 정보 조회
            instance_info = self.get_instance_info()
            if not instance_info:
                self.logger.error("❌ EC2 인스턴스를 찾을 수 없습니다")
                return False, None
            
            instance_id = instance_info['instance_id']
            current_public_ip = instance_info['public_ip']
            
            # 2. Elastic IP 정보 조회
            eip_info = self.get_elastic_ip_info()
            
            # 3. Elastic IP가 없으면 새로 할당
            if not eip_info:
                self.logger.info("🆕 Elastic IP가 없어 새로 할당합니다")
                allocation_id = self.allocate_elastic_ip()
                if not allocation_id:
                    return False, None
                
                # 새로 할당한 EIP 정보 다시 조회
                eip_info = self.get_elastic_ip_info()
                if not eip_info:
                    return False, None
            
            allocation_id = eip_info['allocation_id']
            eip_public_ip = eip_info['public_ip']
            connected_instance = eip_info.get('instance_id')
            
            # 4. EIP가 현재 인스턴스에 연결되어 있는지 확인
            if connected_instance == instance_id:
                self.logger.info(f"✅ Elastic IP가 이미 올바르게 연결되어 있습니다: {eip_public_ip}")
                return True, eip_public_ip
            
            # 5. EIP가 다른 인스턴스에 연결되어 있으면 연결 해제
            if connected_instance and connected_instance != instance_id:
                self.logger.warning(f"⚠️ Elastic IP가 다른 인스턴스에 연결됨: {connected_instance}")
                if eip_info.get('association_id'):
                    self.disassociate_elastic_ip(eip_info['association_id'])
                    time.sleep(2)  # 연결 해제 대기
            
            # 6. 인스턴스가 실행 중인 경우에만 EIP 연결
            if instance_info['state'] == 'running':
                success = self.associate_elastic_ip(instance_id, allocation_id)
                if success:
                    self.logger.info(f"✅ Elastic IP 연결 완료: {eip_public_ip}")
                    return True, eip_public_ip
                else:
                    return False, None
            else:
                self.logger.info(f"ℹ️ 인스턴스가 실행 중이 아님 ({instance_info['state']}). EIP 연결을 건너뜁니다")
                return True, eip_public_ip
            
        except Exception as e:
            self.logger.error(f"❌ Elastic IP 연결 확인 실패: {e}")
            return False, None
    
    def monitor_ip_changes(self, check_interval: int = 300) -> None:
        """IP 변경 모니터링 (5분 간격)"""
        self.logger.info(f"👁️ IP 변경 모니터링 시작 (체크 간격: {check_interval}초)")
        
        last_known_ip = None
        
        try:
            while True:
                instance_info = self.get_instance_info()
                
                if instance_info and instance_info['state'] == 'running':
                    current_ip = instance_info['public_ip']
                    
                    if last_known_ip is None:
                        last_known_ip = current_ip
                        self.logger.info(f"📍 초기 IP 주소: {current_ip}")
                    elif current_ip != last_known_ip:
                        self.logger.warning(f"🚨 IP 주소 변경 감지!")
                        self.logger.warning(f"   이전 IP: {last_known_ip}")
                        self.logger.warning(f"   현재 IP: {current_ip}")
                        
                        # Elastic IP 재연결 시도
                        success, new_ip = self.ensure_elastic_ip_connected()
                        if success:
                            self.logger.info(f"🔧 Elastic IP 자동 복구 완료: {new_ip}")
                            last_known_ip = new_ip
                        else:
                            self.logger.error("❌ Elastic IP 자동 복구 실패")
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            self.logger.info("🛑 모니터링 중단됨")
        except Exception as e:
            self.logger.error(f"❌ 모니터링 중 오류: {e}")
    
    def generate_ip_report(self) -> Dict[str, Any]:
        """IP 상태 보고서 생성"""
        self.logger.info("📊 IP 상태 보고서 생성 중...")
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'instance_info': self.get_instance_info(),
            'elastic_ip_info': self.get_elastic_ip_info(),
            'ip_consistency_check': False,
            'recommendations': []
        }
        
        # IP 일관성 체크
        if report['instance_info'] and report['elastic_ip_info']:
            instance_ip = report['instance_info']['public_ip']
            eip_ip = report['elastic_ip_info']['public_ip']
            connected_instance = report['elastic_ip_info'].get('instance_id')
            current_instance = report['instance_info']['instance_id']
            
            if instance_ip == eip_ip and connected_instance == current_instance:
                report['ip_consistency_check'] = True
                report['status'] = 'healthy'
            else:
                report['status'] = 'inconsistent'
                if instance_ip != eip_ip:
                    report['recommendations'].append(f"인스턴스 IP ({instance_ip})와 EIP ({eip_ip})가 다름")
                if connected_instance != current_instance:
                    report['recommendations'].append(f"EIP가 다른 인스턴스에 연결됨")
        elif not report['elastic_ip_info']:
            report['status'] = 'no_eip'
            report['recommendations'].append("Elastic IP 할당 필요")
        elif not report['instance_info']:
            report['status'] = 'no_instance'
            report['recommendations'].append("EC2 인스턴스 확인 필요")
        else:
            report['status'] = 'unknown'
        
        # 보고서 저장
        report_file = f"aws_ip_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"📄 IP 상태 보고서 저장: {report_file}")
        self.logger.info(f"🎯 현재 상태: {report['status'].upper()}")
        
        return report
    
    def setup_auto_eip_association(self) -> bool:
        """EC2 시작 시 자동 EIP 연결 설정"""
        try:
            self.logger.info("🔧 자동 EIP 연결 설정 중...")
            
            # 인스턴스에 사용자 데이터 스크립트 추가 (다음 부팅 시 적용)
            user_data_script = """#!/bin/bash
# makenaide EIP 자동 연결 스크립트

# AWS CLI 설치 확인
if ! command -v aws &> /dev/null; then
    echo "AWS CLI 설치 중..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
fi

# 메타데이터에서 인스턴스 ID 조회
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)

# EIP 할당 ID 조회
ALLOCATION_ID=$(aws ec2 describe-addresses --region $REGION --filters "Name=tag:Name,Values=makenaide-eip" --query "Addresses[0].AllocationId" --output text)

if [ "$ALLOCATION_ID" != "None" ] && [ "$ALLOCATION_ID" != "" ]; then
    echo "EIP 연결 시도: $ALLOCATION_ID -> $INSTANCE_ID"
    aws ec2 associate-address --region $REGION --instance-id $INSTANCE_ID --allocation-id $ALLOCATION_ID
    echo "EIP 연결 완료"
else
    echo "EIP를 찾을 수 없습니다"
fi

# 로그 기록
echo "$(date): EIP 자동 연결 스크립트 실행 완료" >> /var/log/makenaide-eip.log
"""
            
            # 사용자 데이터 스크립트를 파일로 저장
            script_file = "aws_eip_auto_associate.sh"
            with open(script_file, 'w') as f:
                f.write(user_data_script)
            
            self.logger.info(f"✅ 자동 EIP 연결 스크립트 생성: {script_file}")
            self.logger.info("ℹ️ 다음 EC2 인스턴스 재시작 시 이 스크립트를 사용자 데이터로 설정하세요")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 자동 EIP 연결 설정 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AWS Elastic IP 관리')
    parser.add_argument('--allocate', action='store_true', help='새 Elastic IP 할당')
    parser.add_argument('--associate', action='store_true', help='EIP를 인스턴스에 연결')
    parser.add_argument('--check-status', action='store_true', help='IP 상태 확인')
    parser.add_argument('--monitor', action='store_true', help='IP 변경 모니터링 시작')
    parser.add_argument('--setup-auto', action='store_true', help='자동 EIP 연결 설정')
    parser.add_argument('--interval', type=int, default=300, help='모니터링 간격 (초)')
    
    args = parser.parse_args()
    
    manager = ElasticIPManager()
    
    if args.allocate:
        manager.allocate_elastic_ip()
    
    if args.associate:
        success, ip = manager.ensure_elastic_ip_connected()
        if success:
            print(f"✅ EIP 연결 성공: {ip}")
        else:
            print("❌ EIP 연결 실패")
    
    if args.check_status:
        manager.generate_ip_report()
    
    if args.monitor:
        manager.monitor_ip_changes(args.interval)
    
    if args.setup_auto:
        manager.setup_auto_eip_association()
    
    if not any([args.allocate, args.associate, args.check_status, args.monitor, args.setup_auto]):
        # 기본 동작: 상태 확인 및 필요시 연결
        print("🔍 기본 동작: EIP 상태 확인 및 자동 연결")
        success, ip = manager.ensure_elastic_ip_connected()
        manager.generate_ip_report()
        
        if success:
            print(f"✅ 최종 결과: EIP 정상 연결됨 ({ip})")
        else:
            print("❌ 최종 결과: EIP 연결 실패")


if __name__ == '__main__':
    main() 