#!/usr/bin/env python3
"""
Lambda VPC 보안 설정 스크립트
Lambda를 VPC 내부로 이동하여 RDS와 안전하게 통신하도록 구성
"""

import boto3
import json
import time
from datetime import datetime

def get_vpc_info():
    """현재 VPC 및 서브넷 정보 조회"""
    try:
        ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
        
        # VPC 정보 조회
        vpc_id = 'vpc-0fe04180da4a41d6c'
        
        vpc_response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        vpc = vpc_response['Vpcs'][0]
        
        print(f"📊 VPC 정보: {vpc_id}")
        print(f"   - CIDR: {vpc['CidrBlock']}")
        print(f"   - 상태: {vpc['State']}")
        
        # 서브넷 정보 조회
        subnets_response = ec2_client.describe_subnets(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]}
            ]
        )
        
        subnets = subnets_response['Subnets']
        print(f"\n📍 서브넷 정보: {len(subnets)}개")
        
        private_subnets = []
        public_subnets = []
        
        for subnet in subnets:
            subnet_id = subnet['SubnetId']
            az = subnet['AvailabilityZone']
            cidr = subnet['CidrBlock']
            
            # 라우트 테이블 확인하여 public/private 구분
            try:
                route_tables = ec2_client.describe_route_tables(
                    Filters=[
                        {'Name': 'association.subnet-id', 'Values': [subnet_id]}
                    ]
                )
                
                is_public = False
                for rt in route_tables['RouteTables']:
                    for route in rt['Routes']:
                        if route.get('GatewayId', '').startswith('igw-'):
                            is_public = True
                            break
                
                subnet_info = {
                    'id': subnet_id,
                    'az': az,
                    'cidr': cidr,
                    'type': 'public' if is_public else 'private'
                }
                
                if is_public:
                    public_subnets.append(subnet_info)
                else:
                    private_subnets.append(subnet_info)
                
                print(f"   - {subnet_id} ({az}): {cidr} [{subnet_info['type']}]")
                
            except Exception as e:
                print(f"   - {subnet_id} ({az}): {cidr} [확인 불가]")
        
        return {
            'vpc_id': vpc_id,
            'private_subnets': private_subnets,
            'public_subnets': public_subnets
        }
        
    except Exception as e:
        print(f"❌ VPC 정보 조회 실패: {e}")
        return None

def create_lambda_security_group():
    """Lambda 전용 보안 그룹 생성"""
    try:
        ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
        
        vpc_id = 'vpc-0fe04180da4a41d6c'
        
        # 기존 Lambda 보안 그룹 확인
        try:
            existing_sgs = ec2_client.describe_security_groups(
                Filters=[
                    {'Name': 'group-name', 'Values': ['makenaide-lambda-sg']},
                    {'Name': 'vpc-id', 'Values': [vpc_id]}
                ]
            )
            
            if existing_sgs['SecurityGroups']:
                sg_id = existing_sgs['SecurityGroups'][0]['GroupId']
                print(f"✅ 기존 Lambda 보안 그룹 사용: {sg_id}")
                return sg_id
        except:
            pass
        
        # 새 보안 그룹 생성
        print("🔧 Lambda 전용 보안 그룹 생성 중...")
        
        sg_response = ec2_client.create_security_group(
            GroupName='makenaide-lambda-sg',
            Description='Makenaide Lambda functions security group',
            VpcId=vpc_id
        )
        
        sg_id = sg_response['GroupId']
        print(f"✅ Lambda 보안 그룹 생성 완료: {sg_id}")
        
        # 아웃바운드 규칙 추가 (HTTPS, PostgreSQL)
        ec2_client.authorize_security_group_egress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 443,
                    'ToPort': 443,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'HTTPS outbound'}]
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 5432,
                    'ToPort': 5432,
                    'UserIdGroupPairs': [
                        {
                            'GroupId': 'sg-0357846ae2bbac7c6',
                            'Description': 'PostgreSQL to RDS'
                        }
                    ]
                }
            ]
        )
        
        print("✅ Lambda 보안 그룹 규칙 설정 완료")
        return sg_id
        
    except Exception as e:
        print(f"❌ Lambda 보안 그룹 생성 실패: {e}")
        return None

def update_rds_security_group(lambda_sg_id):
    """RDS 보안 그룹을 Lambda 보안 그룹만 허용하도록 업데이트"""
    try:
        ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
        
        rds_sg_id = 'sg-0357846ae2bbac7c6'
        
        print("🔧 RDS 보안 그룹 업데이트 중...")
        
        # 기존 0.0.0.0/0 규칙 제거
        try:
            ec2_client.revoke_security_group_ingress(
                GroupId=rds_sg_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 5432,
                        'ToPort': 5432,
                        'IpRanges': [
                            {
                                'CidrIp': '0.0.0.0/0',
                                'Description': 'Lambda access'
                            }
                        ]
                    }
                ]
            )
            print("✅ 위험한 0.0.0.0/0 규칙 제거 완료")
        except Exception as e:
            print(f"⚠️ 0.0.0.0/0 규칙 제거 실패 (이미 없을 수 있음): {e}")
        
        # Lambda 보안 그룹에서의 접근 허용
        try:
            ec2_client.authorize_security_group_ingress(
                GroupId=rds_sg_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 5432,
                        'ToPort': 5432,
                        'UserIdGroupPairs': [
                            {
                                'GroupId': lambda_sg_id,
                                'Description': 'Lambda VPC access'
                            }
                        ]
                    }
                ]
            )
            print(f"✅ Lambda 보안 그룹 ({lambda_sg_id}) 접근 허용 규칙 추가")
        except Exception as e:
            if 'already exists' in str(e):
                print("✅ Lambda 접근 규칙이 이미 존재합니다")
            else:
                print(f"❌ Lambda 접근 규칙 추가 실패: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ RDS 보안 그룹 업데이트 실패: {e}")
        return False

def configure_lambda_vpc():
    """Lambda 함수를 VPC 내부로 구성"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        # VPC 정보 조회
        vpc_info = get_vpc_info()
        if not vpc_info:
            return False
        
        # Lambda 보안 그룹 생성
        lambda_sg_id = create_lambda_security_group()
        if not lambda_sg_id:
            return False
        
        # RDS 보안 그룹 업데이트
        if not update_rds_security_group(lambda_sg_id):
            return False
        
        # Lambda 함수 VPC 설정
        print("🔧 Lambda 함수 VPC 설정 중...")
        
        # private 서브넷 사용 (RDS와 같은 VPC 내부)
        subnet_ids = [subnet['id'] for subnet in vpc_info['private_subnets']]
        if not subnet_ids:
            # private 서브넷이 없으면 public 서브넷 사용
            subnet_ids = [subnet['id'] for subnet in vpc_info['public_subnets']]
        
        if not subnet_ids:
            print("❌ 사용 가능한 서브넷이 없습니다")
            return False
        
        print(f"   - 사용할 서브넷: {subnet_ids}")
        print(f"   - 보안 그룹: {lambda_sg_id}")
        
        # Lambda VPC 설정 업데이트
        response = lambda_client.update_function_configuration(
            FunctionName='makenaide-ticker-scanner',
            VpcConfig={
                'SubnetIds': subnet_ids,
                'SecurityGroupIds': [lambda_sg_id]
            }
        )
        
        print("✅ Lambda 함수 VPC 설정 완료")
        print(f"   - VPC ID: {response['VpcConfig']['VpcId']}")
        print(f"   - 서브넷: {response['VpcConfig']['SubnetIds']}")
        print(f"   - 보안 그룹: {response['VpcConfig']['SecurityGroupIds']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lambda VPC 설정 실패: {e}")
        return False

def test_secure_lambda():
    """보안 설정된 Lambda 함수 테스트"""
    try:
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        
        print("\\n🧪 보안 설정된 Lambda 함수 테스트 중...")
        
        # Lambda 함수 호출
        response = lambda_client.invoke(
            FunctionName='makenaide-ticker-scanner',
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        payload = json.loads(response['Payload'].read())
        
        print(f"✅ Lambda 함수 테스트 완료")
        print(f"   - Status Code: {payload.get('statusCode')}")
        
        if 'body' in payload:
            body = json.loads(payload['body'])
            print(f"   - 메시지: {body.get('message')}")
            print(f"   - 티커 수: {body.get('ticker_count')}")
            print(f"   - 실행 시간: {body.get('execution_time', 0):.2f}초")
        
        return payload.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Lambda 함수 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🔒 Makenaide Lambda VPC 보안 설정 시작")
    print("="*60)
    
    # 1. VPC 정보 확인
    print("\\n1️⃣ VPC 정보 확인 중...")
    vpc_info = get_vpc_info()
    if not vpc_info:
        print("❌ VPC 정보 확인 실패")
        return False
    
    # 2. Lambda VPC 구성
    print("\\n2️⃣ Lambda VPC 보안 구성 중...")
    if not configure_lambda_vpc():
        print("❌ Lambda VPC 구성 실패")
        return False
    
    # 3. 설정 적용 대기
    print("\\n3️⃣ 설정 적용 대기 중... (30초)")
    time.sleep(30)
    
    # 4. 보안 설정 테스트
    print("\\n4️⃣ 보안 설정 테스트 중...")
    if not test_secure_lambda():
        print("❌ 보안 설정 테스트 실패")
        return False
    
    print("\\n" + "="*60)
    print("✅ Makenaide Lambda VPC 보안 설정 완료!")
    print("✅ 이제 Lambda는 VPC 내부에서 안전하게 RDS에 접근합니다.")
    print("✅ 0.0.0.0/0 규칙이 제거되어 보안이 강화되었습니다.")
    
    return True

if __name__ == "__main__":
    main()