#!/usr/bin/env python3
"""
공개 Lambda Layer 시도 및 네트워크 문제 해결
"""

import boto3
import json
import logging
import time
import zipfile
import io

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensivePsycopg2Fixer:
    """포괄적 psycopg2 및 네트워크 문제 해결기"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.ec2_client = boto3.client('ec2', region_name='ap-northeast-2')
        self.rds_client = boto3.client('rds', region_name='ap-northeast-2')
        
    def try_public_psycopg2_layers(self):
        """공개 psycopg2 Layer들 시도"""
        public_layers = [
            # jetbridge의 공개 psycopg2 Layer들
            'arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py311:1',
            'arn:aws:lambda:ap-northeast-2:898466741470:layer:psycopg2-py39:1',
            'arn:aws:lambda:us-east-1:898466741470:layer:psycopg2-py311:1',
            # 플랫폼 특정으로 만든 우리 Layer (이미 성공)
            'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-platform-psycopg2:1'
        ]
        
        for layer_arn in public_layers:
            try:
                logger.info(f"🔗 공개 Layer 시도: {layer_arn}")
                
                # Layer 설정
                self.lambda_client.update_function_configuration(
                    FunctionName='makenaide-ticker-scanner',
                    Layers=[layer_arn],
                    Timeout=300,
                    MemorySize=256
                )
                
                # 대기
                waiter = self.lambda_client.get_waiter('function_updated')
                waiter.wait(FunctionName='makenaide-ticker-scanner')
                
                # 테스트
                response = self.lambda_client.invoke(
                    FunctionName='makenaide-ticker-scanner',
                    InvocationType='RequestResponse',
                    Payload=json.dumps({})
                )
                
                response_payload = json.loads(response['Payload'].read())
                
                if 'errorMessage' in response_payload:
                    if 'psycopg2' in response_payload['errorMessage']:
                        logger.warning(f"❌ {layer_arn}에서 여전히 psycopg2 오류")
                        continue
                    else:
                        logger.info(f"✅ {layer_arn}에서 psycopg2 오류 해결, 다른 문제 발생")
                        return layer_arn, response_payload
                else:
                    logger.info(f"🎉 {layer_arn}에서 완전 성공!")
                    return layer_arn, response_payload
                    
            except Exception as e:
                logger.error(f"❌ {layer_arn} 시도 실패: {e}")
                continue
        
        return None, None
    
    def analyze_network_issue(self):
        """네트워크 문제 분석"""
        try:
            logger.info("🔍 네트워크 구성 분석 중...")
            
            # Lambda 함수 정보 확인
            lambda_config = self.lambda_client.get_function_configuration(
                FunctionName='makenaide-ticker-scanner'
            )
            
            vpc_config = lambda_config.get('VpcConfig', {})
            logger.info(f"📍 Lambda VPC 설정: {vpc_config}")
            
            # RDS 정보 확인
            rds_instances = self.rds_client.describe_db_instances(
                DBInstanceIdentifier='makenaide'
            )
            
            rds_instance = rds_instances['DBInstances'][0]
            rds_vpc_id = rds_instance['DBSubnetGroup']['VpcId']
            rds_security_groups = [sg['VpcSecurityGroupId'] for sg in rds_instance['VpcSecurityGroups']]
            
            logger.info(f"🗄️ RDS VPC ID: {rds_vpc_id}")
            logger.info(f"🔒 RDS Security Groups: {rds_security_groups}")
            
            # VPC 정보 수집
            vpcs = self.ec2_client.describe_vpcs(VpcIds=[rds_vpc_id])
            vpc = vpcs['Vpcs'][0]
            
            logger.info(f"🌐 RDS VPC CIDR: {vpc['CidrBlock']}")
            
            # 서브넷 정보 확인
            subnets = self.ec2_client.describe_subnets(
                Filters=[{'Name': 'vpc-id', 'Values': [rds_vpc_id]}]
            )
            
            private_subnets = []
            public_subnets = []
            
            for subnet in subnets['Subnets']:
                # Route table 확인하여 public/private 구분
                route_tables = self.ec2_client.describe_route_tables(
                    Filters=[{'Name': 'association.subnet-id', 'Values': [subnet['SubnetId']]}]
                )
                
                is_public = False
                for rt in route_tables['RouteTables']:
                    for route in rt['Routes']:
                        if route.get('GatewayId', '').startswith('igw-'):
                            is_public = True
                            break
                
                if is_public:
                    public_subnets.append(subnet['SubnetId'])
                else:
                    private_subnets.append(subnet['SubnetId'])
            
            logger.info(f"🔓 Public Subnets: {public_subnets}")
            logger.info(f"🔐 Private Subnets: {private_subnets}")
            
            return {
                'lambda_vpc_config': vpc_config,
                'rds_vpc_id': rds_vpc_id,
                'rds_security_groups': rds_security_groups,
                'vpc_cidr': vpc['CidrBlock'],
                'private_subnets': private_subnets,
                'public_subnets': public_subnets
            }
            
        except Exception as e:
            logger.error(f"❌ 네트워크 분석 실패: {e}")
            return None
    
    def configure_lambda_vpc(self, network_info):
        """Lambda VPC 설정"""
        try:
            logger.info("🔧 Lambda VPC 설정 중...")
            
            # Lambda용 보안 그룹 생성 또는 찾기
            try:
                # 기존 Lambda 보안 그룹 찾기
                sgs = self.ec2_client.describe_security_groups(
                    Filters=[
                        {'Name': 'group-name', 'Values': ['makenaide-lambda-sg']},
                        {'Name': 'vpc-id', 'Values': [network_info['rds_vpc_id']]}
                    ]
                )
                
                if sgs['SecurityGroups']:
                    lambda_sg_id = sgs['SecurityGroups'][0]['GroupId']
                    logger.info(f"✅ 기존 Lambda 보안 그룹 사용: {lambda_sg_id}")
                else:
                    # 새 보안 그룹 생성
                    sg_response = self.ec2_client.create_security_group(
                        GroupName='makenaide-lambda-sg',
                        Description='Security group for Makenaide Lambda functions',
                        VpcId=network_info['rds_vpc_id']
                    )
                    
                    lambda_sg_id = sg_response['GroupId']
                    logger.info(f"✅ 새 Lambda 보안 그룹 생성: {lambda_sg_id}")
                    
                    # 아웃바운드 규칙 추가 (HTTPS, PostgreSQL)
                    self.ec2_client.authorize_security_group_egress(
                        GroupId=lambda_sg_id,
                        IpPermissions=[
                            {
                                'IpProtocol': 'tcp',
                                'FromPort': 443,
                                'ToPort': 443,
                                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                            },
                            {
                                'IpProtocol': 'tcp',
                                'FromPort': 5432,
                                'ToPort': 5432,
                                'UserIdGroupPairs': [{'GroupId': sg_id} for sg_id in network_info['rds_security_groups']]
                            }
                        ]
                    )
                    
            except Exception as sg_error:
                logger.error(f"❌ 보안 그룹 처리 실패: {sg_error}")
                return False
            
            # RDS 보안 그룹에 Lambda 접근 허용 추가
            for rds_sg_id in network_info['rds_security_groups']:
                try:
                    self.ec2_client.authorize_security_group_ingress(
                        GroupId=rds_sg_id,
                        IpPermissions=[
                            {
                                'IpProtocol': 'tcp',
                                'FromPort': 5432,
                                'ToPort': 5432,
                                'UserIdGroupPairs': [{'GroupId': lambda_sg_id}]
                            }
                        ]
                    )
                    logger.info(f"✅ RDS 보안 그룹 {rds_sg_id}에 Lambda 접근 허용 추가")
                except Exception as ingress_error:
                    if 'InvalidGroup.Duplicate' in str(ingress_error):
                        logger.info(f"✅ RDS 보안 그룹 {rds_sg_id}에 이미 규칙 존재")
                    else:
                        logger.warning(f"⚠️ RDS 보안 그룹 {rds_sg_id} 규칙 추가 실패: {ingress_error}")
            
            # Lambda VPC 설정 업데이트
            vpc_config = {
                'SubnetIds': network_info['private_subnets'][:2],  # 최대 2개 서브넷
                'SecurityGroupIds': [lambda_sg_id]
            }
            
            logger.info(f"🔗 Lambda VPC 설정 적용: {vpc_config}")
            
            self.lambda_client.update_function_configuration(
                FunctionName='makenaide-ticker-scanner',
                VpcConfig=vpc_config,
                Timeout=300,
                MemorySize=256
            )
            
            # VPC 설정은 시간이 걸림
            logger.info("⏳ Lambda VPC 설정 대기 중... (최대 5분)")
            
            max_wait_time = 300  # 5분
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                try:
                    config = self.lambda_client.get_function_configuration(
                        FunctionName='makenaide-ticker-scanner'
                    )
                    
                    state = config.get('State', 'Unknown')
                    last_update_status = config.get('LastUpdateStatus', 'Unknown')
                    
                    logger.info(f"📊 Lambda 상태: {state}, 업데이트 상태: {last_update_status}")
                    
                    if state == 'Active' and last_update_status == 'Successful':
                        logger.info("✅ Lambda VPC 설정 완료")
                        return True
                    
                    time.sleep(10)
                    
                except Exception as status_error:
                    logger.warning(f"⚠️ 상태 확인 실패: {status_error}")
                    time.sleep(10)
            
            logger.error("❌ Lambda VPC 설정 타임아웃")
            return False
            
        except Exception as e:
            logger.error(f"❌ Lambda VPC 설정 실패: {e}")
            return False
    
    def test_final_solution(self):
        """최종 솔루션 테스트"""
        try:
            logger.info("🧪 최종 솔루션 테스트")
            
            response = self.lambda_client.invoke(
                FunctionName='makenaide-ticker-scanner',
                InvocationType='RequestResponse',
                Payload=json.dumps({})
            )
            
            status_code = response['StatusCode']
            response_payload = json.loads(response['Payload'].read())
            
            logger.info(f"📱 최종 응답 코드: {status_code}")
            logger.info(f"📄 최종 응답 내용: {response_payload}")
            
            # 성공 확인
            if status_code == 200:
                if 'errorMessage' in response_payload:
                    logger.error(f"❌ 최종 테스트 실패: {response_payload['errorMessage']}")
                    return False
                elif 'body' in response_payload:
                    body = json.loads(response_payload['body'])
                    if 'saved_count' in body:
                        logger.info("🎉 최종 솔루션 완전 성공!")
                        logger.info(f"  - 저장된 티커: {body.get('saved_count', 0)}개")
                        logger.info(f"  - 업데이트된 티커: {body.get('updated_count', 0)}개")
                        logger.info(f"  - 총 티커: {body.get('total_tickers', 0)}개")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ 최종 테스트 실패: {e}")
            return False
    
    def fix_all_issues(self):
        """모든 문제 종합 해결"""
        logger.info("🚀 종합적 문제 해결 시작")
        
        # 1단계: 공개 Layer 시도 (psycopg2 문제 확인)
        logger.info("\n" + "="*60)
        logger.info("1단계: 공개 psycopg2 Layer 확인")
        logger.info("="*60)
        
        layer_arn, response = self.try_public_psycopg2_layers()
        
        if not layer_arn:
            logger.error("❌ 모든 psycopg2 Layer 실패")
            return False
        
        logger.info(f"✅ 작동하는 Layer 발견: {layer_arn}")
        
        # psycopg2 문제는 해결되었지만 네트워크 문제가 있는지 확인
        if response and 'body' in response:
            body = json.loads(response['body'])
            if 'saved_count' in body:
                logger.info("🎉 이미 모든 문제 해결됨!")
                return True
        
        # 2단계: 네트워크 문제 분석
        logger.info("\n" + "="*60)
        logger.info("2단계: 네트워크 문제 분석")
        logger.info("="*60)
        
        network_info = self.analyze_network_issue()
        if not network_info:
            logger.error("❌ 네트워크 분석 실패")
            return False
        
        # Lambda가 VPC에 없다면 VPC 설정 필요
        if not network_info['lambda_vpc_config'].get('VpcId'):
            logger.info("🔧 Lambda VPC 설정 필요")
            
            # 3단계: Lambda VPC 설정
            logger.info("\n" + "="*60)
            logger.info("3단계: Lambda VPC 설정")
            logger.info("="*60)
            
            vpc_success = self.configure_lambda_vpc(network_info)
            if not vpc_success:
                logger.error("❌ Lambda VPC 설정 실패")
                return False
        else:
            logger.info("✅ Lambda가 이미 VPC에 설정됨")
        
        # 4단계: 최종 테스트
        logger.info("\n" + "="*60)
        logger.info("4단계: 최종 솔루션 테스트")
        logger.info("="*60)
        
        final_success = self.test_final_solution()
        
        return final_success

def main():
    """메인 실행 함수"""
    print("🔧 종합적 psycopg2 및 네트워크 문제 해결")
    print("=" * 70)
    
    fixer = ComprehensivePsycopg2Fixer()
    
    try:
        success = fixer.fix_all_issues()
        
        if success:
            print("\n" + "="*70)
            print("🎉 SUCCESS: 모든 문제 해결 완료!")
            print("✅ Lambda 함수가 DB에 정상적으로 데이터를 저장하고 있습니다.")
            print("🔧 해결된 문제:")
            print("  - psycopg2 바이너리 호환성 문제")
            print("  - Lambda-RDS 네트워크 연결 문제")
            print("  - VPC 보안 그룹 설정")
        else:
            print("\n" + "="*70)
            print("❌ FAILURE: 일부 문제가 여전히 남아있습니다.")
            print("🔧 추가 조치 필요")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ 종합 해결 중 예외 발생: {e}")
        return False

if __name__ == "__main__":
    main() 