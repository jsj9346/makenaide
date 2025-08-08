#!/usr/bin/env python3
"""
API Gateway 단독 배포 스크립트
AmazonAPIGatewayAdministrator 권한으로 API Gateway 구성
"""

import boto3
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트 초기화
apigateway_client = boto3.client('apigateway', region_name='ap-northeast-2')
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

def deploy_api_gateway():
    """API Gateway 배포"""
    try:
        logger.info("🚀 API Gateway 배포 시작")
        
        # Lambda 함수 ARN 조회
        lambda_function_name = 'makenaide-api-gateway'
        lambda_response = lambda_client.get_function(FunctionName=lambda_function_name)
        lambda_function_arn = lambda_response['Configuration']['FunctionArn']
        
        logger.info(f"📋 Lambda 함수 확인: {lambda_function_arn}")
        
        # REST API 생성
        api_response = apigateway_client.create_rest_api(
            name='makenaide-api',
            description='Makenaide System REST API - 로그, 리포트, DB조회',
            endpointConfiguration={'types': ['REGIONAL']}
        )
        
        api_id = api_response['id']
        logger.info(f"✅ REST API 생성 완료: {api_id}")
        
        # 루트 리소스 ID 조회
        resources_response = apigateway_client.get_resources(restApiId=api_id)
        root_resource_id = None
        for resource in resources_response['items']:
            if resource['path'] == '/':
                root_resource_id = resource['id']
                break
        
        # API 엔드포인트 정의
        endpoints = [
            {'path': 'status', 'method': 'GET'},
            {'path': 'ticker', 'method': 'GET'},
            {'path': 'logs', 'method': 'GET'},
            {'path': 'performance', 'method': 'GET'},
            {'path': 'tickers', 'method': 'GET'}
        ]
        
        for endpoint in endpoints:
            logger.info(f"🔧 엔드포인트 생성 중: /{endpoint['path']}")
            
            # 리소스 생성
            resource_response = apigateway_client.create_resource(
                restApiId=api_id,
                parentId=root_resource_id,
                pathPart=endpoint['path']
            )
            
            resource_id = resource_response['id']
            
            # GET 메소드 생성
            apigateway_client.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=endpoint['method'],
                authorizationType='NONE'
            )
            
            # Lambda 통합 설정
            integration_uri = f"arn:aws:apigateway:ap-northeast-2:lambda:path/2015-03-31/functions/{lambda_function_arn}/invocations"
            
            apigateway_client.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=endpoint['method'],
                type='AWS_PROXY',
                integrationHttpMethod='POST',
                uri=integration_uri
            )
            
            # CORS 옵션 메소드 추가
            apigateway_client.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                authorizationType='NONE'
            )
            
            apigateway_client.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                type='MOCK',
                requestTemplates={'application/json': '{"statusCode": 200}'}
            )
            
            # OPTIONS 응답 설정
            apigateway_client.put_method_response(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': False,
                    'method.response.header.Access-Control-Allow-Methods': False,
                    'method.response.header.Access-Control-Allow-Origin': False
                }
            )
            
            apigateway_client.put_integration_response(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                    'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS'",
                    'method.response.header.Access-Control-Allow-Origin': "'*'"
                }
            )
            
            logger.info(f"✅ {endpoint['path']} 엔드포인트 생성 완료")
        
        # API 배포
        deployment_response = apigateway_client.create_deployment(
            restApiId=api_id,
            stageName='prod',
            description='Production deployment for Makenaide API'
        )
        
        logger.info("✅ API 배포 완료")
        
        # Lambda 권한 추가 (API Gateway가 Lambda 호출할 수 있도록)
        try:
            lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId='AllowAPIGatewayInvoke',
                Action='lambda:InvokeFunction',
                Principal='apigateway.amazonaws.com',
                SourceArn=f"arn:aws:execute-api:ap-northeast-2:901361833359:{api_id}/*/*"
            )
            logger.info("✅ Lambda 권한 추가 완료")
        except Exception as e:
            if "ResourceConflictException" in str(e):
                logger.info("✅ Lambda 권한 이미 존재함")
            else:
                logger.error(f"⚠️ Lambda 권한 추가 실패: {e}")
        
        # API URL 생성
        api_url = f"https://{api_id}.execute-api.ap-northeast-2.amazonaws.com/prod"
        
        logger.info("=" * 60)
        logger.info("✅ API Gateway 배포 완료!")
        logger.info("=" * 60)
        logger.info(f"🔗 API URL: {api_url}")
        logger.info("📋 사용 가능한 엔드포인트:")
        logger.info(f"   - {api_url}/status")
        logger.info(f"   - {api_url}/ticker?symbol=BTC&days=30")
        logger.info(f"   - {api_url}/logs?hours=24&level=INFO")
        logger.info(f"   - {api_url}/performance")
        logger.info(f"   - {api_url}/tickers")
        logger.info("=" * 60)
        
        return {
            'api_id': api_id,
            'api_url': api_url,
            'lambda_function_arn': lambda_function_arn
        }
        
    except Exception as e:
        logger.error(f"❌ API Gateway 배포 실패: {e}")
        raise

def test_api_endpoints(api_url):
    """API 엔드포인트 테스트"""
    import requests
    
    try:
        logger.info("🧪 API 엔드포인트 테스트 시작")
        
        # /status 엔드포인트 테스트
        status_url = f"{api_url}/status"
        response = requests.get(status_url, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"✅ /status 엔드포인트 정상: {response.status_code}")
            logger.info(f"   응답: {response.json()}")
        else:
            logger.error(f"❌ /status 엔드포인트 오류: {response.status_code}")
            logger.error(f"   응답: {response.text}")
        
        # 기타 엔드포인트도 간단히 테스트
        endpoints_to_test = ['/tickers', '/performance']
        
        for endpoint in endpoints_to_test:
            test_url = f"{api_url}{endpoint}"
            try:
                response = requests.get(test_url, timeout=5)
                logger.info(f"✅ {endpoint} 응답: {response.status_code}")
            except Exception as e:
                logger.warning(f"⚠️ {endpoint} 테스트 실패: {e}")
        
    except Exception as e:
        logger.error(f"❌ API 테스트 실패: {e}")

def main():
    """메인 함수"""
    try:
        # API Gateway 배포
        result = deploy_api_gateway()
        
        # API 테스트
        test_api_endpoints(result['api_url'])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ API Gateway 배포 실패: {e}")
        return None

if __name__ == "__main__":
    main() 