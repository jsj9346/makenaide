#!/usr/bin/env python3
"""
🔑 Phase 3 Lambda에 OpenAI API 키 설정
- 환경변수로 OPENAI_API_KEY 설정
- 보안을 위해 명령줄 인자로 API 키 받기
"""

import boto3
import sys
import getpass

def set_openai_api_key(api_key: str):
    """Lambda 함수에 OpenAI API 키 설정"""
    try:
        lambda_client = boto3.client('lambda')
        function_name = 'makenaide-gpt-analysis-phase3'
        
        print(f"🔧 {function_name} 환경변수 업데이트 중...")
        
        # 현재 설정 가져오기
        current_config = lambda_client.get_function_configuration(
            FunctionName=function_name
        )
        
        current_vars = current_config.get('Environment', {}).get('Variables', {})
        
        # API 키 업데이트
        current_vars['OPENAI_API_KEY'] = api_key
        
        # 환경변수 업데이트
        response = lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={'Variables': current_vars}
        )
        
        print("✅ OpenAI API 키 설정 완료!")
        print(f"   함수: {function_name}")
        print(f"   모델: {current_vars.get('GPT_MODEL', 'gpt-4-turbo-preview')}")
        
        return True
        
    except Exception as e:
        print(f"❌ API 키 설정 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🔑 OpenAI API 키 설정")
    print("="*60)
    
    if len(sys.argv) > 1:
        # 명령줄 인자로 받은 경우
        api_key = sys.argv[1]
    else:
        # 대화형으로 입력받기 (보안)
        api_key = getpass.getpass("OpenAI API 키를 입력하세요: ")
    
    if not api_key or not api_key.startswith('sk-'):
        print("❌ 유효하지 않은 API 키입니다.")
        print("   OpenAI API 키는 'sk-'로 시작해야 합니다.")
        return False
    
    success = set_openai_api_key(api_key)
    
    if success:
        print("\n✅ 설정 완료! 이제 Phase 3를 사용할 수 있습니다.")
        print("테스트하려면 다음 명령을 실행하세요:")
        print("aws lambda invoke --function-name makenaide-gpt-analysis-phase3 --payload '{}' response.json")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)