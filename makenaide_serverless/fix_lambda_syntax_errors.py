#!/usr/bin/env python3
"""
🔧 Lambda 함수 구문 오류 수정 스크립트
- 시간대 전략 코드 주입 시 발생한 구문 오류 수정
- 더 안전한 코드 주입 방식으로 재배포
- Lambda 레이어 연결 상태 확인 및 수정
"""

import boto3
import json
import logging
import zipfile
import os
import time
from typing import Dict, List
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LambdaSyntaxFixer:
    """Lambda 함수 구문 오류 수정 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.region = 'ap-northeast-2'
        
        # 수정이 필요한 Lambda 함수들
        self.target_functions = [
            'makenaide-scanner',
            'makenaide-data-collector', 
            'makenaide-comprehensive-filter-phase2',
            'makenaide-gpt-analysis-phase3',
            'makenaide-4h-analysis-phase4',
            'makenaide-condition-check-phase5',
            'makenaide-trade-execution-phase6'
        ]
        
        self.layer_arn = "arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-timezone-strategy-enhancer:1"
    
    def wait_for_function_ready(self, function_name: str, max_wait_seconds: int = 120) -> bool:
        """함수가 업데이트 가능한 상태가 될 때까지 대기"""
        logger.info(f"{function_name} 상태 확인 중...")
        
        wait_time = 0
        while wait_time < max_wait_seconds:
            try:
                response = self.lambda_client.get_function(FunctionName=function_name)
                state = response.get('Configuration', {}).get('State', 'Unknown')
                
                if state == 'Active':
                    logger.info(f"✅ {function_name} 준비 완료")
                    return True
                elif state == 'Pending':
                    logger.info(f"⏳ {function_name} 대기 중... ({wait_time}s)")
                    time.sleep(5)
                    wait_time += 5
                else:
                    logger.warning(f"⚠️ {function_name} 상태: {state}")
                    time.sleep(3)
                    wait_time += 3
                    
            except Exception as e:
                logger.error(f"상태 확인 실패: {e}")
                return False
        
        logger.error(f"❌ {function_name} 대기 시간 초과")
        return False
    
    def create_minimal_timezone_integration(self) -> str:
        """최소한의 시간대 전략 통합 코드 (구문 오류 방지)"""
        return '''
# === 시간대 전략 통합 (안전 버전) ===
import sys
import json
import logging

# 시간대 전략 모듈 로드 (선택적)
TIMEZONE_STRATEGY_ENABLED = False
try:
    sys.path.append('/opt/python')
    from timezone_strategy_enhancer import TimezoneStrategyEnhancer
    timezone_enhancer = TimezoneStrategyEnhancer()
    TIMEZONE_STRATEGY_ENABLED = True
    logger.info("✅ 시간대 전략 모듈 로드 성공")
except ImportError:
    logger.info("⚠️ 시간대 전략 모듈 없음 - 기본 모드로 실행")
    timezone_enhancer = None
except Exception as e:
    logger.warning(f"⚠️ 시간대 전략 초기화 실패: {e}")
    timezone_enhancer = None

def get_timezone_strategy_config(base_amount=1000000):
    """시간대 전략 설정 조회"""
    if not TIMEZONE_STRATEGY_ENABLED or not timezone_enhancer:
        return {
            'position_size': base_amount * 0.5,
            'stop_loss_pct': 8.0,
            'strategy_enabled': False
        }
    
    try:
        # 현재 BTC 가격 조회
        import urllib3
        http = urllib3.PoolManager()
        response = http.request('GET', 'https://api.upbit.com/v1/ticker?markets=KRW-BTC')
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))[0]
            btc_price = float(data['trade_price'])
        else:
            btc_price = 159348000
        
        # 전략 설정 생성
        strategy = timezone_enhancer.generate_comprehensive_strategy_config(
            entry_price=btc_price,
            base_amount=base_amount
        )
        
        return {
            'position_size': strategy['position_management']['position_size_krw'],
            'stop_loss_pct': strategy['risk_management']['stop_loss']['stop_loss_percentage'],
            'global_activity': strategy['market_context']['global_activity_score'],
            'dominant_region': strategy['market_context']['dominant_region'],
            'strategy_enabled': True
        }
        
    except Exception as e:
        logger.warning(f"전략 설정 생성 실패: {e}")
        return {
            'position_size': base_amount * 0.5,
            'stop_loss_pct': 8.0,
            'strategy_enabled': False
        }
# === 시간대 전략 통합 종료 ===

'''
    
    def fix_function_syntax(self, function_name: str) -> bool:
        """개별 Lambda 함수 구문 오류 수정"""
        try:
            logger.info(f"🔧 {function_name} 구문 오류 수정 시작...")
            
            # 함수 상태 확인
            if not self.wait_for_function_ready(function_name):
                return False
            
            # 현재 함수 코드 다운로드
            response = self.lambda_client.get_function(FunctionName=function_name)
            code_location = response['Code']['Location']
            
            import urllib3
            http = urllib3.PoolManager()
            code_response = http.request('GET', code_location)
            
            if code_response.status != 200:
                logger.error(f"코드 다운로드 실패: {code_response.status}")
                return False
            
            # ZIP 파일에서 코드 추출
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
                tmp_file.write(code_response.data)
                tmp_file_path = tmp_file.name
            
            try:
                original_code = None
                with zipfile.ZipFile(tmp_file_path, 'r') as zip_ref:
                    for file_name in zip_ref.namelist():
                        if file_name.endswith('.py') and ('lambda_function' in file_name or 'main' in file_name):
                            with zip_ref.open(file_name) as f:
                                code_content = f.read().decode('utf-8')
                                if 'lambda_handler' in code_content:
                                    original_code = code_content
                                    break
                
                if not original_code:
                    logger.error(f"메인 Python 파일을 찾을 수 없음: {function_name}")
                    return False
                
                # 기존 시간대 전략 코드 제거 (있다면)
                lines = original_code.split('\n')
                cleaned_lines = []
                skip_until_end = False
                
                for line in lines:
                    if '=== 시간대 전략 통합' in line:
                        skip_until_end = True
                        continue
                    elif skip_until_end and ('=== 시간대 전략 통합 종료 ===' in line or '# === 시간대 전략 통합 코드 종료 ===' in line):
                        skip_until_end = False
                        continue
                    elif not skip_until_end:
                        cleaned_lines.append(line)
                
                # 새로운 안전한 시간대 전략 코드 추가
                # import 구문 뒤에 추가
                insert_index = 0
                for i, line in enumerate(cleaned_lines):
                    if line.strip().startswith('import ') or line.strip().startswith('from '):
                        insert_index = i + 1
                    elif line.strip() and not line.strip().startswith('#'):
                        break
                
                # 시간대 전략 코드 삽입
                strategy_lines = self.create_minimal_timezone_integration().strip().split('\n')
                cleaned_lines[insert_index:insert_index] = strategy_lines
                
                # lambda_handler에 전략 로딩 코드 추가
                final_lines = []
                for i, line in enumerate(cleaned_lines):
                    final_lines.append(line)
                    
                    # lambda_handler 시작 부분에 전략 로딩 추가
                    if 'def lambda_handler(' in line:
                        # 다음 몇 줄에서 적절한 위치 찾기
                        for j in range(1, min(10, len(cleaned_lines) - i)):
                            next_line = cleaned_lines[i + j]
                            if 'try:' in next_line or 'logger.info' in next_line:
                                final_lines.append('    ')
                                final_lines.append('    # 시간대 전략 설정 로드')
                                final_lines.append('    timezone_config = get_timezone_strategy_config()')
                                final_lines.append('    if timezone_config["strategy_enabled"]:')
                                final_lines.append('        logger.info(f"🌏 시간대 전략 적용: 포지션 {timezone_config[\"position_size\"]:,.0f}KRW, 손절 {timezone_config[\"stop_loss_pct\"]:.1f}%")')
                                final_lines.append('    ')
                                break
                        break
                
                fixed_code = '\n'.join(final_lines)
                
                # 새로운 배포 패키지 생성
                deployment_zip = f'{function_name}_syntax_fixed.zip'
                with zipfile.ZipFile(deployment_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.writestr('lambda_function.py', fixed_code)
                
                # 코드 업데이트
                logger.info(f"코드 업데이트 중...")
                with open(deployment_zip, 'rb') as f:
                    zip_content = f.read()
                
                self.lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_content,
                    Publish=True
                )
                
                logger.info(f"✅ {function_name} 구문 오류 수정 완료")
                
                # 정리
                os.remove(deployment_zip)
                
                return True
                
            finally:
                os.unlink(tmp_file_path)
            
        except Exception as e:
            logger.error(f"❌ {function_name} 구문 오류 수정 실패: {e}")
            return False
    
    def ensure_layer_attachment(self, function_name: str) -> bool:
        """Lambda 레이어 연결 상태 확인 및 수정"""
        try:
            logger.info(f"🔗 {function_name} 레이어 연결 확인 중...")
            
            # 함수가 준비 상태인지 확인
            if not self.wait_for_function_ready(function_name):
                return False
            
            # 현재 설정 확인
            config = self.lambda_client.get_function_configuration(FunctionName=function_name)
            current_layers = [layer['Arn'] for layer in config.get('Layers', [])]
            
            # 시간대 전략 레이어가 연결되어 있는지 확인
            if self.layer_arn in current_layers:
                logger.info(f"✅ {function_name} 레이어 이미 연결됨")
                return True
            
            # 레이어 추가
            current_layers.append(self.layer_arn)
            
            self.lambda_client.update_function_configuration(
                FunctionName=function_name,
                Layers=current_layers
            )
            
            logger.info(f"✅ {function_name} 레이어 연결 완료")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ {function_name} 레이어 연결 실패: {e}")
            return False
    
    def test_fixed_function(self, function_name: str) -> bool:
        """수정된 함수 테스트"""
        try:
            logger.info(f"🧪 {function_name} 테스트 중...")
            
            test_event = {
                'test': True,
                'timezone_fix_test': True,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            if response['StatusCode'] == 200 and 'FunctionError' not in response:
                payload = json.loads(response['Payload'].read())
                logger.info(f"✅ {function_name} 테스트 성공!")
                return True
            else:
                error_info = response.get('FunctionError', 'Unknown error')
                logger.error(f"❌ {function_name} 테스트 실패: {error_info}")
                return False
                
        except Exception as e:
            logger.error(f"❌ {function_name} 테스트 중 오류: {e}")
            return False
    
    def fix_all_functions(self) -> Dict[str, bool]:
        """모든 Lambda 함수 구문 오류 수정"""
        logger.info("🚀 Lambda 함수 구문 오류 일괄 수정 시작")
        logger.info("=" * 80)
        
        results = {}
        
        for function_name in self.target_functions:
            logger.info(f"\n🔧 {function_name} 처리 중...")
            
            # 1. 구문 오류 수정
            fix_success = self.fix_function_syntax(function_name)
            if not fix_success:
                results[function_name] = False
                continue
            
            # 2. 레이어 연결 확인
            layer_success = self.ensure_layer_attachment(function_name)
            
            # 3. 테스트
            test_success = self.test_fixed_function(function_name)
            
            # 전체 성공 여부
            overall_success = fix_success and test_success
            results[function_name] = overall_success
            
            if overall_success:
                logger.info(f"✅ {function_name} 수정 완료!")
            else:
                logger.warning(f"⚠️ {function_name} 부분적 성공 (레이어: {layer_success}, 테스트: {test_success})")
            
            # 다음 함수 처리 전 대기
            time.sleep(2)
        
        # 결과 요약
        success_count = sum(results.values())
        total_count = len(results)
        
        logger.info("=" * 80)
        logger.info(f"🎯 구문 오류 수정 완료: {success_count}/{total_count} 함수 성공")
        
        print(f"""
🔧 Lambda 함수 구문 오류 수정 완료!

📊 수정 결과:
   • 총 함수: {total_count}개
   • 성공: {success_count}개
   • 실패: {total_count - success_count}개

🔍 상세 결과:
{chr(10).join(f'   • {func}: {"✅ 성공" if status else "❌ 실패"}' for func, status in results.items())}

🌏 시간대 전략 적용:
   • 안전한 코드 주입 방식으로 구문 오류 방지
   • 선택적 모듈 로딩으로 호환성 확보
   • Lambda 레이어 기반 시간대 분석 기능

🎯 다음 단계:
   • EventBridge 스케줄에서 자동 실행 확인
   • CloudWatch 로그에서 시간대 전략 적용 모니터링
   • Phase별 전용 전략 설정 파일 생성
        """)
        
        return results

def main():
    """메인 실행"""
    fixer = LambdaSyntaxFixer()
    results = fixer.fix_all_functions()
    
    success_count = sum(results.values())
    total_count = len(results)
    
    if success_count >= total_count * 0.8:  # 80% 이상 성공
        print("🎉 구문 오류 수정 성공!")
        exit(0)
    else:
        print("⚠️ 일부 함수 수정 실패!")
        exit(1)

if __name__ == '__main__':
    main()