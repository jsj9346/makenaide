#!/usr/bin/env python3
"""
🚀 Makenaide 전체 파이프라인 실행 테스트
- Phase 2부터 시작하여 전체 분석 파이프라인 실행
- 실제 업비트 데이터를 사용한 End-to-End 테스트
"""

import boto3
import json
import time
import pytz
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class MakenaideFullPipelineRunner:
    """Makenaide 전체 파이프라인 실행기"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.s3_client = boto3.client('s3')
        
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # Phase 함수명
        self.functions = {
            'phase2': 'makenaide-comprehensive-filter-phase2',
            'phase3': 'makenaide-gpt-analysis-phase3',
            'phase4': 'makenaide-4h-analysis-phase4', 
            'phase5': 'makenaide-condition-check-phase5'
        }
        
        self.results = {}
    
    def print_header(self, title: str):
        """헤더 출력"""
        print(f"\n{'='*70}")
        print(f"🚀 {title}")
        print(f"{'='*70}")
    
    def create_phase1_mock_data(self) -> bool:
        """Phase 1 목업 데이터 생성 (실제 업비트 API 기반)"""
        print("📝 Phase 1 목업 데이터 생성 중...")
        
        try:
            import urllib3
            http = urllib3.PoolManager()
            
            # 업비트 마켓 리스트 조회
            market_response = http.request('GET', 'https://api.upbit.com/v1/market/all')
            if market_response.status != 200:
                print("❌ 업비트 마켓 데이터 조회 실패")
                return False
            
            markets = json.loads(market_response.data.decode('utf-8'))
            krw_markets = [m['market'] for m in markets if m['market'].startswith('KRW-')][:50]  # 상위 50개
            
            print(f"   📊 {len(krw_markets)}개 KRW 마켓 확인")
            
            # 각 마켓의 기본 데이터 수집
            collected_data = []
            for i, market in enumerate(krw_markets[:20]):  # 처리 시간을 위해 20개로 제한
                try:
                    # 현재가 정보
                    ticker_url = f"https://api.upbit.com/v1/ticker?markets={market}"
                    ticker_response = http.request('GET', ticker_url)
                    
                    if ticker_response.status == 200:
                        ticker_data = json.loads(ticker_response.data.decode('utf-8'))[0]
                        
                        collected_data.append({
                            'ticker': market,
                            'current_price': float(ticker_data['trade_price']),
                            'volume_24h': float(ticker_data.get('acc_trade_volume_24h', 0)),
                            'volume_24h_krw': float(ticker_data.get('acc_trade_price_24h', 0)),
                            'price_change_24h': float(ticker_data.get('signed_change_rate', 0)) * 100,
                            'high_price': float(ticker_data.get('high_price', 0)),
                            'low_price': float(ticker_data.get('low_price', 0)),
                            'data_timestamp': datetime.now(self.kst).isoformat()
                        })
                        
                        if (i + 1) % 5 == 0:
                            print(f"   📊 데이터 수집 진행: {i+1}/{len(krw_markets[:20])}")
                            time.sleep(0.1)  # API 레이트 제한 준수
                    
                except Exception as e:
                    print(f"   ⚠️ {market} 데이터 수집 실패: {e}")
                    continue
            
            # Phase 1 결과 형태로 저장 (Phase 2가 기대하는 형식)
            phase1_data = {
                'phase': 'selective_data_collection',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'collected_count': len(collected_data),
                'filtered_data': collected_data,  # Phase 2가 기대하는 키명
                'data_source': 'upbit_api_mock',
                'note': 'Pipeline test용 실제 업비트 데이터'
            }
            
            # S3에 Phase 2가 기대하는 파일명으로 저장
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key='phase1/filtered_tickers_with_data.json',  # Phase 2가 기대하는 파일명
                Body=json.dumps(phase1_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            print(f"✅ Phase 1 목업 데이터 생성 완료: {len(collected_data)}개 종목")
            return True
            
        except Exception as e:
            print(f"❌ Phase 1 목업 데이터 생성 실패: {e}")
            return False
    
    def run_phase_with_retry(self, phase_name: str, function_name: str, max_retries: int = 2) -> Dict:
        """Phase 실행 (재시도 포함)"""
        print(f"\n🎯 {phase_name} 실행 중...")
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    print(f"   🔄 재시도 {attempt}/{max_retries}")
                    time.sleep(5)  # 재시도 전 대기
                
                start_time = time.time()
                
                response = self.lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps({
                        'source': 'full_pipeline_test',
                        'trigger': 'end_to_end_test',
                        'timestamp': datetime.now(self.kst).isoformat(),
                        'attempt': attempt + 1
                    })
                )
                
                duration = time.time() - start_time
                result = json.loads(response['Payload'].read())
                
                print(f"   ⏱️ 실행 시간: {duration:.1f}초")
                print(f"   📊 상태 코드: {result.get('statusCode')}")
                
                if result.get('statusCode') == 200:
                    # 성공 시 상세 정보 출력
                    phase = result.get('phase', 'unknown')
                    if phase == 'comprehensive_filtering':
                        passed = result.get('filtered_tickers', 0)
                        total = result.get('input_tickers', 0)
                        print(f"   ✅ 성공: {passed}/{total}개 종목 필터 통과")
                    elif phase == 'gpt_analysis':
                        buy_signals = result.get('buy_signals', 0)
                        total = result.get('analyzed_tickers', 0) 
                        model = result.get('model_used', 'GPT-4')
                        print(f"   ✅ 성공: {buy_signals}/{total}개 BUY 신호 ({model})")
                    elif phase == '4h_analysis':
                        timing_passed = result.get('timing_passed', 0)
                        total = result.get('analyzed_tickers', 0)
                        print(f"   ✅ 성공: {timing_passed}/{total}개 종목 타이밍 통과")
                    elif phase == 'condition_check':
                        buy_signals = result.get('buy_signals', 0)
                        total = result.get('processed_tickers', 0)
                        top_pick = result.get('top_pick', 'None')
                        print(f"   ✅ 성공: {buy_signals}/{total}개 최종 매수 신호")
                        if top_pick != 'None':
                            print(f"   🏆 최우선 종목: {top_pick}")
                    
                    return {
                        'success': True,
                        'duration': duration,
                        'result': result,
                        'attempt': attempt + 1
                    }
                
                else:
                    error_msg = result.get('error', 'Unknown error')
                    message = result.get('message', '')
                    print(f"   ❌ 실패: {error_msg}")
                    if message:
                        print(f"   💬 메시지: {message}")
                    
                    if attempt == max_retries:
                        return {
                            'success': False,
                            'duration': duration,
                            'result': result,
                            'attempt': attempt + 1
                        }
                
            except Exception as e:
                duration = time.time() - start_time if 'start_time' in locals() else 0
                print(f"   💥 실행 오류: {e}")
                
                if attempt == max_retries:
                    return {
                        'success': False,
                        'duration': duration,
                        'error': str(e),
                        'attempt': attempt + 1
                    }
        
        return {'success': False, 'duration': 0, 'attempt': max_retries + 1}
    
    def wait_for_phase_completion(self, phase_name: str, wait_seconds: int = 10):
        """Phase 완료 대기"""
        print(f"   ⏳ {phase_name} 완료 대기 ({wait_seconds}초)...")
        
        for i in range(wait_seconds):
            print(f"   {'▓' * (i + 1)}{'░' * (wait_seconds - i - 1)} {i+1}/{wait_seconds}초", end='\r')
            time.sleep(1)
        print()  # 줄바꿈
    
    def run_full_pipeline(self) -> bool:
        """전체 파이프라인 실행"""
        self.print_header("Makenaide 전체 파이프라인 실행 테스트")
        print(f"📅 시작 시간: {datetime.now(self.kst).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 실행 순서: Phase 1 (목업) → Phase 2 → Phase 3 → Phase 4 → Phase 5")
        
        overall_start = time.time()
        
        # Phase 1 목업 데이터 생성
        if not self.create_phase1_mock_data():
            print("❌ Phase 1 데이터 생성 실패 - 파이프라인 중단")
            return False
        
        # Phase 2-5 순차 실행
        phases = [
            ('Phase 2: Comprehensive Filtering', 'phase2'),
            ('Phase 3: GPT Analysis', 'phase3'), 
            ('Phase 4: 4H Analysis', 'phase4'),
            ('Phase 5: Condition Check', 'phase5')
        ]
        
        for phase_name, phase_key in phases:
            # Phase 간 처리 완료 대기
            if phase_key != 'phase2':
                self.wait_for_phase_completion(f"{phase_name} 준비", 5)
            
            result = self.run_phase_with_retry(phase_name, self.functions[phase_key])
            self.results[phase_key] = result
            
            if not result['success']:
                print(f"❌ {phase_name} 실패 - 파이프라인 중단")
                return False
        
        # 전체 실행 시간 계산
        total_duration = time.time() - overall_start
        
        # 최종 결과 요약
        self.generate_final_report(total_duration)
        
        return True
    
    def generate_final_report(self, total_duration: float):
        """최종 실행 보고서"""
        self.print_header("전체 파이프라인 실행 보고서")
        
        print(f"📊 실행 통계:")
        print(f"   • 총 실행 시간: {total_duration:.1f}초")
        print(f"   • 성공한 Phase: {len([r for r in self.results.values() if r['success']])}/4개")
        
        print(f"\n📋 Phase별 실행 결과:")
        for phase, result in self.results.items():
            duration = result.get('duration', 0)
            attempt = result.get('attempt', 1)
            status = "✅" if result['success'] else "❌"
            print(f"   {status} {phase.upper()}: {duration:.1f}초 (시도: {attempt}회)")
        
        # 최종 결과 확인
        if all(r['success'] for r in self.results.values()):
            try:
                # Phase 5 결과 확인
                phase5_result = self.results['phase5']['result']
                buy_signals = phase5_result.get('buy_signals', 0)
                top_pick = phase5_result.get('top_pick')
                
                print(f"\n🎯 최종 거래 신호:")
                print(f"   • 매수 신호: {buy_signals}개")
                if top_pick:
                    print(f"   • 최우선 종목: {top_pick}")
                else:
                    print(f"   • 최우선 종목: 없음")
                
                print(f"\n🚀 파이프라인 테스트 성공!")
                print(f"   모든 Phase가 정상 작동하여 실제 거래 신호까지 생성되었습니다.")
                
            except Exception as e:
                print(f"⚠️ 최종 결과 분석 중 오류: {e}")
        else:
            print(f"\n❌ 파이프라인 테스트 부분 실패")
            print(f"   일부 Phase에서 문제가 발생했습니다.")
        
        print(f"\n📈 다음 단계:")
        print(f"   1. 실제 거래 실행 연동 (Phase 6)")
        print(f"   2. CloudWatch 모니터링 설정")
        print(f"   3. 스케줄링 및 자동화 구성")

def main():
    """메인 실행 함수"""
    runner = MakenaideFullPipelineRunner()
    success = runner.run_full_pipeline()
    
    if success:
        print("\n🎉 전체 파이프라인 테스트 성공!")
        return True
    else:
        print("\n⚠️ 전체 파이프라인 테스트 완료 (일부 실패)")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)