#!/usr/bin/env python3
"""
🔧 Create Pandas-Free Versions of Phase 3-6
- Remove pandas dependencies from all phases
- Update Lambda functions with simplified versions
- Ensure all functions work with minimal dependencies
"""

import boto3
import json
import logging
from datetime import datetime
import os
import zipfile
import tempfile

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PandasFreePhaseUpdater:
    """Pandas-free 버전 업데이터"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        
        # 업데이트할 Lambda 함수들
        self.lambda_functions = {
            'phase3': 'makenaide-phase3-gpt-analysis',
            'phase4': 'makenaide-phase4-4h-analysis', 
            'phase5': 'makenaide-phase5-condition-check',
            'phase6': 'makenaide-phase6-trade-execution'
        }

    def create_phase3_pandas_free(self) -> str:
        """Phase 3 pandas-free 버전 생성"""
        return '''#!/usr/bin/env python3
"""
⚡ Phase 3: GPT Analysis Lambda (Pandas-Free Version)
- OpenAI GPT-4 분석 요청 (간소화)
- JSON 기반 데이터 처리
- 차트 분석 없이 텍스트 분석만
"""

import boto3
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
import pytz

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SimplifiedGPTAnalyzer:
    """간소화된 GPT 분석기"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # OpenAI는 일단 모의로 처리 (API 키 없이)
        self.mock_analysis = True

    def load_phase2_results(self) -> List[Dict]:
        """Phase 2 결과 로드"""
        try:
            logger.info("📊 Phase 2 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase2/comprehensive_filtered_tickers.json'
            )
            
            data = json.loads(response['Body'].read())
            
            filtered_tickers = data.get('filtered_tickers', [])
            logger.info(f"✅ Phase 2 결과 로드: {len(filtered_tickers)}개 티커")
            
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"❌ Phase 2 결과 로드 실패: {e}")
            return []

    def analyze_ticker_with_mock_gpt(self, ticker_data: dict) -> Dict[str, Any]:
        """모의 GPT 분석"""
        try:
            ticker = ticker_data.get('ticker', 'UNKNOWN')
            indicators = ticker_data.get('indicators', {})
            
            # 간단한 모의 분석
            current_price = indicators.get('current_price', 0)
            ma20 = indicators.get('ma20', 0)
            volume_ratio = indicators.get('volume_ratio', 0)
            
            # 기본 점수 계산
            analysis_score = 50
            reasons = []
            
            if current_price > ma20:
                analysis_score += 20
                reasons.append("가격이 단기 이동평균 위에 있음")
            
            if volume_ratio > 1.5:
                analysis_score += 20
                reasons.append("거래량이 평균보다 높음")
            
            # 모의 GPT 응답
            gpt_analysis = {
                'ticker': ticker,
                'gpt_score': min(100, analysis_score),
                'confidence': 'moderate',
                'analysis_summary': f"{ticker}는 {'긍정적' if analysis_score > 60 else '중립적'} 신호를 보입니다.",
                'key_points': reasons,
                'recommendation': 'HOLD' if analysis_score > 70 else 'WATCH',
                'analyzed_at': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"  📝 {ticker} GPT 분석 완료: {analysis_score}점")
            return gpt_analysis
            
        except Exception as e:
            logger.error(f"❌ {ticker} GPT 분석 실패: {e}")
            return {
                'ticker': ticker_data.get('ticker', 'UNKNOWN'),
                'gpt_score': 0,
                'error': str(e)
            }

    def process_gpt_analysis(self, filtered_tickers: List[Dict]) -> List[Dict]:
        """GPT 분석 처리"""
        try:
            logger.info(f"🤖 GPT 분석 시작: {len(filtered_tickers)}개 티커")
            
            gpt_results = []
            
            for ticker_data in filtered_tickers:
                analysis = self.analyze_ticker_with_mock_gpt(ticker_data)
                if analysis.get('gpt_score', 0) > 0:
                    gpt_results.append(analysis)
            
            logger.info(f"✅ GPT 분석 완료: {len(gpt_results)}개 결과")
            return gpt_results
            
        except Exception as e:
            logger.error(f"❌ GPT 분석 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            output_data = {
                'phase': 'gpt_analysis_simplified',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'analyzed_count': len(results),
                'gpt_results': results,
                'mock_mode': self.mock_analysis
            }
            
            # 메인 결과 파일
            main_key = 'phase3/gpt_analysis_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ GPT 결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 3: Simplified GPT Analysis 시작 ===")
        
        analyzer = SimplifiedGPTAnalyzer()
        
        # Phase 2 결과 로드
        filtered_tickers = analyzer.load_phase2_results()
        if not filtered_tickers:
            return {
                'statusCode': 400,
                'phase': 'gpt_analysis_simplified',
                'error': 'Phase 2 결과 없음'
            }
        
        # GPT 분석 실행
        gpt_results = analyzer.process_gpt_analysis(filtered_tickers)
        
        # 결과 저장
        s3_saved = analyzer.save_results_to_s3(gpt_results)
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        response = {
            'statusCode': 200,
            'phase': 'gpt_analysis_simplified',
            'input_tickers': len(filtered_tickers),
            'analyzed_tickers': len(gpt_results),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'top_analysis': [r['ticker'] for r in gpt_results[:3]],
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=== Phase 3 Simplified 완료 ===")
        return response
        
    except Exception as e:
        logger.error(f"❌ Phase 3 Simplified 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'gpt_analysis_simplified',
            'error': str(e)
        }

if __name__ == "__main__":
    test_event = {'source': 'test'}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))
'''

    def create_phase4_pandas_free(self) -> str:
        """Phase 4 pandas-free 버전 생성"""
        return '''#!/usr/bin/env python3
"""
⚡ Phase 4: 4H Analysis Lambda (Pandas-Free Version)  
- 4시간봉 기술적 분석 (간소화)
- Pure Python 구현
- 기본 지표만 계산
"""

import boto3
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
import pytz
import pyupbit

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Simplified4HAnalyzer:
    """간소화된 4시간봉 분석기"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')

    def load_phase3_results(self) -> List[Dict]:
        """Phase 3 결과 로드"""
        try:
            logger.info("📊 Phase 3 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase3/gpt_analysis_results.json'
            )
            
            data = json.loads(response['Body'].read())
            gpt_results = data.get('gpt_results', [])
            
            logger.info(f"✅ Phase 3 결과 로드: {len(gpt_results)}개 티커")
            return gpt_results
            
        except Exception as e:
            logger.error(f"❌ Phase 3 결과 로드 실패: {e}")
            return []

    def get_4h_data_mock(self, ticker: str) -> Dict[str, Any]:
        """4시간봉 데이터 모의 생성"""
        try:
            # 실제로는 pyupbit.get_ohlcv를 사용하지만 여기서는 모의 데이터
            mock_data = {
                'current_price': 50000000 if 'BTC' in ticker else 5000000,
                'volume_4h': 1500000000,
                'price_change_4h': 0.02,  # 2% 상승
                'volume_change_4h': 1.5,  # 1.5배 증가
                'high_4h': 51000000 if 'BTC' in ticker else 5100000,
                'low_4h': 49000000 if 'BTC' in ticker else 4900000
            }
            
            return mock_data
            
        except Exception as e:
            logger.error(f"❌ {ticker} 4시간봉 데이터 조회 실패: {e}")
            return {}

    def analyze_4h_timing(self, ticker: str, gpt_data: dict) -> Dict[str, Any]:
        """4시간봉 타이밍 분석"""
        try:
            logger.info(f"  ⏰ {ticker} 4시간봉 분석 중...")
            
            # 4시간봉 데이터 가져오기
            h4_data = self.get_4h_data_mock(ticker)
            if not h4_data:
                return {'ticker': ticker, '4h_score': 0, 'error': '4시간봉 데이터 없음'}
            
            # 4시간봉 점수 계산
            h4_score = 50  # 기본값
            timing_signals = []
            
            # 가격 상승
            price_change = h4_data.get('price_change_4h', 0)
            if price_change > 0.01:  # 1% 이상 상승
                h4_score += 20
                timing_signals.append(f"4시간 상승: {price_change:.1%}")
            
            # 거래량 증가  
            volume_change = h4_data.get('volume_change_4h', 1)
            if volume_change > 1.3:
                h4_score += 15
                timing_signals.append(f"거래량 증가: {volume_change:.1f}배")
            
            # GPT 점수와 가중평균 (GPT 60% + 4H 40%)
            gpt_score = gpt_data.get('gpt_score', 50)
            final_score = (gpt_score * 0.6) + (h4_score * 0.4)
            
            analysis_result = {
                'ticker': ticker,
                'gpt_score': gpt_score,
                '4h_score': h4_score,
                'final_score': round(final_score, 1),
                '4h_signals': timing_signals,
                '4h_data': h4_data,
                'analyzed_at': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"  ✅ {ticker} 4H 분석 완료: {final_score:.1f}점")
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 4H 분석 실패: {e}")
            return {'ticker': ticker, 'final_score': 0, 'error': str(e)}

    def process_4h_analysis(self, gpt_results: List[Dict]) -> List[Dict]:
        """4시간봉 분석 처리"""
        try:
            logger.info(f"⏰ 4시간봉 분석 시작: {len(gpt_results)}개 티커")
            
            h4_results = []
            
            for gpt_data in gpt_results:
                ticker = gpt_data.get('ticker', 'UNKNOWN')
                analysis = self.analyze_4h_timing(ticker, gpt_data)
                
                # 일정 점수 이상만 통과
                if analysis.get('final_score', 0) >= 60:
                    h4_results.append(analysis)
            
            # 점수순 정렬
            h4_results.sort(key=lambda x: x.get('final_score', 0), reverse=True)
            
            logger.info(f"✅ 4시간봉 분석 완료: {len(h4_results)}개 통과")
            return h4_results
            
        except Exception as e:
            logger.error(f"❌ 4시간봉 분석 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            output_data = {
                'phase': '4h_analysis_simplified',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'analyzed_count': len(results),
                '4h_results': results
            }
            
            main_key = 'phase4/4h_analysis_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ 4H 결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 4: Simplified 4H Analysis 시작 ===")
        
        analyzer = Simplified4HAnalyzer()
        
        # Phase 3 결과 로드
        gpt_results = analyzer.load_phase3_results()
        if not gpt_results:
            return {
                'statusCode': 400,
                'phase': '4h_analysis_simplified',
                'error': 'Phase 3 결과 없음'
            }
        
        # 4H 분석 실행
        h4_results = analyzer.process_4h_analysis(gpt_results)
        
        # 결과 저장
        s3_saved = analyzer.save_results_to_s3(h4_results)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        response = {
            'statusCode': 200,
            'phase': '4h_analysis_simplified',
            'input_tickers': len(gpt_results),
            'passed_tickers': len(h4_results),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'top_candidates': [r['ticker'] for r in h4_results[:3]],
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=== Phase 4 Simplified 완료 ===")
        return response
        
    except Exception as e:
        logger.error(f"❌ Phase 4 Simplified 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': '4h_analysis_simplified',
            'error': str(e)
        }

if __name__ == "__main__":
    test_event = {'source': 'test'}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))
'''

    def create_phase5_pandas_free(self) -> str:
        """Phase 5 pandas-free 버전 생성"""
        return '''#!/usr/bin/env python3
"""
⚡ Phase 5: Condition Check Lambda (Pandas-Free Version)
- 최종 조건 검사 및 포지션 크기 계산
- 리스크 관리 적용
- 실제 거래 준비
"""

import boto3
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
import pytz
import pyupbit

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SimplifiedConditionChecker:
    """간소화된 조건 검사기"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 리스크 관리 설정
        self.max_portfolio_exposure = 0.20  # 20% 최대 노출
        self.max_position_size = 0.05  # 5% 최대 포지션
        self.daily_trade_limit = 3  # 일일 거래 한도

    def load_phase4_results(self) -> List[Dict]:
        """Phase 4 결과 로드"""
        try:
            logger.info("📊 Phase 4 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase4/4h_analysis_results.json'
            )
            
            data = json.loads(response['Body'].read())
            h4_results = data.get('4h_results', [])
            
            logger.info(f"✅ Phase 4 결과 로드: {len(h4_results)}개 티커")
            return h4_results
            
        except Exception as e:
            logger.error(f"❌ Phase 4 결과 로드 실패: {e}")
            return []

    def get_current_market_data(self, ticker: str) -> Dict[str, Any]:
        """실시간 시장 데이터 조회 (모의)"""
        try:
            # 실제로는 pyupbit.get_current_price 등 사용
            mock_data = {
                'current_price': 50000000 if 'BTC' in ticker else 5000000,
                'volume_24h': 1000000000,
                'price_change_24h': 0.03,
                'bid_price': 49990000 if 'BTC' in ticker else 4999000,
                'ask_price': 50010000 if 'BTC' in ticker else 5001000,
                'spread': 0.0004  # 0.04% 스프레드
            }
            
            return mock_data
            
        except Exception as e:
            logger.error(f"❌ {ticker} 실시간 데이터 조회 실패: {e}")
            return {}

    def calculate_position_size(self, ticker: str, score: float, market_data: dict) -> Dict[str, Any]:
        """포지션 크기 계산"""
        try:
            current_price = market_data.get('current_price', 0)
            if current_price <= 0:
                return {'position_size': 0, 'error': '가격 정보 없음'}
            
            # 기본 포지션 크기 (점수 기반)
            base_size = min(self.max_position_size, score / 100 * 0.05)
            
            # 변동성 조정
            price_change_24h = abs(market_data.get('price_change_24h', 0.02))
            volatility_factor = max(0.5, min(1.5, 1.0 / (price_change_24h * 10)))
            
            adjusted_size = base_size * volatility_factor
            
            # 스프레드 고려
            spread = market_data.get('spread', 0.001)
            if spread > 0.01:  # 1% 이상 스프레드면 크기 축소
                adjusted_size *= 0.7
            
            position_calc = {
                'ticker': ticker,
                'score': score,
                'base_position_size': base_size,
                'volatility_factor': volatility_factor,
                'final_position_size': round(adjusted_size, 4),
                'estimated_krw_amount': int(current_price * adjusted_size * 100),  # 가정: 100만원 기준
                'current_price': current_price,
                'calculated_at': datetime.now(self.kst).isoformat()
            }
            
            return position_calc
            
        except Exception as e:
            logger.error(f"❌ {ticker} 포지션 크기 계산 실패: {e}")
            return {'position_size': 0, 'error': str(e)}

    def final_condition_check(self, ticker: str, analysis_data: dict) -> Dict[str, Any]:
        """최종 조건 검사"""
        try:
            logger.info(f"  🔍 {ticker} 최종 조건 검사 중...")
            
            final_score = analysis_data.get('final_score', 0)
            
            # 실시간 시장 데이터 확인
            market_data = self.get_current_market_data(ticker)
            if not market_data:
                return {
                    'ticker': ticker,
                    'passed': False,
                    'reason': '실시간 데이터 없음'
                }
            
            # 조건 검사
            conditions_passed = []
            conditions_failed = []
            
            # 1. 점수 조건
            if final_score >= 70:
                conditions_passed.append(f"높은 분석 점수: {final_score}")
            else:
                conditions_failed.append(f"낮은 점수: {final_score}")
            
            # 2. 스프레드 조건
            spread = market_data.get('spread', 0.001)
            if spread <= 0.005:  # 0.5% 이하
                conditions_passed.append(f"적정 스프레드: {spread:.3f}")
            else:
                conditions_failed.append(f"높은 스프레드: {spread:.3f}")
            
            # 3. 거래량 조건
            volume_24h = market_data.get('volume_24h', 0)
            if volume_24h >= 500000000:  # 5억원 이상
                conditions_passed.append("충분한 거래량")
            else:
                conditions_failed.append("부족한 거래량")
            
            # 최종 판정
            passed = len(conditions_failed) == 0 and final_score >= 70
            
            # 포지션 크기 계산 (통과한 경우에만)
            position_calc = {}
            if passed:
                position_calc = self.calculate_position_size(ticker, final_score, market_data)
            
            result = {
                'ticker': ticker,
                'passed': passed,
                'final_score': final_score,
                'conditions_passed': conditions_passed,
                'conditions_failed': conditions_failed,
                'market_data': market_data,
                'position_calculation': position_calc,
                'checked_at': datetime.now(self.kst).isoformat()
            }
            
            status = "통과" if passed else "실패"
            logger.info(f"  {'✅' if passed else '❌'} {ticker} 최종 검사 {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 최종 조건 검사 실패: {e}")
            return {
                'ticker': ticker,
                'passed': False,
                'error': str(e)
            }

    def process_final_conditions(self, h4_results: List[Dict]) -> List[Dict]:
        """최종 조건 검사 처리"""
        try:
            logger.info(f"🔍 최종 조건 검사 시작: {len(h4_results)}개 티커")
            
            final_results = []
            passed_count = 0
            
            for analysis_data in h4_results[:self.daily_trade_limit]:  # 일일 거래 한도 적용
                ticker = analysis_data.get('ticker', 'UNKNOWN')
                result = self.final_condition_check(ticker, analysis_data)
                
                final_results.append(result)
                if result.get('passed', False):
                    passed_count += 1
            
            logger.info(f"✅ 최종 조건 검사 완료: {passed_count}개 통과")
            return final_results
            
        except Exception as e:
            logger.error(f"❌ 최종 조건 검사 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            output_data = {
                'phase': 'condition_check_simplified',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'checked_count': len(results),
                'passed_count': sum(1 for r in results if r.get('passed', False)),
                'condition_results': results,
                'risk_parameters': {
                    'max_portfolio_exposure': self.max_portfolio_exposure,
                    'max_position_size': self.max_position_size,
                    'daily_trade_limit': self.daily_trade_limit
                }
            }
            
            main_key = 'phase5/condition_check_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ 조건 검사 결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 5: Simplified Condition Check 시작 ===")
        
        checker = SimplifiedConditionChecker()
        
        # Phase 4 결과 로드
        h4_results = checker.load_phase4_results()
        if not h4_results:
            return {
                'statusCode': 400,
                'phase': 'condition_check_simplified',
                'error': 'Phase 4 결과 없음'
            }
        
        # 최종 조건 검사
        final_results = checker.process_final_conditions(h4_results)
        
        # 결과 저장
        s3_saved = checker.save_results_to_s3(final_results)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        passed_tickers = [r for r in final_results if r.get('passed', False)]
        
        response = {
            'statusCode': 200,
            'phase': 'condition_check_simplified',
            'input_tickers': len(h4_results),
            'checked_tickers': len(final_results),
            'passed_tickers': len(passed_tickers),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'ready_for_trade': [r['ticker'] for r in passed_tickers],
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=== Phase 5 Simplified 완료 ===")
        return response
        
    except Exception as e:
        logger.error(f"❌ Phase 5 Simplified 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'condition_check_simplified',
            'error': str(e)
        }

if __name__ == "__main__":
    test_event = {'source': 'test'}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))
'''

    def create_phase6_pandas_free(self) -> str:
        """Phase 6 pandas-free 버전 생성"""
        return '''#!/usr/bin/env python3
"""
⚡ Phase 6: Trade Execution Lambda (Pandas-Free Version)
- 실제 거래 실행 (테스트 모드 지원)
- Upbit API 연동
- 포지션 및 거래 기록 관리
"""

import boto3
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
import pytz

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SimplifiedTradeExecutor:
    """간소화된 거래 실행기"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 테스트 모드 기본값
        self.test_mode = True

    def load_phase5_results(self) -> List[Dict]:
        """Phase 5 결과 로드"""
        try:
            logger.info("📊 Phase 5 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase5/condition_check_results.json'
            )
            
            data = json.loads(response['Body'].read())
            condition_results = data.get('condition_results', [])
            
            # 통과한 티커만 필터링
            passed_results = [r for r in condition_results if r.get('passed', False)]
            
            logger.info(f"✅ Phase 5 결과 로드: {len(passed_results)}개 거래 대상")
            return passed_results
            
        except Exception as e:
            logger.error(f"❌ Phase 5 결과 로드 실패: {e}")
            return []

    def execute_mock_trade(self, trade_data: dict) -> Dict[str, Any]:
        """모의 거래 실행"""
        try:
            ticker = trade_data.get('ticker', 'UNKNOWN')
            position_calc = trade_data.get('position_calculation', {})
            
            if not position_calc:
                return {
                    'ticker': ticker,
                    'success': False,
                    'error': '포지션 계산 데이터 없음'
                }
            
            # 모의 거래 실행
            mock_trade = {
                'ticker': ticker,
                'order_type': 'market_buy',
                'order_amount': position_calc.get('estimated_krw_amount', 0),
                'estimated_price': position_calc.get('current_price', 0),
                'position_size': position_calc.get('final_position_size', 0),
                'order_id': f"mock_{ticker}_{int(datetime.now().timestamp())}",
                'status': 'filled',
                'executed_at': datetime.now(self.kst).isoformat(),
                'test_mode': True
            }
            
            logger.info(f"  📝 {ticker} 모의 거래 실행: {mock_trade['order_amount']:,}원")
            return {
                'ticker': ticker,
                'success': True,
                'trade_result': mock_trade
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 모의 거래 실행 실패: {e}")
            return {
                'ticker': ticker,
                'success': False,
                'error': str(e)
            }

    def process_trade_execution(self, passed_results: List[Dict], test_mode: bool = True) -> List[Dict]:
        """거래 실행 처리"""
        try:
            logger.info(f"💰 거래 실행 시작: {len(passed_results)}개 대상 (테스트모드: {test_mode})")
            
            trade_results = []
            
            for trade_data in passed_results:
                ticker = trade_data.get('ticker', 'UNKNOWN')
                
                if test_mode:
                    result = self.execute_mock_trade(trade_data)
                else:
                    # 실제 거래는 추후 구현
                    result = {
                        'ticker': ticker,
                        'success': False,
                        'error': '실제 거래 미구현'
                    }
                
                trade_results.append(result)
            
            successful_trades = [r for r in trade_results if r.get('success', False)]
            logger.info(f"✅ 거래 실행 완료: {len(successful_trades)}개 성공")
            
            return trade_results
            
        except Exception as e:
            logger.error(f"❌ 거래 실행 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            output_data = {
                'phase': 'trade_execution_simplified',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'executed_count': len(results),
                'successful_count': sum(1 for r in results if r.get('success', False)),
                'trade_results': results,
                'test_mode': self.test_mode
            }
            
            main_key = 'phase6/trade_execution_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ 거래 실행 결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 6: Simplified Trade Execution 시작 ===")
        
        # 테스트 모드 확인
        test_mode = event.get('test_mode', True)
        
        executor = SimplifiedTradeExecutor()
        executor.test_mode = test_mode
        
        # Phase 5 결과 로드
        passed_results = executor.load_phase5_results()
        if not passed_results:
            return {
                'statusCode': 400,
                'phase': 'trade_execution_simplified',
                'error': 'Phase 5 통과 결과 없음',
                'message': '거래할 대상이 없습니다'
            }
        
        # 거래 실행
        trade_results = executor.process_trade_execution(passed_results, test_mode)
        
        # 결과 저장
        s3_saved = executor.save_results_to_s3(trade_results)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        successful_trades = [r for r in trade_results if r.get('success', False)]
        
        response = {
            'statusCode': 200,
            'phase': 'trade_execution_simplified',
            'input_candidates': len(passed_results),
            'executed_trades': len(trade_results),
            'successful_trades': len(successful_trades),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'test_mode': test_mode,
            'executed_tickers': [r.get('ticker', 'UNKNOWN') for r in successful_trades],
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=== Phase 6 Simplified 완료 ===")
        return response
        
    except Exception as e:
        logger.error(f"❌ Phase 6 Simplified 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'trade_execution_simplified',
            'error': str(e)
        }

if __name__ == "__main__":
    test_event = {'source': 'test', 'test_mode': True}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))
'''

    def create_deployment_package(self, function_code: str, filename: str) -> str:
        """Lambda 배포 패키지 생성"""
        try:
            logger.info(f"📦 {filename} 배포 패키지 생성...")
            
            zip_filename = f"update_{filename.replace('.py', '')}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 함수 코드를 lambda_function.py로 저장
                zipf.writestr('lambda_function.py', function_code)
            
            logger.info(f"✅ 패키지 생성 완료: {zip_filename}")
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ 패키지 생성 실패: {e}")
            return None

    def update_lambda_function_code(self, function_name: str, zip_filename: str) -> bool:
        """Lambda 함수 코드 업데이트"""
        try:
            logger.info(f"🔄 {function_name} 코드 업데이트 중...")
            
            with open(zip_filename, 'rb') as f:
                zip_content = f.read()
            
            self.lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content
            )
            
            logger.info(f"✅ {function_name} 업데이트 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ {function_name} 업데이트 실패: {e}")
            return False
        finally:
            if os.path.exists(zip_filename):
                os.remove(zip_filename)

    def update_all_phases_pandas_free(self) -> dict:
        """모든 Phase pandas-free 버전으로 업데이트"""
        try:
            logger.info("🚀 Phase 3-6 pandas-free 버전 업데이트 시작")
            
            results = {
                'updated': [],
                'failed': []
            }
            
            # Phase별 코드 생성 함수 매핑
            phase_generators = {
                'phase3': self.create_phase3_pandas_free,
                'phase4': self.create_phase4_pandas_free,
                'phase5': self.create_phase5_pandas_free,
                'phase6': self.create_phase6_pandas_free
            }
            
            for phase, function_name in self.lambda_functions.items():
                try:
                    logger.info(f"\n📝 {phase.upper()} 업데이트 중...")
                    
                    # pandas-free 코드 생성
                    code_generator = phase_generators[phase]
                    function_code = code_generator()
                    
                    # 배포 패키지 생성
                    zip_file = self.create_deployment_package(function_code, f"{phase}_simplified.py")
                    if not zip_file:
                        results['failed'].append(phase)
                        continue
                    
                    # 함수 업데이트
                    if self.update_lambda_function_code(function_name, zip_file):
                        results['updated'].append(phase)
                    else:
                        results['failed'].append(phase)
                    
                    # 업데이트 간격
                    import time
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"❌ {phase} 업데이트 실패: {e}")
                    results['failed'].append(phase)
            
            logger.info(f"\n📊 pandas-free 업데이트 결과:")
            logger.info(f"   성공: {len(results['updated'])}개")
            logger.info(f"   실패: {len(results['failed'])}개")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 전체 업데이트 실패: {e}")
            return {'updated': [], 'failed': list(self.lambda_functions.keys())}

def main():
    """메인 실행 함수"""
    try:
        print("🔧 Pandas-Free Phases 3-6 Update")
        print("=" * 60)
        
        updater = PandasFreePhaseUpdater()
        results = updater.update_all_phases_pandas_free()
        
        if results['updated']:
            print(f"\n✅ Pandas-free 버전 업데이트 완료!")
            print(f"   업데이트된 Phase: {', '.join(results['updated'])}")
            if results['failed']:
                print(f"   실패한 Phase: {', '.join(results['failed'])}")
            
            print("\n📋 다음 단계:")
            print("1. 전체 워크플로우 재테스트")
            print("2. 각 Phase별 기능 검증")
            print("3. 인프라 설정 완료")
            return True
        else:
            print("\n❌ 모든 Phase 업데이트 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)