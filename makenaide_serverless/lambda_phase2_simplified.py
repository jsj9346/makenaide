#!/usr/bin/env python3
"""
⚡ Phase 2: Simplified Comprehensive Filtering Lambda (No Pandas)
- 시장 상황 적응형 필터링 시스템
- Pure Python implementation without pandas dependency
- JSON 기반 데이터 처리
"""

import boto3
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pytz

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SimplifiedComprehensiveFilter:
    """간소화된 종합 필터링 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 필터링 기준값들
        self.config = {
            'volume_multiplier': float(os.environ.get('VOLUME_MULTIPLIER', '1.5')),
            'ma_slope_threshold': float(os.environ.get('MA_SLOPE_THRESHOLD', '0.5')),
            'lookback_days': int(os.environ.get('LOOKBACK_DAYS', '200')),
        }

    def load_phase1_data(self) -> Optional[dict]:
        """Phase 1 결과 데이터 로드"""
        try:
            logger.info("📊 Phase 1 결과 데이터 로드 중...")
            
            # S3에서 Phase 1 결과 파일 다운로드
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase1/filtered_tickers_with_data.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 1 데이터 상태 불량: {data.get('status')}")
                return None
            
            # 필터링된 데이터 추출
            filtered_data = data.get('filtered_data', [])
            if not filtered_data:
                logger.warning("Phase 1에서 필터링된 데이터가 없음")
                return None
                
            logger.info(f"✅ Phase 1 데이터 로드 완료: {len(filtered_data)}개 티커")
            return data
            
        except Exception as e:
            logger.error(f"❌ Phase 1 데이터 로드 실패: {e}")
            return None

    def detect_market_condition(self) -> str:
        """시장 상황 판단 (BULL/BEAR/NEUTRAL)"""
        try:
            # 간단한 시장 상황 판단 로직
            # 실제로는 BTC 가격 동향을 분석해야 함
            now = datetime.now(self.kst)
            
            # 임시로 시간대에 따른 시장 상황 가정
            if now.hour < 12:
                market_condition = "BULL"
            elif now.hour < 18:
                market_condition = "NEUTRAL" 
            else:
                market_condition = "BEAR"
            
            logger.info(f"🏛️ 감지된 시장 상황: {market_condition}")
            return market_condition
            
        except Exception as e:
            logger.warning(f"⚠️ 시장 상황 판단 실패: {e}, 기본값 NEUTRAL 사용")
            return "NEUTRAL"

    def calculate_simple_ma(self, prices: List[float], period: int) -> float:
        """간단한 이동평균 계산"""
        if len(prices) < period:
            return 0.0
        
        recent_prices = prices[-period:]
        return sum(recent_prices) / len(recent_prices)

    def analyze_ticker_simplified(self, ticker_data: dict, market_condition: str) -> Dict[str, Any]:
        """간소화된 티커 분석"""
        try:
            ticker = ticker_data.get('ticker', '')
            price_data = ticker_data.get('price_data', {})
            
            if not price_data:
                return {'ticker': ticker, 'passed': False, 'reason': '가격 데이터 없음'}
            
            close_prices = price_data.get('close', [])
            volumes = price_data.get('volume', [])
            
            if len(close_prices) < 50:
                return {'ticker': ticker, 'passed': False, 'reason': '데이터 부족'}
            
            current_price = close_prices[-1]
            
            # 1. 이동평균 계산
            ma20 = self.calculate_simple_ma(close_prices, 20)
            ma50 = self.calculate_simple_ma(close_prices, 50)
            ma200 = self.calculate_simple_ma(close_prices, 200)
            
            # 2. 거래량 분석
            current_volume = volumes[-1] if volumes else 0
            avg_volume = self.calculate_simple_ma(volumes, 20) if volumes else 0
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
            
            # 3. 기본 점수 계산
            score = 0
            reasons = []
            
            # 가격 > MA20
            if current_price > ma20 > 0:
                score += 25
                reasons.append("현재가 > MA20")
            
            # MA 정배열
            if ma20 > ma50 > ma200 > 0:
                score += 25
                reasons.append("이동평균 정배열")
            
            # 거래량 증가
            if volume_ratio >= self.config['volume_multiplier']:
                score += 25
                reasons.append(f"거래량 {volume_ratio:.1f}배 증가")
            
            # 시장 상황별 보정
            if market_condition == "BULL":
                score *= 1.1  # 10% 보정
                reasons.append("강세장 보정")
            elif market_condition == "BEAR":
                score *= 0.8  # 20% 하향 보정
                reasons.append("약세장 보정")
            
            # 통과 여부 결정
            passed = score >= 50  # 50점 이상 통과
            
            analysis_result = {
                'ticker': ticker,
                'passed': passed,
                'score': round(score, 1),
                'reasons': reasons,
                'market_condition': market_condition,
                'indicators': {
                    'current_price': current_price,
                    'ma20': ma20,
                    'ma50': ma50,
                    'ma200': ma200,
                    'volume_ratio': volume_ratio
                },
                'analysis_time': datetime.now(self.kst).isoformat()
            }
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 분석 실패: {e}")
            return {'ticker': ticker, 'passed': False, 'reason': f'분석 오류: {str(e)}'}

    def comprehensive_filter_simplified(self, phase1_data: dict) -> List[Dict]:
        """간소화된 종합 필터링 실행"""
        try:
            filtered_data = phase1_data.get('filtered_data', [])
            logger.info(f"🔍 간소화된 종합 필터링 시작: {len(filtered_data)}개 티커")
            
            # 시장 상황 감지
            market_condition = self.detect_market_condition()
            
            filter_results = []
            
            for idx, ticker_data in enumerate(filtered_data):
                try:
                    ticker = ticker_data.get('ticker', f'ticker_{idx}')
                    logger.info(f"  분석 중: {ticker} ({idx+1}/{len(filtered_data)})")
                    
                    # 티커 분석
                    analysis = self.analyze_ticker_simplified(ticker_data, market_condition)
                    
                    # 통과한 티커만 결과에 추가
                    if analysis.get('passed', False):
                        filter_results.append(analysis)
                        logger.info(f"  ✅ {ticker} 통과 - 점수: {analysis.get('score', 0)}")
                    else:
                        logger.info(f"  ❌ {ticker} 탈락 - {analysis.get('reason', '점수 미달')}")
                
                except Exception as e:
                    logger.error(f"❌ 티커 {ticker} 개별 분석 실패: {e}")
                    continue
            
            logger.info(f"🎯 간소화된 종합 필터링 완료: {len(filter_results)}개 티커 통과")
            return filter_results
            
        except Exception as e:
            logger.error(f"❌ 종합 필터링 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            output_data = {
                'phase': 'comprehensive_filtering_simplified',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'filtered_count': len(results),
                'filtered_tickers': results,
                'config': self.config
            }
            
            # 메인 결과 파일
            main_key = 'phase2/comprehensive_filtered_tickers.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'phase2/backups/comprehensive_filtered_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=backup_key,
                Body=json.dumps(output_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ 결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 2: Simplified Comprehensive Filtering 시작 ===")
        logger.info(f"📥 입력 이벤트: {json.dumps(event, indent=2, ensure_ascii=False)}")
        
        # Phase 2 필터링 실행
        filter_engine = SimplifiedComprehensiveFilter()
        
        # Phase 1 데이터 로드
        phase1_data = filter_engine.load_phase1_data()
        if phase1_data is None:
            return {
                'statusCode': 400,
                'phase': 'comprehensive_filtering_simplified',
                'error': 'Phase 1 데이터 없음',
                'message': 'Phase 1을 먼저 실행해주세요'
            }
        
        # 종합 필터링 실행
        filtered_results = filter_engine.comprehensive_filter_simplified(phase1_data)
        
        # 결과 저장
        s3_saved = filter_engine.save_results_to_s3(filtered_results)
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'phase': 'comprehensive_filtering_simplified',
            'input_tickers': len(phase1_data.get('filtered_data', [])),
            'filtered_tickers': len(filtered_results),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'top_tickers': [r['ticker'] for r in filtered_results[:5]],  # 상위 5개
            'market_condition': filtered_results[0].get('market_condition', 'UNKNOWN') if filtered_results else 'UNKNOWN',
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=== Phase 2 Simplified 완료 ===")
        logger.info(f"📊 결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Phase 2 Simplified 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'comprehensive_filtering_simplified',
            'error': str(e),
            'message': 'Phase 2 Simplified 실행 중 오류 발생'
        }

# 로컬 테스트용
if __name__ == "__main__":
    test_event = {
        'source': 'test',
        'detail-type': 'Phase 1 Completed'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))