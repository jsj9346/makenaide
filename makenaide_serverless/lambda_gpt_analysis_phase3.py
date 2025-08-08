#!/usr/bin/env python3
"""
🤖 Phase 3: GPT Analysis Lambda
- Phase 2에서 필터링된 종목들에 대한 전문가 수준 분석
- OpenAI GPT를 활용한 Weinstein/Minervini/O'Neill 전략 검증
- 최종 매매 신호 생성
"""

import boto3
import json
import logging
import os
import time
from datetime import datetime
import pytz
from typing import Dict, Any, List, Optional
import urllib3

# OpenAI 라이브러리는 Lambda Layer에 추가 필요
# 임시로 requests로 직접 API 호출
http = urllib3.PoolManager()

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class GPTAnalyzer:
    """GPT 기반 종목 분석 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # OpenAI 설정
        self.openai_api_key = os.environ.get('OPENAI_API_KEY')
        self.gpt_model = os.environ.get('GPT_MODEL', 'gpt-4-turbo-preview')
        self.openai_api_url = 'https://api.openai.com/v1/chat/completions'
        
        # 시스템 프롬프트 로드
        self.system_prompt = self.load_system_prompt()
        
    def load_system_prompt(self) -> str:
        """시스템 프롬프트 로드"""
        try:
            with open('system_prompt.txt', 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"시스템 프롬프트 로드 실패: {e}")
            # 기본 프롬프트 사용
            return """You are a professional cryptocurrency trend verification analyst.
Analyze the given ticker data and respond with valid JSON format only."""

    def load_phase2_data(self) -> Optional[List[Dict]]:
        """Phase 2 결과 데이터 로드"""
        try:
            logger.info("Phase 2 결과 데이터 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase2/comprehensive_filtered_tickers.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 2 데이터 상태 불량: {data.get('status')}")
                return None
            
            filtered_tickers = data.get('filtered_tickers', [])
            if not filtered_tickers:
                logger.warning("Phase 2에서 필터링된 데이터가 없음")
                return None
                
            logger.info(f"Phase 2 데이터 로드 완료: {len(filtered_tickers)}개 티커")
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"Phase 2 데이터 로드 실패: {e}")
            return None

    def prepare_analysis_prompt(self, ticker_data: Dict) -> str:
        """GPT 분석을 위한 프롬프트 준비"""
        try:
            ticker = ticker_data.get('ticker', '')
            analysis = ticker_data.get('analysis', {})
            weinstein = ticker_data.get('weinstein', {})
            
            # 핵심 지표 추출
            current_price = analysis.get('current_price', 0)
            ma200 = analysis.get('ma200', 0)
            ma200_slope = analysis.get('ma200_slope', 0)
            adx = analysis.get('adx', 0)  # ADX가 없으면 RSI로 대체
            if adx == 0:  # ADX가 계산되지 않은 경우
                rsi = analysis.get('rsi', 50)
                adx = 20 if rsi > 50 else 15  # RSI 기반 추정
            
            volume_ratio = analysis.get('volume_ratio', 1.0)
            price_from_high = analysis.get('price_from_high', 0)
            
            # Stage 판단을 위한 조건
            price_vs_ma200 = "above" if current_price > ma200 else "below"
            recent_high_breakout = price_from_high > -5  # 고점 대비 5% 이내면 돌파로 간주
            
            # 분석 데이터 구성
            analysis_data = {
                "ticker": ticker,
                "current_price": round(current_price, 2),
                "ma200": round(ma200, 2),
                "ma200_slope": round(ma200_slope, 2),
                "adx": round(adx, 1),
                "volume_ratio": round(volume_ratio, 2),
                "price_vs_ma200": price_vs_ma200,
                "recent_high_breakout": recent_high_breakout,
                "price_from_52w_high": round(price_from_high, 1),
                "rsi": round(analysis.get('rsi', 50), 1),
                "weinstein_score": weinstein.get('stage2_score', 0),
                "technical_reasons": weinstein.get('stage2_reasons', [])
            }
            
            # 사용자 프롬프트 구성
            user_prompt = f"""Analyze this cryptocurrency ticker based on the Weinstein/Minervini/O'Neill strategies:

{json.dumps(analysis_data, indent=2)}

Remember the MANDATORY criteria:
- If Price > MA200 AND ADX > 20 AND recent breakout → ALWAYS Stage2 (BUY)
- If Price < MA200 AND MA200 slope < -0.5% AND ADX > 20 → ALWAYS Stage4 (AVOID)

Respond with ONLY valid JSON format as specified."""
            
            return user_prompt
            
        except Exception as e:
            logger.error(f"프롬프트 준비 실패: {e}")
            return ""

    def call_openai_api(self, prompt: str) -> Optional[Dict]:
        """OpenAI API 직접 호출"""
        try:
            headers = {
                'Authorization': f'Bearer {self.openai_api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'model': self.gpt_model,
                'messages': [
                    {'role': 'system', 'content': self.system_prompt},
                    {'role': 'user', 'content': prompt}
                ],
                'temperature': 0.2,  # 일관성 있는 분석을 위해 낮은 temperature
                'max_tokens': 200,   # JSON 응답에 충분한 토큰
                'response_format': {'type': 'json_object'}  # JSON 응답 강제
            }
            
            encoded_payload = json.dumps(payload).encode('utf-8')
            
            response = http.request(
                'POST',
                self.openai_api_url,
                body=encoded_payload,
                headers=headers,
                timeout=30.0
            )
            
            if response.status == 200:
                result = json.loads(response.data.decode('utf-8'))
                content = result['choices'][0]['message']['content']
                return json.loads(content)
            else:
                logger.error(f"OpenAI API 오류: {response.status} - {response.data}")
                return None
                
        except Exception as e:
            logger.error(f"OpenAI API 호출 실패: {e}")
            return None

    def validate_gpt_response(self, response: Dict, ticker_data: Dict) -> bool:
        """GPT 응답 검증"""
        try:
            # 필수 필드 확인
            required_fields = ["ticker", "score", "confidence", "action", "market_phase", "pattern", "reason"]
            for field in required_fields:
                if field not in response:
                    logger.warning(f"필수 필드 누락: {field}")
                    return False
            
            # Stage 일관성 검증
            analysis = ticker_data.get('analysis', {})
            current_price = analysis.get('current_price', 0)
            ma200 = analysis.get('ma200', 0)
            adx = analysis.get('adx', 20)  # 기본값 20
            
            # Stage 2 강제 조건 확인
            if current_price > ma200 and adx > 20:
                if response.get('market_phase') != 'Stage2':
                    logger.warning(f"Stage 불일치: Price > MA200 & ADX > 20 but {response.get('market_phase')}")
                    # 자동 수정
                    response['market_phase'] = 'Stage2'
                    response['action'] = 'BUY'
                    response['reason'] = 'Strong uptrend confirmed by price > MA200 and ADX > 20'
            
            return True
            
        except Exception as e:
            logger.error(f"응답 검증 실패: {e}")
            return False

    def analyze_ticker(self, ticker_data: Dict) -> Optional[Dict]:
        """개별 종목 GPT 분석"""
        try:
            ticker = ticker_data.get('ticker', '')
            logger.info(f"GPT 분석 시작: {ticker}")
            
            # 프롬프트 준비
            prompt = self.prepare_analysis_prompt(ticker_data)
            if not prompt:
                return None
            
            # GPT API 호출 (재시도 포함)
            max_retries = 3
            for attempt in range(max_retries):
                gpt_response = self.call_openai_api(prompt)
                
                if gpt_response and self.validate_gpt_response(gpt_response, ticker_data):
                    # 분석 시간 추가
                    gpt_response['analysis_timestamp'] = datetime.now(self.kst).isoformat()
                    gpt_response['model_used'] = self.gpt_model
                    
                    logger.info(f"✅ {ticker} GPT 분석 완료 - Action: {gpt_response.get('action')}")
                    return gpt_response
                
                if attempt < max_retries - 1:
                    logger.warning(f"{ticker} 분석 재시도 {attempt + 1}/{max_retries}")
                    time.sleep(2)  # 재시도 전 대기
            
            logger.error(f"❌ {ticker} GPT 분석 실패")
            return None
            
        except Exception as e:
            logger.error(f"종목 분석 실패: {e}")
            return None

    def analyze_all_tickers(self, tickers_data: List[Dict]) -> List[Dict]:
        """모든 종목 분석"""
        gpt_results = []
        
        logger.info(f"GPT 분석 시작: {len(tickers_data)}개 종목")
        
        for i, ticker_data in enumerate(tickers_data):
            try:
                # API 호출 간격 조절 (Rate Limit 대응)
                if i > 0:
                    time.sleep(1)  # 1초 대기
                
                result = self.analyze_ticker(ticker_data)
                if result:
                    gpt_results.append(result)
                    
                logger.info(f"진행 상황: {i+1}/{len(tickers_data)}")
                
            except Exception as e:
                logger.error(f"종목 분석 오류: {e}")
                continue
        
        # 점수순 정렬
        gpt_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        logger.info(f"GPT 분석 완료: {len(gpt_results)}개 종목 성공")
        return gpt_results

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # BUY 액션만 추출
            buy_tickers = [r for r in results if r.get('action') == 'BUY']
            top_buys = [r['ticker'] for r in buy_tickers[:10]]  # 상위 10개
            
            output_data = {
                'phase': 'gpt_analysis',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'analyzed_count': len(results),
                'buy_count': len(buy_tickers),
                'gpt_results': results,
                'top_buys': top_buys,
                'model_used': self.gpt_model,
                'analysis_summary': {
                    'total_analyzed': len(results),
                    'buy_signals': len(buy_tickers),
                    'hold_signals': len([r for r in results if r.get('action') == 'HOLD']),
                    'avoid_signals': len([r for r in results if r.get('action') == 'AVOID']),
                    'avg_confidence': sum(r.get('confidence', 0) for r in results) / len(results) if results else 0
                }
            }
            
            # 메인 결과 파일
            main_key = 'phase3/gpt_analysis_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'phase3/backups/gpt_analysis_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=backup_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info(f"결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"S3 저장 실패: {e}")
            return False

    def trigger_next_phase(self):
        """Phase 4 트리거 이벤트 발송"""
        try:
            event_detail = {
                'phase': 'gpt_analysis',
                'status': 'completed',
                'timestamp': datetime.now(self.kst).isoformat(),
                'next_phase': '4h_analysis'
            }
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.gpt_analysis',
                        'DetailType': 'GPT Analysis Completed',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            
            logger.info("Phase 4 트리거 이벤트 발송 완료")
            
        except Exception as e:
            logger.error(f"Phase 4 트리거 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 3: GPT Analysis 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # OpenAI API 키 확인
        if not os.environ.get('OPENAI_API_KEY'):
            return {
                'statusCode': 500,
                'phase': 'gpt_analysis',
                'error': 'OpenAI API key not configured',
                'message': 'OPENAI_API_KEY 환경변수를 설정해주세요'
            }
        
        # GPT 분석기 초기화
        analyzer = GPTAnalyzer()
        
        # Phase 2 데이터 로드
        phase2_data = analyzer.load_phase2_data()
        if not phase2_data:
            return {
                'statusCode': 400,
                'phase': 'gpt_analysis',
                'error': 'Phase 2 데이터 없음',
                'message': 'Phase 2를 먼저 실행해주세요'
            }
        
        # GPT 분석 실행
        gpt_results = analyzer.analyze_all_tickers(phase2_data)
        
        # 결과 저장
        s3_saved = analyzer.save_results_to_s3(gpt_results)
        
        # 다음 단계 트리거
        if gpt_results and s3_saved:
            analyzer.trigger_next_phase()
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 성공 응답
        buy_signals = [r for r in gpt_results if r.get('action') == 'BUY']
        response = {
            'statusCode': 200,
            'phase': 'gpt_analysis',
            'input_tickers': len(phase2_data),
            'analyzed_tickers': len(gpt_results),
            'buy_signals': len(buy_signals),
            'top_buys': [r['ticker'] for r in buy_signals[:5]],
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'model_used': analyzer.gpt_model,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"=== Phase 3 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Phase 3 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'gpt_analysis',
            'error': str(e),
            'message': 'Phase 3 실행 중 오류 발생'
        }