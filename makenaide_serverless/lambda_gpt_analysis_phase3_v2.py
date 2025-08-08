#!/usr/bin/env python3
"""
⚡ Phase 3: GPT Analysis Lambda
- OpenAI GPT-4를 활용한 전문가급 차트 및 데이터 분석
- JSON 데이터 + 차트 이미지 복합 분석
- Phase 2 결과를 입력으로 받아 최종 투자 의견 제공
"""

import boto3
import json
import logging
import pandas as pd
import numpy as np
import pytz
import os
import base64
import io
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pyupbit
import openai
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# 한글 폰트 설정
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class GPTAnalysisPhase3:
    """GPT-4를 활용한 종합 차트 분석 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.secrets_client = boto3.client('secretsmanager')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # OpenAI API 키 가져오기
        self.openai_api_key = self._get_secret('makenaide-openai-api-key')
        if self.openai_api_key:
            openai.api_key = self.openai_api_key
        
        # GPT 분석 설정
        self.gpt_config = {
            'model': os.environ.get('OPENAI_MODEL', 'gpt-4'),
            'max_tokens': int(os.environ.get('GPT_MAX_TOKENS', '2000')),
            'temperature': float(os.environ.get('GPT_TEMPERATURE', '0.3')),
            'analysis_depth': os.environ.get('ANALYSIS_DEPTH', 'comprehensive')  # basic, detailed, comprehensive
        }

    def _get_secret(self, secret_name: str) -> Optional[str]:
        """AWS Secrets Manager에서 API 키 조회"""
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            return response['SecretString']
        except Exception as e:
            logger.error(f"❌ Secret 조회 실패 {secret_name}: {e}")
            return None

    def load_phase2_data(self) -> Optional[List[Dict[str, Any]]]:
        """Phase 2 결과 데이터 로드"""
        try:
            logger.info("📊 Phase 2 결과 데이터 로드 중...")
            
            # 최신 Phase 2 결과 파일 찾기
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='phase2/comprehensive_filtered_candidates_'
            )
            
            if 'Contents' not in response or not response['Contents']:
                logger.warning("Phase 2 결과 파일이 없습니다")
                return None
            
            # 가장 최신 파일 선택
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=latest_file['Key']
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            candidates = data.get('candidates', [])
            if not candidates:
                logger.warning("Phase 2에서 필터링된 후보가 없습니다")
                return None
                
            logger.info(f"✅ Phase 2 데이터 로드 완료: {len(candidates)}개 후보")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ Phase 2 데이터 로드 실패: {e}")
            return None

    def get_ohlcv_data(self, ticker: str, period: int = 60) -> Optional[pd.DataFrame]:
        """업비트에서 OHLCV 데이터 가져오기"""
        try:
            logger.info(f"📈 {ticker} OHLCV 데이터 조회 중... (최근 {period}일)")
            
            # 업비트 API로 데이터 조회
            df = pyupbit.get_ohlcv(ticker, interval="day", count=period)
            
            if df is None or df.empty:
                logger.warning(f"{ticker} 데이터 조회 실패")
                return None
            
            # 데이터 정리
            df.index.name = 'date'
            df.reset_index(inplace=True)
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            
            logger.info(f"✅ {ticker} 데이터 조회 완료: {len(df)}일")
            return df
            
        except Exception as e:
            logger.error(f"❌ {ticker} OHLCV 데이터 조회 실패: {e}")
            return None

    def create_comprehensive_chart(self, ticker: str, df: pd.DataFrame, candidate_data: Dict) -> Optional[str]:
        """종합 차트 생성 (캔들스틱 + 기술지표)"""
        try:
            logger.info(f"🎨 {ticker} 종합 차트 생성 중...")
            
            # 차트 설정
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), height_ratios=[3, 1, 1])
            fig.suptitle(f'{ticker} 종합 기술 분석', fontsize=16, fontweight='bold')
            
            # 색상 설정
            up_color = '#FF6B6B'    # 상승 - 빨간색
            down_color = '#4ECDC4'  # 하락 - 청록색
            volume_color = '#95E1D3'
            
            # === 1. 메인 차트: 캔들스틱 + 이동평균선 ===
            dates = df['date']
            opens = df['open']
            highs = df['high'] 
            lows = df['low']
            closes = df['close']
            volumes = df['volume']
            
            # 캔들스틱 그리기
            for i in range(len(df)):
                date = i
                open_price = opens.iloc[i]
                high_price = highs.iloc[i]
                low_price = lows.iloc[i]
                close_price = closes.iloc[i]
                
                color = up_color if close_price >= open_price else down_color
                
                # 몸통
                height = abs(close_price - open_price)
                bottom = min(open_price, close_price)
                ax1.add_patch(Rectangle((date-0.3, bottom), 0.6, height, 
                                      facecolor=color, edgecolor='black', alpha=0.8))
                
                # 꼬리
                ax1.plot([date, date], [low_price, high_price], color='black', linewidth=1)
            
            # 이동평균선
            ma5 = closes.rolling(5).mean()
            ma20 = closes.rolling(20).mean() 
            ma60 = closes.rolling(60).mean()
            
            ax1.plot(range(len(df)), ma5, label='MA5', color='#FF9F43', linewidth=2)
            ax1.plot(range(len(df)), ma20, label='MA20', color='#10AC84', linewidth=2)
            ax1.plot(range(len(df)), ma60, label='MA60', color='#5F27CD', linewidth=2)
            
            # 현재 가격과 기본 정보 표시
            current_price = closes.iloc[-1]
            price_change = ((current_price - closes.iloc[-2]) / closes.iloc[-2]) * 100
            
            ax1.set_title(f'현재가: {current_price:,.0f}원 ({price_change:+.2f}%)', 
                         fontsize=12, pad=20)
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            ax1.set_ylabel('가격 (원)', fontsize=10)
            
            # === 2. RSI 차트 ===
            # RSI 계산
            delta = closes.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            ax2.plot(range(len(df)), rsi, color='#6C5CE7', linewidth=2)
            ax2.axhline(y=70, color='red', linestyle='--', alpha=0.7, label='과매수 (70)')
            ax2.axhline(y=30, color='blue', linestyle='--', alpha=0.7, label='과매도 (30)')
            ax2.axhline(y=50, color='gray', linestyle='-', alpha=0.5)
            ax2.fill_between(range(len(df)), 30, 70, alpha=0.1, color='yellow')
            
            current_rsi = rsi.iloc[-1]
            ax2.set_title(f'RSI(14): {current_rsi:.1f}', fontsize=10)
            ax2.set_ylabel('RSI', fontsize=10)
            ax2.legend(loc='upper right', fontsize=8)
            ax2.grid(True, alpha=0.3)
            ax2.set_ylim(0, 100)
            
            # === 3. 거래량 차트 ===
            colors = [up_color if closes.iloc[i] >= opens.iloc[i] else down_color for i in range(len(df))]
            ax3.bar(range(len(df)), volumes, color=colors, alpha=0.6, width=0.8)
            
            volume_ma = volumes.rolling(20).mean()
            ax3.plot(range(len(df)), volume_ma, color='orange', linewidth=2, label='거래량 MA20')
            
            current_volume = volumes.iloc[-1]
            volume_ratio = current_volume / volume_ma.iloc[-1] if pd.notna(volume_ma.iloc[-1]) else 1
            
            ax3.set_title(f'거래량: {current_volume:,.0f} (평균 대비 {volume_ratio:.1f}배)', fontsize=10)
            ax3.set_ylabel('거래량', fontsize=10)
            ax3.legend(loc='upper right', fontsize=8)
            ax3.grid(True, alpha=0.3)
            ax3.set_xlabel('일자 (최근 60일)', fontsize=10)
            
            # X축 라벨 설정 (최근 날짜들만)
            x_ticks = range(0, len(df), max(1, len(df)//6))
            x_labels = [dates.iloc[i].strftime('%m/%d') for i in x_ticks]
            for ax in [ax1, ax2, ax3]:
                ax.set_xticks(x_ticks)
                ax.set_xticklabels(x_labels, rotation=45)
            
            plt.tight_layout()
            
            # 이미지를 base64로 인코딩
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            
            plt.close()
            logger.info(f"✅ {ticker} 차트 생성 완료")
            return image_base64
            
        except Exception as e:
            logger.error(f"❌ {ticker} 차트 생성 실패: {e}")
            return None

    def prepare_analysis_data(self, ticker: str, candidate_data: Dict, df: pd.DataFrame) -> Dict[str, Any]:
        """GPT 분석용 데이터 준비"""
        try:
            # 최신 데이터 계산
            current_price = df['close'].iloc[-1]
            prev_price = df['close'].iloc[-2]
            price_change_1d = ((current_price - prev_price) / prev_price) * 100
            
            # 기술적 지표 계산
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            ma20 = df['close'].rolling(20).mean().iloc[-1]
            ma60 = df['close'].rolling(60).mean().iloc[-1]
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            
            # 거래량 분석
            current_volume = df['volume'].iloc[-1]
            avg_volume_20 = df['volume'].rolling(20).mean().iloc[-1]
            volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 1
            
            # 가격 위치 분석
            high_52w = df['high'].max()
            low_52w = df['low'].min()
            price_position = ((current_price - low_52w) / (high_52w - low_52w)) * 100
            
            analysis_data = {
                'ticker': ticker,
                'timestamp': datetime.now(self.kst).isoformat(),
                'market_condition': candidate_data.get('market_condition', 'NEUTRAL'),
                
                # 가격 정보
                'price_data': {
                    'current_price': current_price,
                    'price_change_1d': price_change_1d,
                    'high_52w': high_52w,
                    'low_52w': low_52w,
                    'price_position_pct': price_position
                },
                
                # 기술적 지표
                'technical_indicators': {
                    'ma5': ma5,
                    'ma20': ma20, 
                    'ma60': ma60,
                    'rsi_14': rsi,
                    'ma_arrangement': 'bullish' if current_price > ma5 > ma20 else 'bearish'
                },
                
                # 거래량 분석
                'volume_analysis': {
                    'current_volume': current_volume,
                    'avg_volume_20': avg_volume_20,
                    'volume_ratio': volume_ratio,
                    'volume_trend': 'high' if volume_ratio > 1.5 else 'normal'
                },
                
                # Phase 2 필터링 결과
                'phase2_analysis': {
                    'final_score': candidate_data.get('final_score', 0),
                    'filter_score': candidate_data.get('filter_score', 0),
                    'pattern_score': candidate_data.get('pattern_score', 0),
                    'pattern_analysis': candidate_data.get('pattern_analysis', {}),
                    'analysis_details': candidate_data.get('analysis_details', {})
                }
            }
            
            return analysis_data
            
        except Exception as e:
            logger.error(f"❌ {ticker} 분석 데이터 준비 실패: {e}")
            return {}

    def create_gpt_prompt(self, ticker: str, analysis_data: Dict, market_condition: str) -> str:
        """GPT 분석용 프롬프트 생성"""
        
        prompt = f"""
당신은 암호화폐 전문 투자 분석가입니다. {ticker}에 대한 종합적인 기술 분석을 수행해 주세요.

## 📊 기본 정보
- 티커: {ticker}
- 현재가: {analysis_data['price_data']['current_price']:,.0f}원
- 1일 변화율: {analysis_data['price_data']['price_change_1d']:+.2f}%
- 시장 상황: {market_condition}
- 52주 가격 위치: {analysis_data['price_data']['price_position_pct']:.1f}%

## 📈 기술적 지표
- MA5: {analysis_data['technical_indicators']['ma5']:,.0f}원
- MA20: {analysis_data['technical_indicators']['ma20']:,.0f}원  
- MA60: {analysis_data['technical_indicators']['ma60']:,.0f}원
- RSI(14): {analysis_data['technical_indicators']['rsi_14']:.1f}
- 이평선 배열: {analysis_data['technical_indicators']['ma_arrangement']}

## 📊 거래량 분석
- 현재 거래량: {analysis_data['volume_analysis']['current_volume']:,.0f}
- 평균 거래량 대비: {analysis_data['volume_analysis']['volume_ratio']:.1f}배
- 거래량 트렌드: {analysis_data['volume_analysis']['volume_trend']}

## 🎯 Phase 2 필터링 결과
- 최종 점수: {analysis_data['phase2_analysis']['final_score']:.1f}점
- 필터 점수: {analysis_data['phase2_analysis']['filter_score']:.1f}점
- 패턴 점수: {analysis_data['phase2_analysis']['pattern_score']:.1f}점
- 와인스타인 Stage2: {analysis_data['phase2_analysis']['pattern_analysis'].get('weinstein_stage2', False)}
- 미너비니 VCP: {analysis_data['phase2_analysis']['pattern_analysis'].get('minervini_vcp', False)}
- 오닐 컵핸들: {analysis_data['phase2_analysis']['pattern_analysis'].get('oneill_cup_handle', False)}

## 🔍 분석 요청사항

다음 관점에서 종합적으로 분석해 주세요:

### 1. 기술적 분석 (Technical Analysis)
- 현재 추세와 모멘텀 평가
- 주요 지지/저항 수준 확인
- 이동평균선 분석 및 돌파 가능성
- RSI 기반 과매수/과매도 상태

### 2. 패턴 분석 (Pattern Recognition)
- 와인스타인 4단계 사이클에서의 현재 위치
- 미너비니 VCP 패턴 존재 여부 및 완성도
- 오닐의 컵앤핸들 또는 기타 돌파 패턴

### 3. 시장 상황별 전략 (Market Context)
- 현재 {market_condition} 시장에서의 적합성
- 하락장/상승장별 맞춤 접근법
- 리스크 요인 및 주의사항

### 4. 투자 의견 (Investment Opinion)
- 매수/관망/매도 추천 (BUY/HOLD/SELL)
- 목표가 및 손절가 제안
- 포지션 크기 권장사항 (1-10 점수)

### 5. 리스크 평가 (Risk Assessment)
- 투자 리스크 수준 (1-10)
- 주요 위험 요인
- 모니터링 포인트

## 📝 응답 형식
JSON 형태로 구조화된 분석 결과를 제공해 주세요:

```json
{{
    "ticker": "{ticker}",
    "analysis_timestamp": "{analysis_data.get('timestamp', '')}",
    "overall_rating": "BUY/HOLD/SELL",
    "confidence_score": 85,
    "technical_analysis": {{
        "trend_direction": "bullish/bearish/neutral",
        "momentum_strength": "strong/moderate/weak",
        "support_level": 50000,
        "resistance_level": 60000,
        "key_insights": ["상승 돌파 임박", "거래량 급증 신호"]
    }},
    "pattern_analysis": {{
        "primary_pattern": "Stage 2 Breakout",
        "pattern_completion": 80,
        "breakout_probability": 75,
        "pattern_insights": ["VCP 패턴 완성도 높음"]
    }},
    "market_context": {{
        "market_suitability": "high/medium/low",
        "strategy_recommendation": "적극 매수",
        "risk_factors": ["시장 전반 변동성"]
    }},
    "investment_opinion": {{
        "recommendation": "BUY",
        "target_price": 65000,
        "stop_loss": 45000,
        "position_size_score": 7,
        "holding_period": "2-4 weeks"
    }},
    "risk_assessment": {{
        "risk_level": 6,
        "major_risks": ["시장 급락 리스크"],
        "monitoring_points": ["거래량 변화", "MA20 이탈 여부"]
    }},
    "summary": "종합적인 투자 의견 요약 (2-3문장)"
}}
```

전문가 수준의 상세하고 실용적인 분석을 제공해 주세요.
"""
        
        return prompt

    def analyze_with_gpt(self, ticker: str, analysis_data: Dict, chart_base64: Optional[str] = None) -> Optional[Dict]:
        """GPT-4를 사용한 종합 분석"""
        try:
            if not self.openai_api_key:
                logger.error("OpenAI API 키가 설정되지 않았습니다")
                return None
                
            logger.info(f"🤖 {ticker} GPT 분석 시작...")
            
            market_condition = analysis_data.get('market_condition', 'NEUTRAL')
            prompt = self.create_gpt_prompt(ticker, analysis_data, market_condition)
            
            # GPT-4 API 호출
            messages = [
                {
                    "role": "system", 
                    "content": "당신은 암호화폐 전문 투자 분석가입니다. 와인스타인, 미너비니, 오닐의 기술적 분석 이론에 정통하며, JSON 형태의 구조화된 분석 결과를 제공합니다."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # 차트 이미지가 있는 경우 추가 (향후 vision API 지원시)
            if chart_base64 and "gpt-4-vision" in self.gpt_config['model']:
                messages[1]["content"] = [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{chart_base64}"}}
                ]
            
            response = openai.ChatCompletion.create(
                model=self.gpt_config['model'],
                messages=messages,
                max_tokens=self.gpt_config['max_tokens'],
                temperature=self.gpt_config['temperature'],
                timeout=60
            )
            
            gpt_result = response.choices[0].message.content.strip()
            
            # JSON 파싱 시도
            try:
                # JSON 부분 추출
                start_idx = gpt_result.find('{')
                end_idx = gpt_result.rfind('}') + 1
                if start_idx != -1 and end_idx != -1:
                    json_str = gpt_result[start_idx:end_idx]
                    analysis_result = json.loads(json_str)
                else:
                    # JSON이 없는 경우 텍스트 결과를 구조화
                    analysis_result = {
                        "ticker": ticker,
                        "analysis_timestamp": analysis_data.get('timestamp', ''),
                        "overall_rating": "HOLD",
                        "confidence_score": 50,
                        "gpt_raw_response": gpt_result,
                        "summary": gpt_result[:200] + "..." if len(gpt_result) > 200 else gpt_result
                    }
            except json.JSONDecodeError:
                logger.warning(f"GPT 응답을 JSON으로 파싱 실패, 원문 저장: {ticker}")
                analysis_result = {
                    "ticker": ticker,
                    "analysis_timestamp": analysis_data.get('timestamp', ''),
                    "overall_rating": "HOLD",
                    "confidence_score": 50,
                    "gpt_raw_response": gpt_result,
                    "summary": "GPT 분석 완료 (JSON 파싱 실패)"
                }
            
            logger.info(f"✅ {ticker} GPT 분석 완료: {analysis_result.get('overall_rating', 'HOLD')}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} GPT 분석 실패: {e}")
            return {
                "ticker": ticker,
                "analysis_timestamp": analysis_data.get('timestamp', ''),
                "overall_rating": "HOLD",
                "confidence_score": 0,
                "error": str(e),
                "summary": f"GPT 분석 실패: {str(e)}"
            }

    def process_candidates(self, candidates: List[Dict]) -> List[Dict[str, Any]]:
        """후보들에 대해 GPT 분석 실행"""
        try:
            logger.info(f"🔍 GPT 분석 시작: {len(candidates)}개 후보")
            analysis_results = []
            
            for idx, candidate in enumerate(candidates):
                ticker = candidate.get('ticker')
                if not ticker:
                    continue
                
                try:
                    logger.info(f"📊 {ticker} 분석 중... ({idx+1}/{len(candidates)})")
                    
                    # 1. OHLCV 데이터 가져오기
                    df = self.get_ohlcv_data(ticker, period=60)
                    if df is None:
                        logger.warning(f"{ticker} 데이터 조회 실패, 건너뛰기")
                        continue
                    
                    # 2. 차트 생성
                    chart_base64 = self.create_comprehensive_chart(ticker, df, candidate)
                    
                    # 3. 분석 데이터 준비
                    analysis_data = self.prepare_analysis_data(ticker, candidate, df)
                    if not analysis_data:
                        logger.warning(f"{ticker} 분석 데이터 준비 실패")
                        continue
                    
                    # 4. GPT 분석 실행
                    gpt_analysis = self.analyze_with_gpt(ticker, analysis_data, chart_base64)
                    if not gpt_analysis:
                        logger.warning(f"{ticker} GPT 분석 실패")
                        continue
                    
                    # 5. 결과 통합
                    final_result = {
                        'ticker': ticker,
                        'analysis_timestamp': datetime.now(self.kst).isoformat(),
                        'phase2_data': candidate,
                        'technical_data': analysis_data,
                        'gpt_analysis': gpt_analysis,
                        'chart_base64': chart_base64,
                        'processing_order': idx + 1
                    }
                    
                    analysis_results.append(final_result)
                    logger.info(f"✅ {ticker} 분석 완료: {gpt_analysis.get('overall_rating', 'HOLD')}")
                    
                    # API 제한 고려하여 대기 (1초)
                    if idx < len(candidates) - 1:  # 마지막이 아닌 경우
                        import time
                        time.sleep(1)
                
                except Exception as e:
                    logger.error(f"❌ {ticker} 개별 분석 실패: {e}")
                    continue
            
            logger.info(f"🎯 GPT 분석 완료: {len(analysis_results)}개 분석 결과")
            return analysis_results
            
        except Exception as e:
            logger.error(f"❌ 후보 처리 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict[str, Any]]) -> bool:
        """분석 결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # 결과 데이터 구성
            output_data = {
                'phase': 'gpt_analysis',
                'status': 'success',
                'timestamp': timestamp,
                'analyzed_count': len(results),
                'gpt_config': self.gpt_config,
                'analysis_results': results,
                'summary': {
                    'total_analyzed': len(results),
                    'ratings': {
                        'BUY': len([r for r in results if r.get('gpt_analysis', {}).get('overall_rating') == 'BUY']),
                        'HOLD': len([r for r in results if r.get('gpt_analysis', {}).get('overall_rating') == 'HOLD']),
                        'SELL': len([r for r in results if r.get('gpt_analysis', {}).get('overall_rating') == 'SELL'])
                    },
                    'avg_confidence': np.mean([r.get('gpt_analysis', {}).get('confidence_score', 0) for r in results]) if results else 0
                }
            }
            
            # 메인 결과 파일
            main_key = f'phase3/gpt_analysis_results_{timestamp}.json'
            
            # 차트 이미지들은 제외하고 저장 (용량 최적화)
            save_data = output_data.copy()
            for result in save_data['analysis_results']:
                if 'chart_base64' in result:
                    del result['chart_base64']  # 차트는 별도 저장
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(save_data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 차트 이미지들 별도 저장
            for result in results:
                if 'chart_base64' in result and result['chart_base64']:
                    chart_key = f'phase3/charts/{result["ticker"]}_chart_{timestamp}.png'
                    self.s3_client.put_object(
                        Bucket=self.s3_bucket,
                        Key=chart_key,
                        Body=base64.b64decode(result['chart_base64']),
                        ContentType='image/png'
                    )
            
            logger.info(f"✅ Phase 3 결과 S3 저장 완료: s3://{self.s3_bucket}/{main_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

    def trigger_next_phase(self) -> bool:
        """Phase 4 트리거"""
        try:
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.phase3',
                        'DetailType': 'Phase 3 GPT Analysis Completed',
                        'Detail': json.dumps({
                            'status': 'completed',
                            'timestamp': datetime.now(self.kst).isoformat(),
                            'next_phase': 'phase4'
                        })
                    }
                ]
            )
            
            logger.info("✅ Phase 4 트리거 이벤트 발송 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ Phase 4 트리거 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 Phase 3 GPT Analysis 시작")
        logger.info(f"📥 입력 이벤트: {json.dumps(event, indent=2, ensure_ascii=False)}")
        
        gpt_analyzer = GPTAnalysisPhase3()
        
        # 1. Phase 2 데이터 로드
        phase2_candidates = gpt_analyzer.load_phase2_data()
        if not phase2_candidates:
            logger.error("❌ Phase 2 후보 데이터가 없습니다")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Phase 2 데이터 없음'})
            }
        
        # 2. GPT 분석 실행 (최대 5개까지)
        top_candidates = phase2_candidates[:5]  # 상위 5개만 분석 (API 비용 최적화)
        analysis_results = gpt_analyzer.process_candidates(top_candidates)
        
        if not analysis_results:
            logger.warning("⚠️ GPT 분석 결과가 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'completed',
                    'analyzed_count': 0,
                    'message': 'GPT 분석 결과 없음'
                })
            }
        
        # 3. 결과 저장
        save_success = gpt_analyzer.save_results_to_s3(analysis_results)
        
        if not save_success:
            logger.error("❌ 결과 저장 실패")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3 저장 실패'})
            }
        
        # 4. Phase 4 트리거 (매수 추천이 있는 경우만)
        buy_recommendations = [r for r in analysis_results if r.get('gpt_analysis', {}).get('overall_rating') == 'BUY']
        
        if buy_recommendations:
            trigger_success = gpt_analyzer.trigger_next_phase()
            if not trigger_success:
                logger.warning("⚠️ Phase 4 트리거 실패")
        else:
            logger.info("📭 매수 추천 종목이 없어 Phase 4 트리거 생략")
        
        # 5. 최종 결과 반환
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'input_candidates': len(phase2_candidates),
                'analyzed_count': len(analysis_results),
                'buy_recommendations': len(buy_recommendations),
                'analysis_summary': {
                    'BUY': len([r for r in analysis_results if r.get('gpt_analysis', {}).get('overall_rating') == 'BUY']),
                    'HOLD': len([r for r in analysis_results if r.get('gpt_analysis', {}).get('overall_rating') == 'HOLD']),
                    'SELL': len([r for r in analysis_results if r.get('gpt_analysis', {}).get('overall_rating') == 'SELL'])
                },
                'top_recommendations': [
                    {
                        'ticker': r['ticker'],
                        'rating': r.get('gpt_analysis', {}).get('overall_rating', 'HOLD'),
                        'confidence': r.get('gpt_analysis', {}).get('confidence_score', 0)
                    } for r in analysis_results[:3]  # 상위 3개
                ],
                'next_phase_triggered': len(buy_recommendations) > 0
            }, ensure_ascii=False, indent=2)
        }
        
        logger.info(f"✅ Phase 3 완료: {len(analysis_results)}개 분석, {len(buy_recommendations)}개 매수 추천")
        return result
        
    except Exception as e:
        logger.error(f"❌ Phase 3 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'status': 'failed'
            })
        }

if __name__ == "__main__":
    # 로컬 테스트용
    test_event = {
        'source': 'makenaide.phase2',
        'detail-type': 'Phase 2 Comprehensive Filtering Completed'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))