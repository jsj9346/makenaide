# 개선된 Hybrid_VCP_Momentum 전략\n# 목표: 승률 40% 이상\n# 생성일: 2025-08-07 14:07:24.446713\n\n
@self._strategy_decorator("momentum")
def improved_vcp_momentum_strategy(data: pd.DataFrame) -> str:
    """개선된 VCP 모멘텀 전략 - 승률 향상 버전"""
    if len(data) < 25:  # 데이터 요구량 증가
        return 'HOLD'
    
    current = data.iloc[-1]
    recent_15 = data.tail(15)
    recent_25 = data.tail(25)
    
    # VCP 패턴 감지 (개선)
    high_20 = recent_25['high'].rolling(20).max().iloc[-1]
    current_price = current['close']
    
    # 풀백 비율 계산
    pullback_ratio = (high_20 - current_price) / high_20 if high_20 > 0 else 0
    
    # 변동성 수축 측정 (개선)
    recent_vol = recent_15['close'].pct_change().std()
    prev_vol = data.iloc[-30:-15]['close'].pct_change().std()
    vol_contraction = prev_vol / recent_vol if recent_vol > 0 else 1
    
    # 모멘텀 분석 (다중 기간)
    momentum_5 = (current_price - recent_15.iloc[0]['close']) / recent_15.iloc[0]['close']
    momentum_10 = (current_price - recent_25.iloc[10]['close']) / recent_25.iloc[10]['close']
    
    # 이동평균
    ma_5 = recent_15['close'].tail(5).mean()
    ma_10 = recent_15['close'].tail(10).mean()
    ma_15 = recent_15['close'].mean()
    
    # RSI 추가 (승률 향상)
    delta = recent_15['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(10).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(10).mean().iloc[-1]
    rsi = 100 - (100 / (1 + gain/loss)) if loss > 0 else 50
    
    # 거래량 프로필
    volume_ma = recent_15['volume'].mean()
    volume_ratio = current['volume'] / volume_ma if volume_ma > 0 else 1
    
    # 개선된 매수 조건 (점수 시스템)
    buy_score = 0
    
    # 1. VCP 패턴 (가중치: 30%)
    if 0.03 < pullback_ratio < 0.20 and vol_contraction > 1.3:  # 적정 풀백 + 변동성 수축
        buy_score += 30
    elif 0.05 < pullback_ratio < 0.25:
        buy_score += 15
    
    # 2. 모멘텀 확인 (가중치: 25%)
    if momentum_5 > -0.05 and momentum_10 > -0.10:  # 양호한 모멘텀
        buy_score += 25
    elif momentum_5 > -0.08:
        buy_score += 12
    
    # 3. 이동평균 정배열 (가중치: 20%)
    if ma_5 > ma_10 > ma_15:
        buy_score += 20
    elif ma_5 > ma_10:
        buy_score += 10
    
    # 4. RSI 적정 구간 (가중치: 15%)
    if 40 < rsi < 65:
        buy_score += 15
    elif 35 < rsi < 70:
        buy_score += 8
    
    # 5. 거래량 확인 (가중치: 10%)
    if volume_ratio > 1.8:
        buy_score += 10
    elif volume_ratio > 1.3:
        buy_score += 5
    
    # 개선된 매도 조건
    sell_score = 0
    
    if (pullback_ratio > 0.25 or  # 과도한 하락
        momentum_5 < -0.12 or     # 모멘텀 악화
        ma_5 < ma_15 * 0.97 or    # 이평 이탈
        rsi > 75 or rsi < 25 or   # RSI 극단
        volume_ratio > 4.0):      # 과도한 거래량
        sell_score = 70
    
    # 신호 생성
    if buy_score >= 65:  # 100점 중 65점 이상
        return 'BUY'
    elif sell_score >= 60:
        return 'SELL'
    
    return 'HOLD'
