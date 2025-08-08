# 개선된 Simple_MA_Crossover 전략\n# 목표: 승률 40% 이상\n# 생성일: 2025-08-07 14:07:24.446796\n\n
@self._strategy_decorator("simple")  
def improved_ma_crossover_strategy(data: pd.DataFrame) -> str:
    """개선된 이동평균 크로스오버 전략 - 승률 향상 버전"""
    if len(data) < 20:
        return 'HOLD'
    
    current = data.iloc[-1]
    recent_15 = data.tail(15)
    recent_20 = data.tail(20)
    
    # 다중 이동평균
    ma_5 = recent_15['close'].tail(5).mean()
    ma_10 = recent_15['close'].tail(10).mean()
    ma_15 = recent_15['close'].mean()
    ma_20 = recent_20['close'].mean()
    
    # 이전 값들 (크로스오버 감지용)
    if len(recent_15) >= 6:
        prev_ma_5 = recent_15['close'].iloc[-6:-1].mean()
        prev_ma_10 = recent_15['close'].iloc[-11:-1].mean()
    else:
        return 'HOLD'
    
    # RSI 필터 추가
    delta = recent_15['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(8).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(8).mean().iloc[-1]
    rsi = 100 - (100 / (1 + gain/loss)) if loss > 0 else 50
    
    # 거래량 확인
    volume_ma = recent_15['volume'].mean()
    volume_ratio = current['volume'] / volume_ma if volume_ma > 0 else 1
    
    # 추세 강도
    trend_strength = (ma_5 - ma_20) / ma_20 if ma_20 > 0 else 0
    
    # 변동성 체크
    volatility = recent_15['close'].pct_change().std()
    
    # 개선된 크로스오버 조건
    buy_conditions = []
    
    # 1. 골든 크로스 확인
    golden_cross = (prev_ma_5 <= prev_ma_10 and ma_5 > ma_10)
    if golden_cross:
        buy_conditions.append("골든크로스")
    
    # 2. 지속적 상승 추세
    elif ma_5 > ma_10 * 1.015 and trend_strength > 0.02:  # 1.5% 이상 차이 + 강한 상승
        buy_conditions.append("강한상승")
    
    # 3. RSI 적정 구간
    rsi_ok = 30 < rsi < 75
    if rsi_ok:
        buy_conditions.append("RSI적정")
    
    # 4. 거래량 증가
    volume_ok = volume_ratio > 1.4
    if volume_ok:
        buy_conditions.append("거래량증가")
    
    # 5. 적정 변동성
    vol_ok = 0.01 < volatility < 0.05
    if vol_ok:
        buy_conditions.append("변동성적정")
    
    # 6. 장기 추세 확인
    longterm_ok = ma_10 > ma_20 * 1.005
    if longterm_ok:
        buy_conditions.append("장기상승")
    
    # 매도 조건
    sell_conditions = []
    
    # 1. 데드 크로스
    dead_cross = (prev_ma_5 >= prev_ma_10 and ma_5 < ma_10)
    if dead_cross:
        sell_conditions.append("데드크로스")
    
    # 2. 지속적 하락
    elif ma_5 < ma_10 * 0.985 and trend_strength < -0.02:
        sell_conditions.append("강한하락")
    
    # 3. RSI 극단
    if rsi > 75 or rsi < 25:
        sell_conditions.append("RSI극단")
    
    # 4. 장기 추세 반전
    if ma_5 < ma_20 * 0.98:
        sell_conditions.append("장기하락")
    
    # 신호 생성 (조건 개수 기반)
    if len(buy_conditions) >= 4:  # 6개 중 4개 이상
        return 'BUY'
    elif len(sell_conditions) >= 2:  # 4개 중 2개 이상
        return 'SELL'
    
    return 'HOLD'
