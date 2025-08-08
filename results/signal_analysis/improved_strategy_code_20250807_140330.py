
def improved_donchian_supertrend_strategy(data: pd.DataFrame) -> str:
    """개선된 돈치안 SuperTrend 전략 - 승률 40%+ 목표"""
    if len(data) < 15:  # 최소 데이터 증가
        return 'HOLD'
    
    current = data.iloc[-1]
    recent = data.tail(15)  # 기간 확장
    
    # 돈치안 채널 (개선된 로직)
    donchian_high = recent['high'].rolling(10).max().iloc[-1]
    donchian_low = recent['low'].rolling(10).min().iloc[-1]
    donchian_mid = (donchian_high + donchian_low) / 2
    
    # 돈치안 채널 위치 (0-1)
    donchian_position = (current['close'] - donchian_low) / (donchian_high - donchian_low) if donchian_high != donchian_low else 0.5
    
    # 이동평균 (개선)
    ma_5 = recent['close'].tail(5).mean()
    ma_10 = recent['close'].tail(10).mean()
    ma_15 = recent['close'].mean()
    
    # RSI 필터 추가
    delta = recent['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(7).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(7).mean().iloc[-1]
    rsi = 100 - (100 / (1 + gain/loss)) if loss > 0 else 50
    
    # 거래량 확인 강화
    volume_ratio = current['volume'] / recent['volume'].mean() if recent['volume'].mean() > 0 else 1
    
    # 변동성 체크
    volatility = recent['close'].pct_change().tail(5).std()
    
    # 개선된 매수 조건 (엄격화)
    buy_conditions = [
        donchian_position > 0.6,  # 돈치안 상위 40% 구간
        current['close'] > ma_5,
        ma_5 > ma_10,
        ma_10 > ma_15 * 1.001,  # 이평 정배열 확실히
        30 < rsi < 75,  # RSI 필터 (과매도/과매수 회피)
        volume_ratio > 1.8,  # 거래량 증가 확실히
        volatility < 0.05  # 과도한 변동성 회피
    ]
    
    # 개선된 매도 조건
    sell_conditions = [
        donchian_position < 0.4,  # 돈치안 하위 40% 구간
        current['close'] < ma_5,
        ma_5 < ma_10 * 0.998,  # 이평 역배열
        rsi > 75 or rsi < 25,  # 극단적 RSI
        current['close'] < ma_15 * 0.97  # 장기 이평 대비 3% 하락
    ]
    
    # 신호 생성 (더 엄격한 조건)
    if sum(buy_conditions) >= 5:  # 7개 중 5개 이상
        return 'BUY'
    elif sum(sell_conditions) >= 3:  # 5개 중 3개 이상
        return 'SELL'
    
    return 'HOLD'
