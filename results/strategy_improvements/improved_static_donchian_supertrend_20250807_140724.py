# 개선된 Static_Donchian_Supertrend 전략\n# 목표: 승률 40% 이상\n# 생성일: 2025-08-07 14:07:24.446612\n\n
@self._strategy_decorator("technical")
def improved_donchian_supertrend_strategy(data: pd.DataFrame) -> str:
    """개선된 돈치안 SuperTrend 전략 - 승률 향상 버전"""
    if len(data) < 20:  # 데이터 요구량 증가 (안정성)
        return 'HOLD'
    
    current = data.iloc[-1]
    recent_10 = data.tail(10)
    recent_20 = data.tail(20)
    
    # 돈치안 채널 (기존)
    donchian_high = recent_10['high'].max()
    donchian_low = recent_10['low'].min()
    donchian_mid = (donchian_high + donchian_low) / 2
    
    # 돈치안 채널 내 위치 (0-1)
    donchian_range = donchian_high - donchian_low
    donchian_position = (current['close'] - donchian_low) / donchian_range if donchian_range > 0 else 0.5
    
    # 이동평균 (개선)
    ma_5 = recent_10['close'].tail(5).mean()
    ma_10 = recent_10['close'].mean()
    ma_20 = recent_20['close'].mean()
    
    # RSI 필터 추가 (7일)
    delta = recent_10['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(7).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(7).mean().iloc[-1]
    rsi = 100 - (100 / (1 + gain/loss)) if loss > 0 else 50
    
    # 거래량 분석 강화
    volume_sma = recent_10['volume'].mean()
    volume_ratio = current['volume'] / volume_sma if volume_sma > 0 else 1
    
    # 변동성 필터
    volatility = recent_10['close'].pct_change().std()
    
    # 추세 강도 측정
    trend_strength = (ma_5 - ma_20) / ma_20 if ma_20 > 0 else 0
    
    # 개선된 매수 조건 (더 엄격하고 정확한 신호)
    buy_score = 0
    
    # 1. 돈치안 위치 (가중치: 25%)
    if donchian_position > 0.65:  # 상위 35% 구간
        buy_score += 25
    elif donchian_position > 0.5:
        buy_score += 10
    
    # 2. 이동평균 정배열 (가중치: 20%) 
    if ma_5 > ma_10 > ma_20:
        buy_score += 20
    elif ma_5 > ma_10:
        buy_score += 10
    
    # 3. RSI 필터 (가중치: 20%)
    if 35 < rsi < 70:  # 적정 구간
        buy_score += 20
    elif 30 < rsi < 75:
        buy_score += 10
    
    # 4. 거래량 확인 (가중치: 20%)
    if volume_ratio > 2.0:  # 강한 거래량
        buy_score += 20
    elif volume_ratio > 1.5:
        buy_score += 12
    
    # 5. 변동성 체크 (가중치: 15%)
    if 0.01 < volatility < 0.04:  # 적정 변동성
        buy_score += 15
    elif volatility < 0.06:
        buy_score += 8
    
    # 개선된 매도 조건
    sell_score = 0
    
    # 1. 돈치안 위치
    if donchian_position < 0.35:  # 하위 35% 구간
        sell_score += 30
    elif donchian_position < 0.5:
        sell_score += 15
    
    # 2. 이동평균 역배열
    if ma_5 < ma_10 < ma_20:
        sell_score += 25
    elif ma_5 < ma_10:
        sell_score += 12
    
    # 3. RSI 극단 구간
    if rsi > 75 or rsi < 25:
        sell_score += 25
    elif rsi > 70 or rsi < 30:
        sell_score += 15
    
    # 4. 추세 반전 신호
    if trend_strength < -0.02:  # 하락 추세 강화
        sell_score += 20
    
    # 신호 생성 (점수 기반)
    if buy_score >= 70:  # 100점 만점에 70점 이상
        return 'BUY'
    elif sell_score >= 60:  # 100점 만점에 60점 이상  
        return 'SELL'
    
    return 'HOLD'
