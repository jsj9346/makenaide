# 모드별 필터링 프리셋 설정
# YAML에서 'mode: tight' 또는 'mode: lose'가 주어졌을 때
# 아래 프리셋이 자동으로 필터 설정에 병합됩니다.

MODE_PRESETS = {
    "tight": {
        # ---- 필수 조건 ----
        "require_price_above_ma200": True,
        "require_price_above_high60": True,

        # ---- 보조 조건 ----
        "check_macd_positive": True,
        "check_adx_strength": True,
        "check_golden_cross": True,
        "check_volume_surge": True,

        # ---- 임계값 ----
        "adx_threshold": 20,
        "min_optional_conditions_passed": 2,
        "max_filtered_tickers": 20,
    },
    "lose": {
        # ---- 필수 조건 ----
        "require_price_above_ma200": False,
        "require_price_above_high60": False,
        "price_above_ma200_ratio": 0.85,     # 완화된 기준
        "price_near_high60_ratio": 0.95,     # 완화된 기준

        # ---- 보조 조건 ----
        "check_macd_positive": True,
        "check_adx_strength": True,
        "check_golden_cross": True,
        "check_volume_surge": True,

        # ---- 임계값 ----
        "adx_threshold": 14,
        "min_optional_conditions_passed": 1,
        "max_filtered_tickers": 30,
    },
} 