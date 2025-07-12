"""
Makenaide 시스템 중앙 설정 관리 모듈

📚 트레이딩 전략 기반 설정:
- 스탠 와인스타인 4단계 사이클 전략 
- 마크 미너비니 VCP (Volatility Contraction Pattern) 전략
- 윌리엄 오닐 CANSLIM 전략

🔧 성능 최적화 설정:
- DB 연결 풀링 설정
- 메모리 사용량 제한
- 배치 처리 크기 최적화
"""

import os
from typing import Dict, Set, Any
from dotenv import load_dotenv
from dataclasses import dataclass

# 환경변수 로딩
load_dotenv()

# ===== 트레이딩 전략 기반 지표 설정 =====

# 지표 설정은 하단의 통합 설정에서 자동 생성됩니다.
# 개별 설정이 필요한 경우 아래 변수들을 직접 수정하세요.

# 초기 기본값 설정 (통합 과정에서 업데이트됨)
ESSENTIAL_TREND_INDICATORS: Set[str] = set()
NON_ESSENTIAL_INDICATORS: Set[str] = set()
INDICATOR_MIN_PERIODS: Dict[str, int] = {}

# ===== DB 연결 설정 =====

# DB 연결 풀링 설정
DB_POOL_CONFIG: Dict[str, Any] = {
    'minconn': 2,           # 최소 연결 수
    'maxconn': 10,          # 최대 연결 수  
    'connection_timeout': 30,  # 연결 타임아웃 (초)
    'idle_timeout': 300,    # 유휴 연결 타임아웃 (초)
    'max_retries': 3,       # 최대 재시도 횟수
    'retry_delay': 1.0      # 재시도 지연 시간 (초)
}

# DB 연결 정보
DB_CONFIG: Dict[str, str] = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': os.getenv('PG_PORT', '5432'),
    'dbname': os.getenv('PG_DATABASE', 'makenaide'),
    'user': os.getenv('PG_USER', 'postgres'),
    'password': os.getenv('PG_PASSWORD', '')
}

# ===== 성능 튜닝 파라미터 =====

# 배치 처리 설정
BATCH_PROCESSING_CONFIG: Dict[str, int] = {
    'BATCH_SIZE': 50,              # 배치 크기
    'CHECKPOINT_INTERVAL': 10,      # 체크포인트 간격
    'MAX_CONCURRENT_WORKERS': 4,   # 최대 동시 작업자 수
    'MEMORY_CLEANUP_INTERVAL': 100  # 메모리 정리 간격
}

# 메모리 사용량 제한 (MB)
MEMORY_LIMITS: Dict[str, int] = {
    'MAX_TOTAL_MEMORY_MB': 2048,      # 전체 최대 메모리
    'MAX_SINGLE_PROCESS_MB': 512,     # 단일 프로세스 최대 메모리
    'DETAIL_ISSUES_LIMIT': 1000,      # 상세 이슈 저장 개수 제한
    'INDICATOR_MEMORY_THRESHOLD': 100  # 지표별 메모리 알림 임계값
}

# API 호출 설정
API_CONFIG: Dict[str, Any] = {
    'CACHE_SIZE': 100,         # 캐시 크기
    'API_SLEEP_TIME': 0.2,     # API 호출 간 대기 시간
    'MAX_RETRY_ATTEMPTS': 3,   # 최대 재시도 횟수
    'BACKOFF_FACTOR': 2        # 재시도 지연 배수
}

# ===== 알림 임계값 설정 =====

# 데이터 품질 알림 임계값
QUALITY_ALERT_THRESHOLDS: Dict[str, float] = {
    'api_error_rate': 15.0,           # API 오류율 (%)
    'indicator_fail_rate': 20.0,      # 지표 계산 실패율 (%)
    'db_fail_rate': 10.0,             # DB 업데이트 실패율 (%)
    'memory_usage_mb': 1024,          # 메모리 사용량 알림 (MB)
    'api_1970_error_rate': 10.0,      # 1970-01-01 오류율 (%)
    'indicator_success_rate': 70.0    # 지표 성공률 최소 기준 (%)
}

# 성능 모니터링 설정
PERFORMANCE_MONITORING: Dict[str, Any] = {
    'enable_detailed_logging': True,     # 상세 로깅 활성화
    'enable_memory_profiling': True,     # 메모리 프로파일링 활성화
    'enable_performance_alerts': True,   # 성능 알림 활성화
    'benchmark_interval': 3600,          # 벤치마크 주기 (초)
    'report_generation_interval': 86400  # 보고서 생성 주기 (초)
}

# 로그 관리 설정
LOG_MANAGEMENT: Dict[str, Any] = {
    'retention_days': 7,                 # 로그 파일 보관 기간 (일)
    'restricted_logging': True,          # 제한된 로깅 사용 (makenaide.log만 생성)
    'enable_log_cleanup': True,          # 자동 로그 정리 활성화
    'log_cleanup_on_startup': True,      # 파이프라인 시작 시 로그 정리
    'max_log_size_mb': 100,              # 최대 로그 파일 크기 (MB)
    'excluded_log_files': [              # 생성하지 않을 로그 파일 목록
        'gpt_analysis.log',
        'data_fetcher.log', 
        'scanner.log'
    ]
}

# ===== GPT 분석 결과 보관 및 삭제 설정 =====

# GPT 분석 결과 라이프사이클 관리 설정
GPT_ANALYSIS_LIFECYCLE: Dict[str, Any] = {
    'name': 'GPT Analysis Lifecycle Management',
    'description': 'GPT 분석 결과의 보관, 캐싱, 삭제 정책 관리',
    'enabled': True,
    
    # 기본 보관 정책
    'retention_policy': {
        'default_retention_hours': 24,      # 기본 보관 시간 (24시간)
        'high_confidence_retention_hours': 48,  # 고신뢰도 결과 보관 시간 (48시간)
        'low_confidence_retention_hours': 12,   # 저신뢰도 결과 보관 시간 (12시간)
        'error_result_retention_hours': 6,      # 오류 결과 보관 시간 (6시간)
    },
    
    # 캐싱 정책
    'caching_policy': {
        'enable_caching': True,              # 캐싱 활성화
        'cache_ttl_minutes': 720,            # 캐시 유효 시간 (12시간)
        'max_cache_entries': 1000,           # 최대 캐시 엔트리 수
        'max_cache_memory_mb': 100,          # 최대 캐시 메모리 사용량 (MB)
        'cache_cleanup_interval_minutes': 300,  # 캐시 정리 주기 (5분)
    },
    
    # 자동 삭제 정책
    'cleanup_policy': {
        'enable_auto_cleanup': True,         # 자동 정리 활성화
        'cleanup_interval_hours': 6,         # 정리 실행 주기 (6시간)
        'batch_cleanup_size': 100,           # 배치 정리 크기
        'dry_run_mode': False,               # 실제 삭제 전 시뮬레이션 모드
    },
    
    # 품질 기반 보관 정책
    'quality_based_retention': {
        'enabled': True,
        'score_thresholds': {
            'high_quality': 80,              # 고품질 기준 (80점 이상)
            'medium_quality': 60,            # 중간품질 기준 (60점 이상)
            'low_quality': 40,               # 저품질 기준 (40점 이상)
        },
        'confidence_thresholds': {
            'high_confidence': 0.8,          # 고신뢰도 기준 (0.8 이상)
            'medium_confidence': 0.6,        # 중간신뢰도 기준 (0.6 이상)
            'low_confidence': 0.4,           # 저신뢰도 기준 (0.4 이상)
        },
        'retention_multipliers': {
            'high_quality': 2.0,             # 고품질 결과 보관 시간 배수
            'medium_quality': 1.5,           # 중간품질 결과 보관 시간 배수
            'low_quality': 1.0,              # 저품질 결과 보관 시간 배수
        }
    },
    
    # 시장 상황 기반 보관 정책
    'market_condition_retention': {
        'enabled': True,
        'market_phase_multipliers': {
            'Stage1': 1.5,                   # Stage1 결과 보관 시간 배수
            'Stage2': 1.3,                   # Stage2 결과 보관 시간 배수
            'Stage3': 1.0,                   # Stage3 결과 보관 시간 배수
            'Stage4': 0.8,                   # Stage4 결과 보관 시간 배수
        },
        'action_based_retention': {
            'BUY': 1.5,                      # 매수 신호 보관 시간 배수
            'STRONG_BUY': 2.0,               # 강력 매수 신호 보관 시간 배수
            'HOLD': 1.0,                     # 보유 신호 보관 시간 배수
            'AVOID': 0.7,                    # 회피 신호 보관 시간 배수
        }
    },
    
    # 모니터링 및 알림
    'monitoring': {
        'enable_cleanup_logging': True,      # 정리 작업 로깅
        'enable_retention_alerts': True,     # 보관 정책 알림
        'enable_performance_tracking': True, # 성능 추적
        'alert_thresholds': {
            'high_cleanup_rate': 0.3,        # 높은 정리 비율 임계값 (30%)
            'low_cache_hit_rate': 0.5,       # 낮은 캐시 히트율 임계값 (50%)
            'memory_usage_threshold': 0.8,   # 메모리 사용량 임계값 (80%)
        }
    },
    
    # 백업 및 복구
    'backup_policy': {
        'enable_backup': True,               # 백업 활성화
        'backup_interval_hours': 24,         # 백업 주기 (24시간)
        'backup_retention_days': 7,          # 백업 보관 기간 (7일)
        'backup_high_quality_only': True,    # 고품질 결과만 백업
    }
}

# ===== GPT 분석 필터링 조건 설정 =====

# GPT 분석 결과 기반 매수 조건 필터링 설정
GPT_FILTERING_CONFIG: Dict[str, Any] = {
    'strict_mode': {
        'enabled': True,
        'min_score': 80,                    # 최소 점수 (0-100)
        'min_confidence': 0.9,              # 최소 신뢰도 (0-1)
        'allowed_actions': ['BUY', 'STRONG_BUY'],  # 허용되는 액션
        'allowed_market_phases': ['Stage1', 'Stage2'],  # 허용되는 시장 단계
        'description': '엄격한 필터링 모드 - 고품질 매수 신호만 허용'
    },
    'moderate_mode': {
        'enabled': False,
        'min_score': 60,                    # 중간 점수 기준
        'min_confidence': 0.7,              # 중간 신뢰도 기준
        'allowed_actions': ['BUY', 'STRONG_BUY'],
        'allowed_market_phases': ['Stage1', 'Stage2', 'Stage3'],
        'description': '중간 필터링 모드 - 보다 많은 신호 허용'
    },
    'loose_mode': {
        'enabled': False,
        'min_score': 40,                    # 낮은 점수 기준
        'min_confidence': 0.5,              # 낮은 신뢰도 기준
        'allowed_actions': ['BUY', 'STRONG_BUY', 'HOLD'],
        'allowed_market_phases': ['Stage1', 'Stage2', 'Stage3', 'Stage4'],
        'description': '느슨한 필터링 모드 - 많은 신호 허용 (테스트용)'
    }
}

# ===== 트레이딩 전략별 설정 =====

# 전략 설정은 하단에서 통합 정의됩니다.

# ===== 피라미딩 전략 설정 =====

# 고도화된 피라미딩 전략 설정 (notepad 요구사항 반영)
PYRAMIDING_CONFIG: Dict[str, Any] = {
    'name': 'Advanced Pyramiding Strategy',
    'description': '고도화된 피라미딩 전략 - 고점 돌파 + 기술적 지표 조합',
    'enabled': True,
    
    # 기본 설정
    'max_add_ons': 3,                    # 최대 피라미딩 횟수 (2~3회)
    'add_on_ratio': 0.5,                 # 추가 매수 비율 (초기 진입의 50%)
    'pyramid_threshold_pct': 5.0,        # 추가 매수 가격 임계값 (5% 상승)
    'max_total_position_pct': 8.0,       # 최대 총 포지션 비율 (8%)
    
    # 조건 A: 고점 돌파 + 거래량 증가
    'condition_a': {
        'enabled': True,
        'high_breakout_pct': 1.0,        # 최근 고점 돌파 임계값 (1%)
        'volume_surge_ratio': 1.3,       # 거래량 증가 임계값 (30% 증가)
        'lookback_days': 20,             # 고점 조회 기간 (20일)
    },
    
    # 조건 B: 기술적 지표 조합
    'condition_b': {
        'enabled': True,
        'supertrend_required': True,      # Supertrend 매수 신호 필수
        'adx_threshold': 25.0,            # ADX 임계값 (추세 강도)
        'ma20_rising_required': True,     # MA20 상승 중 필수
        'price_advance_pct': 5.0,         # 직전 진입가 대비 상승률 (5%)
    },
    
    # 안전 조건
    'safety_conditions': {
        'rsi_overbought_threshold': 75.0, # RSI 과매수 방지 (75 이하)
        'max_volatility_pct': 10.0,      # 최대 변동성 제한 (10%)
        'min_liquidity_ratio': 1.0,      # 최소 유동성 비율
    },
    
    # ATR 기반 리스크 조절
    'risk_management': {
        'atr_based_sizing': True,         # ATR 기반 포지션 사이징
        'volatility_adjustment': True,    # 변동성 기반 조정
        'max_volatility_reduction_pct': 30.0, # 최대 변동성 조정 (30% 축소)
        'size_decay_factor': 0.3,        # 피라미딩 횟수별 크기 감소 (30%)
    },
    
    # 평균 단가 기반 손절/익절
    'exit_management': {
        'use_average_entry_price': True,  # 평균 진입가 기준 관리
        'trailing_stop_enabled': True,    # 트레일링 스탑 활용
        'profit_taking_levels': [6.0, 12.0, 20.0], # 수익 실현 레벨 (%)
        'stop_loss_from_avg': 8.0,       # 평균가 기준 손절선 (8%)
    },
    
    # 로깅 및 모니터링
    'monitoring': {
        'log_all_conditions': True,       # 모든 조건 로깅
        'performance_tracking': True,     # 성과 추적
        'alert_on_execution': True,       # 실행 시 알림
        'detailed_analysis': True,        # 상세 분석 로깅
    }
}

# ===== 트레일링스탑 설정 =====
TRAILING_STOP_CONFIG: Dict[str, Any] = {
    'name': 'Enhanced Trailing Stop Strategy',
    'description': '트레일링스탑 설정 - 너무 일찍 발동되는 문제 해결',
    'enabled': True,
    
    # 기본 활성화 조건
    'min_rise_pct': 8.0,                 # 트레일링스탑 활성화 최소 상승률 (3% → 8%)
    'min_holding_days': 3,               # 최소 보유기간 (일)
    'recent_trend_check_days': 3,        # 최근 추세 확인 기간
    
    # 트레일링스탑 비율 설정
    'min_trailing_pct': 3.0,             # 최소 트레일링스탑 비율 (1.5% → 3%)
    'max_trailing_pct': 10.0,            # 최대 트레일링스탑 비율 (8% → 10%)
    
    # 보유기간별 조정 계수
    'holding_adjustments': {
        3: 2.0,    # 3일 이내: 100% 완화
        7: 1.5,    # 7일 이내: 50% 완화
        14: 1.2,   # 14일 이내: 20% 완화
    },
    
    # 변동성별 기본 배수
    'volatility_multipliers': {
        'high': 1.5,    # 고변동성 (ATR > 5%)
        'medium': 2.0,  # 중변동성 (ATR 3-5%)
        'low': 2.5,     # 저변동성 (ATR < 3%)
    },
    
    # 시장 상황 기반 비활성화
    'strong_uptrend_disable': True,      # 강한 상승추세 시 비활성화
    'strong_uptrend_conditions': {
        'rsi_min': 60,                   # RSI 최소값
        'rsi_max': 80,                   # RSI 최대값
        'ma20_rise_pct': 2.0,            # MA20 대비 최소 상승률
        'macd_positive': True,           # MACD 양수 여부
        'min_profit_pct': 10.0,          # 최소 수익률
    },
    
    # 켈리 기반 손절매 설정
    'kelly_stop_loss': {
        'enabled': True,                 # 켈리 기반 손절매 활성화
        'min_holding_days': 3,           # 최소 보유기간 (켈리 손절매 적용 전)
        'min_win_rate': 0.4,             # 최소 승률 (40% 미만 시 비활성화)
        'min_kelly_ratio': 0.05,         # 최소 켈리비율 (5% 미만 시 비활성화)
        'max_stop_loss_pct': 15.0,       # 최대 손절 비율 (15%)
        'min_stop_loss_pct': 5.0,        # 최소 손절 비율 (5%)
        'atr_multiplier': 2.0,           # ATR 배수 (기본 2배)
        'profit_threshold_pct': 5.0,     # 수익 구간 진입 임계값 (5% 이상)
        'volatility_adjustment': True,   # 변동성 기반 조정
        'trend_consideration': True,     # 추세 고려
    },
    
    # 로깅 설정
    'logging': {
        'log_trailing_stop_checks': True,    # 트레일링스탑 체크 로깅
        'log_activation_conditions': True,   # 활성화 조건 로깅
        'log_deactivation_reasons': True,    # 비활성화 사유 로깅
        'log_kelly_calculations': True,      # 켈리 계산 로깅
    }
}

# === UNUSED: 전략별 설정 (strategy_configs.py 통합) ===
# @dataclass
# class StrategyIndicators:
#     """전략별 필수 지표 클래스"""
#     essential: Set[str]
#     optional: Set[str]
#     min_periods: Dict[str, int]

# # 스탠 와인스타인 4단계 사이클 전략용 지표
# WEINSTEIN_INDICATORS = StrategyIndicators(
#     essential={
#         'ma_50', 'ma_200',  # 중장기 이평선 (단기 제거)
#         'volume_20ma',      # 거래량 확인
#         'atr',             # 변동성
#         'adx', 'plus_di', 'minus_di'  # 추세 강도 측정
#     },
#     optional={
#         'supertrend', 'supertrend_direction',
#         'donchian_high', 'donchian_low'
#     },
#     min_periods={
#         'ma_50': 50, 'ma_200': 200,
#         'volume_20ma': 20, 'atr': 14,
#         'adx': 14, 'plus_di': 14, 'minus_di': 14
#     }
# )

# # 미너비니 VCP 전략용 지표
# MINERVINI_INDICATORS = StrategyIndicators(
#     essential={
#         'atr',                                    # 변동성 측정
#         'bb_upper', 'bb_middle', 'bb_lower',     # 볼린저 밴드
#         'ma_50', 'ma_200',                       # 이동평균
#         'volume_20ma'                            # 거래량
#     },
#     optional={
#         'rsi_14', 'macd', 'macd_signal'
#     },
#     min_periods={
#         'atr': 14, 'bb_upper': 20, 'bb_middle': 20, 'bb_lower': 20,
#         'ma_50': 50, 'ma_200': 200, 'volume_20ma': 20,
#         'rsi_14': 14, 'macd': 34, 'macd_signal': 34
#     }
# )

# # 오닐 CANSLIM 전략용 지표
# ONEIL_INDICATORS = StrategyIndicators(
#     essential={
#         'rsi_14',                                # 상대강도지수
#         'macd', 'macd_signal', 'macd_histogram', # MACD 계열
#         'volume_20ma',                           # 거래량
#         'donchian_high', 'donchian_low'          # 돌파 지점
#     },
#     optional={
#         'ma_50', 'ma_200', 'atr'
#     },
#     min_periods={
#         'rsi_14': 14, 'macd': 34, 'macd_signal': 34,
#         'macd_histogram': 34, 'volume_20ma': 20,
#         'donchian_high': 20, 'donchian_low': 20
#     }
# )

# # 와인스타인 4단계 사이클 설정 (통합 버전)
# WEINSTEIN_CONFIG: Dict[str, Any] = {
#     'name': 'Weinstein 4-Stage Cycle',
#     'description': '스탠 와인스타인의 4단계 사이클 전략',
#     'parameters': {
#         'ma_slope_threshold': 0.5,        # MA200 기울기 임계값 (%)
#         'volume_change_threshold': 0.05,  # 거래량 변화 임계값
#         'trend_confirmation_days': 20,    # 추세 확인 기간 (일)
#         'stage_transition_threshold': 0.03 # 단계 전환 임계값
#     },
#     'risk_management': {
#         'stop_loss_pct': 8.0,            # 손절매 비율
#         'take_profit_pct': 20.0,         # 익절 비율
#         'position_sizing': 'equal_weight' # 포지션 크기 방식
#     },
#     'indicators': WEINSTEIN_INDICATORS
# }

# # 미너비니 VCP 설정 (통합 버전)
# MINERVINI_CONFIG: Dict[str, Any] = {
#     'name': 'Minervini VCP',
#     'description': '마크 미너비니의 VCP (Volatility Contraction Pattern) 전략',
#     'parameters': {
#         'atr_multiplier': 2.0,           # ATR 배수
#         'bb_squeeze_threshold': 0.1,     # 볼린저 밴드 수축 임계값
#         'volume_confirmation_ratio': 1.5, # 거래량 확인 비율
#         'vcp_contraction_pct': 25.0,     # VCP 수축 비율
#         'breakout_volume_multiple': 2.0   # 돌파 시 거래량 배수
#     },
#     'risk_management': {
#         'stop_loss_pct': 7.0,
#         'take_profit_pct': 20.0,
#         'position_sizing': 'volatility_adjusted'
#     },
#     'indicators': MINERVINI_INDICATORS
# }

# # 오닐 CANSLIM 설정 (통합 버전)
# ONEIL_CONFIG: Dict[str, Any] = {
#     'name': 'O\'Neil CANSLIM',
#     'description': '윌리엄 오닐의 CANSLIM 전략',
#     'parameters': {
#         'rsi_overbought': 70,            # RSI 과매수 기준
#         'rsi_oversold': 30,              # RSI 과매도 기준
#         'macd_signal_threshold': 0.0,    # MACD 신호 임계값
#         'donchian_breakout_period': 20,  # 돈치안 돌파 기간
#         'earnings_growth_min': 25.0,     # 최소 실적 증가율
#         'sales_growth_min': 25.0         # 최소 매출 증가율
#     },
#     'risk_management': {
#         'stop_loss_pct': 8.0,
#         'take_profit_pct': 25.0,
#         'position_sizing': 'kelly_criterion'
#     },
#     'indicators': ONEIL_INDICATORS
# }

# # 통합 지표 세트 생성 함수
# def get_combined_indicators() -> Dict[str, Any]:
#     """모든 전략의 지표를 통합하여 반환"""
#     essential_indicators = set()
#     non_essential_indicators = set()
#     indicator_min_periods = {}
#     
#     strategies = [WEINSTEIN_INDICATORS, MINERVINI_INDICATORS, ONEIL_INDICATORS]
#     
#     for strategy in strategies:
#         essential_indicators.update(strategy.essential)
#         non_essential_indicators.update(strategy.optional)
#         indicator_min_periods.update(strategy.min_periods)
#     
#     # 필수에 포함된 지표는 비필수에서 제거
#     non_essential_indicators = non_essential_indicators - essential_indicators
#     
#     return {
#         'ESSENTIAL_TREND_INDICATORS': essential_indicators,
#         'NON_ESSENTIAL_INDICATORS': non_essential_indicators,
#         'INDICATOR_MIN_PERIODS': indicator_min_periods
#     }

# def get_strategy_config(strategy_name: str) -> Dict[str, Any]:
#     """전략명으로 설정 조회"""
#     strategy_map = {
#         'weinstein': WEINSTEIN_CONFIG,
#         'minervini': MINERVINI_CONFIG,
#         'oneil': ONEIL_CONFIG
#     }
#     return strategy_map.get(strategy_name.lower(), {})

# def validate_strategy_config(config: Dict[str, Any]) -> bool:
#     """전략 설정 유효성 검증"""
#     required_keys = ['name', 'description', 'parameters', 'risk_management', 'indicators']
#     return all(key in config for key in required_keys)

# # 모든 전략 설정 export
# ALL_STRATEGIES = {
#     'weinstein': WEINSTEIN_CONFIG,
#     'minervini': MINERVINI_CONFIG,
#     'oneil': ONEIL_CONFIG
# }

# # 통합 지표 설정 생성 및 기존 설정 업데이트
# COMBINED_INDICATORS = get_combined_indicators()

# # 기존 설정을 통합된 지표로 업데이트
# ESSENTIAL_TREND_INDICATORS = COMBINED_INDICATORS['ESSENTIAL_TREND_INDICATORS']
# NON_ESSENTIAL_INDICATORS = COMBINED_INDICATORS['NON_ESSENTIAL_INDICATORS']
# INDICATOR_MIN_PERIODS.update(COMBINED_INDICATORS['INDICATOR_MIN_PERIODS'])

# ===== 환경별 설정 =====

# 개발/운영 환경 구분
ENVIRONMENT = os.getenv('MAKENAIDE_ENV', 'development')

# 환경별 로그 레벨
LOG_LEVELS: Dict[str, str] = {
    'development': 'DEBUG',
    'staging': 'INFO', 
    'production': 'WARNING'
}

# 환경별 최적화 설정
if ENVIRONMENT == 'production':
    # 운영 환경: 성능 우선
    API_CONFIG['API_SLEEP_TIME'] = 0.1
    BATCH_PROCESSING_CONFIG['BATCH_SIZE'] = 100
    MEMORY_LIMITS['MAX_TOTAL_MEMORY_MB'] = 4096
    PERFORMANCE_MONITORING['enable_detailed_logging'] = False
    
elif ENVIRONMENT == 'development':
    # 개발 환경: 디버깅 우선
    API_CONFIG['API_SLEEP_TIME'] = 0.3
    BATCH_PROCESSING_CONFIG['BATCH_SIZE'] = 20
    MEMORY_LIMITS['MAX_TOTAL_MEMORY_MB'] = 1024
    PERFORMANCE_MONITORING['enable_detailed_logging'] = True

# === UNUSED: 설정 검증 함수 ===
# def validate_config() -> bool:
#     """
#     설정값의 유효성을 검증합니다.
#     
#     Returns:
#         bool: 설정이 유효한지 여부
#     """
#     # DB 환경변수 확인
#     required_db_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
#     missing_vars = [var for var in required_db_vars if not os.getenv(var)]
#     
#     if missing_vars:
#         print(f"❌ 필수 DB 환경변수 누락: {missing_vars}")
#         return False
#     
#     # 지표 설정 일관성 확인
#     essential_not_in_periods = ESSENTIAL_TREND_INDICATORS - set(INDICATOR_MIN_PERIODS.keys())
#     if essential_not_in_periods:
#         print(f"⚠️ INDICATOR_MIN_PERIODS에 없는 필수 지표: {essential_not_in_periods}")
#     
#     # 메모리 제한 설정 확인
#     if MEMORY_LIMITS['MAX_SINGLE_PROCESS_MB'] > MEMORY_LIMITS['MAX_TOTAL_MEMORY_MB']:
#         print(f"⚠️ 단일 프로세스 메모리 제한이 전체 제한보다 큽니다")
#         return False
#     
#     print("✅ 모든 설정 검증 완료")
#     return True

# === UNUSED: 설정 정보 출력 함수 ===
# def print_config_summary():
#     """현재 설정 요약을 출력합니다."""
#     print("📋 Makenaide 시스템 설정 요약")
#     print("=" * 50)
#     print(f"🌍 환경: {ENVIRONMENT}")
#     print(f"🔗 DB 호스트: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
#     print(f"💾 DB 연결 풀: {DB_POOL_CONFIG['minconn']}~{DB_POOL_CONFIG['maxconn']} 연결")
#     print(f"📊 필수 지표 수: {len(ESSENTIAL_TREND_INDICATORS)}개")
#     print(f"🧠 최대 메모리: {MEMORY_LIMITS['MAX_TOTAL_MEMORY_MB']}MB")
#     print(f"⚡ 배치 크기: {BATCH_PROCESSING_CONFIG['BATCH_SIZE']}")
#     print(f"🔄 API 대기시간: {API_CONFIG['API_SLEEP_TIME']}초")
#     print("=" * 50)

# if __name__ == "__main__":
#     # 설정 검증 및 요약 출력
#     if validate_config():
#         print_config_summary()
#     else:
#         print("❌ 설정 검증 실패") 