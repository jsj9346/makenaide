"""
Config 패키지

시스템 설정 관련 모듈들을 포함합니다.
통합된 구조로 간소화되었습니다.
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 통합된 config.py에서 모든 설정 import
try:
    # 루트 config.py에서 모든 설정 import
    import sys
    import importlib.util
    
    # 루트 config.py 경로 계산
    root_config_path = project_root / 'config.py'
    
    if root_config_path.exists():
        spec = importlib.util.spec_from_file_location("root_config_module", root_config_path)
        root_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(root_config)
        
        # 기본 설정값들
        MEMORY_LIMITS = getattr(root_config, 'MEMORY_LIMITS', {})
        QUALITY_ALERT_THRESHOLDS = getattr(root_config, 'QUALITY_ALERT_THRESHOLDS', {})
        PERFORMANCE_MONITORING = getattr(root_config, 'PERFORMANCE_MONITORING', {})
        DB_CONFIG = getattr(root_config, 'DB_CONFIG', {})
        DB_POOL_CONFIG = getattr(root_config, 'DB_POOL_CONFIG', {})
        BATCH_PROCESSING_CONFIG = getattr(root_config, 'BATCH_PROCESSING_CONFIG', {})
        API_CONFIG = getattr(root_config, 'API_CONFIG', {})
        ENVIRONMENT = getattr(root_config, 'ENVIRONMENT', 'development')
        LOG_LEVELS = getattr(root_config, 'LOG_LEVELS', {})
        
        # 지표 설정 (통합됨)
        ESSENTIAL_TREND_INDICATORS = getattr(root_config, 'ESSENTIAL_TREND_INDICATORS', set())
        NON_ESSENTIAL_INDICATORS = getattr(root_config, 'NON_ESSENTIAL_INDICATORS', set())
        INDICATOR_MIN_PERIODS = getattr(root_config, 'INDICATOR_MIN_PERIODS', {})
        
        # 전략 설정 (통합됨)
        WEINSTEIN_CONFIG = getattr(root_config, 'WEINSTEIN_CONFIG', {})
        MINERVINI_CONFIG = getattr(root_config, 'MINERVINI_CONFIG', {})
        ONEIL_CONFIG = getattr(root_config, 'ONEIL_CONFIG', {})
        ALL_STRATEGIES = getattr(root_config, 'ALL_STRATEGIES', {})
        
        # 지표 클래스 및 객체들
        StrategyIndicators = getattr(root_config, 'StrategyIndicators', None)
        WEINSTEIN_INDICATORS = getattr(root_config, 'WEINSTEIN_INDICATORS', None)
        MINERVINI_INDICATORS = getattr(root_config, 'MINERVINI_INDICATORS', None)
        ONEIL_INDICATORS = getattr(root_config, 'ONEIL_INDICATORS', None)
        COMBINED_INDICATORS = getattr(root_config, 'COMBINED_INDICATORS', {})
        
        # 피라미딩 설정
        PYRAMIDING_CONFIG = getattr(root_config, 'PYRAMIDING_CONFIG', {})
        
        # 트레일링스탑 설정
        TRAILING_STOP_CONFIG = getattr(root_config, 'TRAILING_STOP_CONFIG', {})
        
        # GPT 필터링 설정
        GPT_FILTERING_CONFIG = getattr(root_config, 'GPT_FILTERING_CONFIG', {})
        
        # 로그 관리 설정
        LOG_MANAGEMENT = getattr(root_config, 'LOG_MANAGEMENT', {})
        
        # 함수들
        validate_config = getattr(root_config, 'validate_config', lambda: True)
        print_config_summary = getattr(root_config, 'print_config_summary', lambda: None)
        get_strategy_config = getattr(root_config, 'get_strategy_config', lambda x: {})
        validate_strategy_config = getattr(root_config, 'validate_strategy_config', lambda x: True)
        get_combined_indicators = getattr(root_config, 'get_combined_indicators', lambda: {})
        
    else:
        raise ImportError(f"루트 config.py 파일을 찾을 수 없습니다: {root_config_path}")
    
    # 로컬 모듈들
    from .mode_presets import MODE_PRESETS
    
    # 성공적으로 로드된 경우
    _CONFIG_LOADED = True
    
except ImportError as e:
    # Import 실패 시 안전한 fallback 제공
    print(f"⚠️ Config 모듈 로딩 중 오류 발생: {e}")
    _CONFIG_LOADED = False
    
    # 기본값 설정
    MODE_PRESETS = {}
    MEMORY_LIMITS = {'MAX_TOTAL_MEMORY_MB': 1024}
    QUALITY_ALERT_THRESHOLDS = {}
    PERFORMANCE_MONITORING = {'enable_detailed_logging': False}
    ESSENTIAL_TREND_INDICATORS = set()
    NON_ESSENTIAL_INDICATORS = set()
    INDICATOR_MIN_PERIODS = {}
    DB_CONFIG = {}
    DB_POOL_CONFIG = {}
    BATCH_PROCESSING_CONFIG = {}
    API_CONFIG = {}
    WEINSTEIN_CONFIG = {}
    MINERVINI_CONFIG = {}
    ONEIL_CONFIG = {}
    ALL_STRATEGIES = {}
    COMBINED_INDICATORS = {}
    ENVIRONMENT = 'development'
    LOG_LEVELS = {}
    
    # 피라미딩 설정
    PYRAMIDING_CONFIG = {}
    
    # 트레일링스탑 설정
    TRAILING_STOP_CONFIG = {}
    
    # GPT 필터링 설정
    GPT_FILTERING_CONFIG = {}
    
    # 로그 관리 설정
    LOG_MANAGEMENT = {}
    
    # 클래스 및 객체들
    StrategyIndicators = None
    WEINSTEIN_INDICATORS = None
    MINERVINI_INDICATORS = None
    ONEIL_INDICATORS = None
    
    def validate_config():
        return False
    
    def print_config_summary():
        print("⚠️ Config 로딩 실패 - 기본 설정 사용 중")
    
    def get_strategy_config(strategy_name: str):
        return {}
    
    def validate_strategy_config(config):
        return False
    
    def get_combined_indicators():
        return {}

# 런타임 검증 함수
def validate_imports() -> bool:
    """Import 성공 여부 검증"""
    return _CONFIG_LOADED

def get_config_status() -> dict:
    """Config 상태 정보 반환"""
    return {
        'loaded': _CONFIG_LOADED,
        'project_root': str(project_root),
        'sys_path_updated': str(project_root) in sys.path,
        'available_modules': [name for name in globals() if not name.startswith('_')],
        'strategies_count': len(ALL_STRATEGIES),
        'essential_indicators_count': len(ESSENTIAL_TREND_INDICATORS),
        'config_unified': True  # 통합 버전임을 표시
    }

__all__ = [
    # 기본 설정
    'MODE_PRESETS',
    'MEMORY_LIMITS',
    'QUALITY_ALERT_THRESHOLDS', 
    'PERFORMANCE_MONITORING',
    'DB_CONFIG',
    'DB_POOL_CONFIG',
    'BATCH_PROCESSING_CONFIG',
    'API_CONFIG',
    'ENVIRONMENT',
    'LOG_LEVELS',
    
    # 지표 설정 (통합됨)
    'ESSENTIAL_TREND_INDICATORS',
    'NON_ESSENTIAL_INDICATORS',
    'INDICATOR_MIN_PERIODS',
    'COMBINED_INDICATORS',
    
    # 전략 설정 (통합됨)
    'WEINSTEIN_CONFIG',
    'MINERVINI_CONFIG',
    'ONEIL_CONFIG',
    'ALL_STRATEGIES',
    'StrategyIndicators',
    'WEINSTEIN_INDICATORS',
    'MINERVINI_INDICATORS',
    'ONEIL_INDICATORS',
    
    # 피라미딩 설정
    'PYRAMIDING_CONFIG',
    
    # 트레일링스탑 설정
    'TRAILING_STOP_CONFIG',
    
    # GPT 필터링 설정
    'GPT_FILTERING_CONFIG',
    
    # 로그 관리 설정
    'LOG_MANAGEMENT',
    
    # 함수들
    'validate_config',
    'print_config_summary',
    'get_strategy_config',
    'validate_strategy_config',
    'get_combined_indicators',
    'validate_imports',
    'get_config_status'
] 