#!/usr/bin/env python3
"""
백테스팅 모듈 패키지

리팩토링된 백테스팅 시스템의 메인 패키지입니다.
기존 backtester.py의 거대한 파일을 작은 모듈들로 분할했습니다.

Modules:
    - data_manager: 백테스트 데이터 관리
    - backtest_types: 데이터 클래스 및 타입 정의
    - kelly_backtester: 켈리 공식 백테스터
    - strategy_registry: 전략 등록 및 관리
    - performance_analyzer: 성과 분석
    - integrated_backtester: 통합 백테스터 (메인)

Author: Backtesting Refactoring
Version: 1.0.0
"""

# 메인 클래스들 import
from .data_manager import BacktestDataManager
from .backtest_types import (
    BacktestResult, 
    KellyBacktestResult, 
    StrategyConfig, 
    BacktestSummary,
    create_default_strategy_configs,
    validate_backtest_result
)
from .kelly_backtester import KellyBacktester
from .strategy_registry import StrategyRegistry
from .performance_analyzer import PerformanceAnalyzer
from .integrated_backtester import (
    IntegratedBacktester, 
    create_integrated_backtester,
    create_timezone_enhanced_backtester,
    create_standard_backtester,
    run_quick_backtest
)
from .timezone_backtester import TimezoneBacktester, create_timezone_backtester

# 버전 정보
__version__ = "1.0.0"
__author__ = "Backtesting Refactoring Team"

# 패키지 레벨 편의 함수들
def get_version():
    """패키지 버전 반환"""
    return __version__

def list_available_modules():
    """사용 가능한 모듈 목록"""
    return [
        "data_manager",
        "backtest_types", 
        "kelly_backtester",
        "strategy_registry",
        "performance_analyzer",
        "integrated_backtester",
        "timezone_backtester"
    ]

def create_backtester(timezone_analysis: bool = True):
    """통합 백테스터 인스턴스 생성 (편의 함수)"""
    return create_integrated_backtester(timezone_analysis)

def create_timezone_backtester():
    """시간대 분석 백테스터 인스턴스 생성 (편의 함수)"""
    return create_timezone_enhanced_backtester()

# 주요 클래스들을 __all__에 명시
__all__ = [
    # 메인 클래스들
    'BacktestDataManager',
    'BacktestResult',
    'KellyBacktestResult', 
    'StrategyConfig',
    'BacktestSummary',
    'KellyBacktester',
    'StrategyRegistry',
    'PerformanceAnalyzer',
    'IntegratedBacktester',
    'TimezoneBacktester',
    
    # 편의 함수들
    'create_integrated_backtester',
    'create_timezone_enhanced_backtester',
    'create_standard_backtester',
    'create_timezone_backtester',
    'run_quick_backtest',
    'create_default_strategy_configs',
    'validate_backtest_result',
    'create_backtester',
    'create_timezone_backtester',
    'get_version',
    'list_available_modules'
]

# 패키지 로딩 시 초기화 작업
import logging

logger = logging.getLogger(__name__)
logger.info(f"✅ 백테스팅 모듈 패키지 로드 완료 (v{__version__})")
logger.info(f"📦 사용 가능한 모듈: {', '.join(list_available_modules())}")

# 호환성을 위한 별칭들 (기존 코드와의 호환성 유지)
MakenaideBacktestManager = IntegratedBacktester  # 기존 이름과 호환
ComprehensiveBacktestEngine = IntegratedBacktester  # 기존 이름과 호환