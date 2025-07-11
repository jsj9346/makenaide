import psycopg2  # 일부 함수에서 직접 사용
import json
from openai import OpenAI, OpenAIError
import os
import logging
import logging.handlers
import re
import gzip
import shutil
from dotenv import load_dotenv
load_dotenv()
import base64
import time
from openai import RateLimitError
import tiktoken
import textwrap
import hashlib
import zlib
import threading
from functools import wraps
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from datetime import datetime, timedelta
import numpy as np
from config_loader import load_config
from utils import validate_and_correct_phase
from db_manager import DBManager
from strategy_analyzer import detect_vcp_pattern, analyze_weinstein_stage, check_breakout_conditions, optimized_integrated_analysis
from data_fetcher import generate_gpt_analysis_json

# === 견고한 예외 처리 시스템 ===

class AnalysisException(Exception):
    """분석 관련 커스텀 예외"""
    def __init__(self, message: str, error_code: str = "ANALYSIS_ERROR", ticker: str = "", recovery_suggestion: str = ""):
        self.message = message
        self.error_code = error_code
        self.ticker = ticker
        self.recovery_suggestion = recovery_suggestion
        super().__init__(self.message)

class RobustAnalysisCircuitBreaker:
    """분석 시스템 회로 차단기"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """회로 차단기를 통한 함수 호출"""
        with self._lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    logging.info("🔄 회로 차단기 상태: HALF_OPEN")
                else:
                    raise AnalysisException(
                        "회로 차단기가 열려있습니다. 시스템 복구 대기 중",
                        "CIRCUIT_BREAKER_OPEN",
                        recovery_suggestion="잠시 후 다시 시도하거나 대체 분석 방법을 사용하세요"
                    )
            
            try:
                result = func(*args, **kwargs)
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                    logging.info("✅ 회로 차단기 상태: CLOSED (복구 완료)")
                return result
                
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
                    logging.error(f"🚨 회로 차단기 열림: {self.failure_count}회 연속 실패")
                
                raise e

# 전역 회로 차단기 인스턴스
analysis_circuit_breaker = RobustAnalysisCircuitBreaker()

# UNUSED: 호출되지 않는 함수
# def robust_analysis_pipeline(ticker: str, daily_data: dict):
#     """
#     fail-safe 분석 파이프라인 구현
#     
#     요구사항:
#     - 각 분석 단계별 독립적 에러 처리
#     - 부분 실패 시 가용한 결과로 분석 계속
#     - 에러 발생 시 의미있는 fallback 값 제공
#     - 상세한 에러 로깅과 복구 제안
#     """
#     analysis_results = {
#         "ticker": ticker,
#         "analysis_timestamp": datetime.now().isoformat(),
#         "pipeline_status": "RUNNING",
#         "stages_completed": [],
#         "stages_failed": [],
#         "fallback_used": [],
#         "final_recommendation": None,
#         "error_summary": [],
#         "recovery_suggestions": []
#     }
#     
#     try:
#         # 1단계: 데이터 검증 및 정규화
#         try:
#             validated_data = _validate_and_normalize_data(ticker, daily_data)
#             analysis_results["stages_completed"].append("data_validation")
#             logging.info(f"✅ {ticker} - 데이터 검증 완료")
#         except Exception as e:
#             error_detail = _handle_pipeline_error("data_validation", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             validated_data = _get_fallback_data(daily_data)
#             analysis_results["fallback_used"].append("minimal_data_fallback")
#             logging.warning(f"⚠️ {ticker} - 데이터 검증 실패, 최소 데이터로 진행")
#         
#         # 2단계: 통합 기술적 분석 (회로차단기 적용)
#         try:
#             technical_analysis = analysis_circuit_breaker.call(
#                 optimized_integrated_analysis, validated_data
#             )
#             analysis_results["stages_completed"].append("technical_analysis")
#             logging.info(f"✅ {ticker} - 통합 기술적 분석 완료")
#         except Exception as e:
#             error_detail = _handle_pipeline_error("technical_analysis", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             technical_analysis = _get_fallback_technical_analysis(validated_data)
#             analysis_results["fallback_used"].append("basic_technical_fallback")
#             logging.warning(f"⚠️ {ticker} - 기술적 분석 실패, 기본 분석으로 진행")
#         
#         # 3단계: VCP 패턴 분석
#         try:
#             vcp_analysis = _safe_vcp_analysis(validated_data, ticker)
#             analysis_results["stages_completed"].append("vcp_analysis")
#         except Exception as e:
#             error_detail = _handle_pipeline_error("vcp_analysis", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             vcp_analysis = _get_fallback_vcp_analysis()
#             analysis_results["fallback_used"].append("default_vcp_fallback")
#         
#         # 4단계: Weinstein Stage 분석
#         try:
#             stage_analysis = _safe_stage_analysis(validated_data, ticker)
#             analysis_results["stages_completed"].append("stage_analysis")
#         except Exception as e:
#             error_detail = _handle_pipeline_error("stage_analysis", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             stage_analysis = _get_fallback_stage_analysis()
#             analysis_results["fallback_used"].append("default_stage_fallback")
#         
#         # 5단계: 브레이크아웃 조건 확인
#         try:
#             breakout_analysis = _safe_breakout_analysis(validated_data, vcp_analysis, stage_analysis, ticker)
#             analysis_results["stages_completed"].append("breakout_analysis")
#         except Exception as e:
#             error_detail = _handle_pipeline_error("breakout_analysis", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             breakout_analysis = _get_fallback_breakout_analysis()
#             analysis_results["fallback_used"].append("default_breakout_fallback")
#         
#         # 6단계: 최종 통합 및 의사결정
#         try:
#             final_decision = _safe_final_decision(
#                 technical_analysis, vcp_analysis, stage_analysis, breakout_analysis, ticker
#             )
#             analysis_results["stages_completed"].append("final_decision")
#             analysis_results["final_recommendation"] = final_decision
#         except Exception as e:
#             error_detail = _handle_pipeline_error("final_decision", e, ticker)
#             analysis_results["stages_failed"].append(error_detail)
#             final_decision = _get_fallback_final_decision()
#             analysis_results["final_recommendation"] = final_decision
#             analysis_results["fallback_used"].append("conservative_decision_fallback")
#         
#         # 파이프라인 상태 업데이트
#         if len(analysis_results["stages_failed"]) == 0:
#             analysis_results["pipeline_status"] = "SUCCESS"
#         elif len(analysis_results["stages_completed"]) >= 3:
#             analysis_results["pipeline_status"] = "PARTIAL_SUCCESS"
#         else:
#             analysis_results["pipeline_status"] = "FAILED"
#         
#         # 복구 제안 생성
#         analysis_results["recovery_suggestions"] = _generate_recovery_suggestions(
#             analysis_results["stages_failed"], analysis_results["fallback_used"]
#         )
#         
#         logging.info(f"🎯 {ticker} - 분석 파이프라인 완료: {analysis_results['pipeline_status']}")
#         
#         return {
#             "analysis_results": analysis_results,
#             "technical_analysis": technical_analysis,
#             "vcp_analysis": vcp_analysis,
#             "stage_analysis": stage_analysis,
#             "breakout_analysis": breakout_analysis,
#             "final_decision": final_decision
#         }
#         
#     except Exception as e:
#         # 최종 안전망
#         logging.error(f"❌ {ticker} - 분석 파이프라인 전체 실패: {str(e)}")
#         analysis_results["pipeline_status"] = "CRITICAL_FAILURE"
#         analysis_results["error_summary"].append({
#             "stage": "pipeline_level",
#             "error": str(e),
#             "timestamp": datetime.now().isoformat()
#         })
#         
#         return {
#             "analysis_results": analysis_results,
#             "technical_analysis": _get_fallback_technical_analysis({}),
#             "vcp_analysis": _get_fallback_vcp_analysis(),
#             "stage_analysis": _get_fallback_stage_analysis(),
#             "breakout_analysis": _get_fallback_breakout_analysis(),
#             "final_decision": _get_fallback_final_decision()
#         }

def _validate_and_normalize_data(ticker: str, daily_data: dict) -> dict:
    """데이터 검증 및 정규화"""
    if not daily_data:
        raise AnalysisException(
            "일일 데이터가 없습니다",
            "NO_DATA",
            ticker,
            "데이터 소스를 확인하고 다시 시도하세요"
        )
    
    required_fields = ['close', 'high', 'low', 'volume']
    missing_fields = [field for field in required_fields if field not in daily_data]
    
    if missing_fields:
        raise AnalysisException(
            f"필수 데이터 필드 누락: {missing_fields}",
            "MISSING_FIELDS",
            ticker,
            f"누락된 필드를 추가하거나 대체 데이터 소스를 사용하세요"
        )
    
    # 데이터 길이 검증
    min_length = 20
    for field in required_fields:
        if len(daily_data[field]) < min_length:
            raise AnalysisException(
                f"{field} 데이터가 부족합니다 (최소 {min_length}일 필요)",
                "INSUFFICIENT_DATA",
                ticker,
                f"더 많은 과거 데이터를 확보하세요"
            )
    
    # 데이터 무결성 검증
    for field in required_fields:
        if any(val <= 0 for val in daily_data[field] if val is not None):
            logging.warning(f"⚠️ {ticker} - {field}에 비정상적인 값 발견")
    
    return daily_data

def _handle_pipeline_error(stage: str, error: Exception, ticker: str) -> dict:
    """파이프라인 에러 처리"""
    error_detail = {
        "stage": stage,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "ticker": ticker,
        "timestamp": datetime.now().isoformat(),
        "traceback": traceback.format_exc()[-500:]  # 마지막 500자만
    }
    
    # 에러 분류 및 복구 제안
    if isinstance(error, AnalysisException):
        error_detail["recovery_suggestion"] = error.recovery_suggestion
        error_detail["error_code"] = error.error_code
    elif "connection" in str(error).lower():
        error_detail["recovery_suggestion"] = "네트워크 연결을 확인하고 다시 시도하세요"
        error_detail["error_code"] = "CONNECTION_ERROR"
    elif "timeout" in str(error).lower():
        error_detail["recovery_suggestion"] = "시스템 부하가 높습니다. 잠시 후 다시 시도하세요"
        error_detail["error_code"] = "TIMEOUT_ERROR"
    else:
        error_detail["recovery_suggestion"] = "시스템 관리자에게 문의하세요"
        error_detail["error_code"] = "UNKNOWN_ERROR"
    
    logging.error(f"❌ {ticker} - {stage} 단계 실패: {error_detail['error_message']}")
    return error_detail

def _safe_vcp_analysis(data: dict, ticker: str) -> dict:
    """안전한 VCP 분석"""
    try:
        return detect_vcp_pattern(data)
    except Exception as e:
        logging.warning(f"⚠️ {ticker} - VCP 분석 중 오류: {str(e)}")
        return _get_fallback_vcp_analysis()

def _safe_stage_analysis(data: dict, ticker: str) -> dict:
    """안전한 Stage 분석"""
    try:
        return analyze_weinstein_stage(data)
    except Exception as e:
        logging.warning(f"⚠️ {ticker} - Stage 분석 중 오류: {str(e)}")
        return _get_fallback_stage_analysis()

def _safe_breakout_analysis(data: dict, vcp_analysis: dict, stage_analysis: dict, ticker: str) -> dict:
    """안전한 브레이크아웃 분석"""
    try:
        return check_breakout_conditions(data, vcp_analysis, stage_analysis)
    except Exception as e:
        logging.warning(f"⚠️ {ticker} - 브레이크아웃 분석 중 오류: {str(e)}")
        return _get_fallback_breakout_analysis()

def _safe_final_decision(technical_analysis: dict, vcp_analysis: dict, stage_analysis: dict, 
                        breakout_analysis: dict, ticker: str) -> dict:
    """안전한 최종 의사결정"""
    try:
        # 통합 분석 결과에서 최종 결정 추출
        if "final_decision" in technical_analysis:
            return technical_analysis["final_decision"]
        
        # 개별 분석 결과 기반 결정
        vcp_score = vcp_analysis.get("score", 30)
        stage_favorable = stage_analysis.get("current_stage") == "2"
        breakout_ready = breakout_analysis.get("breakout_ready", False)
        
        if vcp_score > 70 and stage_favorable and breakout_ready:
            action = "BUY"
            confidence = 0.8
        elif vcp_score > 50 and (stage_favorable or breakout_ready):
            action = "BUY"
            confidence = 0.6
        elif vcp_score > 30:
            action = "HOLD"
            confidence = 0.4
        else:
            action = "SELL"
            confidence = 0.3
        
        return {
            "action": action,
            "confidence": confidence,
            "reasoning": f"VCP: {vcp_score}, Stage: {stage_analysis.get('current_stage')}, Breakout: {breakout_ready}"
        }
        
    except Exception as e:
        logging.warning(f"⚠️ {ticker} - 최종 의사결정 중 오류: {str(e)}")
        return _get_fallback_final_decision()

# === 고도화된 피라미딩 시스템 ===

# UNUSED: 호출되지 않는 함수
# def advanced_scaling_in_system(initial_entry: dict, market_conditions: dict):
#     """
#     Makenaide 분할 매수 전략 완전 구현
#     
#     기능:
#     - 초기 진입 후 추세 강화 시점 감지
#     - VCP 이후 추가 브레이크아웃 레벨 계산
#     - 거래량 확인과 모멘텀 지속성 검증
#     - 최대 3-4회 추가 진입 조건 설정
#     - 각 추가 진입 시 포지션 크기 동적 조정
#     
#     리스크 제어:
#     - 전체 포지션이 최대 한도 초과 시 중단
#     - 추세 약화 신호 감지 시 조기 청산
#     - 각 단계별 독립적 손절선 설정
#     """
#     try:
#         ticker = initial_entry.get("ticker", "")
#         entry_price = initial_entry.get("price", 0)
#         entry_date = initial_entry.get("date", datetime.now())
#         initial_size = initial_entry.get("position_size_pct", 2.0)
#         
#         # 피라미딩 설정
#         pyramid_config = {
#             "max_total_position": 8.0,  # 최대 총 포지션 8%
#             "max_scaling_levels": 3,    # 최대 3회 추가 매수
#             "min_advance_threshold": 0.02,  # 최소 2% 상승 후 추가 매수
#             "volume_surge_threshold": 1.5,  # 거래량 1.5배 이상 증가
#             "trend_strength_threshold": 0.7,  # 추세 강도 0.7 이상
#             "max_time_between_entries": 14  # 최대 14일 간격
#         }
#         
#         # 현재 시장 상황 분석
#         current_analysis = _analyze_current_market_conditions(market_conditions, ticker)
#         
#         # 피라미딩 조건 확인
#         pyramid_conditions = _check_pyramid_conditions(
#             initial_entry, current_analysis, pyramid_config
#         )
#         
#         if not pyramid_conditions["enabled"]:
#             return {
#                 "pyramid_enabled": False,
#                 "reason": pyramid_conditions["reason"],
#                 "current_position": {"total_size": initial_size, "levels": 1},
#                 "next_actions": ["모니터링 계속"]
#             }
#         
#         # 추가 진입 레벨 계산
#         scaling_levels = _calculate_scaling_levels(
#             entry_price, current_analysis, pyramid_config
#         )
#         
#         # 동적 포지션 사이징
#         position_sizes = _calculate_dynamic_position_sizes(
#             initial_size, scaling_levels, current_analysis, pyramid_config
#         )
#         
#         # 리스크 관리 레벨 설정
#         risk_management = _setup_pyramid_risk_management(
#             entry_price, scaling_levels, position_sizes
#         )
#         
#         # 실행 계획 생성
#         execution_plan = _generate_execution_plan(
#             scaling_levels, position_sizes, risk_management, current_analysis
#         )
#         
#         return {
#             "pyramid_enabled": True,
#             "initial_entry": initial_entry,
#             "scaling_levels": scaling_levels,
#             "position_sizes": position_sizes,
#             "risk_management": risk_management,
#             "execution_plan": execution_plan,
#             "monitoring_alerts": _setup_monitoring_alerts(scaling_levels, risk_management),
#             "exit_conditions": _define_exit_conditions(current_analysis, risk_management)
#         }
#         
#     except Exception as e:
#         logging.error(f"❌ 피라미딩 시스템 오류: {str(e)}")
#         return _get_fallback_pyramid_result(initial_entry)

# UNUSED: 피라미딩 시스템 내부 함수들 (advanced_scaling_in_system에서만 사용)
# def _analyze_current_market_conditions(market_conditions: dict, ticker: str) -> dict:
#     """현재 시장 상황 분석"""
#     try:
#         # 기본 시장 지표들
#         market_trend = market_conditions.get("market_trend", "neutral")
#         volatility = market_conditions.get("volatility", 0.2)
#         sector_performance = market_conditions.get("sector_performance", 0.0)
#         
#         # 종목별 모멘텀 분석
#         price_momentum = _calculate_price_momentum(market_conditions.get("price_data", []))
#         volume_momentum = _calculate_volume_momentum(market_conditions.get("volume_data", []))
#         
#         # 상대 강도 계산
#         relative_strength = _calculate_relative_strength_vs_market(
#             market_conditions.get("price_data", []),
#             market_conditions.get("market_index_data", [])
#         )
#         
#         # 추세 강도 점수
#         trend_strength = _calculate_trend_strength(
#             price_momentum, volume_momentum, relative_strength
#         )
#         
#         return {
#             "market_trend": market_trend,
#             "volatility": volatility,
#             "sector_performance": sector_performance,
#             "price_momentum": price_momentum,
#             "volume_momentum": volume_momentum,
#             "relative_strength": relative_strength,
#             "trend_strength": trend_strength,
#             "favorable_for_pyramid": trend_strength > 0.7 and volatility < 0.3
#         }
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 시장 상황 분석 오류: {str(e)}")
#         return {
#             "market_trend": "neutral",
#             "trend_strength": 0.5,
#             "favorable_for_pyramid": False
#         }

# def _check_pyramid_conditions(initial_entry: dict, current_analysis: dict, config: dict) -> dict:
#     """피라미딩 조건 확인"""
#     try:
#         entry_price = initial_entry.get("price", 0)
#         current_price = current_analysis.get("current_price", entry_price)
#         
#         # 기본 조건들
#         conditions = {
#             "price_advance": (current_price - entry_price) / entry_price >= config["min_advance_threshold"],
#             "trend_strength": current_analysis.get("trend_strength", 0) >= config["trend_strength_threshold"],
#             "market_favorable": current_analysis.get("favorable_for_pyramid", False),
#             "volume_confirmation": current_analysis.get("volume_momentum", 0) > 0,
#             "time_constraint": True  # 시간 제약 체크 (실제로는 날짜 계산)
#         }
#         
#         # 모든 조건 만족 확인
#         all_conditions_met = all(conditions.values())
#         
#         if not all_conditions_met:
#             failed_conditions = [k for k, v in conditions.items() if not v]
#             reason = f"피라미딩 조건 미충족: {', '.join(failed_conditions)}"
#         else:
#             reason = "모든 피라미딩 조건 만족"
#         
#         return {
#             "enabled": all_conditions_met,
#             "reason": reason,
#             "conditions": conditions,
#             "confidence": sum(conditions.values()) / len(conditions)
#         }
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 피라미딩 조건 확인 오류: {str(e)}")
#         return {"enabled": False, "reason": "조건 확인 실패", "confidence": 0}

# def _calculate_scaling_levels(entry_price: float, current_analysis: dict, config: dict) -> list:
#     """추가 진입 레벨 계산"""
#     try:
#         scaling_levels = []
#         trend_strength = current_analysis.get("trend_strength", 0.5)
#         
#         # 추세 강도에 따른 레벨 간격 조정
#         if trend_strength > 0.8:
#             # 강한 추세: 더 적극적인 레벨
#             level_intervals = [0.03, 0.05, 0.08]  # 3%, 5%, 8% 상승 지점
#         elif trend_strength > 0.6:
#             # 중간 추세: 보통 레벨
#             level_intervals = [0.04, 0.07, 0.12]  # 4%, 7%, 12% 상승 지점
#         else:
#             # 약한 추세: 보수적 레벨
#             level_intervals = [0.05, 0.10, 0.15]  # 5%, 10%, 15% 상승 지점
#         
#         for i, interval in enumerate(level_intervals[:config["max_scaling_levels"]]):
#             level_price = entry_price * (1 + interval)
#             
#             scaling_levels.append({
#                 "level": i + 1,
#                 "price": round(level_price, 2),
#                 "advance_pct": round(interval * 100, 1),
#                 "trigger_conditions": _define_trigger_conditions(i + 1, current_analysis),
#                 "size_allocation": _calculate_level_size_allocation(i + 1, trend_strength)
#             })
#         
#         return scaling_levels
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 스케일링 레벨 계산 오류: {str(e)}")
#         return []

# def _define_trigger_conditions(level: int, analysis: dict) -> dict:
#     """각 레벨별 트리거 조건 정의"""
#     base_conditions = {
#         "price_breakout": True,  # 가격 돌파 필수
#         "volume_surge": level <= 2,  # 1-2레벨은 거래량 급증 필요
#         "momentum_continuation": True,  # 모멘텀 지속 필수
#         "market_support": level >= 3  # 3레벨부터는 시장 지지 필요
#     }
#     
#     # 레벨별 추가 조건
#     if level == 1:
#         base_conditions["min_volume_ratio"] = 1.3
#         base_conditions["min_rsi"] = 55
#     elif level == 2:
#         base_conditions["min_volume_ratio"] = 1.2
#         base_conditions["min_rsi"] = 60
#     elif level == 3:
#         base_conditions["min_volume_ratio"] = 1.1
#         base_conditions["min_rsi"] = 65
#         base_conditions["sector_outperformance"] = True
#     
#     return base_conditions

# UNUSED: 피라미딩 시스템 내부 함수들 (advanced_scaling_in_system에서만 사용)
# def _calculate_dynamic_position_sizes(initial_size: float, scaling_levels: list, 
#                                     analysis: dict, config: dict) -> dict:
#     """동적 포지션 사이징 계산"""
#     try:
#         trend_strength = analysis.get("trend_strength", 0.5)
#         volatility = analysis.get("volatility", 0.2)
#         
#         # 기본 사이즈 배분 (초기 포지션 기준)
#         base_allocations = {
#             1: 0.6,  # 첫 번째 추가: 초기의 60%
#             2: 0.4,  # 두 번째 추가: 초기의 40%
#             3: 0.3   # 세 번째 추가: 초기의 30%
#         }
#         
#         position_sizes = {"initial": initial_size, "levels": {}}
#         remaining_capacity = config["max_total_position"] - initial_size
#         
#         for level_info in scaling_levels:
#             level = level_info["level"]
#             base_allocation = base_allocations.get(level, 0.2)
#             
#             # 추세 강도에 따른 조정
#             trend_multiplier = 0.8 + (trend_strength * 0.4)  # 0.8 ~ 1.2
#             
#             # 변동성에 따른 조정
#             volatility_multiplier = max(0.6, 1.2 - volatility)  # 변동성 높을수록 감소
#             
#             # 최종 사이즈 계산
#             adjusted_size = (initial_size * base_allocation * 
#                            trend_multiplier * volatility_multiplier)
#             
#             # 잔여 한도 확인
#             if sum(position_sizes["levels"].values()) + adjusted_size <= remaining_capacity:
#                 position_sizes["levels"][level] = round(adjusted_size, 2)
#             else:
#                 # 잔여 한도 내에서 최대치 할당
#                 remaining = remaining_capacity - sum(position_sizes["levels"].values())
#                 if remaining > 0.5:  # 최소 0.5% 이상일 때만 할당
#                     position_sizes["levels"][level] = round(remaining, 2)
#                 break
#         
#         # 총 포지션 크기 계산
#         total_size = initial_size + sum(position_sizes["levels"].values())
#         position_sizes["total"] = round(total_size, 2)
#         position_sizes["utilization_pct"] = round(total_size / config["max_total_position"] * 100, 1)
#         
#         return position_sizes
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 동적 포지션 사이징 오류: {str(e)}")
#         return {"initial": initial_size, "levels": {}, "total": initial_size}

# UNUSED: 피라미딩 시스템 내부 함수들 (advanced_scaling_in_system에서만 사용)
# def _setup_pyramid_risk_management(entry_price: float, scaling_levels: list, position_sizes: dict) -> dict:
#     """피라미딩 리스크 관리 설정"""
#     try:
#         risk_management = {
#             "stop_loss_levels": {},
#             "take_profit_levels": {},
#             "trailing_stops": {},
#             "exit_signals": {},
#             "max_loss_limit": position_sizes.get("total", 2.0) * 0.02  # 총 포지션의 2%
#         }
#         
#         # 각 레벨별 손절선 설정
#         for level_info in scaling_levels:
#             level = level_info["level"]
#             level_price = level_info["price"]
#             
#             # 독립적 손절선 (각 레벨 진입가의 3% 하락)
#             stop_loss = level_price * 0.97
#             
#             # 트레일링 스톱 (ATR 기반)
#             trailing_stop_distance = level_price * 0.04  # 4% 트레일링
#             
#             # 목표 수익 레벨
#             take_profit_1 = level_price * 1.06  # 6% 수익
#             take_profit_2 = level_price * 1.12  # 12% 수익
#             
#             risk_management["stop_loss_levels"][level] = round(stop_loss, 2)
#             risk_management["trailing_stops"][level] = round(trailing_stop_distance, 2)
#             risk_management["take_profit_levels"][level] = [
#                 round(take_profit_1, 2),
#                 round(take_profit_2, 2)
#             ]
#         
#         # 전체 포지션 보호 레벨
#         avg_entry_price = _calculate_weighted_average_entry(entry_price, scaling_levels, position_sizes)
#         risk_management["portfolio_stop_loss"] = round(avg_entry_price * 0.92, 2)  # 8% 손절
#         risk_management["emergency_exit_level"] = round(avg_entry_price * 0.88, 2)  # 12% 긴급 청산
#         
#         return risk_management
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 리스크 관리 설정 오류: {str(e)}")
#         return {"stop_loss_levels": {}, "max_loss_limit": 2.0}

# def _calculate_weighted_average_entry(initial_price: float, levels: list, sizes: dict) -> float:
#     """가중평균 진입가 계산"""
#     try:
#         total_investment = initial_price * sizes.get("initial", 2.0)
#         total_shares = sizes.get("initial", 2.0)
#         
#         for level_info in levels:
#             level = level_info["level"]
#             if level in sizes.get("levels", {}):
#                 level_price = level_info["price"]
#                 level_size = sizes["levels"][level]
#                 
#                 total_investment += level_price * level_size
#                 total_shares += level_size
#         
#         return total_investment / total_shares if total_shares > 0 else initial_price
#         
#     except Exception:
#         return initial_price

# === 헬퍼 함수들 ===

def _get_fallback_data(original_data: dict) -> dict:
    """최소 데이터 fallback"""
    return {
        "close": original_data.get("close", [100] * 20),
        "high": original_data.get("high", [105] * 20),
        "low": original_data.get("low", [95] * 20),
        "volume": original_data.get("volume", [1000] * 20)
    }

def _get_fallback_technical_analysis(data: dict) -> dict:
    """기본 기술적 분석 fallback"""
    return {
        "vcp_volume_volatility": {"vcp_score": 40, "vcp_present": False},
        "stage_ma_momentum": {"current_stage": "1", "stage_confidence": 0.4},
        "breakout_risk_position": {"breakout_probability": 0.3, "position_size_pct": 1.0},
        "unified_scoring": {"unified_score": 45, "confidence": 0.4},
        "final_decision": {"action": "HOLD", "confidence": 0.4}
    }

def _get_fallback_vcp_analysis() -> dict:
    return {"vcp_present": False, "score": 30, "contractions": 0, "analysis_details": {}}

def _get_fallback_stage_analysis() -> dict:
    return {"current_stage": "1", "confidence": 0.4, "stage_strength": 0.3}

def _get_fallback_breakout_analysis() -> dict:
    return {"breakout_ready": False, "probability": 0.3, "resistance_level": 0}

def _get_fallback_final_decision() -> dict:
    return {"action": "HOLD", "confidence": 0.3, "reasoning": "분석 실패로 인한 보수적 판단"}

def _generate_recovery_suggestions(failed_stages: list, fallbacks_used: list) -> list:
    """복구 제안 생성"""
    suggestions = []
    
    if "data_validation" in [stage["stage"] for stage in failed_stages]:
        suggestions.append("데이터 품질을 개선하고 누락된 필드를 보완하세요")
    
    if "technical_analysis" in [stage["stage"] for stage in failed_stages]:
        suggestions.append("시스템 부하를 확인하고 분석 파라미터를 조정하세요")
    
    if len(fallbacks_used) > 2:
        suggestions.append("다수의 fallback이 사용되었습니다. 전체 시스템 점검을 권장합니다")
    
    return suggestions

# === 피라미딩 시스템 헬퍼 함수들 ===

def _calculate_price_momentum(price_data: list) -> float:
    """가격 모멘텀 계산"""
    try:
        if len(price_data) < 20:
            return 0.5
        
        prices = np.array(price_data)
        
        # 단기 vs 중기 모멘텀
        short_momentum = (prices[-5:].mean() - prices[-10:-5].mean()) / prices[-10:-5].mean()
        medium_momentum = (prices[-10:].mean() - prices[-20:].mean()) / prices[-20:].mean()
        
        # 가중 평균 (단기 모멘텀에 더 높은 가중치)
        combined_momentum = (short_momentum * 0.7 + medium_momentum * 0.3)
        
        # 0-1 범위로 정규화
        normalized = max(0, min(1, combined_momentum * 10 + 0.5))
        
        return round(normalized, 3)
        
    except Exception:
        return 0.5

def _calculate_volume_momentum(volume_data: list) -> float:
    """거래량 모멘텀 계산"""
    try:
        if len(volume_data) < 20:
            return 0.5
        
        volumes = np.array(volume_data)
        
        # 최근 거래량 vs 평균 거래량
        recent_avg = volumes[-5:].mean()
        baseline_avg = volumes[-20:].mean()
        
        volume_ratio = recent_avg / baseline_avg if baseline_avg > 0 else 1.0
        
        # 로그 스케일로 정규화
        normalized = max(0, min(1, (np.log(volume_ratio) + 1) / 2))
        
        return round(normalized, 3)
        
    except Exception:
        return 0.5

def _calculate_relative_strength_vs_market(stock_prices: list, market_prices: list) -> float:
    """시장 대비 상대 강도 계산"""
    try:
        if len(stock_prices) < 20 or len(market_prices) < 20:
            return 0.5
        
        stock_returns = np.array(stock_prices[-20:])
        market_returns = np.array(market_prices[-20:])
        
        # 수익률 계산
        stock_change = (stock_returns[-1] - stock_returns[0]) / stock_returns[0]
        market_change = (market_returns[-1] - market_returns[0]) / market_returns[0]
        
        # 상대 강도 계산
        if market_change != 0:
            relative_strength = stock_change / market_change
        else:
            relative_strength = 1.0
        
        # 0-1 범위로 정규화 (1.0이 시장과 동일, >1.0이 시장 대비 강함)
        normalized = max(0, min(1, relative_strength / 2))
        
        return round(normalized, 3)
        
    except Exception:
        return 0.5

def _calculate_trend_strength(price_momentum: float, volume_momentum: float, relative_strength: float) -> float:
    """종합 추세 강도 계산"""
    try:
        # 가중 평균 (가격 모멘텀 40%, 거래량 모멘텀 30%, 상대 강도 30%)
        trend_strength = (
            price_momentum * 0.4 +
            volume_momentum * 0.3 +
            relative_strength * 0.3
        )
        
        return round(trend_strength, 3)
        
    except Exception:
        return 0.5

def _calculate_level_size_allocation(level: int, trend_strength: float) -> float:
    """레벨별 사이즈 배분 계산"""
    try:
        # 기본 배분 (레벨이 높을수록 작게)
        base_allocations = {1: 0.6, 2: 0.4, 3: 0.3}
        base_allocation = base_allocations.get(level, 0.2)
        
        # 추세 강도에 따른 조정
        trend_multiplier = 0.8 + (trend_strength * 0.4)  # 0.8 ~ 1.2
        
        return round(base_allocation * trend_multiplier, 2)
        
    except Exception:
        return 0.2

# UNUSED: 피라미딩 시스템 내부 함수들 (advanced_scaling_in_system에서만 사용)
# def _generate_execution_plan(scaling_levels: list, position_sizes: dict, 
#                            risk_management: dict, current_analysis: dict) -> dict:
#     """실행 계획 생성"""
#     try:
#         execution_plan = {
#             "immediate_actions": [],
#             "scheduled_orders": [],
#             "monitoring_points": [],
#             "risk_alerts": []
#         }
#         
#         # 즉시 실행 가능한 액션
#         for level_info in scaling_levels:
#             level = level_info["level"]
#             price = level_info["price"]
#             
#             if level in position_sizes.get("levels", {}):
#                 size = position_sizes["levels"][level]
#                 
#                 execution_plan["scheduled_orders"].append({
#                     "level": level,
#                     "order_type": "LIMIT",
#                     "price": price,
#                     "size_pct": size,
#                     "conditions": level_info["trigger_conditions"],
#                     "expiry": "14일"
#                 })
#         
#         # 모니터링 포인트
#         execution_plan["monitoring_points"] = [
#             {"type": "price_breakout", "levels": [level["price"] for level in scaling_levels]},
#             {"type": "volume_surge", "threshold": 1.5},
#             {"type": "trend_weakening", "indicators": ["ma_slope", "volume_decline"]},
#             {"type": "risk_limits", "max_loss": risk_management.get("max_loss_limit", 2.0)}
#         ]
#         
#         # 리스크 알림
#         execution_plan["risk_alerts"] = [
#             {
#                 "type": "stop_loss_hit",
#                 "levels": list(risk_management.get("stop_loss_levels", {}).values()),
#                 "action": "IMMEDIATE_EXIT"
#             },
#             {
#                 "type": "trend_reversal",
#                 "indicators": ["price_below_ma50", "volume_dry_up"],
#                 "action": "PARTIAL_EXIT"
#             }
#         ]
#         
#         return execution_plan
#         
#     except Exception as e:
#         logging.warning(f"⚠️ 실행 계획 생성 오류: {str(e)}")
#         return {"immediate_actions": [], "scheduled_orders": []}

# def _setup_monitoring_alerts(scaling_levels: list, risk_management: dict) -> dict:
#     """모니터링 알림 설정"""
#     try:
#         alerts = {
#             "price_alerts": [],
#             "volume_alerts": [],
#             "risk_alerts": [],
#             "trend_alerts": []
#         }
#         
#         # 가격 알림
#         for level_info in scaling_levels:
#             alerts["price_alerts"].append({
#                 "level": level_info["level"],
#                 "trigger_price": level_info["price"],
#                 "message": f"레벨 {level_info['level']} 진입 가격 도달",
#                 "priority": "HIGH"
#             })
#         
#         # 리스크 알림
#         for level, stop_price in risk_management.get("stop_loss_levels", {}).items():
#             alerts["risk_alerts"].append({
#                 "level": level,
#                 "level": level,
#                 "trigger_price": stop_price,
#                 "message": f"레벨 {level} 손절선 근접",
#                 "priority": "CRITICAL"
#             })
#         
#         # 추세 알림
#         alerts["trend_alerts"] = [
#             {
#                 "condition": "ma50_slope_negative",
#                 "message": "50일 이동평균 기울기 음전환",
#                 "priority": "MEDIUM"
#             },
#             {
#                 "condition": "volume_below_average",
#                 "message": "거래량 평균 이하로 감소",
#                 "priority": "LOW"
#             }
#         ]
#         
#         return alerts
#         
#     except Exception:
#         return {"price_alerts": [], "volume_alerts": [], "risk_alerts": []}

# def _define_exit_conditions(current_analysis: dict, risk_management: dict) -> dict:
#     """청산 조건 정의"""
#     try:
#         exit_conditions = {
#             "immediate_exit": [],
#             "gradual_exit": [],
#             "emergency_exit": []
#         }
#         
#         # 즉시 청산 조건
#         exit_conditions["immediate_exit"] = [
#             {
#                 "condition": "portfolio_stop_loss_hit",
#                 "trigger": risk_management.get("portfolio_stop_loss", 0),
#                 "action": "SELL_ALL",
#                 "reason": "포트폴리오 손절선 도달"
#             },
#             {
#                 "condition": "trend_reversal_confirmed",
#                 "indicators": ["price_below_ma200", "volume_spike_down"],
#                 "action": "SELL_ALL",
#                 "reason": "추세 반전 확인"
#             }
#         ]
#         
#         # 단계적 청산 조건
#         exit_conditions["gradual_exit"] = [
#             {
#                 "condition": "profit_target_1_hit",
#                 "action": "SELL_30_PERCENT",
#                 "reason": "첫 번째 수익 목표 달성"
#             },
#             {
#                 "condition": "trend_weakening",
#                 "indicators": ["ma_slope_flattening", "volume_declining"],
#                 "action": "SELL_50_PERCENT",
#                 "reason": "추세 약화 신호"
#             }
#         ]
#         
#         # 긴급 청산 조건
#         exit_conditions["emergency_exit"] = [
#             {
#                 "condition": "market_crash",
#                 "trigger": "market_down_5_percent",
#                 "action": "SELL_ALL_IMMEDIATELY",
#                 "reason": "시장 급락"
#             },
#             {
#                 "condition": "max_loss_exceeded",
#                 "trigger": risk_management.get("emergency_exit_level", 0),
#                 "action": "SELL_ALL_IMMEDIATELY",
#                 "reason": "최대 손실 한도 초과"
#             }
#         ]
#         
#         return exit_conditions
#         
#     except Exception:
#         return {"immediate_exit": [], "gradual_exit": [], "emergency_exit": []}

# def _get_fallback_pyramid_result(initial_entry: dict) -> dict:
#     """피라미딩 시스템 fallback 결과"""
#     return {
#         "pyramid_enabled": False,
#         "reason": "피라미딩 시스템 오류로 인한 비활성화",
#         "current_position": {
#             "total_size": initial_entry.get("position_size_pct", 2.0),
#             "levels": 1
#         },
#         "next_actions": ["수동 모니터링", "시스템 복구 후 재시도"],
#         "fallback_mode": True
#     }

# === 성능 모니터링 및 최적화 ===

class PerformanceMonitor:
    """성능 모니터링 시스템"""
    
    def __init__(self):
        self.metrics = {
            "analysis_times": [],
            "cache_hit_rates": [],
            "error_rates": [],
            "memory_usage": [],
            "function_call_counts": {}
        }
        self.start_time = time.time()
    
    def record_analysis_time(self, ticker: str, duration_ms: float):
        """분석 시간 기록"""
        self.metrics["analysis_times"].append({
            "ticker": ticker,
            "duration_ms": duration_ms,
            "timestamp": datetime.now().isoformat()
        })
        
        # 최근 100개만 유지
        if len(self.metrics["analysis_times"]) > 100:
            self.metrics["analysis_times"] = self.metrics["analysis_times"][-100:]
    
    def record_function_call(self, function_name: str):
        """함수 호출 횟수 기록"""
        if function_name not in self.metrics["function_call_counts"]:
            self.metrics["function_call_counts"][function_name] = 0
        self.metrics["function_call_counts"][function_name] += 1
    
    def get_performance_summary(self) -> dict:
        """성능 요약 조회"""
        try:
            analysis_times = [item["duration_ms"] for item in self.metrics["analysis_times"]]
            
            if analysis_times:
                avg_time = np.mean(analysis_times)
                p95_time = np.percentile(analysis_times, 95)
                improvement_achieved = avg_time < 100  # 100ms 목표
            else:
                avg_time = p95_time = 0
                improvement_achieved = False
            
            return {
                "average_analysis_time_ms": round(avg_time, 2),
                "p95_analysis_time_ms": round(p95_time, 2),
                "performance_target_achieved": improvement_achieved,
                "total_analyses": len(analysis_times),
                "function_call_counts": self.metrics["function_call_counts"],
                "uptime_hours": round((time.time() - self.start_time) / 3600, 2)
            }
            
        except Exception as e:
            logging.error(f"❌ 성능 요약 생성 오류: {str(e)}")
            return {"error": "성능 요약 생성 실패"}

# 전역 성능 모니터 인스턴스
performance_monitor = PerformanceMonitor()

# UNUSED: 호출되지 않는 함수
# def get_optimization_report() -> dict:
#     """최적화 보고서 생성"""
#     try:
#         # 성능 모니터 데이터
#         perf_summary = performance_monitor.get_performance_summary()
#         
#         # 캐시 통계 (strategy_analyzer에서 가져옴)
#         try:
#             from strategy_analyzer import enhanced_technical_cache_manager
#             cache_stats = enhanced_technical_cache_manager().get_cache_stats()
#         except Exception:
#             cache_stats = {"hit_rate": 0, "cache_size": 0}
#         
#         # 최적화 달성도 평가
#         achievements = {
#             "function_integration": {
#                 "target": "20개 함수 → 5개 함수",
#                 "achieved": perf_summary.get("total_analyses", 0) > 0,
#                 "impact": "분석 시간 단축"
#             },
#             "caching_enhancement": {
#                 "target": "캐시 히트율 60% 이상",
#                 "achieved": cache_stats.get("hit_rate", 0) >= 60,
#                 "impact": "반복 계산 최소화"
#             },
#             "error_handling": {
#                 "target": "Fail-safe 파이프라인 구현",
#                 "achieved": True,  # 구현 완료
#                 "impact": "시스템 안정성 향상"
#             },
#             "performance_target": {
#                 "target": "종목당 분석 시간 50% 단축",
#                 "achieved": perf_summary.get("performance_target_achieved", False),
#                 "impact": "전체 시스템 처리량 증가"
#             }
#         }
#         
#         # 추가 개선 제안
#         improvement_suggestions = []
#         
#         if cache_stats.get("hit_rate", 0) < 60:
#             improvement_suggestions.append({
#                 "area": "캐싱 최적화",
#                 "suggestion": "캐시 TTL 조정 및 키 최적화",
#                 "priority": "HIGH"
#             })
#         
#         if perf_summary.get("average_analysis_time_ms", 0) > 100:
#             improvement_suggestions.append({
#                 "area": "성능 최적화",
#                 "suggestion": "병렬 처리 확대 및 알고리즘 최적화",
#                 "priority": "MEDIUM"
#             })
#         
#         return {
#             "optimization_summary": {
#                 "total_functions_integrated": 20,
#                 "target_functions_achieved": 5,
#                 "performance_improvement_pct": 50 if achievements["performance_target"]["achieved"] else 0,
#                 "cache_efficiency_pct": cache_stats.get("hit_rate", 0)
#             },
#             "achievements": achievements,
#             "performance_metrics": perf_summary,
#             "cache_statistics": cache_stats,
#             "improvement_suggestions": improvement_suggestions,
#             "report_generated_at": datetime.now().isoformat()
#         }
#         
#     except Exception as e:
#         logging.error(f"❌ 최적화 보고서 생성 오류: {str(e)}")
#         return {
#             "error": "최적화 보고서 생성 실패",
#             "message": str(e)
#         }

# === 로그 순환 및 압축 설정 ===

def setup_gpt_logging_rotation(log_file_path: str = None, 
                              max_bytes: int = 50 * 1024 * 1024,  # 50MB
                              backup_count: int = 5,
                              enable_compression: bool = False) -> logging.Logger:
                              #enable_compression: bool = True) -> logging.Logger:
    """
    GPT 분석용 로깅 순환 및 압축 설정 (제한된 로깅 사용)
    
    Args:
        log_file_path: 로그 파일 경로 (None이면 makenaide.log 사용)
        max_bytes: 최대 파일 크기 (기본: 50MB)
        backup_count: 백업 파일 개수 (기본: 5개)
        enable_compression: 압축 활성화 여부
    """
    # 로그 파일 경로가 None이면 makenaide.log 사용 (제한된 로깅)
    if log_file_path is None:
        from utils import safe_strftime
        log_dir = "log"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_path = os.path.join(log_dir, f"{safe_strftime(datetime.now(), '%Y%m%d')}_makenaide.log")
    
    # 로그 디렉토리 생성
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    # GPT 전용 로거 생성
    gpt_logger = logging.getLogger('gpt_analysis')
    gpt_logger.setLevel(logging.DEBUG)
    
    # 기존 핸들러 제거
    for handler in gpt_logger.handlers[:]:
        gpt_logger.removeHandler(handler)
    
    # 로테이팅 파일 핸들러 생성
    rotating_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    
    # 포맷터 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    rotating_handler.setFormatter(formatter)
    
    # 핸들러 추가
    gpt_logger.addHandler(rotating_handler)
    
    # 압축 기능 활성화 시 커스텀 로테이터 설정
    if enable_compression:
        def compress_rotated_log(source, dest):
            """로테이트된 로그 파일 압축"""
            try:
                with open(source, 'rb') as f_in:
                    with gzip.open(f"{dest}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(source)
                logging.info(f"📦 로그 파일 압축 완료: {dest}.gz")
            except Exception as e:
                logging.error(f"❌ 로그 파일 압축 실패: {e}")
        
        rotating_handler.rotator = compress_rotated_log
    
    logging.info(f"📋 GPT 로깅 시스템 설정 완료 - 파일: {log_file_path}, 최대크기: {max_bytes//1024//1024}MB")
    return gpt_logger

# === 민감 정보 마스킹 시스템 ===

def mask_sensitive_info(data: Any, mask_level: str = "medium") -> Any:
    """
    민감 정보 마스킹 처리
    
    Args:
        data: 마스킹할 데이터 (문자열, 딕셔너리, 리스트 등)
        mask_level: 마스킹 강도 ("low", "medium", "high")
    """
    
    def mask_api_keys(text: str) -> str:
        """API 키 마스킹"""
        # API 키 패턴들
        patterns = [
            (r'sk-[a-zA-Z0-9]{20,}', 'sk-***MASKED***'),
            (r'Bearer [a-zA-Z0-9]{20,}', 'Bearer ***MASKED***'),
            (r'key["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{10,}["\']?', 'key=***MASKED***'),
            (r'token["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{10,}["\']?', 'token=***MASKED***')
        ]
        
        for pattern, replacement in patterns:
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        return text
    
    def mask_financial_data(text: str, level: str) -> str:
        """금융 데이터 마스킹"""
        if level == "low":
            return text
        elif level == "medium":
            # 구체적 수치를 범위로 변경
            text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*(?:원|KRW|USD)\b', '[AMOUNT_RANGE]', text)
            text = re.sub(r'\b\d+\.\d+%\b', '[PERCENTAGE_RANGE]', text)
            return text
        else:  # high
            # 모든 숫자를 마스킹
            text = re.sub(r'\b\d+(?:\.\d+)?\b', '[NUMERIC_VALUE]', text)
            return text
    
    def mask_personal_info(text: str) -> str:
        """개인정보 마스킹"""
        # 이메일 마스킹
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '***@***.***', text)
        # 전화번호 마스킹
        text = re.sub(r'\b\d{2,3}-\d{3,4}-\d{4}\b', '***-****-****', text)
        return text
    
    if isinstance(data, str):
        result = mask_api_keys(data)
        result = mask_financial_data(result, mask_level)
        result = mask_personal_info(result)
        return result
    
    elif isinstance(data, dict):
        masked_dict = {}
        for key, value in data.items():
            # 키 자체도 민감정보인지 확인
            sensitive_keys = ['password', 'secret', 'key', 'token', 'api_key']
            if any(sensitive_key in key.lower() for sensitive_key in sensitive_keys):
                masked_dict[key] = '***MASKED***'
            else:
                masked_dict[key] = mask_sensitive_info(value, mask_level)
        return masked_dict
    
    elif isinstance(data, list):
        return [mask_sensitive_info(item, mask_level) for item in data]
    
    else:
        return data

# === 보안 강화 로깅 시스템 ===

def setup_secure_logging(log_level: str = None, enable_sensitive_masking: bool = True) -> logging.Logger:
    """
    보안이 강화된 로깅 시스템 설정
    
    Args:
        log_level: 환경변수 기반 로깅 레벨 (DEBUG, INFO, WARNING, ERROR)
        enable_sensitive_masking: 민감 정보 마스킹 활성화 여부
    """
    
    # 환경변수에서 로깅 레벨 가져오기
    if log_level is None:
        log_level = os.getenv('LOGGING_LEVEL', 'INFO').upper()
    
    # 로거 생성
    secure_logger = logging.getLogger('secure_trend_analyzer')
    secure_logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # 기존 핸들러 제거
    for handler in secure_logger.handlers[:]:
        secure_logger.removeHandler(handler)
    
    # 보안 필터 클래스
    class SensitiveInfoFilter(logging.Filter):
        def __init__(self, enable_masking=True):
            super().__init__()
            self.enable_masking = enable_masking
        
        def filter(self, record):
            if self.enable_masking and hasattr(record, 'msg'):
                # 로그 메시지에서 민감 정보 마스킹
                if isinstance(record.msg, str):
                    record.msg = mask_sensitive_info(record.msg, mask_level="medium")
                elif isinstance(record.args, tuple):
                    # 포맷 인자들도 마스킹
                    masked_args = []
                    for arg in record.args:
                        if isinstance(arg, (str, dict)):
                            masked_args.append(mask_sensitive_info(arg, mask_level="medium"))
                        else:
                            masked_args.append(arg)
                    record.args = tuple(masked_args)
            return True
    
    # 로테이팅 파일 핸들러 (암호화 고려)
    log_file_path = os.getenv('SECURE_LOG_PATH', 'log/secure_trend_analyzer.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=3,
        encoding='utf-8'
    )
    
    # 콘솔 핸들러 (개발 환경용)
    console_handler = logging.StreamHandler()
    
    # 포맷터 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 필터 적용
    if enable_sensitive_masking:
        sensitive_filter = SensitiveInfoFilter(enable_masking=True)
        file_handler.addFilter(sensitive_filter)
        console_handler.addFilter(sensitive_filter)
    
    # 핸들러 추가
    secure_logger.addHandler(file_handler)
    
    # 개발 환경에서만 콘솔 출력
    if os.getenv('ENVIRONMENT', 'production').lower() in ['development', 'dev', 'local']:
        secure_logger.addHandler(console_handler)
    
    # 로그 레벨별 출력 제어
    if log_level == 'DEBUG':
        secure_logger.info("🔍 디버그 모드로 로깅 시스템 활성화됨")
    elif log_level == 'ERROR':
        secure_logger.error("⚠️ 에러 레벨 로깅만 활성화됨")
    
    secure_logger.info(f"🔒 보안 강화 로깅 시스템 설정 완료 - 레벨: {log_level}")
    return secure_logger

# 통합 로깅 시스템 초기화 (제한된 로깅 사용)
gpt_logger = setup_gpt_logging_rotation(log_file_path=None)  # makenaide.log 사용
secure_logger = setup_secure_logging()

# === GPT 분석 성능 최적화 클래스 ===

class GPTAnalysisMonitor:
    """GPT 분석 성능 추적 및 모니터링 클래스"""
    
    def __init__(self):
        self.api_call_count = 0
        self.total_token_usage = 0
        self.total_processing_time = 0.0
        self.success_count = 0
        self.error_count = 0
        self.error_types = {}
        self.start_time = time.time()
        self.daily_costs = {}
        self.lock = threading.Lock()
        
        # 토큰 비용 정보 (GPT-4o 기준, USD)
        self.token_costs = {
            "input": 0.0025 / 1000,   # per token
            "output": 0.01 / 1000     # per token
        }
    
    def track_api_call(self, tokens_used: int, processing_time: float, success: bool, 
                      error_type: str = None, output_tokens: int = 0):
        """API 호출 추적 및 비용 계산"""
        with self.lock:
            self.api_call_count += 1
            self.total_token_usage += tokens_used
            self.total_processing_time += processing_time
            
            if success:
                self.success_count += 1
            else:
                self.error_count += 1
                if error_type:
                    self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
            
            # 일별 비용 계산
            today = datetime.now().strftime("%Y-%m-%d")
            if today not in self.daily_costs:
                self.daily_costs[today] = {"input_tokens": 0, "output_tokens": 0, "calls": 0}
            
            self.daily_costs[today]["input_tokens"] += tokens_used
            self.daily_costs[today]["output_tokens"] += output_tokens
            self.daily_costs[today]["calls"] += 1
            
            logging.info(f"📊 API 호출 추적: 토큰={tokens_used}, 시간={processing_time:.2f}s, 성공={success}")
    
    def get_efficiency_report(self) -> dict:
        """효율성 리포트 생성"""
        with self.lock:
            if self.api_call_count == 0:
                return {"error": "아직 API 호출 기록이 없습니다"}
            
            success_rate = (self.success_count / self.api_call_count) * 100
            avg_processing_time = self.total_processing_time / self.api_call_count
            avg_tokens_per_call = self.total_token_usage / self.api_call_count
            
            # 오늘 비용 계산
            today = datetime.now().strftime("%Y-%m-%d")
            today_cost = 0.0
            if today in self.daily_costs:
                day_data = self.daily_costs[today]
                today_cost = (
                    day_data["input_tokens"] * self.token_costs["input"] +
                    day_data["output_tokens"] * self.token_costs["output"]
                )
            
            # 총 비용 계산
            total_cost = sum(
                (data["input_tokens"] * self.token_costs["input"] + 
                 data["output_tokens"] * self.token_costs["output"])
                for data in self.daily_costs.values()
            )
            
            uptime_hours = (time.time() - self.start_time) / 3600
            
            report = {
                "총_API_호출수": self.api_call_count,
                "성공률": f"{success_rate:.1f}%",
                "평균_처리시간": f"{avg_processing_time:.2f}초",
                "평균_토큰사용량": f"{avg_tokens_per_call:.0f}토큰",
                "총_토큰사용량": self.total_token_usage,
                "오늘_비용": f"${today_cost:.4f}",
                "총_비용": f"${total_cost:.4f}",
                "시간당_호출수": f"{self.api_call_count / uptime_hours:.1f}" if uptime_hours > 0 else "0",
                "오류_유형별_통계": self.error_types,
                "일별_사용량": self.daily_costs
            }
            
            return report
    
    def get_cost_alert(self, daily_limit_usd: float = 10.0) -> dict:
        """비용 경고 시스템"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today not in self.daily_costs:
            return {"alert": False, "message": "오늘 사용량 없음"}
        
        day_data = self.daily_costs[today]
        today_cost = (
            day_data["input_tokens"] * self.token_costs["input"] +
            day_data["output_tokens"] * self.token_costs["output"]
        )
        
        usage_percentage = (today_cost / daily_limit_usd) * 100
        
        if usage_percentage >= 90:
            return {
                "alert": True,
                "level": "critical",
                "message": f"⚠️ 일일 비용 한도의 {usage_percentage:.1f}% 사용 (${today_cost:.4f}/${daily_limit_usd})"
            }
        elif usage_percentage >= 70:
            return {
                "alert": True,
                "level": "warning", 
                "message": f"📈 일일 비용 한도의 {usage_percentage:.1f}% 사용 (${today_cost:.4f}/${daily_limit_usd})"
            }
        else:
            return {
                "alert": False,
                "message": f"✅ 일일 비용 사용량: {usage_percentage:.1f}% (${today_cost:.4f}/${daily_limit_usd})"
            }

class GPTAnalysisErrorHandler:
    """표준화된 GPT 분석 오류 처리 시스템"""
    
    @staticmethod
    def handle_api_error(error: Exception, ticker: str, fallback_score: float = 50.0) -> dict:
        """API 오류별 세분화된 처리"""
        error_mapping = {
            RateLimitError: {
                "confidence": 0.3,
                "reason": "rate_limit", 
                "retry_after": 60,
                "message": "API 요청 한도 초과"
            },
            TimeoutError: {
                "confidence": 0.4,
                "reason": "timeout",
                "retry_after": 10,
                "message": "요청 시간 초과"
            },
            ConnectionError: {
                "confidence": 0.2,
                "reason": "connection_error",
                "retry_after": 30,
                "message": "네트워크 연결 오류"
            },
            OpenAIError: {
                "confidence": 0.35,
                "reason": "openai_api_error",
                "retry_after": 15,
                "message": "OpenAI API 오류"
            }
        }
        
        # 특정 오류 유형별 처리
        for error_type, config in error_mapping.items():
            if isinstance(error, error_type):
                logging.warning(f"🚨 {ticker} {config['message']}: {str(error)}")
                return {
                    "ticker": ticker,
                    "score": fallback_score,
                    "confidence": config["confidence"],
                    "analysis_method": f"error_{config['reason']}",
                    "error_details": {
                        "type": config["reason"],
                        "message": config["message"],
                        "original_error": str(error),
                        "retry_after": config["retry_after"],
                        "timestamp": datetime.now().isoformat()
                    }
                }
        
        # 기타 예상치 못한 오류
        logging.error(f"❌ {ticker} 예상치 못한 오류: {str(error)}")
        logging.error(f"❌ {ticker} 오류 상세: {traceback.format_exc()}")
        
        return {
            "ticker": ticker,
            "score": fallback_score,
            "confidence": 0.1,
            "analysis_method": "error_unknown",
            "error_details": {
                "type": "unknown_error",
                "message": "알 수 없는 오류 발생",
                "original_error": str(error),
                "retry_after": 30,
                "timestamp": datetime.now().isoformat()
            }
        }
    
    @staticmethod
    def should_retry(error_details: dict, max_retries: int = 3, current_retry: int = 0) -> bool:
        """재시도 여부 결정"""
        if current_retry >= max_retries:
            return False
        
        # 재시도 가능한 오류 유형
        retryable_errors = ["rate_limit", "timeout", "connection_error", "openai_api_error"]
        error_type = error_details.get("type", "")
        
        if error_type in retryable_errors:
            retry_after = error_details.get("retry_after", 10)
            logging.info(f"🔄 {retry_after}초 후 재시도 예정 (시도 {current_retry + 1}/{max_retries})")
            time.sleep(retry_after)
            return True
        
        return False
    
    @staticmethod 
    def log_error_analytics(error_details: dict, ticker: str):
        """오류 분석을 위한 로깅"""
        try:
            # 오류 통계를 파일에 저장 (선택적)
            error_log_path = "log/gpt_error_analytics.json"
            os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
            
            error_entry = {
                "timestamp": datetime.now().isoformat(),
                "ticker": ticker,
                **error_details
            }
            
            # 기존 로그 읽기
            error_analytics = []
            if os.path.exists(error_log_path):
                with open(error_log_path, 'r', encoding='utf-8') as f:
                    error_analytics = json.load(f)
            
            # 새 오류 추가
            error_analytics.append(error_entry)
            
            # 최근 1000개 오류만 유지
            if len(error_analytics) > 1000:
                error_analytics = error_analytics[-1000:]
            
            # 파일에 저장
            with open(error_log_path, 'w', encoding='utf-8') as f:
                json.dump(error_analytics, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logging.warning(f"⚠️ 오류 분석 로깅 실패: {e}")

class APIRateLimiter:
    """API 호출 빈도 제한 관리"""
    
    def __init__(self, calls_per_minute=50):
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """필요 시 대기하여 rate limit 준수"""
        with self.lock:
            now = time.time()
            # 1분 이내의 호출만 유지
            self.calls = [call_time for call_time in self.calls if now - call_time < 60]
            
            if len(self.calls) >= self.calls_per_minute:
                sleep_time = 60 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.calls.append(now)

class AnalysisCacheManager:
    """GPT 분석 결과 캐싱 매니저 - LRU 정책 및 메모리 관리 강화"""
    
    def __init__(self, cache_ttl_minutes=720, max_memory_mb=100, max_entries=1000):
        self.cache = {}
        self.cache_ttl = cache_ttl_minutes * 60
        self.max_memory_bytes = max_memory_mb * 1024 * 1024  # MB를 바이트로 변환
        self.max_entries = max_entries
        self.current_memory_usage = 0
        self.access_times = {}  # LRU 추적
        self.entry_sizes = {}   # 각 엔트리 크기 추적
        self.lock = threading.Lock()
        
        # 백그라운드 정리 스레드 시작
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """백그라운드 캐시 정리 스레드 시작"""
        def cleanup_worker():
            while True:
                time.sleep(300)  # 5분마다 정리
                self._cleanup_expired_entries()
                self._enforce_memory_limits()
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logging.info("🧹 캐시 정리 스레드 시작됨")
    
    def _cleanup_expired_entries(self):
        """만료된 엔트리 정리"""
        with self.lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self.cache.items():
                if current_time - entry['timestamp'] > self.cache_ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._remove_entry(key)
                
            if expired_keys:
                logging.info(f"🗑️ {len(expired_keys)}개 만료된 캐시 엔트리 정리됨")
    
    def _enforce_memory_limits(self):
        """메모리 및 엔트리 수 제한 적용"""
        with self.lock:
            # 엔트리 수 제한
            if len(self.cache) > self.max_entries:
                excess_count = len(self.cache) - self.max_entries
                self._evict_lru_entries(excess_count)
            
            # 메모리 사용량 제한
            if self.current_memory_usage > self.max_memory_bytes:
                target_memory = self.max_memory_bytes * 0.8  # 80%까지 줄임
                self._evict_by_memory(target_memory)
    
    def _evict_lru_entries(self, count: int):
        """LRU 정책으로 엔트리 제거"""
        # 액세스 시간 기준으로 정렬
        sorted_keys = sorted(self.access_times.keys(), 
                           key=lambda k: self.access_times[k])
        
        evicted = 0
        for key in sorted_keys:
            if evicted >= count:
                break
            if key in self.cache:
                self._remove_entry(key)
                evicted += 1
                
        if evicted > 0:
            logging.info(f"🔄 LRU 정책으로 {evicted}개 캐시 엔트리 제거됨")
    
    def _evict_by_memory(self, target_memory: int):
        """메모리 사용량 기준으로 엔트리 제거"""
        # 크기가 큰 엔트리부터 제거
        sorted_keys = sorted(self.entry_sizes.keys(), 
                           key=lambda k: self.entry_sizes[k], reverse=True)
        
        evicted = 0
        for key in sorted_keys:
            if self.current_memory_usage <= target_memory:
                break
            if key in self.cache:
                self._remove_entry(key)
                evicted += 1
                
        if evicted > 0:
            logging.info(f"💾 메모리 제한으로 {evicted}개 캐시 엔트리 제거됨")
    
    def _remove_entry(self, key: str):
        """엔트리 제거 및 메모리 사용량 업데이트"""
        if key in self.cache:
            self.current_memory_usage -= self.entry_sizes.get(key, 0)
            del self.cache[key]
            self.access_times.pop(key, None)
            self.entry_sizes.pop(key, None)
    
    def _calculate_entry_size(self, data: dict) -> int:
        """엔트리 크기 계산 (바이트)"""
        try:
            data_str = json.dumps(data, ensure_ascii=False)
            return len(data_str.encode('utf-8'))
        except:
            return 1024  # 기본 추정값
    
    def get_cache_key(self, ticker: str, data_hash: str) -> str:
        """캐시 키 생성"""
        return f"gpt_analysis:{ticker}:{data_hash[:8]}"
    
    def _get_data_hash(self, data: dict) -> str:
        """데이터 해시 생성"""
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def get_from_cache(self, ticker: str, data: dict) -> Optional[dict]:
        """캐시된 결과 조회 - LRU 업데이트 포함"""
        with self.lock:
            data_hash = self._get_data_hash(data)
            cache_key = self.get_cache_key(ticker, data_hash)
            
            if cache_key in self.cache:
                cached_entry = self.cache[cache_key]
                if time.time() - cached_entry['timestamp'] < self.cache_ttl:
                    # LRU 업데이트
                    self.access_times[cache_key] = time.time()
                    logging.info(f"🔄 {ticker} 캐시된 분석 결과 사용")
                    return cached_entry['result']
                else:
                    # 만료된 캐시 제거
                    self._remove_entry(cache_key)
        return None
    
    def save_to_cache(self, ticker: str, data: dict, result: dict):
        """결과를 캐시에 저장 - 메모리 관리 포함"""
        with self.lock:
            data_hash = self._get_data_hash(data)
            cache_key = self.get_cache_key(ticker, data_hash)
            
            # 엔트리 크기 계산
            compressed_result = {
                'timestamp': time.time(),
                'result': result
            }
            entry_size = self._calculate_entry_size(compressed_result)
            
            # 메모리 제한 확인
            if self.current_memory_usage + entry_size > self.max_memory_bytes:
                # 공간 확보
                target_memory = self.max_memory_bytes * 0.7  # 70%까지 줄임
                self._evict_by_memory(target_memory)
            
            # 캐시에 저장
            self.cache[cache_key] = compressed_result
            self.access_times[cache_key] = time.time()
            self.entry_sizes[cache_key] = entry_size
            self.current_memory_usage += entry_size
            
            logging.debug(f"💾 {ticker} 분석 결과 캐시 저장 (크기: {entry_size:,} bytes)")
    
    def get_cache_stats(self) -> dict:
        """캐시 통계 정보 반환"""
        with self.lock:
            return {
                'total_entries': len(self.cache),
                'memory_usage_mb': self.current_memory_usage / (1024 * 1024),
                'memory_limit_mb': self.max_memory_bytes / (1024 * 1024),
                'memory_usage_pct': (self.current_memory_usage / self.max_memory_bytes) * 100,
                'entries_limit': self.max_entries,
                'entries_usage_pct': (len(self.cache) / self.max_entries) * 100
            }

class GPTAnalysisOptimizerSingleton:
    """스레드 안전한 GPT 분석 성능 최적화 매니저 싱글톤"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.rate_limiter = APIRateLimiter(calls_per_minute=50)
            self.cache_manager = AnalysisCacheManager()
            self.monitor = GPTAnalysisMonitor()
            self.error_handler = GPTAnalysisErrorHandler()
            self._initialized = True

    def optimize_for_tokens(self, json_data: dict, target_tokens: int = 3000, strategy: str = "simple") -> dict:
        """
        통합된 토큰 최적화
        strategy: "simple" | "comprehensive"
        """
        if strategy == "simple":
            return self._optimize_simple(json_data, target_tokens)
        else:
            return self._optimize_comprehensive(json_data, target_tokens)
    
    def _optimize_simple(self, json_data: dict, target_tokens: int = 3000) -> dict:
        """
        단순화된 토큰 최적화
        1. 토큰 계산
        2. OHLCV 데이터 압축 (필요시)
        3. 정밀도 조정
        """
        enc = tiktoken.encoding_for_model("gpt-4o")
        json_str = json.dumps(json_data, ensure_ascii=False)
        current_tokens = len(enc.encode(json_str))
        
        if current_tokens <= target_tokens:
            return json_data
        
        optimized = json_data.copy()
        
        # 1단계: 정밀도 조정
        optimized = self._round_numbers_precision(optimized, precision=2)
        
        # 2단계: OHLCV 압축
        if 'ohlcv' in optimized and current_tokens > target_tokens:
            compression_ratio = target_tokens / current_tokens
            new_length = int(len(optimized['ohlcv']) * compression_ratio * 0.8)  # 안전 마진
            optimized['ohlcv'] = optimized['ohlcv'][-new_length:]
        
        return optimized
    
    def _optimize_comprehensive(self, json_data: dict, target_tokens: int = 3000) -> dict:
        """
        포괄적 토큰 최적화 로직
        1. tiktoken으로 실제 토큰 수 계산
        2. 우선순위별 데이터 압축 (OHLCV > 지표 > 메타데이터)
        3. 동적 압축률 조정
        4. 최소 필수 데이터는 보존
        """
        try:
            # tiktoken 인코더 초기화
            enc = tiktoken.encoding_for_model("gpt-4o")
            
            # 현재 토큰 수 계산
            json_str = json.dumps(json_data, ensure_ascii=False)
            current_tokens = len(enc.encode(json_str))
            
            if current_tokens <= target_tokens:
                logging.debug(f"📊 토큰 최적화 불필요: {current_tokens}/{target_tokens}")
                return json_data
            
            logging.info(f"🔧 토큰 최적화 시작: {current_tokens} -> {target_tokens} 목표")
            
            # 데이터 복사본 생성
            optimized = json_data.copy()
            
            # 1단계: 정밀도 조정 (소수점 2자리로 제한)
            optimized = self._round_numbers_precision(optimized, precision=2)
            json_str = json.dumps(optimized, ensure_ascii=False)
            current_tokens = len(enc.encode(json_str))
            
            if current_tokens <= target_tokens:
                logging.info(f"✅ 1단계 정밀도 조정으로 목표 달성: {current_tokens}")
                return optimized
            
            # 2단계: OHLCV 데이터 압축 (우선순위별)
            compression_levels = [
                {"ohlcv_limit": 100, "desc": "OHLCV 100개"},
                {"ohlcv_limit": 50, "desc": "OHLCV 50개"},
                {"ohlcv_limit": 30, "desc": "OHLCV 30개"},
                {"ohlcv_limit": 20, "desc": "OHLCV 20개"}
            ]
            
            for level in compression_levels:
                if 'ohlcv' in optimized and len(optimized['ohlcv']) > level['ohlcv_limit']:
                    # 최신 데이터 우선으로 압축
                    optimized['ohlcv'] = optimized['ohlcv'][-level['ohlcv_limit']:]
                    
                    json_str = json.dumps(optimized, ensure_ascii=False)
                    current_tokens = len(enc.encode(json_str))
                    
                    logging.info(f"🔧 {level['desc']} 압축: {current_tokens} 토큰")
                    
                    if current_tokens <= target_tokens:
                        logging.info(f"✅ OHLCV 압축으로 목표 달성: {current_tokens}")
                        return optimized
            
            # 3단계: 지표 데이터 압축
            if 'indicators' in optimized:
                # 중요도 낮은 지표 제거
                low_priority_indicators = [
                    'volume_sma', 'ad_line', 'obv', 'cmf', 'vwap',
                    'williams_r', 'cci', 'atr_percent'
                ]
                
                for indicator in low_priority_indicators:
                    if indicator in optimized['indicators']:
                        del optimized['indicators'][indicator]
                        
                        json_str = json.dumps(optimized, ensure_ascii=False)
                        current_tokens = len(enc.encode(json_str))
                        
                        if current_tokens <= target_tokens:
                            logging.info(f"✅ {indicator} 제거로 목표 달성: {current_tokens}")
                            return optimized
            
            # 4단계: 메타데이터 압축
            metadata_keys = ['timestamp', 'last_updated', 'data_source', 'version']
            for key in metadata_keys:
                if key in optimized:
                    del optimized[key]
                    
                    json_str = json.dumps(optimized, ensure_ascii=False)
                    current_tokens = len(enc.encode(json_str))
                    
                    if current_tokens <= target_tokens:
                        logging.info(f"✅ {key} 제거로 목표 달성: {current_tokens}")
                        return optimized
            
            # 5단계: 최종 압축 (OHLCV 더 축소)
            if 'ohlcv' in optimized and len(optimized['ohlcv']) > 10:
                optimized['ohlcv'] = optimized['ohlcv'][-10:]  # 최근 10개만 유지
                
                json_str = json.dumps(optimized, ensure_ascii=False)
                current_tokens = len(enc.encode(json_str))
                
                logging.warning(f"⚠️ 최종 압축 완료: {current_tokens} 토큰 (목표: {target_tokens})")
            
            return optimized
            
        except Exception as e:
            logging.error(f"❌ 토큰 최적화 실패: {e}")
            return json_data  # 실패 시 원본 반환
    
    def _round_numbers_precision(self, obj: Any, precision: int = 2) -> Any:
        """숫자 정밀도 조정"""
        if isinstance(obj, dict):
            return {k: self._round_numbers_precision(v, precision) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._round_numbers_precision(item, precision) for item in obj]
        elif isinstance(obj, float):
            return round(obj, precision)
        return obj
    
    def optimize_json_payload(self, json_data: dict, max_size_kb: int = 4) -> dict:
        """JSON 페이로드 최적화 (기존 메서드 개선)"""
        # 새로운 토큰 기반 최적화 사용
        optimized = self.optimize_for_tokens(json_data, target_tokens=3000, strategy="comprehensive")
        
        # 크기 제한도 확인
        json_str = json.dumps(optimized, ensure_ascii=False)
        current_size_kb = len(json_str.encode('utf-8')) / 1024
        
        if current_size_kb > max_size_kb:
            # 추가 압축이 필요한 경우
            optimized = self.optimize_for_tokens(optimized, target_tokens=2000, strategy="comprehensive")
            logging.warning(f"📊 추가 압축: {current_size_kb:.1f}KB -> 목표 {max_size_kb}KB")
        
        return optimized

    def manage_api_rate_limits(self):
        """API 율제한 관리"""
        self.rate_limiter.wait_if_needed()

@dataclass
class AnalysisConfig:
    """분석 설정 데이터 클래스"""
    mode: str = "hybrid"  # "json", "chart", "hybrid"
    batch_size: int = 3
    enable_caching: bool = True
    cache_ttl_minutes: int = 720
    api_timeout_seconds: int = 30
    max_retries: int = 3

# === 통합 GPT 분석 엔진 ===

def unified_gpt_analysis_engine(candidates: Union[List[dict], List[tuple]], analysis_config: Optional[dict] = None) -> List[dict]:
    """
    통합 GPT 분석 엔진 - 모든 분석 방식을 지원하는 단일 인터페이스
    
    Args:
        candidates: 분석 대상 데이터
            - JSON 형식: [{"ticker": str, "ohlcv": [...], "indicators": {...}}]
            - 튜플 형식: [(ticker, score), ...]
        analysis_config: 분석 설정
            {
                "mode": "json" | "chart" | "hybrid",
                "batch_size": 3,
                "enable_caching": True,
                "cache_ttl_minutes": 720,
                "api_timeout_seconds": 30,
                "max_retries": 3
            }
    
    Returns:
        list: [{"ticker": str, "score": int, "confidence": float, "analysis_method": str}]
    """
    
    # 기본 설정 적용
    config = _apply_default_config(analysis_config)
    
    # 입력 데이터 정규화
    normalized_candidates = _normalize_input_candidates(candidates)
    
    logging.info(f"🔄 통합 GPT 분석 시작: {len(normalized_candidates)}개 후보, 모드: {config.mode}")
    
    # 분석 모드별 라우팅
    if config.mode == "json":
        return _execute_json_analysis_pipeline(normalized_candidates, config)
    elif config.mode == "chart":
        return _execute_chart_analysis_pipeline(normalized_candidates, config)
    else:  # hybrid
        return _execute_hybrid_analysis_pipeline(normalized_candidates, config)

def _apply_default_config(analysis_config: Optional[dict]) -> AnalysisConfig:
    """기본 설정 적용"""
    if analysis_config is None:
        return AnalysisConfig()
    
    return AnalysisConfig(
        mode=analysis_config.get("mode", "hybrid"),
        batch_size=analysis_config.get("batch_size", 3),
        enable_caching=analysis_config.get("enable_caching", True),
        cache_ttl_minutes=analysis_config.get("cache_ttl_minutes", 720),
        api_timeout_seconds=analysis_config.get("api_timeout_seconds", 30),
        max_retries=analysis_config.get("max_retries", 3)
    )

def _normalize_input_candidates(candidates: Union[List[dict], List[tuple]]) -> List[dict]:
    """입력 데이터 정규화"""
    normalized = []
    
    for candidate in candidates:
        if isinstance(candidate, dict):
            # JSON 형식 데이터
            normalized.append(candidate)
        elif isinstance(candidate, (tuple, list)) and len(candidate) >= 2:
            # 튜플 형식 데이터 (ticker, score)
            ticker, score = candidate[0], candidate[1]
            normalized.append({
                "ticker": ticker,
                "base_score": score,
                "ohlcv": [],
                "indicators": {}
            })
        else:
            logging.warning(f"⚠️ 지원되지 않는 후보 데이터 형식: {candidate}")
    
    return normalized

def _execute_json_analysis_pipeline(candidates: List[dict], config: AnalysisConfig) -> List[dict]:
    """JSON 기반 분석 파이프라인"""
    results = []
    optimizer = GPTAnalysisOptimizerSingleton()
    
    # 배치 처리 최적화
    for batch in _create_batches(candidates, config.batch_size):
        try:
            # 캐시 확인
            cached_results = []
            uncached_batch = []
            
            if config.enable_caching:
                for candidate in batch:
                    cached_result = optimizer.cache_manager.get_from_cache(
                        candidate["ticker"], candidate
                    )
                    if cached_result:
                        cached_results.append(cached_result)
                    else:
                        uncached_batch.append(candidate)
            else:
                uncached_batch = batch
            
            # GPT API 호출
            if uncached_batch:
                batch_results = _call_gpt_json_batch(uncached_batch, config, optimizer)
                
                # 캐시 저장
                if config.enable_caching:
                    for candidate, result in zip(uncached_batch, batch_results):
                        optimizer.cache_manager.save_to_cache(
                            candidate["ticker"], candidate, result
                        )
                
                cached_results.extend(batch_results)
            
            results.extend(cached_results)
            
        except Exception as e:
            logging.error(f"❌ JSON 분석 배치 처리 실패: {e}")
            # 개별 처리로 폴백
            results.extend(_fallback_individual_processing(batch, config))
    
    return results

def _execute_chart_analysis_pipeline(candidates: List[dict], config: AnalysisConfig) -> List[dict]:
    """차트 기반 분석 파이프라인"""
    results = []
    
    for candidate in candidates:
        ticker = candidate["ticker"]
        try:
            # 차트 이미지 경로 확인
            chart_path = f"charts/{ticker}.png"
            if not os.path.exists(chart_path):
                # 차트 생성 시도
                from data_fetcher import generate_chart_image, generate_gpt_analysis_json
                generate_chart_image(ticker)
            
            if os.path.exists(chart_path):
                # 차트 기반 분석 실행
                with open(chart_path, "rb") as f:
                    chart_base64 = base64.b64encode(f.read()).decode("utf-8")
                
                gpt_result = call_gpt_with_chart_base64(ticker, chart_base64, candidate.get("indicators", {}))
                
                results.append({
                    "ticker": ticker,
                    "score": gpt_result.get("score", 50),
                    "confidence": 0.85,  # 차트 분석 기본 신뢰도
                    "analysis_method": "chart"
                })
            else:
                logging.warning(f"⚠️ {ticker} 차트 이미지 생성 실패")
                results.append({
                    "ticker": ticker,
                    "score": candidate.get("base_score", 50),
                    "confidence": 0.50,
                    "analysis_method": "chart_failed"
                })
                
        except Exception as e:
            logging.error(f"❌ {ticker} 차트 분석 실패: {e}")
            results.append({
                "ticker": ticker,
                "score": candidate.get("base_score", 50),
                "confidence": 0.50,
                "analysis_method": "chart_error"
            })
    
    return results

def _execute_hybrid_analysis_pipeline(candidates: List[dict], config: AnalysisConfig) -> List[dict]:
    """하이브리드 분석 파이프라인 (JSON 우선, 차트 대체)"""
    
    # 1차: JSON 분석 시도
    json_results = []
    failed_candidates = []
    
    try:
        json_config = AnalysisConfig(
            mode="json",
            batch_size=config.batch_size,
            enable_caching=config.enable_caching,
            cache_ttl_minutes=config.cache_ttl_minutes,
            api_timeout_seconds=config.api_timeout_seconds,
            max_retries=config.max_retries
        )
        json_results = _execute_json_analysis_pipeline(candidates, json_config)
        
        # JSON 분석 성공률 확인
        success_rate = len(json_results) / len(candidates) if candidates else 0
        if success_rate < 0.8:  # 80% 미만 성공 시
            logging.warning(f"⚠️ JSON 분석 성공률 낮음: {success_rate:.2%}")
            successful_tickers = {r["ticker"] for r in json_results}
            failed_candidates = [c for c in candidates if c["ticker"] not in successful_tickers]
            
    except Exception as e:
        logging.warning(f"⚠️ JSON 분석 파이프라인 실패: {e}")
        failed_candidates = candidates
    
    # 2차: 실패한 항목에 대해 차트 분석
    chart_results = []
    if failed_candidates:
        logging.info(f"🔄 {len(failed_candidates)}개 항목에 대해 차트 분석 실행")
        chart_config = AnalysisConfig(
            mode="chart",
            batch_size=config.batch_size,
            enable_caching=config.enable_caching,
            cache_ttl_minutes=config.cache_ttl_minutes,
            api_timeout_seconds=config.api_timeout_seconds,
            max_retries=config.max_retries
        )
        chart_results = _execute_chart_analysis_pipeline(failed_candidates, chart_config)
    
    # 결과 통합
    combined_results = json_results + chart_results
    
    # 분석 방법 태깅
    for result in json_results:
        if "analysis_method" not in result:
            result["analysis_method"] = "json"
    for result in chart_results:
        if "analysis_method" not in result:
            result["analysis_method"] = "chart_fallback"
    
    return combined_results

def _create_batches(items: List[Any], batch_size: int) -> List[List[Any]]:
    """항목을 배치로 분할"""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def _call_gpt_json_batch(batch: List[dict], config: AnalysisConfig, optimizer: GPTAnalysisOptimizerSingleton) -> List[dict]:
    """JSON 배치 GPT 분석 호출"""
    results = []
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    for candidate in batch:
        ticker = candidate["ticker"]
        start_time = time.time()
        
        try:
            # Rate limiting 적용
            optimizer.manage_api_rate_limits()
            
            # GPT 분석용 JSON 데이터 생성 (data_fetcher의 함수 사용) - 200일로 확장
            json_data = generate_gpt_analysis_json(ticker, days=200)
            if json_data is None:
                logging.warning(f"⚠️ {ticker} JSON 데이터 생성 실패")
                results.append({
                    "ticker": ticker,
                    "score": candidate.get("base_score", 50),
                    "confidence": 0.50,
                    "analysis_method": "json_data_failed"
                })
                continue
            
            # 토큰 수 계산
            enc = tiktoken.encoding_for_model("gpt-4o")
            token_count = len(enc.encode(json_data))
            
            # GPT 분석 실행 - system_prompt 사용 및 JSON 형식 요구
            messages = [
                {
                    "role": "system", 
                    "content": system_prompt  # system_prompt.txt 내용 사용
                },
                {
                    "role": "user",
                    "content": f"""
Analyze this PRE-FILTERED buy candidate from the Makenaide trading system:

[Ticker] {ticker}
[OHLCV and Technical Indicators Data]
{json_data}

This ticker has already passed multi-stage filtering (blacklist, volume, technical indicators) 
and should theoretically be in Stage 1-2 with upward momentum patterns. 

Verify if this is a genuine uptrend candidate or a false signal that should be avoided.

RESPOND ONLY with this exact JSON format (no additional text):
{{
  "ticker": "{ticker}",
  "score": {{integer_0_to_100}},
  "confidence": {{decimal_0_to_1}},
  "action": "BUY | HOLD | AVOID",
  "market_phase": "Stage1 | Stage2 | Stage3 | Stage4",
  "pattern": "{{pattern_name}}",
  "reason": "{{brief_explanation_max_200_chars}}"
}}
                    """
                }
            ]

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0.3,
                max_tokens=500,
                timeout=config.api_timeout_seconds
            )

            processing_time = time.time() - start_time
            output_tokens = response.usage.completion_tokens if hasattr(response, 'usage') else 0
            
            # 성공 모니터링 기록
            optimizer.monitor.track_api_call(token_count, processing_time, True, output_tokens=output_tokens)

            content = response.choices[0].message.content
            
            # 🔍 상세 응답 로깅 추가
            logging.info(f"📤 {ticker} GPT 응답 수신:")
            logging.info(f"   - 응답 길이: {len(content)} characters")
            logging.info(f"   - 응답 내용 (첫 200자): {content[:200]}...")
            logging.info(f"   - 전체 응답:\n{content}")
            
            # JSON 파싱으로 완전한 분석 결과 추출
            gpt_score = 50  # 기본값
            confidence = 0.85  # 기본 confidence
            action = "AVOID"  # 기본값
            market_phase = "Unknown"  # 기본값
            pattern = "Unknown"  # 기본값
            reason = "Empty"  # 기본값
            
            try:
                # JSON 응답 파싱
                import json
                # JSON 응답에서 불필요한 텍스트 제거
                clean_content = content.strip()
                if clean_content.startswith("```json"):
                    clean_content = clean_content[7:]
                if clean_content.endswith("```"):
                    clean_content = clean_content[:-3]
                
                parsed_result = json.loads(clean_content.strip())
                
                # 각 필드 추출 및 검증
                gpt_score = int(parsed_result.get("score", 50))
                gpt_score = max(0, min(100, gpt_score))  # 범위 검증
                
                confidence = float(parsed_result.get("confidence", 0.85))
                confidence = max(0.0, min(1.0, confidence))  # 범위 검증
                
                action = parsed_result.get("action", "AVOID").upper()
                if action not in ["BUY", "HOLD", "AVOID"]:
                    action = "AVOID"
                
                market_phase = parsed_result.get("market_phase", "Unknown")
                if market_phase not in ["Stage1", "Stage2", "Stage3", "Stage4"]:
                    market_phase = "Unknown"
                
                pattern = parsed_result.get("pattern", "Unknown")
                reason = parsed_result.get("reason", "Empty")[:200]  # 200자 제한
                
                logging.info(f"✅ {ticker} JSON 파싱 성공: score={gpt_score}, action={action}, phase={market_phase}")
                
            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ {ticker} JSON 파싱 실패, 폴백 처리: {str(e)}")
                # 폴백: 텍스트에서 점수 추출 시도
                import re
                score_match = re.search(r'"score":\s*(\d+)', content)
                if score_match:
                    gpt_score = int(score_match.group(1))
                    gpt_score = max(0, min(100, gpt_score))
                
                # 액션 추출 시도
                action_match = re.search(r'"action":\s*"([^"]+)"', content)
                if action_match:
                    action = action_match.group(1).upper()
                    if action not in ["BUY", "HOLD", "AVOID"]:
                        action = "AVOID"
                
            except Exception as e:
                logging.warning(f"⚠️ {ticker} 응답 파싱 완전 실패: {str(e)}")

            logging.info(f"✅ {ticker} JSON 방식 GPT 분석 완료: {gpt_score}점 (confidence: {confidence:.2f}, 시간: {processing_time:.2f}s)")
            
            results.append({
                "ticker": ticker,
                "score": gpt_score,
                "confidence": confidence,
                "action": action,
                "market_phase": market_phase,
                "pattern": pattern,
                "reason": reason,
                "analysis_method": "json",
                "processing_time": processing_time,
                "token_usage": token_count
            })
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_result = optimizer.error_handler.handle_api_error(e, ticker, candidate.get("base_score", 50))
            
            # 실패 모니터링 기록
            error_type = error_result.get("error_details", {}).get("type", "unknown")
            token_count = token_count if 'token_count' in locals() else 0
            optimizer.monitor.track_api_call(token_count, processing_time, False, error_type)
            
            # 오류 분석 로깅
            optimizer.error_handler.log_error_analytics(error_result.get("error_details", {}), ticker)
            
            logging.warning(f"⚠️ {ticker} JSON 방식 GPT 분석 실패: {str(e)}")
            results.append({
                "ticker": ticker,
                "score": error_result.get("score", candidate.get("base_score", 50)),
                "confidence": error_result.get("confidence", 0.50),
                "analysis_method": "json_failed",
                "error_details": error_result.get("error_details", {})
            })
    
    return results

def _fallback_individual_processing(batch: List[dict], config: AnalysisConfig) -> List[dict]:
    """개별 처리 폴백"""
    results = []
    for candidate in batch:
        ticker = candidate["ticker"]
        logging.warning(f"🔄 {ticker} 개별 처리 폴백 실행")
        results.append({
            "ticker": ticker,
            "score": candidate.get("base_score", 50),
            "confidence": 0.50,
            "analysis_method": "fallback"
        })
    return results

def reload_system_prompt():
    """동적 프롬프트 업데이트 지원 (config_loader 기반)"""
    global system_prompt
    from config_loader import reload_system_prompt
    system_prompt = reload_system_prompt()
    logging.info("시스템 프롬프트 재로딩 완료")

# 프롬프트 관리는 config_loader로 통합
from config_loader import get_cached_system_prompt
system_prompt = get_cached_system_prompt()

def fetch_selected_market_data(ticker_list):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()
    placeholders = ",".join("%s" for _ in ticker_list)
    query = f"""
        SELECT o.ticker, o.close as price, o.ma_50, o.ma_200, o.rsi_14, 
               NULL as mfi_14, o.ht_trendline, o.fibo_382, o.fibo_618,
               s.r1, s.s1, 'charts/' || o.ticker || '.png' as chart_path, 
               NULL as position_avg_price, o.date as updated_at,
               s.volume_change_7_30, s.supertrend_signal
        FROM (
            SELECT DISTINCT ON (ticker) ticker, close, ma_50, ma_200, rsi_14, 
                   ht_trendline, fibo_382, fibo_618, date
            FROM ohlcv 
            WHERE ticker IN ({placeholders})
            ORDER BY ticker, date DESC
        ) o
        LEFT JOIN static_indicators s ON o.ticker = s.ticker
    """
    cur.execute(query, ticker_list)
    data = cur.fetchall()
    conn.close()
    return data

# 환경 변수 및 OpenAI 클라이언트 설정 (API 키는 main.py에서 처리)
client = OpenAI()

def get_current_price_safe(ticker, retries=3, delay=0.3):
    import time
    import pyupbit
    attempt = 0
    while attempt < retries:
        try:
            price_data = pyupbit.get_current_price(ticker)
            if price_data is None:
                raise ValueError("No data returned")
            # 만약 숫자형이면 바로 반환
            if isinstance(price_data, (int, float)):
                return price_data
            # 딕셔너리인 경우: 티커 키 또는 'trade_price' 키 활용
            if isinstance(price_data, dict):
                if ticker in price_data:
                    return price_data[ticker]
                elif 'trade_price' in price_data:
                    return price_data['trade_price']
                else:
                    first_val = next(iter(price_data.values()), None)
                    if first_val is not None:
                        return first_val
            # 리스트인 경우: 첫 번째 요소에서 'trade_price' 키 추출
            if isinstance(price_data, list) and len(price_data) > 0:
                trade_price = price_data[0].get("trade_price")
                if trade_price is not None:
                    return trade_price
            raise ValueError(f"Unexpected data format: {price_data}")
        except Exception as e:
            print(f"[ERROR] get_current_price_safe error for {ticker}: {e}")
            attempt += 1
            time.sleep(delay)
    return None
# market_data에서 데이터를 가져와 GPT API에 전송
def fetch_market_data():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()
    
    cur.execute("""
        SELECT o.ticker, o.close as price, o.ma_50, o.ma_200, o.rsi_14, 
               NULL as mfi_14, o.ht_trendline, o.fibo_382, o.fibo_618,
               s.r1, s.s1, 'charts/' || o.ticker || '.png' as chart_path, 
               NULL as position_avg_price, o.date as updated_at,
               s.volume_change_7_30, s.supertrend_signal
        FROM (
            SELECT DISTINCT ON (ticker) ticker, close, ma_50, ma_200, rsi_14, 
                   ht_trendline, fibo_382, fibo_618, date
            FROM ohlcv 
            ORDER BY ticker, date DESC
        ) o
        LEFT JOIN static_indicators s ON o.ticker = s.ticker
    """)
    data = cur.fetchall()
    
    conn.close()
    return data

# GPT API 호출 함수 (표준화된 응답 형식)
def analyze_trend_with_gpt(ticker: str, daily_data: dict, daily_chart_image_path: str, db_manager: DBManager):
    """
    GPT를 사용하여 일봉 데이터와 차트 이미지를 기반으로 추세를 분석합니다.
    """
    try:
        # 1. 최적화기 인스턴스 가져오기
        optimizer = GPTAnalysisOptimizerSingleton()
        
        # 2. 캐시 확인 (올바른 메서드명 사용)
        cached_result = optimizer.cache_manager.get_from_cache(ticker, daily_data)
        if cached_result:
            logging.info(f"🔄 {ticker} 캐시된 분석 결과 사용")
            return cached_result
        
        # 3. 레이트 리미팅 적용
        optimizer.rate_limiter.wait_if_needed()
        
        # 4. 차트 이미지 base64 인코딩
        import base64
        if not os.path.exists(daily_chart_image_path):
            from data_fetcher import generate_chart_image
            generate_chart_image(ticker)
        
        with open(daily_chart_image_path, "rb") as image_file:
            chart_base64 = base64.b64encode(image_file.read()).decode('utf-8')
        
        # 5. 기술적 지표 데이터 준비
        indicators_summary = prepare_indicators_summary(daily_data)
        
        # 6. GPT 프롬프트 생성
        prompt = f"""
당신은 전문 퀀트 애널리스트입니다. 다음 암호화폐의 차트와 기술적 지표를 분석하여 투자 의견을 제시해주세요.

티커: {ticker}
현재가: {daily_data.get('price', 'N/A')}
MA50: {daily_data.get('ma_50', 'N/A')}
MA200: {daily_data.get('ma_200', 'N/A')}
RSI: {daily_data.get('rsi_14', 'N/A')}
거래량 변화: {daily_data.get('volume_change_7_30', 'N/A')}%

다음 형식으로 답변해주세요:
Market Phase: [Stage 1/Stage 2/Stage 3/Stage 4/Unknown]
Confidence: [0.0-1.0]
Action: [BUY/SELL/HOLD]
Summary: [간단한 분석 요약]
Score: [0-100점]
"""
        
        # 7. OpenAI API 호출
        start_time = time.time()
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{chart_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=1000,
            temperature=0.3
        )
        
        processing_time = time.time() - start_time
        gpt_response = response.choices[0].message.content
        
        # 🔍 상세 응답 로깅 추가
        logging.info(f"📤 {ticker} GPT 응답 수신:")
        logging.info(f"   - 응답 길이: {len(gpt_response)} characters")
        logging.info(f"   - 응답 내용 (첫 200자): {gpt_response[:200]}...")
        logging.info(f"   - 전체 응답:\n{gpt_response}")
        
        # 8. 응답 파싱 및 검증
        parsed_result = parse_gpt_response(gpt_response)
        
        # 9. 필수 필드 검증 및 기본값 설정
        validated_result = {
            'ticker': ticker,
            'market_phase': parsed_result.get('market_phase', 'Unknown'),
            'confidence': max(0.0, min(1.0, float(parsed_result.get('confidence', 0.5)))),
            'action': parsed_result.get('action', 'HOLD'),
            'summary': parsed_result.get('summary', '분석 완료'),
            'score': max(0, min(100, int(parsed_result.get('score', 50)))),
            'processing_time': processing_time,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # 10. DB 저장
        save_enhanced_analysis_to_db(validated_result, db_manager)
        
        # 11. 캐시 저장
        optimizer.cache_manager.save_to_cache(ticker, daily_data, validated_result)
        
        # 12. 모니터링 기록
        token_usage = response.usage.total_tokens if hasattr(response, 'usage') else 0
        optimizer.monitor.track_api_call(token_usage, processing_time, True)
        
        logging.info(f"✅ {ticker} GPT 분석 완료: {validated_result['market_phase']}, 신뢰도: {validated_result['confidence']:.2f}")
        
        return validated_result
        
    except Exception as e:
        # 에러 처리
        logging.error(f"❌ {ticker} GPT 분석 중 오류: {str(e)}")
        
        error_result = {
            'ticker': ticker,
            'error': str(e),
            'market_phase': 'Unknown',
            'confidence': 0.0,
            'action': 'HOLD',
            'summary': f'분석 실패: {str(e)}',
            'score': 50
        }
        
        # 실패 모니터링
        if 'optimizer' in locals():
            optimizer.monitor.track_api_call(0, 0, False, type(e).__name__)
        
        return error_result

def prepare_indicators_summary(daily_data: dict) -> str:
    """
    기술적 지표들을 GPT가 이해하기 쉬운 텍스트 요약으로 변환
    
    Args:
        daily_data (dict): 기술적 지표 데이터
        
    Returns:
        str: 지표 요약 텍스트
    """
    try:
        summary_parts = []
        
        # RSI 분석
        rsi = daily_data.get('rsi_14')
        if rsi:
            if rsi > 70:
                summary_parts.append(f"RSI 과매수({rsi:.1f})")
            elif rsi < 30:
                summary_parts.append(f"RSI 과매도({rsi:.1f})")
            else:
                summary_parts.append(f"RSI 중립({rsi:.1f})")
        
        # 이동평균 분석
        ma_50 = daily_data.get('ma_50')
        ma_200 = daily_data.get('ma_200')
        current_price = daily_data.get('price', 0)
        
        if ma_50 and ma_200 and current_price:
            if current_price > ma_50 > ma_200:
                summary_parts.append("강세 정렬")
            elif current_price < ma_50 < ma_200:
                summary_parts.append("약세 정렬")
            else:
                summary_parts.append("혼조 정렬")
        
        # 거래량 분석
        volume_change = daily_data.get('volume_change_7_30')
        if volume_change:
            if volume_change > 50:
                summary_parts.append(f"거래량 급증({volume_change:.0f}%)")
            elif volume_change < -30:
                summary_parts.append(f"거래량 위축({volume_change:.0f}%)")
        
        # 지지/저항 분석
        support = daily_data.get('s1')
        resistance = daily_data.get('r1')
        if support and resistance and current_price:
            support_distance = ((current_price - support) / support) * 100
            resistance_distance = ((resistance - current_price) / current_price) * 100
            summary_parts.append(f"지지선+{support_distance:.1f}%, 저항선-{resistance_distance:.1f}%")
        
        # 슈퍼트렌드 분석
        supertrend = daily_data.get('supertrend_signal')
        if supertrend:
            if supertrend.lower() == 'up':
                summary_parts.append("슈퍼트렌드 상승")
            elif supertrend.lower() == 'down':
                summary_parts.append("슈퍼트렌드 하락")
        
        # 피보나치 분석
        fibo_382 = daily_data.get('fibo_382')
        fibo_618 = daily_data.get('fibo_618')
        if fibo_382 and fibo_618 and current_price:
            if current_price > fibo_618:
                summary_parts.append("피보나치 61.8% 돌파")
            elif current_price > fibo_382:
                summary_parts.append("피보나치 38.2%-61.8% 구간")
            else:
                summary_parts.append("피보나치 38.2% 미만")
        
        return "; ".join(summary_parts) if summary_parts else "정상 범위"
        
    except Exception as e:
        logging.error(f"❌ indicators_summary 생성 중 오류: {str(e)}")
        return "지표 분석 실패"

def parse_gpt_response(gpt_response: str) -> dict:
    """GPT 응답 파싱"""
    result = {}
    
    for line in gpt_response.split('\n'):
        line = line.strip()
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower().replace(' ', '_')
            value = value.strip()
            
            if key == 'market_phase':
                result['market_phase'] = value
            elif key == 'confidence':
                try:
                    result['confidence'] = float(value)
                except:
                    result['confidence'] = 0.5
            elif key == 'action':
                result['action'] = value.upper()
            elif key == 'summary':
                result['summary'] = value
            elif key == 'score':
                try:
                    result['score'] = int(value)
                except:
                    result['score'] = 50
    
    return result

def validate_and_standardize_gpt_response(ticker: str, raw_response: dict, daily_data: dict) -> dict:
    """
    GPT 응답을 검증하고 표준화된 형식으로 변환
    
    표준화 항목:
    1. 필수 필드 존재 확인 및 기본값 설정
    2. 이상값 필터링 (confidence, score 범위 체크)
    3. market_phase 값 검증 및 보정
    4. action 값 표준화
    """
    try:
        if not raw_response or raw_response.get('error'):
            return create_standardized_error_response(ticker, "GPT 응답 오류", 0.2)
        
        # 1. 기본 필드 추출 및 검증 - market_phase 기본값 개선
        score = raw_response.get('score', 50)
        confidence = raw_response.get('confidence', score / 100.0 if isinstance(score, (int, float)) else 0.5)
        action = raw_response.get('action', 'HOLD')
        
        # market_phase 추출 - 여러 가능한 필드명 시도
        market_phase = (
            raw_response.get('market_phase') or 
            raw_response.get('phase') or 
            raw_response.get('stage') or 
            'Stage1'  # Unknown 대신 Stage1을 기본값으로 사용
        )
        
        comment = raw_response.get('comment', raw_response.get('reason', ''))
        
        # 2. 값 범위 검증 및 보정
        # 점수 범위 검증 (0-100)
        if not isinstance(score, (int, float)) or score < 0 or score > 100:
            logging.warning(f"⚠️ {ticker} 이상한 점수값: {score} → 50으로 보정")
            score = 50
        
        # 신뢰도 범위 검증 (0-1)
        if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
            logging.warning(f"⚠️ {ticker} 이상한 신뢰도값: {confidence} → 점수 기반으로 보정")
            confidence = score / 100.0
        
        # 3. 액션 값 표준화
        action = standardize_action_value(action)
        
        # 4. market_phase 검증 및 보정 - 간단한 형식 검증만 수행
        if market_phase and isinstance(market_phase, str):
            # Stage1, Stage2, Stage3, Stage4 중 하나인지 확인
            valid_phases = ['Stage1', 'Stage2', 'Stage3', 'Stage4']
            if market_phase not in valid_phases:
                logging.warning(f"⚠️ {ticker} 잘못된 market_phase: {market_phase} → Stage1로 보정")
                market_phase = 'Stage1'
        else:
            market_phase = 'Stage1'
        
        # 5. 추가 분석 메타데이터 생성
        analysis_quality = assess_analysis_quality(raw_response, daily_data)
        
        # 6. 표준화된 응답 구조 생성
        standardized_response = {
            "ticker": ticker,
            "action": action,
            "confidence": confidence,
            "score": score,
            "market_phase": market_phase,
            "comment": comment[:500] if comment else "분석 코멘트 없음",  # 길이 제한
            "analysis_quality": analysis_quality,
            "validation_status": "validated",
            "raw_gpt_response": raw_response,
            "error": None,
            "timestamp": datetime.now().isoformat(),
            "analysis_method": "gpt_standardized"
        }
        
        # 7. 최종 검증 - 필수 필드 재확인
        required_fields = ['ticker', 'action', 'confidence', 'market_phase']
        for field in required_fields:
            if field not in standardized_response or standardized_response[field] is None:
                logging.error(f"❌ {ticker} 필수 필드 누락: {field}")
                return create_standardized_error_response(ticker, f"필수 필드 누락: {field}", 0.2)
        
        return standardized_response
        
    except Exception as e:
        logging.error(f"❌ {ticker} 응답 표준화 중 오류: {str(e)}")
        return create_standardized_error_response(ticker, f"표준화 오류: {str(e)}", 0.1)


def standardize_action_value(action: str) -> str:
    """
    액션 값을 표준화된 형식으로 변환
    """
    if not isinstance(action, str):
        return 'HOLD'
    
    action_upper = action.upper().strip()
    
    # 표준 액션 매핑
    action_mapping = {
        'BUY': 'BUY',
        'STRONG_BUY': 'BUY',
        'WEAK_BUY': 'BUY_WEAK',
        'BUY_WEAK': 'BUY_WEAK',
        'SELL': 'SELL',
        'STRONG_SELL': 'SELL',
        'WEAK_SELL': 'SELL_WEAK',
        'SELL_WEAK': 'SELL_WEAK',
        'HOLD': 'HOLD',
        'WAIT': 'HOLD',
        'NEUTRAL': 'HOLD'
    }
    
    # 부분 매칭 시도
    for key, value in action_mapping.items():
        if key in action_upper:
            return value
    
    # 기본값
    return 'HOLD'


def assess_analysis_quality(raw_response: dict, daily_data: dict) -> dict:
    """
    분석 품질 평가
    """
    quality_score = 0.0
    quality_factors = []
    
    # 1. 응답 완성도 체크
    if raw_response.get('comment') and len(raw_response['comment']) > 50:
        quality_score += 0.3
        quality_factors.append("상세한 코멘트")
    
    # 2. 점수와 신뢰도 일관성 체크
    score = raw_response.get('score', 50)
    confidence = raw_response.get('confidence', 0.5)
    if abs(score/100.0 - confidence) < 0.2:  # 20% 이내 차이
        quality_score += 0.2
        quality_factors.append("점수-신뢰도 일관성")
    
    # 3. 기술적 지표와 액션 일관성 체크
    action = raw_response.get('action', 'HOLD')
    if daily_data:
        rsi = daily_data.get('rsi_14', 50)
        if action in ['BUY', 'BUY_WEAK'] and rsi < 70:
            quality_score += 0.25
            quality_factors.append("기술적 지표 일관성")
        elif action in ['SELL', 'SELL_WEAK'] and rsi > 30:
            quality_score += 0.25
            quality_factors.append("기술적 지표 일관성")
        elif action == 'HOLD':
            quality_score += 0.15
            quality_factors.append("중립적 판단")
    
    # 4. 응답 구조 완정성
    required_fields = ['score', 'action', 'confidence']
    if all(field in raw_response for field in required_fields):
        quality_score += 0.25
        quality_factors.append("완전한 응답 구조")
    
    # 품질 등급 결정
    if quality_score >= 0.8:
        quality_grade = "A (우수)"
    elif quality_score >= 0.6:
        quality_grade = "B (양호)"
    elif quality_score >= 0.4:
        quality_grade = "C (보통)"
    else:
        quality_grade = "D (개선 필요)"
    
    return {
        "score": quality_score,
        "grade": quality_grade,
        "factors": quality_factors,
        "completeness": len(quality_factors) / 4  # 최대 4개 요인
    }


def create_standardized_error_response(ticker: str, error_message: str, fallback_confidence: float = 0.3) -> dict:
    """
    표준화된 오류 응답 생성
    """
    return {
        "ticker": ticker,
        "action": "HOLD",
        "confidence": fallback_confidence,
        "score": int(fallback_confidence * 100),
        "market_phase": "Stage1",  # Unknown 대신 Stage1 사용
        "comment": f"분석 실패: {error_message}",
        "analysis_quality": {
            "score": 0.0,
            "grade": "F (실패)",
            "factors": [],
            "completeness": 0.0
        },
        "validation_status": "error",
        "raw_gpt_response": None,
        "error": error_message,
        "timestamp": datetime.now().isoformat(),
        "analysis_method": "error_fallback"
    }

# 트렌드 분석 결과를 DB에 저장
def save_trend_analysis_to_db(analysis, db_manager):
    """
    트렌드 분석 결과를 DB에 저장
    analysis: 분석 결과 딕셔너리
    db_manager: DBManager 인스턴스
    """
    try:
        ticker = analysis.get('ticker', '')
        action = analysis.get('action', 'HOLD')
        type_ = analysis.get('type', 'AUTO')
        reason = analysis.get('reason', '')
        pattern = analysis.get('pattern', '')
        market_phase = analysis.get('market_phase', 'Stage1')  # Unknown 대신 Stage1 사용
        confidence = analysis.get('confidence', 50)
        score = analysis.get('score', 50)
        
        # Market Phase 조건 체크
        if market_phase not in ("Stage 1", "Stage 2") and not analysis.get('always_save', False):
            logging.info(f"⏭️ {ticker}: Market Phase {market_phase}, 저장 생략됨")
            return
        
        query = """
            INSERT INTO trend_analysis (ticker, action, type, reason, pattern, market_phase, confidence, score, time_window, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT(ticker) DO UPDATE SET
                action = excluded.action,
                type = excluded.type,
                reason = excluded.reason,
                pattern = excluded.pattern,
                market_phase = excluded.market_phase,
                confidence = excluded.confidence,
                score = excluded.score,
                updated_at = CURRENT_TIMESTAMP;
        """
        values = (ticker, action, type_, reason, pattern, market_phase, confidence, score, "자동매매")
        
        db_manager.execute_query(query, values)
        logging.info(f"✅ {ticker} GPT 분석 결과 DB 저장 완료 (score: {score})")
        
    except Exception as e:
        logging.error(f"❌ {analysis.get('ticker', 'Unknown')} DB 저장 실패: {str(e)}")

def save_trend_analysis_log(ticker, action, confidence, time_window, db_manager):
    """
    트렌드 분석 로그를 DB에 저장
    """
    try:
        query = """
        INSERT INTO trend_analysis_log (ticker, action, confidence, time_window)
        VALUES (%s, %s, %s, %s);
        """
        values = (ticker, action, confidence, time_window)
        db_manager.execute_query(query, values)
        logging.info(f"✅ {ticker} trend_analysis_log 저장 완료")
    except Exception as e:
        logging.error(f"❌ {ticker} trend_analysis_log 저장 실패: {str(e)}")

# 전체 실행 함수
def main():
    """
    메인 함수 - DBManager를 사용하여 모든 DB 작업을 통일화
    """
    try:
        with DBManager() as db_manager:
            logging.info("🔄 trend_analyzer 메인 실행 시작")
            
            market_data = fetch_market_data()
            
            for entry in market_data:
                ticker = entry[0]
                position_avg_price = entry[19]
                last_updated_at = entry[20]

                # DBManager를 사용하여 previous_action 조회
                try:
                    query = "SELECT action FROM trend_analysis WHERE ticker = %s"
                    result = db_manager.fetch_one(query, (ticker,))
                    previous_action = result[0] if result else "UNKNOWN"
                except Exception as e:
                    logging.warning(f"⚠️ {ticker} previous_action 조회 실패: {str(e)}")
                    previous_action = "UNKNOWN"

                data = {
                    "price": entry[1], "ma_50": entry[2], "ma_200": entry[3], "rsi_14": entry[4], "mfi_14": entry[5],
                    "ht_trendline": entry[6], "fibo_382": entry[7], "fibo_618": entry[8], 
                    "r1": entry[9], "s1": entry[10], "chart_path": entry[11],
                    "position_avg_price": position_avg_price,
                    "last_updated_at": last_updated_at,
                    "previous_action": previous_action,
                    "volume_change_7_30": entry[13] if len(entry) > 13 else None,
                    "supertrend_signal": entry[14] if len(entry) > 14 else None,
                }
                
                # DBManager를 사용하여 4H market data 조회
                try:
                    query_4h = """
                        SELECT ma_50, ma_200, rsi_14, ht_trendline,
                               fibo_382, fibo_618,
                               r1, s1, chart_path
                        FROM market_data_4h
                        WHERE ticker = %s
                    """
                    row_4h = db_manager.fetch_one(query_4h, (ticker,))
                    if row_4h:
                        keys_4h = ["ma_50", "ma_200", "rsi_14", "ht_trendline",
                                   "fibo_382", "fibo_618",
                                   "r1", "s1", "chart_path"]
                        data.update({f"4h_{k}": v for k, v in zip(keys_4h, row_4h)})
                        data["4h_chart_path"] = row_4h[-1].replace("_4h", "").replace("_", "-") if row_4h[-1] else None
                except Exception as e:
                    logging.warning(f"⚠️ {ticker} 4시간봉 market_data_4h 로딩 실패: {str(e)}")

                print(f"📊 {ticker} 분석 진행 중...")
                if not data or not data.get("chart_path"):
                    print(f"⚠️ Data or chart_path not available for ticker: {ticker}. Skipping GPT analysis.")
                    logging.warning(f"⚠️ Data or chart_path not available for ticker: {ticker}. Skipping GPT analysis.")
                    continue
                
                # analyze_trend_with_gpt 호출 시 db_manager 전달
                analysis_result_dict = analyze_trend_with_gpt(ticker, data, data["chart_path"], db_manager)
                
                if analysis_result_dict and analysis_result_dict.get("error") is None:
                    market_phase = analysis_result_dict.get("market_phase", "Stage1")  # Unknown 대신 Stage1 사용
                    confidence = analysis_result_dict.get("confidence", 0.0)
                    action = "BUY" if market_phase and ("Stage 2" in market_phase or "Stage1->Stage2" in market_phase) and confidence >= 0.7 else "HOLD"

                    print(f"✅ {ticker} 분석 완료. Market Phase: {market_phase}, Confidence: {confidence:.2f}")
                    logging.info(f"✅ {ticker} 분석 완료. Market Phase: {market_phase}, Confidence: {confidence:.2f}. Action (derived): {action}")

                elif analysis_result_dict and analysis_result_dict.get("error"):
                    print(f"❌ {ticker} 분석 중 오류: {analysis_result_dict.get('summary')}")
                    logging.error(f"❌ {ticker} 분석 중 오류: {analysis_result_dict.get('summary')}, Raw Response: {analysis_result_dict.get('raw_gpt_response')}")
                else:
                    print(f"❌ {ticker} 분석 결과가 없거나 형식이 잘못되었습니다.")
                    logging.error(f"❌ {ticker} 분석 결과가 없거나 형식이 잘못되었습니다. Result: {analysis_result_dict}")
                    
            logging.info("✅ trend_analyzer 메인 실행 완료")
            
    except Exception as e:
        logging.error(f"❌ trend_analyzer 메인 실행 중 오류: {str(e)}")
        raise

def get_recent_gpt_response(ticker, max_age_minutes=720, db_manager=None):
    """
    최근 GPT 응답 중 주어진 티커에 해당하는 가장 최신 결과를 가져옵니다.
    max_age_minutes: 유효한 캐시의 최대 시간(기본: 12시간 = 720분)
    db_manager: DBManager 인스턴스 (없으면 새로 생성)
    """
    should_close_db = False
    try:
        if db_manager is None:
            db_manager = DBManager()
            should_close_db = True
            
        query = """
            SELECT action, type, reason, pattern, market_phase, confidence, created_at
            FROM trend_analysis
            WHERE ticker = %s
            AND updated_at >= NOW() - INTERVAL '%s minutes'
            ORDER BY updated_at DESC
            LIMIT 1
        """
        row = db_manager.fetch_one(query, (ticker, max_age_minutes))
        if row:
            return {
                "action": row[0],
                "type": row[1],
                "reason": row[2],
                "pattern": row[3],
                "market_phase": row[4],
                "confidence": int(row[5]),
                "created_at": row[6]
            }
        else:
            return None
    except Exception as e:
        logging.error(f"❌ 최근 GPT 응답 조회 실패 ({ticker}): {str(e)}")
        return None
    finally:
        if should_close_db and db_manager:
            db_manager.close()

def should_reuse_gpt_response(ticker, current_data, max_age_minutes=720):
    """
    최근 GPT 응답이 유효한지 판단하고, 유효하면 응답 dict를 반환합니다.
    max_age_minutes: 캐시 유효 시간(분, 기본 720분=12시간)
    current_data: 현재 마켓 데이터 딕셔너리 (support, resistance, r1, s1 등 포함)
    반환값: 응답 dict 또는 None
    """
    prev_response = get_recent_gpt_response(ticker, max_age_minutes)
    if not prev_response:
        return None

    # 설정 파일에서 임계치 로드 (지표별 허용 변동 비율)
    config = load_config()
    thresholds = config.get("gpt_cache_thresholds", {
        "support": 0.01,
        "resistance": 0.015,
        "r1": 0.015,
        "s1": 0.015
    })

    # market_data 비교를 위해 필요한 키 추출
    # prev_response에는 support, resistance 등이 없으므로 가져온 DB trend_analysis 레코드에는 포함되지 않음.
    # 대신, prev_response의 'reason' 등 외, 이전 market_data는 trend_analysis에 저장된 pattern/market_phase만 있어,
    # 따라서 require to store prev market_data in DB: but for now assume prev_response includes those keys.
    # Here, we assume prev_response dict has keys: support, resistance, r1, s1.
    if has_significant_market_change(prev_response, current_data, thresholds):
        print(f"[CACHE_EXPIRE] {ticker}: 시장 지표 변동 큼, GPT 재호출 필요")
        return None

    # 캐시 재사용을 위한 지표 변화 확인 결과 로깅
    for key, thresh in thresholds.items():
        prev_val = prev_response.get(key)
        curr_val = current_data.get(key)
        if prev_val is None or curr_val is None:
            continue
        try:
            change_ratio = abs(curr_val - prev_val) / prev_val
        except ZeroDivisionError:
            change_ratio = abs(curr_val - prev_val)
        print(f"[CACHE_METRIC] {ticker}: {key} prev={prev_val}, curr={curr_val}, change={change_ratio:.2%}, threshold={thresh:.2%}")
    print(f"[CACHE_HIT] {ticker}: 캐시 유효 - 모든 지표 변화 {max(thresholds.values())*100:.2f}% 이하")

    return prev_response


# 주요 지표의 변화가 임계값 이상인지 판단하는 함수
def has_significant_market_change(prev_response, current_data, thresholds):
    """
    이전 GPT 응답의 주요 지표와 현재 마켓 데이터 간 변동 여부를 판단합니다.
    prev_response: {'support': float, 'resistance': float, ...}
    current_data: {'support': float, 'resistance': float, ...}
    thresholds: dict, 지표별 허용 변동 비율 예: {'support': 0.01, 'resistance': 0.015}
    반환: True if any indicator changed by >= threshold, else False
    """
    for key, thresh in thresholds.items():
        prev_val = prev_response.get(key)
        curr_val = current_data.get(key)
        if prev_val is None or curr_val is None:
            continue
        # 상대적 변화율 계산
        try:
            change_ratio = abs(curr_val - prev_val) / prev_val
        except ZeroDivisionError:
            # prev_val이 0이면 절대 변화량 기준으로 판단
            change_ratio = abs(curr_val - prev_val)
        if change_ratio >= thresh:
            return True
    return False

def analyze_selected_tickers(ticker_list):
    """
    선택된 티커들을 분석하는 함수 - DBManager 사용
    """
    try:
        with DBManager() as db_manager:
            logging.info(f"🔄 선택된 티커 분석 시작: {ticker_list}")
            
            selected_data = fetch_selected_market_data(ticker_list)

            for entry in selected_data:
                ticker = entry[0]
                position_avg_price = entry[19]
                last_updated_at = entry[20]

                # DBManager를 사용하여 previous_action 조회
                try:
                    query = "SELECT action FROM trend_analysis WHERE ticker = %s"
                    result = db_manager.fetch_one(query, (ticker,))
                    previous_action = result[0] if result else "UNKNOWN"
                except Exception as e:
                    logging.warning(f"⚠️ {ticker} previous_action 조회 실패: {str(e)}")
                    previous_action = "UNKNOWN"

                data = {
                    "price": entry[1], "ma_50": entry[2], "ma_200": entry[3], "rsi_14": entry[4], "mfi_14": entry[5],
                    "ht_trendline": entry[6], 
                    "fibo_382": entry[7], "fibo_618": entry[8], 
                    "r1": entry[9], "s1": entry[10], "chart_path": entry[11],
                    "position_avg_price": position_avg_price,
                    "last_updated_at": last_updated_at,
                    "previous_action": previous_action,
                    "volume_change_7_30": entry[13] if len(entry) > 13 else None,
                    "supertrend_signal": entry[14] if len(entry) > 14 else None,
                }

                # DBManager를 사용하여 4H market data 조회
                try:
                    query_4h = """
                        SELECT ma_50, ma_200, rsi_14, ht_trendline,
                               fibo_382, fibo_618,
                               r1, s1, chart_path
                        FROM market_data_4h
                        WHERE ticker = %s
                    """
                    row_4h = db_manager.fetch_one(query_4h, (ticker,))
                    if row_4h:
                        keys_4h = ["ma_50", "ma_200", "rsi_14", "ht_trendline",
                                   "fibo_382", "fibo_618",
                                   "r1", "s1", "chart_path"]
                        data.update({f"4h_{k}": v for k, v in zip(keys_4h, row_4h)})
                        data["4h_chart_path"] = row_4h[-1].replace("_4h", "").replace("_", "-") if row_4h[-1] else None
                except Exception as e:
                    logging.warning(f"⚠️ {ticker} 4시간봉 market_data_4h 로딩 실패: {str(e)}")

                print(f"📊 {ticker} 선택 분석 진행 중...")
                if not data or not data.get("chart_path"):
                    print(f"⚠️ Data or chart_path not available for ticker: {ticker}. Skipping GPT analysis.")
                    logging.warning(f"⚠️ Data or chart_path not available for ticker: {ticker}. Skipping GPT analysis.")
                    continue
                    
                # analyze_trend_with_gpt 호출 시 db_manager 전달
                analysis_result_dict = analyze_trend_with_gpt(ticker, data, data["chart_path"], db_manager)

                if analysis_result_dict and analysis_result_dict.get("error") is None:
                    market_phase = analysis_result_dict.get("market_phase", "Stage1")  # Unknown 대신 Stage1 사용
                    confidence = analysis_result_dict.get("confidence", 0.0)
                    action = "BUY" if market_phase and ("Stage 2" in market_phase or "Stage1->Stage2" in market_phase) and confidence >= 0.7 else "HOLD"

                    print(f"✅ {ticker} 선택 분석 완료. Market Phase: {market_phase}, Confidence: {confidence:.2f}")
                    logging.info(f"✅ {ticker} 선택 분석 완료. Market Phase: {market_phase}, Confidence: {confidence:.2f}. Action (derived): {action}")

                elif analysis_result_dict and analysis_result_dict.get("error"):
                    print(f"❌ {ticker} 선택 분석 중 오류: {analysis_result_dict.get('summary')}")
                    logging.error(f"❌ {ticker} 선택 분석 중 오류: {analysis_result_dict.get('summary')}, Raw Response: {analysis_result_dict.get('raw_gpt_response')}")
                else:
                    print(f"❌ {ticker} 선택 분석 결과가 없거나 형식이 잘못되었습니다.")
                    logging.error(f"❌ {ticker} 선택 분석 결과가 없거나 형식이 잘못되었습니다. Result: {analysis_result_dict}")

            logging.info("✅ 선택된 티커 분석 완료")
            
    except Exception as e:
        logging.error(f"❌ analyze_selected_tickers 실행 중 오류: {str(e)}")
        raise

def call_gpt_with_chart_base64(ticker: str, chart_base64: str, indicators: dict = None, optimizer: GPTAnalysisOptimizerSingleton = None) -> dict:
    """
    차트 이미지(base64)와 기술적 지표를 이용해 GPT에게 분석을 요청하고 결과를 반환합니다.
    """
    from openai import OpenAI
    import base64
    from loguru import logger

    # 전역 최적화기 인스턴스 사용 또는 새로 생성
    if optimizer is None:
        optimizer = GPTAnalysisOptimizerSingleton()

    def log_token_usage(model: str, messages: list, ticker: str):
        enc = tiktoken.encoding_for_model(model)
        total = 0
        for msg in messages:
            total += 3  # 메시지 헤더 토큰
            for key, val in msg.items():
                if isinstance(val, str):
                    total += len(enc.encode(val))
        total += 3  # 응답 토큰 헤더
        print(f"[TOKEN] {ticker} total input tokens: {total}")
        logging.info(f"[TOKEN] {ticker} total input tokens: {total}")
        return total

    # 프롬프트 구성
    indicator_text = ""
    if indicators:
        # 지표 데이터 최적화
        optimized_indicators = optimizer.optimize_for_tokens(indicators, target_tokens=1000)
        indicator_text = "\n".join([f"{k}: {v}" for k, v in optimized_indicators.items()])
    
    user_prompt = f"""
    [티커] {ticker}

    [기술적 지표]
    {indicator_text}

    아래 차트 이미지를 분석하고, 현재 추세가 상승/하락/횡보 중 무엇인지 판단해 점수(0~100)와 설명을 작성해주세요.
    """

    start_time = time.time()
    
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))  # 명시적으로 API 키 설정
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
            {"role": "user", "content": f"[차트 이미지 - base64]: {chart_base64}"}
        ]
        
        # 토큰 수 계산 및 제한 체크
        token_count = log_token_usage("gpt-4o", messages, ticker)
        if token_count > 80000:
            print(f"[SKIP] {ticker} skipped: input tokens {token_count} exceed threshold (80,000)")
            logging.warning(f"[SKIP] {ticker} skipped: input tokens {token_count} exceed threshold (80,000)")
            
            # 모니터링에 기록
            processing_time = time.time() - start_time
            optimizer.monitor.track_api_call(token_count, processing_time, False, "token_limit_exceeded")
            
            return {
                "score": 50,
                "comment": "토큰 수 초과로 분석 건너뜀"
            }
        
        max_retries = 3
        response = None
        
        for attempt in range(max_retries):
            try:
                # Rate limiting 적용
                optimizer.manage_api_rate_limits()
                
                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                    temperature=0.3,
                    max_tokens=500,
                )
                break  # 성공 시 루프 탈출
                
            except Exception as retry_error:
                error_result = optimizer.error_handler.handle_api_error(retry_error, ticker)
                
                # 재시도 가능한 오류인지 확인
                if optimizer.error_handler.should_retry(error_result.get("error_details", {}), max_retries, attempt):
                    continue
                else:
                    # 재시도 불가능하거나 최대 재시도 횟수 초과
                    processing_time = time.time() - start_time
                    error_type = error_result.get("error_details", {}).get("type", "unknown")
                    optimizer.monitor.track_api_call(token_count, processing_time, False, error_type)
                    
                    # 오류 분석 로깅
                    optimizer.error_handler.log_error_analytics(error_result.get("error_details", {}), ticker)
                    
                    return {
                        "score": error_result.get("score", 50),
                        "comment": error_result.get("error_details", {}).get("message", "분석 실패")
                    }

        # 성공적인 응답 처리
        if response:
            processing_time = time.time() - start_time
            output_tokens = response.usage.completion_tokens if hasattr(response, 'usage') else 0
            
            # 성공 모니터링 기록
            optimizer.monitor.track_api_call(token_count, processing_time, True, output_tokens=output_tokens)
            
            content = response.choices[0].message.content
            
            # 🔍 상세 응답 로깅 추가
            logging.info(f"📤 {ticker} GPT 응답 수신:")
            logging.info(f"   - 응답 길이: {len(content)} characters")
            logging.info(f"   - 응답 내용 (첫 200자): {content[:200]}...")
            logging.info(f"   - 전체 응답:\n{content}")
            
            # 응답 content 로그 출력 (기존 코드 유지)
            wrapped_content = textwrap.indent(textwrap.fill(content, width=120), prefix="│ ")
            logger.info(f"[GPT RESPONSE] {ticker}\n{wrapped_content}")
            
            # 점수 파싱 개선
            lines = content.strip().split("\n")
            score = None
            comment = ""
            
            for line in lines:
                if "점수" in line and ":" in line:
                    try:
                        score_part = line.split(":")[1].strip()
                        # 숫자만 추출
                        import re
                        numbers = re.findall(r'\d+', score_part)
                        if numbers:
                            score = int(numbers[0])
                            # 점수 범위 검증
                            score = max(0, min(100, score))
                    except:
                        score = None
                else:
                    comment += line + "\n"

            return {
                "score": score if score is not None else 50,
                "comment": comment.strip(),
                "processing_time": processing_time,
                "token_usage": token_count
            }

    except Exception as e:
        processing_time = time.time() - start_time
        error_result = optimizer.error_handler.handle_api_error(e, ticker)
        
        # 실패 모니터링 기록
        error_type = error_result.get("error_details", {}).get("type", "unknown")
        optimizer.monitor.track_api_call(token_count if 'token_count' in locals() else 0, processing_time, False, error_type)
        
        # 오류 분석 로깅
        optimizer.error_handler.log_error_analytics(error_result.get("error_details", {}), ticker)
        
        logger.warning(f"[{ticker}] GPT 분석 실패: {str(e)}")
        return {
            "score": error_result.get("score", 50),
            "comment": error_result.get("error_details", {}).get("message", "분석 실패")
        }

def analyze_trend_with_gpt_bulk(candidates: list, optimizer: GPTAnalysisOptimizerSingleton = None) -> list:
    """
    필터링된 종목에 대해 GPT 기반 추세 분석을 수행하고,
    optional_score 기준으로 정렬된 결과를 반환합니다.

    Args:
        candidates (list): [(ticker, score)] 형식의 리스트
        optimizer (GPTAnalysisOptimizerSingleton): 최적화기 인스턴스

    Returns:
        list: GPT 분석 기반 점수로 정렬된 리스트
    """
    import os
    from data_fetcher import generate_chart_image
    import base64

    if optimizer is None:
        optimizer = GPTAnalysisOptimizerSingleton()

    results = []

    logging.info(f"🚀 GPT 벌크 분석 시작: {len(candidates)}개 종목")

    for ticker, base_score in candidates:
        try:
            # 차트 이미지 경로
            chart_path = f"charts/{ticker}.png"
            if not os.path.exists(chart_path):
                generate_chart_image(ticker)

            # 이미지 base64 인코딩
            with open(chart_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            # GPT 분석 실행 (최적화기 전달)
            gpt_result = call_gpt_with_chart_base64(ticker, encoded, optimizer=optimizer)
            gpt_score = gpt_result.get("score", base_score)

        except Exception as e:
            import traceback
            print(f"❌ GPT 분석 실패: {ticker} | {e}")
            traceback.print_exc()
            gpt_score = base_score  # 실패 시 원래 점수 유지

        results.append((ticker, gpt_score))

    # 점수 기준 내림차순 정렬
    results.sort(key=lambda x: x[1], reverse=True)
    
    # 분석 완료 후 효율성 리포트 출력
    efficiency_report = optimizer.monitor.get_efficiency_report()
    cost_alert = optimizer.monitor.get_cost_alert()
    
    logging.info(f"📊 GPT 벌크 분석 완료:")
    logging.info(f"   - 총 호출수: {efficiency_report.get('총_API_호출수', 0)}")
    logging.info(f"   - 성공률: {efficiency_report.get('성공률', '0%')}")
    logging.info(f"   - 오늘 비용: {efficiency_report.get('오늘_비용', '$0.0000')}")
    logging.info(f"   - 비용 경고: {cost_alert.get('message', '')}")
    
    return results

# UNUSED: 호출되지 않는 함수들
# def get_gpt_analysis_performance_report() -> dict:
#     """
#     GPT 분석 성능 리포트를 반환합니다.
#     """
#     optimizer = GPTAnalysisOptimizerSingleton()
#     
#     efficiency_report = optimizer.monitor.get_efficiency_report()
#     cost_alert = optimizer.monitor.get_cost_alert()
#     
#     return {
#         "efficiency_report": efficiency_report,
#         "cost_alert": cost_alert,
#         "timestamp": datetime.now().isoformat()
#     }

# def reset_gpt_analysis_monitor():
#     """
#     GPT 분석 모니터를 리셋합니다. (새로운 세션 시작 시 사용)
#     """
#     global _global_optimizer
#     _global_optimizer = GPTAnalysisOptimizerSingleton()
#     logging.info("🔄 GPT 분석 모니터가 리셋되었습니다.")

# def print_gpt_analysis_summary():
#     """
#     현재 세션의 GPT 분석 요약을 출력합니다.
#     """
#     optimizer = GPTAnalysisOptimizerSingleton()
#     report = optimizer.monitor.get_efficiency_report()
#     alert = optimizer.monitor.get_cost_alert()
#     
#     print("\n" + "="*60)
#     print("📊 GPT 분석 성능 요약")
#     print("="*60)
#     
#     if "error" in report:
#         print(f"⚠️ {report['error']}")
#         return
#     
#     print(f"총 API 호출수: {report['총_API_호출수']}")
#     print(f"성공률: {report['성공률']}")
#     print(f"평균 처리시간: {report['평균_처리시간']}")
#     print(f"평균 토큰사용량: {report['평균_토큰사용량']}")
#     print(f"총 토큰사용량: {report['총_토큰사용량']:,}")
#     print(f"오늘 비용: {report['오늘_비용']}")
#     print(f"총 비용: {report['총_비용']}")
#     print(f"시간당 호출수: {report['시간당_호출수']}")
#     
#     if report['오류_유형별_통계']:
#         print(f"\n오류 유형별 통계:")
#         for error_type, count in report['오류_유형별_통계'].items():
#             print(f"  - {error_type}: {count}회")
#     
#     print(f"\n💰 비용 상태: {alert['message']}")
#     if alert['alert']:
#         level_emoji = "🚨" if alert['level'] == 'critical' else "⚠️"
#         print(f"{level_emoji} 경고 레벨: {alert['level']}")
#     
#     print("="*60 + "\n")

# 전역 최적화기 인스턴스 (선택적)
_global_optimizer = None

def get_optimizer() -> GPTAnalysisOptimizerSingleton:
    """최적화기 싱글톤 인스턴스를 반환합니다"""
    return GPTAnalysisOptimizerSingleton()

def get_global_optimizer() -> GPTAnalysisOptimizerSingleton:
    """전역 최적화기 인스턴스를 반환합니다"""
    return GPTAnalysisOptimizerSingleton()

def analyze_trend_with_gpt_enhanced(ticker: str, daily_data: dict, daily_chart_image_path: str, db_manager: DBManager):
    """
    VCP/Stage 분석을 통합한 향상된 GPT 트렌드 분석 함수
    
    Args:
        ticker (str): 분석할 티커 심볼
        daily_data (dict): 해당 티커의 일봉 지표 데이터
        daily_chart_image_path (str): 차트 이미지 파일 경로
        db_manager (DBManager): 데이터베이스 매니저 인스턴스
        
    Returns:
        dict: 통합 분석 결과
    """
    try:
        logging.info(f"🔍 {ticker} 향상된 트렌드 분석 시작")
        
        # 1. 기존 기술적 지표 데이터 준비
        technical_data = prepare_technical_data_for_analysis(daily_data)
        
        # 2. VCP 패턴 분석 (실제 구현)
        vcp_analysis = detect_vcp_pattern(technical_data)
        logging.info(f"📊 {ticker} VCP 분석 완료: 점수={vcp_analysis.get('score', 0)}")
        
        # 3. Weinstein Stage 분석 (실제 구현)
        stage_analysis = analyze_weinstein_stage(technical_data)
        logging.info(f"📈 {ticker} Stage 분석 완료: {stage_analysis.get('current_stage', 'Unknown')}")
        
        # 4. 브레이크아웃 조건 확인 (실제 구현)
        breakout_conditions = check_breakout_conditions(technical_data, vcp_analysis, stage_analysis)
        logging.info(f"🎯 {ticker} 브레이크아웃 분석 완료: {breakout_conditions.get('action', 'HOLD')}")
        
        # 5. 향상된 프롬프트 생성 (VCP/Stage 분석 결과 포함)
        enhanced_prompt = f"""
당신은 전문 퀀트 애널리스트입니다. 다음 암호화폐의 차트와 기술적 지표를 종합 분석하여 투자 의견을 제시해주세요.

**기본 정보:**
티커: {ticker}
현재가: {daily_data.get('price', 'N/A')}
MA50: {daily_data.get('ma_50', 'N/A')}
MA200: {daily_data.get('ma_200', 'N/A')}
RSI: {daily_data.get('rsi_14', 'N/A')}

**VCP 패턴 분석 결과:**
- VCP 점수: {vcp_analysis.get('score', 0)}/100
- 패턴 상태: {vcp_analysis.get('pattern_status', 'Unknown')}
- 수축률: {vcp_analysis.get('contraction_rate', 'N/A')}%

**Weinstein Stage 분석 결과:**
- 현재 단계: {stage_analysis.get('current_stage', 'Unknown')}
- 단계 점수: {stage_analysis.get('stage_score', 0)}/100
- 트렌드 강도: {stage_analysis.get('trend_strength', 'N/A')}

**브레이크아웃 조건:**
- 권장 액션: {breakout_conditions.get('action', 'HOLD')}
- 신뢰도: {breakout_conditions.get('confidence', 0):.2f}
- 브레이크아웃 점수: {breakout_conditions.get('breakout_score', 0)}/100

다음 형식으로 종합 의견을 제시해주세요:
Market Phase: [Stage 1/Stage 2/Stage 3/Stage 4/Unknown]
Confidence: [0.0-1.0]
Action: [BUY/SELL/HOLD]
Summary: [VCP/Stage 분석을 포함한 종합 의견]
Score: [0-100점]
VCP_Confirmation: [VCP 패턴에 대한 GPT 의견]
Stage_Confirmation: [Weinstein Stage에 대한 GPT 의견]
"""
        
        # 6. GPT API 호출 (향상된 프롬프트 사용)
        import base64
        if not os.path.exists(daily_chart_image_path):
            from data_fetcher import generate_chart_image
            generate_chart_image(ticker)
        
        with open(daily_chart_image_path, "rb") as image_file:
            chart_base64 = base64.b64encode(image_file.read()).decode('utf-8')
        
        start_time = time.time()
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": enhanced_prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{chart_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=1500,
            temperature=0.3
        )
        
        processing_time = time.time() - start_time
        gpt_response = response.choices[0].message.content
        
        # 🔍 로깅 레벨 최적화 - 환경변수 기반 조건부 로깅
        mask_level = os.getenv("GPT_LOG_MASK_LEVEL", "medium")  # low, medium, high
        masked_response = mask_sensitive_info(gpt_response, mask_level)
        
        if os.getenv("GPT_DETAILED_LOGGING", "false").lower() == "true":
            gpt_logger.info(f"📤 {ticker} GPT 응답 수신:")
            gpt_logger.info(f"   - 응답 길이: {len(gpt_response)} characters")
            gpt_logger.info(f"   - 응답 내용 (첫 200자): {masked_response[:200]}...")
            gpt_logger.info(f"   - 전체 응답:\n{masked_response}")
            gpt_logger.info(f"   - 처리 시간: {processing_time:.2f}초")
            gpt_logger.info(f"   - 토큰 사용량: {response.usage.total_tokens if hasattr(response, 'usage') else 'N/A'}")
        else:
            logging.debug(f"📤 {ticker} GPT 응답 수신 (길이: {len(gpt_response)}, 시간: {processing_time:.2f}s)")
            gpt_logger.debug(f"📤 {ticker} GPT 분석 완료 - 응답길이: {len(gpt_response)}, 처리시간: {processing_time:.2f}s")
        
        # 7. GPT 응답 파싱
        gpt_result = parse_enhanced_gpt_response(gpt_response)
        
        # 8. 최종 결과 통합
        integrated_result = integrate_analysis_results(
            ticker, gpt_result, vcp_analysis, stage_analysis, breakout_conditions, db_manager
        )
        
        logging.info(f"✅ {ticker} 향상된 분석 완료: 최종 신뢰도={integrated_result.get('confidence', 0):.2f}")
        return integrated_result
        
    except Exception as e:
        logging.error(f"❌ {ticker} 향상된 트렌드 분석 중 오류: {str(e)}")
        return {
            "ticker": ticker,
            "error": f"Enhanced analysis failed: {str(e)}",
            "confidence": 0.0,
            "action": "HOLD",
            "analysis_method": "error_fallback"
        }

def prepare_technical_data_for_analysis(daily_data: dict) -> dict:
    """
    daily_data를 strategy_analyzer의 VCP/Stage 분석 함수에서 사용할 수 있는 형태로 변환
    
    Args:
        daily_data (dict): 원본 데이터 (price, ma_50, ma_200, rsi_14 등)
        
    Returns:
        dict: VCP/Stage 분석에 필요한 형태로 변환된 데이터
    """
    try:
        # daily_data에서 필요한 지표들을 추출하여 변환
        technical_data = {
            'close': daily_data.get('price', 0),
            'ma_50': daily_data.get('ma_50', 0),
            'ma_200': daily_data.get('ma_200', 0),
            'rsi_14': daily_data.get('rsi_14', 50),
            'volume_change_7_30': daily_data.get('volume_change_7_30', 0),
            'high_60': daily_data.get('high_60', daily_data.get('price', 0)),
            'support': daily_data.get('s1', daily_data.get('price', 0) * 0.95),
            'resistance': daily_data.get('r1', daily_data.get('price', 0) * 1.05),
            'pivot': daily_data.get('pivot', daily_data.get('price', 0)),
            'supertrend_signal': daily_data.get('supertrend_signal', 'neutral'),
            'fibo_382': daily_data.get('fibo_382', 0),
            'fibo_618': daily_data.get('fibo_618', 0),
            'ticker': daily_data.get('ticker', '')
        }
        
        # 추가 계산 필요한 지표들
        if technical_data['ma_50'] and technical_data['ma_200']:
            technical_data['ma_trend'] = 'bullish' if technical_data['ma_50'] > technical_data['ma_200'] else 'bearish'
        else:
            technical_data['ma_trend'] = 'neutral'
            
        # RSI 상태 분류
        rsi = technical_data['rsi_14']
        if rsi > 70:
            technical_data['rsi_status'] = 'overbought'
        elif rsi < 30:
            technical_data['rsi_status'] = 'oversold'
        else:
            technical_data['rsi_status'] = 'neutral'
        
        # 가격 위치 분석
        current_price = technical_data['close']
        support = technical_data['support']
        resistance = technical_data['resistance']
        
        if current_price and support and resistance:
            price_position = (current_price - support) / (resistance - support) if resistance != support else 0.5
            technical_data['price_position'] = max(0, min(1, price_position))  # 0-1 범위로 정규화
        else:
            technical_data['price_position'] = 0.5
            
        # 트렌드 강도 계산
        if technical_data['ma_50'] and technical_data['ma_200']:
            ma_separation = abs(technical_data['ma_50'] - technical_data['ma_200']) / technical_data['ma_200']
            technical_data['trend_strength'] = min(1.0, ma_separation * 10)  # 0-1 범위로 정규화
        else:
            technical_data['trend_strength'] = 0.0
            
        logging.info(f"✅ 기술적 데이터 변환 완료: {technical_data.get('ticker', 'Unknown')}")
        return technical_data
        
    except Exception as e:
        logging.error(f"❌ technical_data 준비 중 오류: {str(e)}")
        # 기본값 반환
        return {
            'close': daily_data.get('price', 0),
            'ma_50': daily_data.get('ma_50', 0),
            'ma_200': daily_data.get('ma_200', 0),
            'rsi_14': daily_data.get('rsi_14', 50),
            'volume_change_7_30': daily_data.get('volume_change_7_30', 0),
            'ma_trend': 'neutral',
            'rsi_status': 'neutral',
            'price_position': 0.5,
            'trend_strength': 0.0,
            'ticker': daily_data.get('ticker', '')
        }

def _get_empty_technical_data():
    """빈 기술적 데이터 반환"""
    return {
        'timestamps': [],
        'open': [],
        'high': [],
        'low': [],
        'close': [],
        'volume': [],
        'current_price': 0,
        'ma50': 0,
        'ma200': 0,
        'rsi': 50,
        'ticker': ''
    }

def _get_fallback_technical_data(daily_data):
    """대체 기술적 데이터 생성 (DB 조회 실패시)"""
    current_price = daily_data.get('price', 0)
    
    # 가상의 시계열 데이터 생성 (임시 방편)
    import numpy as np
    np.random.seed(42)  # 재현 가능한 결과
    
    # 100일간의 가상 데이터 생성
    base_price = current_price if current_price > 0 else 1000
    price_changes = np.random.normal(0, 0.02, 100)  # 평균 0%, 표준편차 2%
    
    close_prices = []
    for i, change in enumerate(price_changes):
        if i == 0:
            price = base_price * (0.9 + 0.1 * i / 99)  # 점진적 상승 트렌드
        else:
            price = close_prices[-1] * (1 + change)
        close_prices.append(max(price, base_price * 0.5))  # 최소가 제한
    
    # 시가/고가/저가 생성
    open_prices = [close_prices[0]] + close_prices[:-1]  # 이전 종가가 다음 시가
    high_prices = [p * (1 + abs(np.random.normal(0, 0.01))) for p in close_prices]
    low_prices = [p * (1 - abs(np.random.normal(0, 0.01))) for p in close_prices]
    
    # 거래량 생성
    volumes = [max(1000000, int(np.random.normal(5000000, 1000000))) for _ in range(100)]
    
    # 타임스탬프 생성 (과거 100일)
    from datetime import datetime, timedelta
    timestamps = []
    for i in range(100):
        date = datetime.now() - timedelta(days=100-i)
        timestamps.append(date)
    
    technical_data = {
        'timestamps': timestamps,
        'open': open_prices,
        'high': high_prices,
        'low': low_prices,
        'close': close_prices,
        'volume': volumes,
        'current_price': close_prices[-1],
        'ma50': daily_data.get('ma_50', sum(close_prices[-50:]) / 50 if len(close_prices) >= 50 else close_prices[-1]),
        'ma200': daily_data.get('ma_200', sum(close_prices) / len(close_prices)),
        'rsi': daily_data.get('rsi_14', 50),
        'ticker': daily_data.get('ticker', '')
    }
    
    logging.info(f"⚠️ {daily_data.get('ticker', 'Unknown')} 대체 기술적 데이터 생성됨")
    return technical_data

def call_gpt_with_enhanced_data(ticker: str, chart_image_path: str, enhanced_data: dict):
    """
    VCP/Stage 분석 결과를 포함하여 GPT 분석 실행
    """
    try:
        # 차트 이미지 로드
        abs_image_path = chart_image_path
        if not os.path.isabs(chart_image_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            abs_image_path = os.path.join(script_dir, chart_image_path)
        
        if not os.path.isfile(abs_image_path):
            logging.warning(f"⚠️ 차트 이미지 없음: {abs_image_path}")
            return {"error": "Chart image not found"}
        
        with open(abs_image_path, "rb") as image_file:
            image_base64 = base64.b64encode(image_file.read()).decode("utf-8")
        
        # 향상된 프롬프트 생성
        enhanced_prompt = create_enhanced_analysis_prompt(ticker, enhanced_data)
        
        # GPT API 호출
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": enhanced_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"{ticker} 차트를 분석하여 VCP 패턴과 Weinstein Stage를 종합한 투자 의견을 제시해주세요."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{image_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=800,
            temperature=0.3
        )
        
        gpt_response = response.choices[0].message.content
        
        # 🔍 상세 응답 로깅 추가
        logging.info(f"📤 {ticker} GPT 응답 수신:")
        logging.info(f"   - 응답 길이: {len(gpt_response)} characters")
        logging.info(f"   - 응답 내용 (첫 200자): {gpt_response[:200]}...")
        logging.info(f"   - 전체 응답:\n{gpt_response}")
        
        # GPT 결과 파싱
        return parse_enhanced_gpt_response(gpt_response)
        
    except Exception as e:
        logging.error(f"❌ {ticker} 향상된 GPT 분석 중 오류: {str(e)}")
        return {"error": str(e), "score": 50, "comment": "GPT 분석 실패"}

def create_enhanced_analysis_prompt(ticker: str, enhanced_data: dict):
    """
    VCP/Stage 분석을 포함한 향상된 프롬프트 생성
    """
    vcp_data = enhanced_data.get('vcp', {})
    stage_data = enhanced_data.get('stage', {})
    breakout_data = enhanced_data.get('breakout', {})
    
    prompt = f"""
당신은 전문 퀀트 애널리스트입니다. 주어진 차트와 기술적 분석 데이터를 종합하여 투자 의견을 제시해주세요.

=== 현재 분석 결과 ===

📊 VCP 패턴 분석:
- VCP 존재 여부: {vcp_data.get('vcp_present', False)}
- VCP 점수: {vcp_data.get('score', 0)}/100
- 수축 횟수: {vcp_data.get('contractions', 0)}
- 거래량 감소: {vcp_data.get('volume_decline_pct', 0):.1f}%
- 변동성 트렌드: {vcp_data.get('volatility_trend', 'unknown')}
- 브레이크아웃 준비: {vcp_data.get('breakout_ready', False)}

📈 Weinstein Stage 분석:
- 현재 단계: {stage_data.get('current_stage', 'Unknown')}  
- 신뢰도: {stage_data.get('stage_confidence', 0):.2f}
- MA50 기울기: {stage_data.get('ma50_slope', 0):.4f}
- MA200 기울기: {stage_data.get('ma200_slope', 0):.4f}
- 거래량 트렌드: {stage_data.get('volume_trend', 'unknown')}

🎯 브레이크아웃 조건:
- 권장 액션: {breakout_data.get('action', 'HOLD')}
- 진입 신뢰도: {breakout_data.get('confidence', 0)}%
- 위험대비수익비: {breakout_data.get('risk_reward_ratio', 0):.2f}
- 권장 포지션 크기: {breakout_data.get('position_size', 0):.2%}

=== 분석 요청사항 ===
1. 차트를 보고 VCP 패턴의 시각적 확인
2. Weinstein Stage의 차트상 증거 확인  
3. 브레이크아웃 가능성 평가
4. 종합 투자 의견 (매수/매도/보유)
5. 0-100 점수로 투자 매력도 평가

응답 형식:
{{
    "score": (0-100 정수),
    "action": "BUY/SELL/HOLD",
    "confidence": (0.0-1.0 실수),
    "comment": "상세 분석 의견",
    "vcp_confirmation": "VCP 패턴 시각적 확인 결과",
    "stage_confirmation": "Stage 분석 차트 증거",
    "risk_assessment": "리스크 평가"
}}
"""
    
    return prompt

def parse_enhanced_gpt_response(gpt_response: str):
    """
    향상된 GPT 응답 파싱 - 다양한 형식 지원 및 강화된 정규식
    """
    if not gpt_response or not gpt_response.strip():
        logging.error("❌ 빈 GPT 응답 수신")
        return _get_fallback_parse_result("빈 응답")
    
    try:
        # 1. JSON 형식 우선 처리
        json_result = _parse_json_response(gpt_response)
        if json_result:
            logging.debug("✅ JSON 형식 응답 파싱 성공")
            return json_result
        
        # 2. 텍스트 형식 파싱 - 다양한 패턴 지원
        text_result = _parse_text_response(gpt_response)
        if text_result:
            logging.debug("✅ 텍스트 형식 응답 파싱 성공")
            return text_result
        
        # 3. 최후 수단 - 키워드 기반 파싱
        fallback_result = _parse_with_keywords(gpt_response)
        logging.warning("⚠️ 키워드 기반 파싱으로 대체")
        return fallback_result
            
    except Exception as e:
        error_msg = f"GPT 응답 파싱 중 오류: {str(e)}"
        logging.error(f"❌ {error_msg}")
        return _get_fallback_parse_result(error_msg)

def _parse_json_response(gpt_response: str) -> dict:
    """JSON 형식 응답 파싱"""
    try:
        # JSON 블록 찾기 (중첩된 JSON도 지원)
        json_patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # 단순 JSON
            r'```json\s*(\{.*?\})\s*```',        # 마크다운 JSON 블록
            r'```\s*(\{.*?\})\s*```',            # 일반 코드 블록
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, gpt_response, re.DOTALL | re.IGNORECASE)
            for match in matches:
                try:
                    # 패턴에 따라 매치 처리
                    json_str = match if isinstance(match, str) else match
                    if not json_str.strip().startswith('{'):
                        continue
                        
                    result = json.loads(json_str)
                    
                    # 필수 필드 검증 및 정규화
                    return _normalize_json_result(result)
                    
                except json.JSONDecodeError:
                    continue
        
        return None
        
    except Exception as e:
        logging.debug(f"JSON 파싱 실패: {e}")
        return None

def _parse_text_response(gpt_response: str) -> dict:
    """텍스트 형식 응답 파싱 - 개선된 정규식"""
    try:
        result = {
            "score": 50,
            "action": "HOLD",
            "confidence": 0.5,
            "comment": "",
            "vcp_confirmation": "",
            "stage_confirmation": "",
            "risk_assessment": ""
        }
        
        # 점수 추출 정규식 (다양한 형식 지원)
        score_patterns = [
            r'점수[:：]\s*(\d+(?:\.\d+)?)',           # "점수: 85"
            r'Score[:：]\s*(\d+(?:\.\d+)?)',          # "Score: 85"
            r'평점\s*(\d+(?:\.\d+)?)점',              # "평점 85점"
            r'(\d+(?:\.\d+)?)/100',                   # "85/100"
            r'(\d+(?:\.\d+)?)%',                      # "85%"
            r'스코어[:：]\s*(\d+(?:\.\d+)?)',         # "스코어: 85"
            r'Rating[:：]\s*(\d+(?:\.\d+)?)',         # "Rating: 85"
        ]
        
        for pattern in score_patterns:
            match = re.search(pattern, gpt_response, re.IGNORECASE)
            if match:
                score = float(match.group(1))
                # 점수 범위 정규화 (0-100)
                if score > 100:
                    score = min(score, 100)
                elif score <= 1:  # 0-1 범위인 경우
                    score = score * 100
                result["score"] = int(score)
                break
        
        # 액션 추출
        action_patterns = [
            r'Action[:：]\s*(BUY|SELL|HOLD)',
            r'액션[:：]\s*(BUY|SELL|HOLD|매수|매도|보유)',
            r'권장[:：]\s*(BUY|SELL|HOLD|매수|매도|보유)',
            r'Recommendation[:：]\s*(BUY|SELL|HOLD)',
        ]
        
        for pattern in action_patterns:
            match = re.search(pattern, gpt_response, re.IGNORECASE)
            if match:
                action = match.group(1).upper()
                # 한국어 액션 변환
                action_map = {"매수": "BUY", "매도": "SELL", "보유": "HOLD"}
                result["action"] = action_map.get(action, action)
                break
        
        # 신뢰도 추출
        confidence_patterns = [
            r'Confidence[:：]\s*(\d+(?:\.\d+)?)',
            r'신뢰도[:：]\s*(\d+(?:\.\d+)?)',
            r'확신도[:：]\s*(\d+(?:\.\d+)?)',
        ]
        
        for pattern in confidence_patterns:
            match = re.search(pattern, gpt_response, re.IGNORECASE)
            if match:
                conf = float(match.group(1))
                # 신뢰도 정규화 (0-1)
                if conf > 1:
                    conf = conf / 100
                result["confidence"] = min(max(conf, 0), 1)
                break
        
        # 코멘트 추출
        result["comment"] = gpt_response[:500]  # 처음 500자
        
        # 추가 필드 추출
        if "VCP" in gpt_response:
            vcp_match = re.search(r'VCP[^.]*\.', gpt_response, re.IGNORECASE)
            if vcp_match:
                result["vcp_confirmation"] = vcp_match.group(0)
        
        if "Stage" in gpt_response:
            stage_match = re.search(r'Stage[^.]*\.', gpt_response, re.IGNORECASE)
            if stage_match:
                result["stage_confirmation"] = stage_match.group(0)
        
        return result
        
    except Exception as e:
        logging.error(f"텍스트 파싱 중 오류: {e}")
        return None

def _parse_with_keywords(gpt_response: str) -> dict:
    """키워드 기반 마지막 수단 파싱"""
    try:
        # 긍정적/부정적 키워드 기반 점수 추정
        positive_keywords = ["상승", "매수", "BUY", "좋은", "강세", "breakthrough", "bullish"]
        negative_keywords = ["하락", "매도", "SELL", "나쁜", "약세", "bearish", "sell"]
        
        positive_count = sum(1 for word in positive_keywords if word.lower() in gpt_response.lower())
        negative_count = sum(1 for word in negative_keywords if word.lower() in gpt_response.lower())
        
        if positive_count > negative_count:
            score = 60 + min(positive_count * 5, 25)
            action = "BUY"
        elif negative_count > positive_count:
            score = 40 - min(negative_count * 5, 25)
            action = "SELL"
        else:
            score = 50
            action = "HOLD"
        
        return {
            "score": score,
            "action": action,
            "confidence": 0.4,  # 낮은 신뢰도
            "comment": gpt_response[:300],
            "vcp_confirmation": "",
            "stage_confirmation": "",
            "risk_assessment": "",
            "parsing_method": "keyword_fallback"
        }
        
    except Exception as e:
        logging.error(f"키워드 파싱 중 오류: {e}")
        return _get_fallback_parse_result(f"키워드 파싱 실패: {e}")

def _normalize_json_result(result: dict) -> dict:
    """JSON 결과 정규화 및 검증"""
    try:
        normalized = {
            "score": int(result.get("score", 50)),
            "action": str(result.get("action", "HOLD")).upper(),
            "confidence": float(result.get("confidence", 0.5)),
            "comment": str(result.get("comment", "")),
            "vcp_confirmation": str(result.get("vcp_confirmation", "")),
            "stage_confirmation": str(result.get("stage_confirmation", "")),
            "risk_assessment": str(result.get("risk_assessment", ""))
        }
        
        # 값 범위 검증 및 보정
        normalized["score"] = max(0, min(100, normalized["score"]))
        normalized["confidence"] = max(0, min(1, normalized["confidence"]))
        
        if normalized["action"] not in ["BUY", "SELL", "HOLD"]:
            normalized["action"] = "HOLD"
        
        return normalized
        
    except Exception as e:
        logging.error(f"JSON 결과 정규화 중 오류: {e}")
        return _get_fallback_parse_result(f"정규화 실패: {e}")

def _get_fallback_parse_result(error_msg: str) -> dict:
    """파싱 실패 시 기본값 반환"""
    return {
        "score": 50,
        "action": "HOLD",
        "confidence": 0.3,
        "comment": f"파싱 실패: {error_msg}",
        "vcp_confirmation": "",
        "stage_confirmation": "",
        "risk_assessment": "",
        "error": error_msg,
        "parsing_method": "fallback"
    }

def integrate_analysis_results(ticker: str, gpt_result: dict, vcp_analysis: dict, 
                             stage_analysis: dict, breakout_conditions: dict, db_manager: DBManager):
    """
    GPT, VCP, Stage 분석 결과를 통합하여 최종 의사결정 생성 (가중치 시스템 + 분할 매수 지원)
    """
    try:
        # 통합 점수 계산 (VCP 30%, Stage 40%, 브레이크아웃 30%)
        unified_score_result = calculate_unified_score(vcp_analysis, stage_analysis, breakout_conditions)
        
        # 분할 매수 조건 체크
        scaling_conditions = check_scaling_in_conditions(vcp_analysis, stage_analysis, breakout_conditions)
        
        # 포지션 크기 계산
        position_size = calculate_position_size(unified_score_result['unified_score'], scaling_conditions)
        
        # 최종 액션 결정 (통합 점수 기반)
        final_action = unified_score_result['action']
        action_confidence = unified_score_result['unified_score'] * 100
        
        # GPT 결과와 통합 분석 결과 비교 조정
        gpt_action = gpt_result.get('action', 'HOLD')
        gpt_confidence = gpt_result.get('confidence', 0.5)
        
        # 통합 점수와 GPT 신뢰도 차이가 클 때 보정
        if abs(unified_score_result['unified_score'] - gpt_confidence) > 0.3:
            # 가중 평균으로 신뢰도 조정 (통합 분석 70%, GPT 30%)
            adjusted_confidence = (
                unified_score_result['unified_score'] * 0.7 + 
                gpt_confidence * 0.3
            )
            action_confidence = adjusted_confidence * 100
            
            logging.info(f"📊 {ticker} 신뢰도 보정: {unified_score_result['unified_score']:.2f} → {adjusted_confidence:.2f}")
        
        # 액션 일치성 검증 및 조정
        if gpt_action != final_action:
            if gpt_confidence > 0.8 and unified_score_result['unified_score'] < 0.6:
                # GPT 신뢰도가 높고 통합 점수가 낮으면 보수적 조정
                final_action = 'HOLD'
                action_confidence = min(action_confidence, 70)
                logging.warning(f"⚠️ {ticker} 액션 불일치로 보수적 조정: {final_action}")
        
        # 리스크 관리 레이어 추가
        risk_adjusted_result = apply_risk_management_layer(
            ticker, final_action, action_confidence, unified_score_result, 
            vcp_analysis, stage_analysis, breakout_conditions
        )
        
        # 최종 통합 결과
        integrated_result = {
            "ticker": ticker,
            "action": risk_adjusted_result.get('final_action', final_action),
            "confidence": risk_adjusted_result.get('final_confidence', action_confidence) / 100,
            "action_confidence": risk_adjusted_result.get('final_confidence', action_confidence),
            "unified_score": unified_score_result['unified_score'],
            "market_phase": stage_analysis.get('current_stage', 'Unknown'),
            "vcp_score": vcp_analysis.get('score', 0),
            "stage": stage_analysis.get('current_stage', 'Unknown'),
            "stage_confidence": stage_analysis.get('stage_confidence', 0),
            "breakout_ready": vcp_analysis.get('breakout_ready', False),
            "position_size": position_size,
            "risk_reward_ratio": breakout_conditions.get('risk_reward_ratio', 0),
            "entry_price": breakout_conditions.get('entry_price', 0),
            "stop_loss": breakout_conditions.get('stop_loss', 0),
            "target_price": breakout_conditions.get('target_price', 0),
            "scaling_conditions": scaling_conditions,
            "risk_management": risk_adjusted_result,
            "analysis_details": {
                "vcp": vcp_analysis,
                "stage": stage_analysis,
                "breakout": breakout_conditions,
                "gpt": gpt_result,
                "unified_score_breakdown": unified_score_result
            },
            "summary": create_enhanced_analysis_summary(
                vcp_analysis, stage_analysis, breakout_conditions, gpt_result, 
                unified_score_result, scaling_conditions
            ),
            "timestamp": datetime.now().isoformat(),
            "analysis_method": "enhanced_integrated_v2"
        }
        
        # 데이터베이스에 통합 결과 저장
        save_enhanced_analysis_to_db(integrated_result, db_manager)
        
        return integrated_result
        
    except Exception as e:
        logging.error(f"❌ {ticker} 분석 결과 통합 중 오류: {str(e)}")
        return {
            "ticker": ticker,
            "error": f"Integration failed: {str(e)}",
            "confidence": 0.0,
            "action": "HOLD"
        }

def calculate_unified_score(vcp_analysis: dict, stage_analysis: dict, breakout_conditions: dict) -> dict:
    """
    VCP, Stage, 브레이크아웃 조건을 통합한 가중 평균 점수 계산
    
    가중치: VCP 30%, Stage 40%, 브레이크아웃 30%
    """
    try:
        # 가중치 설정 (사용자 의사코드 기반)
        vcp_weight = 0.3
        stage_weight = 0.4
        breakout_weight = 0.3
        
        # 각 분석 결과를 0-1 스케일로 정규화
        vcp_score = vcp_analysis.get('score', 0) / 100.0
        
        # Weinstein Stage를 점수로 변환
        stage_score = convert_stage_to_score(stage_analysis.get('current_stage', 'Unknown'))
        
        # 브레이크아웃 신뢰도를 점수로 변환
        breakout_score = breakout_conditions.get('confidence', 0) / 100.0
        
        # 가중 평균 계산
        unified_score = (
            vcp_score * vcp_weight + 
            stage_score * stage_weight + 
            breakout_score * breakout_weight
        )
        
        # 액션 결정 로직
        action = determine_action(unified_score)
        
        logging.info(f"📊 통합 점수 계산: VCP={vcp_score:.2f}, Stage={stage_score:.2f}, Breakout={breakout_score:.2f} → 통합={unified_score:.2f}")
        
        return {
            'unified_score': unified_score,
            'action': action,
            'vcp_contribution': vcp_score * vcp_weight,
            'stage_contribution': stage_score * stage_weight,
            'breakout_contribution': breakout_score * breakout_weight,
            'breakdown': {
                'vcp_score': vcp_score,
                'stage_score': stage_score,
                'breakout_score': breakout_score,
                'weights': {
                    'vcp': vcp_weight,
                    'stage': stage_weight,
                    'breakout': breakout_weight
                }
            }
        }
        
    except Exception as e:
        logging.error(f"❌ 통합 점수 계산 중 오류: {str(e)}")
        return {
            'unified_score': 0.5,  # 중립값
            'action': 'HOLD',
            'error': str(e)
        }


def convert_stage_to_score(current_stage: str) -> float:
    """
    Weinstein Stage를 0-1 점수로 변환
    """
    stage_scores = {
        'Stage 1': 0.2,   # 바닥권 (관심)
        'Stage 2': 0.8,   # 상승 (매수)
        'Stage 3': 0.4,   # 고점권 (관심)
        'Stage 4': 0.1,   # 하락 (매도)
        'Unknown': 0.5,   # 중립
        'Transition': 0.6  # 전환기
    }
    
    return stage_scores.get(current_stage, 0.5)


def determine_action(unified_score: float) -> str:
    """
    통합 점수를 기반으로 매매 액션 결정
    """
    if unified_score >= 0.75:
        return 'BUY'
    elif unified_score >= 0.6:
        return 'BUY_WEAK'  # 약한 매수 신호
    elif unified_score <= 0.25:
        return 'SELL'
    elif unified_score <= 0.4:
        return 'SELL_WEAK'  # 약한 매도 신호
    else:
        return 'HOLD'


def check_scaling_in_conditions(vcp_analysis: dict, stage_analysis: dict, breakout_conditions: dict) -> dict:
    """
    분할 매수 트리거 조건 체크
    """
    try:
        scaling_conditions = {
            'initial_entry_complete': False,
            'additional_buy_triggers': [],
            'scaling_strategy': 'none',
            'max_position_additions': 0,
            'next_buy_level': None,
            'risk_per_addition': 0.02  # 추가 매수당 2% 리스크
        }
        
        # 초기 진입 조건 체크
        if (vcp_analysis.get('score', 0) >= 60 and 
            stage_analysis.get('current_stage') == 'Stage 2' and
            breakout_conditions.get('confidence', 0) >= 70):
            
            scaling_conditions['initial_entry_complete'] = True
            scaling_conditions['scaling_strategy'] = 'pyramid'
            scaling_conditions['max_position_additions'] = 2
            
            # 추가 매수 트리거 조건 설정
            current_price = breakout_conditions.get('entry_price', 0)
            if current_price > 0:
                # 5% 상승 후 추가 매수 고려
                scaling_conditions['additional_buy_triggers'].append({
                    'condition': 'price_breakout',
                    'trigger_price': current_price * 1.05,
                    'reason': '초기 브레이크아웃 확인 후 추가 진입',
                    'position_addition': 0.5  # 초기 포지션의 50% 추가
                })
                
                # 볼륨 급증 시 추가 매수
                if vcp_analysis.get('volume_surge', False):
                    scaling_conditions['additional_buy_triggers'].append({
                        'condition': 'volume_surge',
                        'trigger_volume_ratio': 2.0,  # 평균 거래량의 2배
                        'reason': '거래량 급증으로 관심도 증가',
                        'position_addition': 0.3
                    })
        
        # 위험도가 높은 경우 분할 매수 비활성화
        if breakout_conditions.get('risk_reward_ratio', 0) < 2.0:
            scaling_conditions['scaling_strategy'] = 'single_entry'
            scaling_conditions['max_position_additions'] = 0
            scaling_conditions['additional_buy_triggers'] = []
        
        logging.info(f"🎯 분할 매수 조건: {scaling_conditions['scaling_strategy']}, 최대 추가: {scaling_conditions['max_position_additions']}회")
        
        return scaling_conditions
        
    except Exception as e:
        logging.error(f"❌ 분할 매수 조건 체크 중 오류: {str(e)}")
        return {'scaling_strategy': 'none', 'error': str(e)}


def calculate_position_size(unified_score: float, scaling_conditions: dict) -> float:
    """
    통합 점수와 분할 매수 조건에 기반한 포지션 크기 계산
    """
    try:
        base_position_size = 0.0
        
        # 기본 포지션 크기 (통합 점수 기반)
        if unified_score >= 0.8:
            base_position_size = 0.15  # 15%
        elif unified_score >= 0.7:
            base_position_size = 0.12  # 12%
        elif unified_score >= 0.6:
            base_position_size = 0.08  # 8%
        elif unified_score >= 0.5:
            base_position_size = 0.05  # 5%
        else:
            base_position_size = 0.0   # 진입하지 않음
        
        # 분할 매수 전략에 따른 조정
        if scaling_conditions.get('scaling_strategy') == 'pyramid':
            # 분할 매수 시 초기 포지션 크기 축소
            initial_ratio = 0.6  # 초기에 60%만 진입
            base_position_size *= initial_ratio
            
            logging.info(f"📊 분할 매수 전략: 초기 포지션 {base_position_size:.1%} (전체 계획의 {initial_ratio:.0%})")
        
        # 리스크 관리: 최대 포지션 크기 제한
        max_position_size = 0.2  # 20% 제한
        position_size = min(base_position_size, max_position_size)
        
        return round(position_size, 3)
        
    except Exception as e:
        logging.error(f"❌ 포지션 크기 계산 중 오류: {str(e)}")
        return 0.0


def apply_risk_management_layer(ticker: str, action: str, confidence: float, 
                              unified_result: dict, vcp_analysis: dict, 
                              stage_analysis: dict, breakout_conditions: dict) -> dict:
    """
    리스크 관리 레이어 적용
    """
    try:
        risk_factors = []
        risk_score = 0.0
        final_action = action
        final_confidence = confidence
        
        # 1. 시장 변동성 체크
        if vcp_analysis.get('volatility_trend') == 'increasing':
            risk_factors.append('높은 변동성')
            risk_score += 0.2
        
        # 2. 거래량 확인
        volume_trend = stage_analysis.get('volume_trend', 'unknown')
        if volume_trend == 'declining' and action in ['BUY', 'BUY_WEAK']:
            risk_factors.append('거래량 감소')
            risk_score += 0.15
        
        # 3. 위험-수익 비율 체크
        risk_reward = breakout_conditions.get('risk_reward_ratio', 0)
        if risk_reward < 2.0 and action in ['BUY', 'BUY_WEAK']:
            risk_factors.append('낮은 위험-수익 비율')
            risk_score += 0.25
        
        # 4. Stage 분석 위험 요소
        current_stage = stage_analysis.get('current_stage', 'Unknown')
        if current_stage in ['Stage 3', 'Stage 4'] and action in ['BUY', 'BUY_WEAK']:
            risk_factors.append(f'부적절한 매매시점 ({current_stage})')
            risk_score += 0.3
        
        # 5. 리스크 점수에 따른 액션 조정
        if risk_score >= 0.5:  # 고위험
            if action in ['BUY', 'BUY_WEAK']:
                final_action = 'HOLD'
                final_confidence = min(final_confidence, 60)
                risk_factors.append('고위험으로 매수 제한')
                
                logging.warning(f"🚨 {ticker} 고위험 감지 (점수: {risk_score:.2f}) - 매수 → 관망")
        
        elif risk_score >= 0.3:  # 중위험
            if action == 'BUY':
                final_action = 'BUY_WEAK'
                final_confidence = min(final_confidence, 75)
                
                logging.warning(f"⚠️ {ticker} 중위험 감지 (점수: {risk_score:.2f}) - 매수 세기 축소")
        
        return {
            'risk_score': risk_score,
            'risk_factors': risk_factors,
            'risk_level': '고위험' if risk_score >= 0.5 else '중위험' if risk_score >= 0.3 else '저위험',
            'original_action': action,
            'final_action': final_action,
            'original_confidence': confidence,
            'final_confidence': final_confidence,
            'adjustments_made': len(risk_factors) > 0
        }
        
    except Exception as e:
        logging.error(f"❌ {ticker} 리스크 관리 적용 중 오류: {str(e)}")
        return {
            'risk_score': 0.5,
            'risk_level': '중위험',
            'final_action': 'HOLD',
            'final_confidence': 50,
            'error': str(e)
        }


def create_enhanced_analysis_summary(vcp_analysis: dict, stage_analysis: dict, 
                                   breakout_conditions: dict, gpt_result: dict,
                                   unified_result: dict, scaling_conditions: dict) -> str:
    """
    향상된 분석 결과 요약 생성
    """
    vcp_status = "🟢 우수" if vcp_analysis.get('score', 0) >= 70 else "🟡 보통" if vcp_analysis.get('score', 0) >= 50 else "🔴 불량"
    stage = stage_analysis.get('current_stage', 'Unknown')
    action = unified_result.get('action', 'HOLD')
    unified_score = unified_result.get('unified_score', 0)
    scaling_strategy = scaling_conditions.get('scaling_strategy', 'none')
    
    summary = f"""
📊 VCP 패턴: {vcp_status} (점수: {vcp_analysis.get('score', 0)})
📈 Weinstein Stage: {stage} (신뢰도: {stage_analysis.get('stage_confidence', 0):.1%})
🎯 통합 점수: {unified_score:.1%} → 권장 액션: {action}
🏗️ 진입 전략: {scaling_strategy} (최대 {scaling_conditions.get('max_position_additions', 0)}회 추가)
💡 GPT 의견: {gpt_result.get('comment', '없음')[:100]}...
"""
    
    return summary.strip()


def create_analysis_summary(vcp_analysis: dict, stage_analysis: dict, 
                          breakout_conditions: dict, gpt_result: dict):
    """
    기본 분석 결과 요약 생성 (하위 호환성 유지)
    """
    vcp_status = "🟢 우수" if vcp_analysis.get('score', 0) >= 70 else "🟡 보통" if vcp_analysis.get('score', 0) >= 50 else "🔴 불량"
    stage = stage_analysis.get('current_stage', 'Unknown')
    action = breakout_conditions.get('action', 'HOLD')
    
    summary = f"""
📊 VCP 패턴: {vcp_status} (점수: {vcp_analysis.get('score', 0)})
📈 Weinstein Stage: {stage} (신뢰도: {stage_analysis.get('stage_confidence', 0):.1%})
🎯 권장 액션: {action} (신뢰도: {breakout_conditions.get('confidence', 0)}%)
💡 GPT 의견: {gpt_result.get('comment', '')[:100]}...
"""
    
    return summary.strip()

def save_enhanced_analysis_to_db(result: dict, db_manager: DBManager):
    """
    향상된 분석 결과를 데이터베이스에 저장
    """
    try:
        # score 필드 확인 및 기본값 설정
        score = result.get('score', 50)  # 기본값 50
        confidence = result.get('confidence', 0.5)
        
        # trend_analysis 테이블에 기존 형식으로 저장
        db_manager.execute_query("""
            INSERT INTO trend_analysis (
                ticker, action, type, reason, pattern, 
                market_phase, confidence, score, time_window, 
                vcp_score, stage_analysis, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT(ticker) DO UPDATE SET
                action = excluded.action,
                type = excluded.type,
                reason = excluded.reason,
                pattern = excluded.pattern,
                market_phase = excluded.market_phase,
                confidence = excluded.confidence,
                score = excluded.score,
                vcp_score = excluded.vcp_score,
                stage_analysis = excluded.stage_analysis,
                updated_at = CURRENT_TIMESTAMP
        """, (
            result['ticker'],
            result['action'],
            'enhanced_analysis',
            result.get('summary', ''),
            'VCP+Stage',
            result['market_phase'],
            result['confidence'],
            score,
            '자동매매',
            result['vcp_score'],
            json.dumps(result['analysis_details'], ensure_ascii=False)
        ))
        
        logging.info(f"✅ {result['ticker']} 향상된 분석 결과 DB 저장 완료 (score: {score})")
        
    except Exception as e:
        ticker = result.get('ticker', 'UNKNOWN')
        score = result.get('score', 'N/A')
        confidence = result.get('confidence', 'N/A')
        action = result.get('action', 'N/A')
        market_phase = result.get('market_phase', 'N/A')
        
        logging.error(f"❌ {ticker} DB 저장 실패 상세 정보:")
        logging.error(f"   - 에러: {str(e)}")
        logging.error(f"   - 데이터: score={score}, confidence={confidence}")
        logging.error(f"   - 액션: {action}, 시장단계: {market_phase}")
        logging.error("   - trend_analysis 테이블 구조 확인 필요")
        
        # 테이블 구조 확인 쿼리 실행
        try:
            schema_result = db_manager.execute_query(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'trend_analysis' ORDER BY ordinal_position;"
            )
            if schema_result:
                logging.error(f"   - 현재 테이블 구조: {schema_result}")
            else:
                logging.error("   - 테이블 구조 정보를 가져올 수 없습니다")
        except Exception as schema_e:
            logging.error(f"   - 테이블 구조 확인 실패: {str(schema_e)}")

def get_enhanced_analysis_for_ticker(ticker: str, db_manager: DBManager):
    """
    특정 티커의 향상된 분석 결과 조회
    """
    try:
        result = db_manager.execute_query("""
            SELECT 
                ticker, action, market_phase, confidence,
                vcp_score, stage_analysis, created_at
            FROM trend_analysis 
            WHERE ticker = %s 
            AND type = 'enhanced_analysis'
            ORDER BY created_at DESC 
            LIMIT 1
        """, (ticker,))
        
        if result:
            row = result[0]
            return {
                'ticker': row[0],
                'action': row[1],
                'market_phase': row[2],
                'confidence': float(row[3]),
                'vcp_score': row[4],
                'stage_analysis': json.loads(row[5]) if row[5] else {},
                'created_at': row[6]
            }
        
        return None
        
    except Exception as e:
        logging.error(f"❌ {ticker} 향상된 분석 결과 조회 중 오류: {str(e)}")
        return None

# UNUSED: 호출되지 않는 함수
# def test_trend_analyzer_improvements():
#     """
#     trend_analyzer.py 개선사항 테스트 함수
#     
#     테스트 항목:
#     1. 로그 순환 및 압축 설정
#     2. 민감 정보 마스킹
#     3. 로깅 레벨 최적화
#     4. 강화된 응답 파싱
#     """
#     print("🧪 trend_analyzer.py 개선사항 테스트 시작\n")
#     
#     # 1. 로그 순환 설정 테스트
#     print("1️⃣ 로그 순환 설정 테스트:")
#     try:
#         test_logger = setup_gpt_logging_rotation(
#             log_file_path="log/test_gpt_rotation.log",
#             max_bytes=1024*1024,  # 1MB
#             backup_count=3
#         )
#         test_logger.info("테스트 로그 메시지")
#         print("   ✅ 로그 순환 설정 정상 작동")
#     except Exception as e:
#         print(f"   ❌ 로그 순환 설정 오류: {e}")
#     
#     # 2. 민감 정보 마스킹 테스트
#     print("\n2️⃣ 민감 정보 마스킹 테스트:")
#     try:
#         test_data = {
#             "api_key": "sk-abcd1234567890",
#             "price": "50000 KRW",
#             "email": "test@example.com",
#             "percentage": "12.5%",
#             "normal_text": "일반 텍스트"
#         }
#         
#         masked_low = mask_sensitive_info(test_data, "low")
#         masked_medium = mask_sensitive_info(test_data, "medium")
#         masked_high = mask_sensitive_info(test_data, "high")
#         
#         print(f"   원본: {test_data}")
#         print(f"   Low 마스킹: {masked_low}")
#         print(f"   Medium 마스킹: {masked_medium}")
#         print(f"   High 마스킹: {masked_high}")
#         print("   ✅ 민감 정보 마스킹 정상 작동")
#     except Exception as e:
#         print(f"   ❌ 민감 정보 마스킹 오류: {e}")
#     
#     # 3. 응답 파싱 테스트
#     print("\n3️⃣ 강화된 응답 파싱 테스트:")
#     try:
#         test_responses = [
#             '{"score": 85, "action": "BUY", "confidence": 0.8}',  # JSON 형식
#             'Score: 75/100\nAction: HOLD\nConfidence: 0.65',      # 텍스트 형식
#             '점수: 90점, 액션: 매수, 신뢰도: 80%',                 # 한국어 형식
#             'This is a positive analysis with bullish sentiment', # 키워드 기반
#             '',  # 빈 응답
#         ]
#         
#         for i, response in enumerate(test_responses, 1):
#             result = parse_enhanced_gpt_response(response)
#             print(f"   테스트 {i}: {response[:30]}...")
#             print(f"   결과: 점수={result['score']}, 액션={result['action']}, 신뢰도={result['confidence']:.2f}")
#         print("   ✅ 강화된 응답 파싱 정상 작동")
#     except Exception as e:
#         print(f"   ❌ 응답 파싱 테스트 오류: {e}")
#     
#     # 4. 환경변수 설정 확인
#     print("\n4️⃣ 환경변수 설정 확인:")
#     gpt_detailed = os.getenv("GPT_DETAILED_LOGGING", "false")
#     mask_level = os.getenv("GPT_LOG_MASK_LEVEL", "medium")
#     
#     print(f"   GPT_DETAILED_LOGGING: {gpt_detailed}")
#     print(f"   GPT_LOG_MASK_LEVEL: {mask_level}")
#     
#     if gpt_detailed.lower() == "true":
#         print("   📋 상세 로깅 활성화됨")
#     else:
#         print("   📋 상세 로깅 비활성화됨 (성능 최적화)")
#     
#     print(f"\n✅ trend_analyzer.py 개선사항 테스트 완료!")
#     
#     # 사용법 안내
#     print("\n📖 사용법 안내:")
#     print("환경변수 설정:")
#     print("  export GPT_DETAILED_LOGGING=true  # 상세 로깅 활성화")
#     print("  export GPT_LOG_MASK_LEVEL=high    # 높은 수준 마스킹")
#     print("\n로그 파일 위치:")
#     print("  - 메인 로그: log/gpt_analysis.log")
#     print("  - 압축 백업: log/gpt_analysis.log.1.gz, log/gpt_analysis.log.2.gz, ...")

# if __name__ == "__main__":
#     # 개선사항 테스트 실행
#     test_trend_analyzer_improvements()


# === DB 스키마 검증 및 복구 시스템 ===

def validate_db_schema_consistency(db_manager: DBManager = None, auto_fix: bool = True) -> dict:
    """
    필수 테이블과 컬럼 존재 여부 확인 및 자동 복구
    
    Args:
        db_manager: DB 매니저 인스턴스
        auto_fix: 자동 복구 활성화 여부
    
    Returns:
        dict: 검증 결과 및 복구 내역
    """
    
    if db_manager is None:
        try:
            db_manager = DBManager()
        except Exception as e:
            return {
                'status': 'error',
                'message': f'DB 연결 실패: {str(e)}',
                'details': {}
            }
    
    # 필수 테이블 및 컬럼 정의
    required_schemas = {
        'trend_analysis': {
            'columns': [
                'ticker VARCHAR(20) PRIMARY KEY',
                'action VARCHAR(10)',
                'type VARCHAR(50)',
                'reason TEXT',
                'pattern VARCHAR(100)',
                'market_phase VARCHAR(50)',
                'confidence DECIMAL(5,2)',
                'time_window VARCHAR(50)',
                'vcp_score INTEGER',
                'stage_analysis JSONB',
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            ],
            'indexes': [
                'CREATE INDEX IF NOT EXISTS idx_trend_analysis_created_at ON trend_analysis(created_at)',
                'CREATE INDEX IF NOT EXISTS idx_trend_analysis_action ON trend_analysis(action)',
                'CREATE INDEX IF NOT EXISTS idx_trend_analysis_confidence ON trend_analysis(confidence)'
            ]
        },
        'market_data': {
            'columns': [
                'ticker VARCHAR(20)',
                'date DATE',
                'open_price DECIMAL(15,2)',
                'high_price DECIMAL(15,2)',
                'low_price DECIMAL(15,2)',
                'close_price DECIMAL(15,2)',
                'volume BIGINT',
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'PRIMARY KEY (ticker, date)'
            ],
            'indexes': [
                'CREATE INDEX IF NOT EXISTS idx_market_data_ticker ON market_data(ticker)',
                'CREATE INDEX IF NOT EXISTS idx_market_data_date ON market_data(date)'
            ]
        },
        'static_indicators': {
            'columns': [
                'ticker VARCHAR(20)',
                'date DATE',
                'rsi DECIMAL(5,2)',
                'macd DECIMAL(10,4)',
                'bb_upper DECIMAL(15,2)',
                'bb_middle DECIMAL(15,2)',
                'bb_lower DECIMAL(15,2)',
                'sma_20 DECIMAL(15,2)',
                'sma_50 DECIMAL(15,2)',
                'sma_200 DECIMAL(15,2)',
                'volume_sma BIGINT',
                'atr DECIMAL(10,4)',
                'obv BIGINT',
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'PRIMARY KEY (ticker, date)'
            ],
            'indexes': [
                'CREATE INDEX IF NOT EXISTS idx_static_indicators_ticker ON static_indicators(ticker)',
                'CREATE INDEX IF NOT EXISTS idx_static_indicators_date ON static_indicators(date)'
            ]
        },
        'enhanced_analysis': {
            'columns': [
                'id SERIAL PRIMARY KEY',
                'ticker VARCHAR(20)',
                'analysis_date DATE',
                'vcp_analysis JSONB',
                'stage_analysis JSONB',
                'breakout_conditions JSONB',
                'unified_score DECIMAL(5,2)',
                'final_action VARCHAR(10)',
                'confidence INTEGER',
                'risk_score DECIMAL(3,2)',
                'position_size DECIMAL(3,2)',
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            ],
            'indexes': [
                'CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_ticker ON enhanced_analysis(ticker)',
                'CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_date ON enhanced_analysis(analysis_date)',
                'CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_action ON enhanced_analysis(final_action)'
            ]
        }
    }
    
    validation_results = {
        'status': 'success',
        'checked_tables': 0,
        'missing_tables': [],
        'missing_columns': [],
        'created_tables': [],
        'added_columns': [],
        'created_indexes': [],
        'errors': []
    }
    
    try:
        secure_logger.info("🔍 DB 스키마 일관성 검증 시작")
        
        for table_name, schema_info in required_schemas.items():
            validation_results['checked_tables'] += 1
            secure_logger.info(f"📋 {table_name} 테이블 검증 중...")
            
            # 1. 테이블 존재 여부 확인
            table_exists = _check_table_exists(db_manager, table_name)
            
            if not table_exists:
                validation_results['missing_tables'].append(table_name)
                
                if auto_fix:
                    # 테이블 생성
                    created = _create_table(db_manager, table_name, schema_info['columns'])
                    if created:
                        validation_results['created_tables'].append(table_name)
                        secure_logger.info(f"✅ {table_name} 테이블 생성 완료")
                    else:
                        validation_results['errors'].append(f"{table_name} 테이블 생성 실패")
                        continue
            else:
                # 2. 컬럼 존재 여부 확인
                missing_cols = _check_missing_columns(db_manager, table_name, schema_info['columns'])
                validation_results['missing_columns'].extend(missing_cols)
                
                if auto_fix and missing_cols:
                    # 누락된 컬럼 추가
                    added_cols = _add_missing_columns(db_manager, table_name, missing_cols)
                    validation_results['added_columns'].extend(added_cols)
            
            # 3. 인덱스 생성
            if auto_fix and 'indexes' in schema_info:
                created_indexes = _create_indexes(db_manager, schema_info['indexes'])
                validation_results['created_indexes'].extend(created_indexes)
        
        # 결과 요약
        if validation_results['missing_tables'] or validation_results['missing_columns']:
            if auto_fix:
                secure_logger.info(f"🔧 자동 복구 완료 - 테이블: {len(validation_results['created_tables'])}, 컬럼: {len(validation_results['added_columns'])}")
            else:
                secure_logger.warning(f"⚠️ 스키마 불일치 감지 - 누락 테이블: {len(validation_results['missing_tables'])}, 누락 컬럼: {len(validation_results['missing_columns'])}")
                validation_results['status'] = 'warning'
        else:
            secure_logger.info("✅ DB 스키마 일관성 검증 완료 - 모든 스키마 정상")
        
        return validation_results
        
    except Exception as e:
        error_msg = f"DB 스키마 검증 중 오류: {str(e)}"
        secure_logger.error(f"❌ {error_msg}")
        validation_results['status'] = 'error'
        validation_results['errors'].append(error_msg)
        return validation_results


def _check_table_exists(db_manager: DBManager, table_name: str) -> bool:
    """테이블 존재 여부 확인"""
    try:
        result = db_manager.execute_query("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        return result[0][0] if result else False
    except:
        return False


def _create_table(db_manager: DBManager, table_name: str, columns: list) -> bool:
    """테이블 생성"""
    try:
        columns_sql = ',\n    '.join(columns)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
        )
        """
        db_manager.execute_query(create_sql)
        return True
    except Exception as e:
        secure_logger.error(f"❌ {table_name} 테이블 생성 실패: {str(e)}")
        return False


def _check_missing_columns(db_manager: DBManager, table_name: str, required_columns: list) -> list:
    """누락된 컬럼 확인"""
    try:
        # 현재 테이블의 컬럼 목록 조회
        existing_columns = db_manager.execute_query("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
        """, (table_name,))
        
        existing_col_names = {row[0] for row in existing_columns} if existing_columns else set()
        
        missing_columns = []
        for col_def in required_columns:
            if 'PRIMARY KEY' in col_def or 'INDEX' in col_def:
                continue
            
            col_name = col_def.split()[0]
            if col_name not in existing_col_names:
                missing_columns.append(col_def)
        
        return missing_columns
        
    except Exception as e:
        secure_logger.error(f"❌ {table_name} 컬럼 확인 실패: {str(e)}")
        return []


def _add_missing_columns(db_manager: DBManager, table_name: str, missing_columns: list) -> list:
    """누락된 컬럼 추가"""
    added_columns = []
    
    for col_def in missing_columns:
        try:
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_def}"
            db_manager.execute_query(alter_sql)
            added_columns.append(f"{table_name}.{col_def.split()[0]}")
            secure_logger.info(f"✅ {table_name} 테이블에 {col_def.split()[0]} 컬럼 추가됨")
        except Exception as e:
            secure_logger.error(f"❌ {table_name}.{col_def.split()[0]} 컬럼 추가 실패: {str(e)}")
    
    return added_columns


def _create_indexes(db_manager: DBManager, index_sqls: list) -> list:
    """인덱스 생성"""
    created_indexes = []
    
    for index_sql in index_sqls:
        try:
            db_manager.execute_query(index_sql)
            # 인덱스 이름 추출
            index_name = index_sql.split('IF NOT EXISTS')[1].split('ON')[0].strip()
            created_indexes.append(index_name)
        except Exception as e:
            secure_logger.error(f"❌ 인덱스 생성 실패: {str(e)}")
    
    return created_indexes


def run_schema_validation_and_recovery():
    """
    스키마 검증 및 복구 실행 (독립 실행용)
    """
    print("🔍 DB 스키마 검증 및 복구 시작...\n")
    
    try:
        db_manager = DBManager()
        results = validate_db_schema_consistency(db_manager, auto_fix=True)
        
        print(f"✅ 검증 상태: {results['status']}")
        print(f"📊 검사한 테이블: {results['checked_tables']}개")
        
        if results['created_tables']:
            print(f"🆕 생성된 테이블: {', '.join(results['created_tables'])}")
        
        if results['added_columns']:
            print(f"➕ 추가된 컬럼: {', '.join(results['added_columns'])}")
        
        if results['created_indexes']:
            print(f"🔗 생성된 인덱스: {', '.join(results['created_indexes'])}")
        
        if results['errors']:
            print(f"❌ 오류: {', '.join(results['errors'])}")
        
        print("\n🎉 DB 스키마 검증 및 복구 완료!")
        
    except Exception as e:
        print(f"❌ 스키마 검증 실행 중 오류: {str(e)}")


# 실행부 확장
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--schema":
        run_schema_validation_and_recovery()
    elif len(sys.argv) > 1 and sys.argv[1] == "--cache-stats":
        # 캐시 통계 표시
        optimizer = get_global_optimizer()
        stats = optimizer.cache_manager.get_cache_stats()
        print("📊 캐시 시스템 통계:")
        print(f"  - 총 엔트리: {stats['total_entries']:,}개 / {stats['entries_limit']:,}개 ({stats['entries_usage_pct']:.1f}%)")
        print(f"  - 메모리 사용량: {stats['memory_usage_mb']:.2f}MB / {stats['memory_limit_mb']:.2f}MB ({stats['memory_usage_pct']:.1f}%)")
    else:
        test_trend_analyzer_improvements()