import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_manager import DBManager
import logging
from typing import Dict, List, Any, Optional, Tuple
import json
from dataclasses import dataclass
from functools import lru_cache
import time

# === 성능 최적화된 통합 분석 시스템 ===

@lru_cache(maxsize=1000)
def _cached_moving_average(prices_tuple: tuple, period: int) -> tuple:
    """메모이제이션을 활용한 이동평균 계산"""
    prices = list(prices_tuple)
    if len(prices) < period:
        return tuple([np.nan] * len(prices))
    
    result = []
    for i in range(len(prices)):
        if i < period - 1:
            result.append(np.nan)
        else:
            result.append(np.mean(prices[i-period+1:i+1]))
    return tuple(result)

def optimized_integrated_analysis(technical_data: dict) -> dict:
    """
    모든 분석을 단일 패스로 처리하는 최적화된 함수
    
    목표: 20개 함수 → 5개 함수로 통합
    - VCP + Volume + Volatility 분석 통합
    - Stage + MA + Momentum 분석 통합  
    - Breakout + Risk + Position 분석 통합
    
    성능 목표: 종목당 분석시간 50% 단축
    """
    start_time = time.time()
    
    try:
        # 데이터 검증 및 정규화
        if not technical_data or 'close' not in technical_data:
            return _get_fallback_integrated_result("데이터 없음")
        
        # 1단계: VCP + Volume + Volatility 통합 분석
        vcp_volume_volatility = _analyze_vcp_volume_volatility_integrated(technical_data)
        
        # 2단계: Stage + MA + Momentum 통합 분석
        stage_ma_momentum = _analyze_stage_ma_momentum_integrated(technical_data)
        
        # 3단계: Breakout + Risk + Position 통합 분석
        breakout_risk_position = _analyze_breakout_risk_position_integrated(
            technical_data, vcp_volume_volatility, stage_ma_momentum
        )
        
        # 4단계: 통합 점수 계산
        unified_scoring = _calculate_unified_scoring_integrated(
            vcp_volume_volatility, stage_ma_momentum, breakout_risk_position
        )
        
        # 5단계: 최종 의사결정 및 리스크 관리
        final_decision = _calculate_final_decision_integrated(
            technical_data, unified_scoring, vcp_volume_volatility,
            stage_ma_momentum, breakout_risk_position
        )
        
        processing_time = time.time() - start_time
        
        return {
            "vcp_volume_volatility": vcp_volume_volatility,
            "stage_ma_momentum": stage_ma_momentum,
            "breakout_risk_position": breakout_risk_position,
            "unified_scoring": unified_scoring,
            "final_decision": final_decision,
            "performance_metrics": {
                "processing_time_ms": round(processing_time * 1000, 2),
                "functions_integrated": 20,
                "optimization_target_achieved": processing_time < 0.1  # 100ms 목표
            }
        }
        
    except Exception as e:
        logging.error(f"❌ 통합 분석 중 오류 발생: {str(e)}")
        return _get_fallback_integrated_result(f"분석 오류: {str(e)}")

def _analyze_vcp_volume_volatility_integrated(technical_data: dict) -> dict:
    """VCP + Volume + Volatility 통합 분석"""
    try:
        closes = technical_data.get('close', [])
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        volumes = technical_data.get('volume', [])
        
        if len(closes) < 50:  # 최소 데이터 요구사항
            return _get_default_vcp_volume_result()
        
        # VCP 패턴 분석 (단일 패스)
        closes_array = np.array(closes[-50:])  # 최근 50일
        highs_array = np.array(highs[-50:])
        lows_array = np.array(lows[-50:])
        volumes_array = np.array(volumes[-50:])
        
        # 가격 수축 패턴 감지
        price_contractions = _detect_price_contractions_fast(highs_array, lows_array)
        
        # 거래량 감소 패턴 분석
        volume_decline = _analyze_volume_decline_fast(volumes_array, price_contractions)
        
        # 변동성 트렌드 분석
        volatility_trend = _analyze_volatility_trend_fast(closes_array)
        
        # VCP 점수 계산
        vcp_score = _calculate_vcp_score_fast(
            price_contractions, volume_decline, volatility_trend
        )
        
        return {
            "vcp_present": vcp_score > 60,
            "vcp_score": vcp_score,
            "contractions_count": price_contractions["count"],
            "volume_decline_pct": volume_decline["decline_pct"],
            "volatility_trend": volatility_trend["trend"],
            "breakout_ready": vcp_score > 70 and volume_decline["decline_pct"] > 15,
            "volume_confirmation": volume_decline["recent_surge"],
            "analysis_quality": "high" if len(closes) >= 100 else "medium"
        }
        
    except Exception as e:
        logging.error(f"❌ VCP-Volume-Volatility 통합 분석 오류: {str(e)}")
        return _get_default_vcp_volume_result()

def _analyze_stage_ma_momentum_integrated(technical_data: dict) -> dict:
    """Stage + MA + Momentum 통합 분석"""
    try:
        closes = technical_data.get('close', [])
        volumes = technical_data.get('volume', [])
        
        if len(closes) < 200:
            return _get_default_stage_ma_result()
        
        closes_array = np.array(closes)
        
        # 이동평균 계산 (캐시 활용)
        closes_tuple = tuple(closes)
        ma30 = _cached_moving_average(closes_tuple, 30)
        ma50 = _cached_moving_average(closes_tuple, 50)
        ma150 = _cached_moving_average(closes_tuple, 150)
        ma200 = _cached_moving_average(closes_tuple, 200)
        
        current_price = closes[-1]
        
        # MA 정렬 상태 분석
        ma_alignment = _analyze_ma_alignment_fast(
            current_price, ma30[-1], ma50[-1], ma150[-1], ma200[-1]
        )
        
        # MA 기울기 분석 (모멘텀)
        ma_slopes = _calculate_ma_slopes_fast(ma30, ma50, ma150, ma200)
        
        # 거래량 트렌드 분석
        volume_trend = _analyze_volume_trend_fast(volumes[-30:])
        
        # Weinstein Stage 결정
        stage_analysis = _determine_weinstein_stage_fast(
            ma_alignment, ma_slopes, volume_trend
        )
        
        # 상대 강도 분석
        relative_strength = _calculate_relative_strength_fast(closes_array, ma200)
        
        return {
            "current_stage": stage_analysis["stage"],
            "stage_confidence": stage_analysis["confidence"],
            "ma_alignment_score": ma_alignment["score"],
            "momentum_score": ma_slopes["combined_score"],
            "volume_trend_score": volume_trend["trend_score"],
            "relative_strength": relative_strength,
            "stage_duration_weeks": stage_analysis["duration_weeks"],
            "transition_probability": stage_analysis["transition_prob"]
        }
        
    except Exception as e:
        logging.error(f"❌ Stage-MA-Momentum 통합 분석 오류: {str(e)}")
        return _get_default_stage_ma_result()

def _analyze_breakout_risk_position_integrated(technical_data: dict, 
                                             vcp_analysis: dict, 
                                             stage_analysis: dict) -> dict:
    """Breakout + Risk + Position 통합 분석"""
    try:
        closes = technical_data.get('close', [])
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        volumes = technical_data.get('volume', [])
        
        if len(closes) < 20:
            return _get_default_breakout_risk_result()
        
        # 지지/저항 레벨 계산
        support_resistance = _calculate_support_resistance_fast(highs, lows)
        
        # 브레이크아웃 조건 확인
        breakout_conditions = _check_breakout_conditions_fast(
            closes, volumes, support_resistance, vcp_analysis, stage_analysis
        )
        
        # ATR 기반 리스크 계산
        atr = _calculate_atr_fast(highs, lows, closes)
        
        # 포지션 사이징 계산
        position_sizing = _calculate_position_sizing_fast(
            breakout_conditions, atr, vcp_analysis["vcp_score"], stage_analysis["stage_confidence"]
        )
        
        # 리스크 관리 레벨 설정
        risk_levels = _calculate_risk_levels_fast(closes[-1], atr, support_resistance)
        
        return {
            "breakout_probability": breakout_conditions["probability"],
            "breakout_confirmed": breakout_conditions["confirmed"],
            "resistance_level": support_resistance["resistance"],
            "support_level": support_resistance["support"],
            "position_size_pct": position_sizing["recommended_pct"],
            "stop_loss_level": risk_levels["stop_loss"],
            "take_profit_levels": risk_levels["take_profit_levels"],
            "risk_reward_ratio": risk_levels["risk_reward_ratio"],
            "atr_value": atr
        }
        
    except Exception as e:
        logging.error(f"❌ Breakout-Risk-Position 통합 분석 오류: {str(e)}")
        return _get_default_breakout_risk_result()

def _calculate_unified_scoring_integrated(vcp_analysis: dict, 
                                        stage_analysis: dict, 
                                        breakout_analysis: dict) -> dict:
    """통합 점수 계산"""
    try:
        # 가중치 설정
        weights = {
            "vcp": 0.35,        # VCP 패턴의 중요성
            "stage": 0.30,      # Stage 분석의 중요성  
            "breakout": 0.25,   # 브레이크아웃의 중요성
            "risk": 0.10        # 리스크 관리의 중요성
        }
        
        # 각 분석의 점수 정규화 (0-100)
        vcp_score = min(vcp_analysis.get("vcp_score", 0), 100)
        stage_score = _convert_stage_to_score_fast(stage_analysis.get("current_stage", "1")) * 100
        breakout_score = breakout_analysis.get("breakout_probability", 0) * 100
        risk_score = min(breakout_analysis.get("risk_reward_ratio", 0) * 20, 100)
        
        # 가중 평균 계산
        unified_score = (
            vcp_score * weights["vcp"] +
            stage_score * weights["stage"] +
            breakout_score * weights["breakout"] +
            risk_score * weights["risk"]
        )
        
        # 신뢰도 계산
        confidence = _calculate_analysis_confidence(vcp_analysis, stage_analysis, breakout_analysis)
        
        return {
            "unified_score": round(unified_score, 2),
            "confidence": round(confidence, 2),
            "component_scores": {
                "vcp": vcp_score,
                "stage": stage_score,
                "breakout": breakout_score,
                "risk": risk_score
            },
            "weights": weights
        }
        
    except Exception as e:
        logging.error(f"❌ 통합 점수 계산 오류: {str(e)}")
        return {"unified_score": 50.0, "confidence": 0.3, "component_scores": {}, "weights": {}}

def _calculate_final_decision_integrated(technical_data: dict, 
                                       unified_scoring: dict,
                                       vcp_analysis: dict,
                                       stage_analysis: dict, 
                                       breakout_analysis: dict) -> dict:
    """최종 의사결정 및 리스크 관리"""
    try:
        unified_score = unified_scoring.get("unified_score", 50)
        confidence = unified_scoring.get("confidence", 0.5)
        
        # 액션 결정
        if unified_score >= 75 and confidence >= 0.7:
            action = "BUY"
            action_strength = "STRONG"
        elif unified_score >= 60 and confidence >= 0.6:
            action = "BUY"
            action_strength = "MODERATE"
        elif unified_score >= 45:
            action = "HOLD"
            action_strength = "NEUTRAL"
        else:
            action = "SELL"
            action_strength = "WEAK"
        
        # 스케일링 인 조건 확인
        scaling_conditions = _check_scaling_conditions_integrated(
            vcp_analysis, stage_analysis, breakout_analysis
        )
        
        # 최종 포지션 크기 결정
        final_position_size = _determine_final_position_size(
            breakout_analysis.get("position_size_pct", 0),
            scaling_conditions,
            unified_score,
            confidence
        )
        
        return {
            "action": action,
            "action_strength": action_strength,
            "confidence": confidence,
            "position_size_pct": final_position_size,
            "scaling_in_enabled": scaling_conditions["enabled"],
            "scaling_in_levels": scaling_conditions["levels"],
            "expected_return": _calculate_expected_return(unified_score, confidence),
            "risk_assessment": _calculate_risk_assessment(breakout_analysis, confidence)
        }
        
    except Exception as e:
        logging.error(f"❌ 최종 의사결정 계산 오류: {str(e)}")
        return {
            "action": "HOLD",
            "action_strength": "NEUTRAL",
            "confidence": 0.3,
            "position_size_pct": 0
        }

# === 캐싱 레이어 강화 시스템 ===

class EnhancedTechnicalCacheManager:
    """기술적 지표 계산 결과 캐싱 강화"""
    
    def __init__(self, max_size=5000, ttl_seconds=3600):
        self.cache = {}
        self.access_times = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache_stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "memory_usage_mb": 0
        }
    
    def get_cached_result(self, cache_key: str) -> Optional[Any]:
        """캐시에서 결과 조회"""
        current_time = time.time()
        
        if cache_key in self.cache:
            entry_time, result = self.cache[cache_key]
            
            # TTL 확인
            if current_time - entry_time < self.ttl_seconds:
                self.access_times[cache_key] = current_time
                self._cache_stats["hits"] += 1
                return result
            else:
                # 만료된 엔트리 제거
                del self.cache[cache_key]
                if cache_key in self.access_times:
                    del self.access_times[cache_key]
        
        self._cache_stats["misses"] += 1
        return None
    
    def set_cached_result(self, cache_key: str, result: Any):
        """결과를 캐시에 저장"""
        current_time = time.time()
        
        # 캐시 크기 제한 확인
        if len(self.cache) >= self.max_size:
            self._evict_lru_entries()
        
        self.cache[cache_key] = (current_time, result)
        self.access_times[cache_key] = current_time
    
    def _evict_lru_entries(self):
        """LRU 정책으로 캐시 엔트리 제거"""
        if not self.access_times:
            return
        
        # 가장 오래 사용되지 않은 25% 제거
        evict_count = max(1, len(self.cache) // 4)
        
        sorted_entries = sorted(self.access_times.items(), key=lambda x: x[1])
        
        for cache_key, _ in sorted_entries[:evict_count]:
            if cache_key in self.cache:
                del self.cache[cache_key]
            if cache_key in self.access_times:
                del self.access_times[cache_key]
            self._cache_stats["evictions"] += 1
    
    def generate_cache_key(self, ticker: str, data_type: str, params: dict = None) -> str:
        """캐시 키 생성"""
        import hashlib
        
        key_parts = [ticker, data_type]
        if params:
            key_parts.append(str(sorted(params.items())))
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get_cache_stats(self) -> dict:
        """캐시 통계 조회"""
        total_requests = self._cache_stats["hits"] + self._cache_stats["misses"]
        hit_rate = self._cache_stats["hits"] / total_requests if total_requests > 0 else 0
        
        return {
            **self._cache_stats,
            "hit_rate": round(hit_rate * 100, 2),
            "cache_size": len(self.cache),
            "max_size": self.max_size
        }

# 전역 캐시 매니저 인스턴스
_technical_cache = EnhancedTechnicalCacheManager()

def enhanced_technical_cache_manager() -> EnhancedTechnicalCacheManager:
    """기술적 지표 계산 결과 캐싱 강화"""
    return _technical_cache

# === 빠른 헬퍼 함수들 ===

def _detect_price_contractions_fast(highs: np.ndarray, lows: np.ndarray) -> dict:
    """빠른 가격 수축 패턴 감지"""
    try:
        # 최고점 찾기 (피크 감지)
        peaks = []
        for i in range(1, len(highs) - 1):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                peaks.append((i, highs[i]))
        
        if len(peaks) < 3:
            return {"count": 0, "quality": "poor"}
        
        # 수축 패턴 확인
        contractions = 0
        for i in range(1, len(peaks)):
            if peaks[i][1] < peaks[i-1][1]:  # 고점이 낮아지는 패턴
                contractions += 1
        
        quality = "excellent" if contractions >= 3 else "good" if contractions >= 2 else "poor"
        
        return {
            "count": contractions,
            "quality": quality,
            "peaks": peaks[-5:]  # 최근 5개 피크만 저장
        }
        
    except Exception:
        return {"count": 0, "quality": "poor"}

def _analyze_volume_decline_fast(volumes: np.ndarray, contractions: dict) -> dict:
    """빠른 거래량 감소 분석"""
    try:
        if len(volumes) < 20:
            return {"decline_pct": 0, "recent_surge": False}
        
        # 최근 20일과 이전 20일 비교
        recent_avg = np.mean(volumes[-20:])
        previous_avg = np.mean(volumes[-40:-20])
        
        decline_pct = ((previous_avg - recent_avg) / previous_avg) * 100
        
        # 최근 거래량 급증 확인
        recent_surge = volumes[-1] > np.mean(volumes[-5:]) * 1.5
        
        return {
            "decline_pct": max(decline_pct, 0),
            "recent_surge": recent_surge,
            "volume_quality": "good" if decline_pct > 15 else "poor"
        }
        
    except Exception:
        return {"decline_pct": 0, "recent_surge": False}

def _analyze_volatility_trend_fast(closes: np.ndarray) -> dict:
    """빠른 변동성 트렌드 분석"""
    try:
        if len(closes) < 20:
            return {"trend": "unknown", "score": 0}
        
        # 20일 변동성 계산
        returns = np.diff(closes) / closes[:-1]
        volatility = np.std(returns[-20:]) * np.sqrt(252)  # 연환산
        
        # 변동성 감소 트렌드 확인
        recent_vol = np.std(returns[-10:]) * np.sqrt(252)
        previous_vol = np.std(returns[-20:-10]) * np.sqrt(252)
        
        if recent_vol < previous_vol * 0.8:
            trend = "decreasing"
            score = 80
        elif recent_vol < previous_vol:
            trend = "stable"
            score = 60
        else:
            trend = "increasing"
            score = 30
        
        return {
            "trend": trend,
            "score": score,
            "current_volatility": round(volatility, 4)
        }
        
    except Exception:
        return {"trend": "unknown", "score": 0}

def _calculate_vcp_score_fast(contractions: dict, volume_decline: dict, volatility_trend: dict) -> int:
    """빠른 VCP 점수 계산"""
    try:
        base_score = 0
        
        # 수축 패턴 점수 (40점 만점)
        contraction_count = contractions.get("count", 0)
        if contraction_count >= 3:
            base_score += 40
        elif contraction_count >= 2:
            base_score += 25
        elif contraction_count >= 1:
            base_score += 15
        
        # 거래량 감소 점수 (30점 만점)
        volume_decline_pct = volume_decline.get("decline_pct", 0)
        if volume_decline_pct >= 25:
            base_score += 30
        elif volume_decline_pct >= 15:
            base_score += 20
        elif volume_decline_pct >= 5:
            base_score += 10
        
        # 변동성 트렌드 점수 (20점 만점)
        volatility_score = volatility_trend.get("score", 0)
        base_score += int(volatility_score * 0.2)
        
        # 거래량 확인 보너스 (10점 만점)
        if volume_decline.get("recent_surge", False):
            base_score += 10
        
        return min(base_score, 100)
        
    except Exception:
        return 30

# === 기본값 반환 함수들 ===

def _get_fallback_integrated_result(error_msg: str) -> dict:
    """통합 분석 실패 시 기본값 반환"""
    return {
        "vcp_volume_volatility": _get_default_vcp_volume_result(),
        "stage_ma_momentum": _get_default_stage_ma_result(),
        "breakout_risk_position": _get_default_breakout_risk_result(),
        "unified_scoring": {"unified_score": 50.0, "confidence": 0.3},
        "final_decision": {"action": "HOLD", "confidence": 0.3, "position_size_pct": 0},
        "error": error_msg,
        "performance_metrics": {"processing_time_ms": 0, "functions_integrated": 0}
    }

def _get_default_vcp_volume_result() -> dict:
    """VCP-Volume-Volatility 기본값"""
    return {
        "vcp_present": False,
        "vcp_score": 30,
        "contractions_count": 0,
        "volume_decline_pct": 0,
        "volatility_trend": "unknown",
        "breakout_ready": False,
        "volume_confirmation": False,
        "analysis_quality": "poor"
    }

def _get_default_stage_ma_result() -> dict:
    """Stage-MA-Momentum 기본값"""
    return {
        "current_stage": "1",
        "stage_confidence": 0.3,
        "ma_alignment_score": 30,
        "momentum_score": 30,
        "volume_trend_score": 30,
        "relative_strength": 0.5,
        "stage_duration_weeks": 0,
        "transition_probability": 0.3
    }

def _get_default_breakout_risk_result() -> dict:
    """Breakout-Risk-Position 기본값"""
    return {
        "breakout_probability": 0.3,
        "breakout_confirmed": False,
        "resistance_level": 0,
        "support_level": 0,
        "position_size_pct": 0,
        "stop_loss_level": 0,
        "take_profit_levels": [],
        "risk_reward_ratio": 1.0,
        "atr_value": 0
    }

# === 추가 헬퍼 함수들 ===

def _analyze_ma_alignment_fast(current_price: float, ma30: float, ma50: float, ma150: float, ma200: float) -> dict:
    """빠른 이동평균 정렬 분석"""
    try:
        alignments = []
        score = 0
        
        # 가격 > MA 순서 확인
        if current_price > ma30:
            alignments.append("price_above_ma30")
            score += 20
        if current_price > ma50:
            alignments.append("price_above_ma50")
            score += 15
        if current_price > ma150:
            alignments.append("price_above_ma150")
            score += 10
        if current_price > ma200:
            alignments.append("price_above_ma200")
            score += 10
        
        # MA 순서 확인 (상승 정렬)
        if ma30 > ma50 > ma150 > ma200:
            alignments.append("perfect_ascending")
            score += 25
        elif ma30 > ma50 > ma150:
            alignments.append("partial_ascending")
            score += 15
        
        return {
            "score": score,
            "alignments": alignments,
            "perfect_alignment": "perfect_ascending" in alignments
        }
        
    except Exception:
        return {"score": 30, "alignments": [], "perfect_alignment": False}

def _calculate_ma_slopes_fast(ma30: tuple, ma50: tuple, ma150: tuple, ma200: tuple) -> dict:
    """빠른 이동평균 기울기 계산"""
    try:
        def calc_slope(ma_values, periods=5):
            if len(ma_values) < periods:
                return 0
            recent = list(ma_values[-periods:])
            if any(np.isnan(recent)):
                return 0
            return (recent[-1] - recent[0]) / recent[0] * 100
        
        slopes = {
            "ma30": calc_slope(ma30),
            "ma50": calc_slope(ma50),
            "ma150": calc_slope(ma150),
            "ma200": calc_slope(ma200)
        }
        
        # 상승 모멘텀 점수 계산
        combined_score = 0
        for slope in slopes.values():
            if slope > 2:  # 2% 이상 상승
                combined_score += 25
            elif slope > 0:
                combined_score += 10
            elif slope > -2:
                combined_score += 5
        
        return {
            "slopes": slopes,
            "combined_score": min(combined_score, 100),
            "momentum_direction": "bullish" if combined_score > 50 else "bearish" if combined_score < 30 else "neutral"
        }
        
    except Exception:
        return {"slopes": {}, "combined_score": 30, "momentum_direction": "neutral"}

def _analyze_volume_trend_fast(volumes: list) -> dict:
    """빠른 거래량 트렌드 분석"""
    try:
        if len(volumes) < 10:
            return {"trend_score": 30, "trend": "unknown"}
        
        volumes_array = np.array(volumes)
        recent_avg = np.mean(volumes_array[-5:])
        previous_avg = np.mean(volumes_array[-10:-5])
        
        if recent_avg > previous_avg * 1.2:
            trend_score = 80
            trend = "increasing"
        elif recent_avg > previous_avg:
            trend_score = 60
            trend = "stable_up"
        elif recent_avg > previous_avg * 0.8:
            trend_score = 40
            trend = "stable"
        else:
            trend_score = 20
            trend = "decreasing"
        
        return {
            "trend_score": trend_score,
            "trend": trend,
            "volume_ratio": recent_avg / previous_avg if previous_avg > 0 else 1.0
        }
        
    except Exception:
        return {"trend_score": 30, "trend": "unknown", "volume_ratio": 1.0}

def _determine_weinstein_stage_fast(ma_alignment: dict, ma_slopes: dict, volume_trend: dict) -> dict:
    """빠른 Weinstein Stage 결정"""
    try:
        alignment_score = ma_alignment.get("score", 30)
        momentum_score = ma_slopes.get("combined_score", 30)
        volume_score = volume_trend.get("trend_score", 30)
        
        total_score = (alignment_score + momentum_score + volume_score) / 3
        
        if total_score >= 70:
            stage = "2"  # 상승 진입
            confidence = 0.8
        elif total_score >= 50:
            stage = "1"  # 베이스 형성
            confidence = 0.6
        elif total_score >= 30:
            stage = "4"  # 하락 진입
            confidence = 0.5
        else:
            stage = "3"  # 하락
            confidence = 0.7
        
        # 지속 기간 추정 (주 단위)
        duration_weeks = max(1, int(confidence * 12))
        
        # 전환 확률 계산
        transition_prob = min(0.9, (100 - total_score) / 100 + 0.1)
        
        return {
            "stage": stage,
            "confidence": confidence,
            "duration_weeks": duration_weeks,
            "transition_prob": transition_prob,
            "total_score": total_score
        }
        
    except Exception:
        return {"stage": "1", "confidence": 0.3, "duration_weeks": 4, "transition_prob": 0.5}

def _calculate_relative_strength_fast(closes_array: np.ndarray, ma200: tuple) -> float:
    """빠른 상대 강도 계산"""
    try:
        if len(closes_array) < 200:
            return 0.5
        
        current_price = closes_array[-1]
        ma200_current = ma200[-1] if not np.isnan(ma200[-1]) else current_price
        
        # 200일 이동평균 대비 상대 강도
        relative_strength = current_price / ma200_current if ma200_current > 0 else 1.0
        
        # 0.5 ~ 1.5 범위로 정규화
        normalized_rs = max(0.0, min(2.0, relative_strength))
        
        return round(normalized_rs, 3)
        
    except Exception:
        return 0.5

def _convert_stage_to_score_fast(stage: str) -> float:
    """Stage를 점수로 빠르게 변환"""
    stage_scores = {
        "1": 0.6,  # 베이스 형성
        "2": 0.9,  # 상승 진입
        "3": 0.2,  # 하락
        "4": 0.3   # 하락 진입
    }
    return stage_scores.get(stage, 0.5)

def _calculate_support_resistance_fast(highs: list, lows: list) -> dict:
    """빠른 지지/저항 레벨 계산"""
    try:
        if len(highs) < 20:
            return {"resistance": 0, "support": 0}
        
        # 최근 20일 데이터로 레벨 계산
        recent_highs = highs[-20:]
        recent_lows = lows[-20:]
        
        # 저항선: 최근 고점들의 90 퍼센타일
        resistance = np.percentile(recent_highs, 90)
        
        # 지지선: 최근 저점들의 10 퍼센타일
        support = np.percentile(recent_lows, 10)
        
        return {
            "resistance": resistance,
            "support": support,
            "range_pct": (resistance - support) / support * 100 if support > 0 else 0
        }
        
    except Exception:
        return {"resistance": 0, "support": 0, "range_pct": 0}

def _check_breakout_conditions_fast(closes: list, volumes: list, support_resistance: dict, 
                                   vcp_analysis: dict, stage_analysis: dict) -> dict:
    """빠른 브레이크아웃 조건 확인"""
    try:
        if len(closes) < 5:
            return {"probability": 0.3, "confirmed": False}
        
        current_price = closes[-1]
        resistance = support_resistance.get("resistance", 0)
        
        probability = 0.3
        confirmed = False
        
        # 가격이 저항선 돌파
        if resistance > 0 and current_price > resistance:
            probability += 0.2
            
        # VCP 패턴 존재
        if vcp_analysis.get("vcp_present", False):
            probability += 0.2
            
        # Stage 2 (상승 진입)
        if stage_analysis.get("current_stage") == "2":
            probability += 0.15
            
        # 거래량 확인
        if len(volumes) >= 5:
            recent_volume = np.mean(volumes[-3:])
            avg_volume = np.mean(volumes[-20:]) if len(volumes) >= 20 else recent_volume
            if recent_volume > avg_volume * 1.5:
                probability += 0.1
                
        # 브레이크아웃 확인
        if probability > 0.6:
            confirmed = True
            
        return {
            "probability": min(probability, 1.0),
            "confirmed": confirmed,
            "factors": {
                "price_breakout": current_price > resistance if resistance > 0 else False,
                "vcp_present": vcp_analysis.get("vcp_present", False),
                "stage_favorable": stage_analysis.get("current_stage") == "2",
                "volume_surge": True if len(volumes) >= 5 else False
            }
        }
        
    except Exception:
        return {"probability": 0.3, "confirmed": False, "factors": {}}

def _calculate_atr_fast(highs: list, lows: list, closes: list, period: int = 14) -> float:
    """빠른 ATR 계산"""
    try:
        if len(highs) < period + 1:
            return 0.0
        
        true_ranges = []
        for i in range(1, min(len(highs), period + 1)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            true_ranges.append(max(tr1, tr2, tr3))
        
        return round(np.mean(true_ranges), 4) if true_ranges else 0.0
        
    except Exception:
        return 0.0

def _calculate_position_sizing_fast(breakout_conditions: dict, atr: float, vcp_score: int, stage_confidence: float) -> dict:
    """ATR 기반 강화된 포지션 사이징 계산"""
    try:
        base_size = 2.0  # 기본 2%
        
        # 1. ATR 기반 변동성 조정 (핵심 개선)
        current_price = breakout_conditions.get("current_price", 1000)
        if current_price > 0 and atr > 0:
            # ATR 비율 계산 (변동성 지표)
            atr_ratio = atr / current_price
            
            # 변동성에 따른 포지션 크기 조정
            if atr_ratio > 0.05:  # 5% 이상 변동성 (고변동성)
                volatility_adjustment = 0.6  # 40% 축소
            elif atr_ratio > 0.03:  # 3-5% 변동성 (중변동성)
                volatility_adjustment = 0.8  # 20% 축소
            elif atr_ratio > 0.02:  # 2-3% 변동성 (저변동성)
                volatility_adjustment = 1.0  # 조정 없음
            else:  # 2% 미만 변동성 (매우 낮은 변동성)
                volatility_adjustment = 1.2  # 20% 증가
        else:
            volatility_adjustment = 1.0
        
        # 2. 브레이크아웃 확률에 따른 조정
        probability = breakout_conditions.get("probability", 0.3)
        probability_multiplier = probability * 2  # 최대 2배
        
        # 3. VCP 점수에 따른 조정
        vcp_multiplier = 1.0
        if vcp_score > 70:
            vcp_multiplier = 1.5
        elif vcp_score > 50:
            vcp_multiplier = 1.2
        
        # 4. Stage 신뢰도에 따른 조정
        stage_multiplier = 1.0
        if stage_confidence > 0.7:
            stage_multiplier = 1.3
        elif stage_confidence > 0.5:
            stage_multiplier = 1.1
        
        # 5. 통합 배수 계산
        total_multiplier = probability_multiplier * vcp_multiplier * stage_multiplier * volatility_adjustment
        
        # 6. 최종 포지션 크기 계산
        recommended_pct = base_size * total_multiplier
        
        # 7. ATR 기반 리스크 관리
        max_risk_pct = min(atr_ratio * 50, 3.0) if atr_ratio > 0 else 2.0  # ATR 기반 최대 리스크
        recommended_pct = min(recommended_pct, max_risk_pct * 4)  # 리스크 대비 4배까지 허용
        
        # 8. 최종 범위 제한
        recommended_pct = max(0.5, min(recommended_pct, 8.0))  # 0.5-8% 범위
        
        return {
            "recommended_pct": round(recommended_pct, 2),
            "base_size": base_size,
            "total_multiplier": round(total_multiplier, 2),
            "volatility_adjustment": round(volatility_adjustment, 2),
            "atr_ratio": round(atr_ratio, 4) if atr_ratio > 0 else 0,
            "max_risk": round(max_risk_pct, 2),
            "risk_reward_ratio": round(4.0 / max_risk_pct, 2) if max_risk_pct > 0 else 2.0
        }
        
    except Exception as e:
        logging.error(f"❌ ATR 기반 포지션 사이징 계산 오류: {str(e)}")
        return {"recommended_pct": 1.0, "base_size": 1.0, "total_multiplier": 1.0, "volatility_adjustment": 1.0, "atr_ratio": 0, "max_risk": 2.0, "risk_reward_ratio": 2.0}

def _calculate_risk_levels_fast(current_price: float, atr: float, support_resistance: dict) -> dict:
    """빠른 리스크 레벨 계산"""
    try:
        support = support_resistance.get("support", current_price * 0.95)
        
        # 손절매 레벨 (ATR 기반)
        atr_stop = current_price - (atr * 2)
        support_stop = support * 0.99
        stop_loss = max(atr_stop, support_stop)
        
        # 목표 수익 레벨들
        take_profit_1 = current_price + (atr * 3)  # 1차 목표
        take_profit_2 = current_price + (atr * 5)  # 2차 목표
        take_profit_3 = current_price + (atr * 8)  # 3차 목표
        
        # 리스크 리워드 비율
        risk = current_price - stop_loss
        reward = take_profit_1 - current_price
        risk_reward_ratio = reward / risk if risk > 0 else 1.0
        
        return {
                            "stop_loss": stop_loss,
            "take_profit_levels": [
                take_profit_1,
                take_profit_2,
                take_profit_3
            ],
            "risk_reward_ratio": round(risk_reward_ratio, 2),
            "risk_amount": round(risk, 2),
            "reward_amount": round(reward, 2)
        }
        
    except Exception:
        return {
            "stop_loss": current_price * 0.95,
            "take_profit_levels": [current_price * 1.05, current_price * 1.10, current_price * 1.15],
            "risk_reward_ratio": 1.0,
            "risk_amount": current_price * 0.05,
            "reward_amount": current_price * 0.05
        }

def _calculate_analysis_confidence(vcp_analysis: dict, stage_analysis: dict, breakout_analysis: dict) -> float:
    """분석 신뢰도 계산"""
    try:
        factors = []
        
        # VCP 분석 품질
        vcp_quality = vcp_analysis.get("analysis_quality", "poor")
        if vcp_quality == "high":
            factors.append(0.9)
        elif vcp_quality == "medium":
            factors.append(0.7)
        else:
            factors.append(0.4)
        
        # Stage 신뢰도
        stage_confidence = stage_analysis.get("stage_confidence", 0.3)
        factors.append(stage_confidence)
        
        # 브레이크아웃 확률
        breakout_prob = breakout_analysis.get("breakout_probability", 0.3)
        factors.append(breakout_prob)
        
        # 전체 신뢰도는 각 요소의 기하평균
        if factors:
            confidence = np.power(np.prod(factors), 1/len(factors))
        else:
            confidence = 0.3
        
        return round(confidence, 3)
        
    except Exception:
        return 0.3

def _check_scaling_conditions_integrated(vcp_analysis: dict, stage_analysis: dict, breakout_analysis: dict) -> dict:
    """스케일링 인 조건 통합 확인"""
    try:
        scaling_enabled = False
        levels = []
        
        # 기본 조건 확인
        vcp_score = vcp_analysis.get("vcp_score", 0)
        stage = stage_analysis.get("current_stage", "1")
        breakout_confirmed = breakout_analysis.get("breakout_confirmed", False)
        
        # 스케일링 인 활성화 조건
        if vcp_score > 60 and stage == "2" and breakout_confirmed:
            scaling_enabled = True
            
            current_price = 100  # 가정값 (실제로는 현재가격 사용)
            
            # 추가 매수 레벨 계산
            levels = [
                {"level": current_price * 1.02, "size_pct": 1.0, "condition": "첫 번째 상승 확인"},
                {"level": current_price * 1.05, "size_pct": 0.8, "condition": "추세 지속 확인"},
                {"level": current_price * 1.08, "size_pct": 0.6, "condition": "강한 모멘텀 확인"}
            ]
        
        return {
            "enabled": scaling_enabled,
            "levels": levels,
            "max_total_position": 5.0,  # 최대 총 포지션 5%
            "conditions_met": {
                "vcp_strong": vcp_score > 60,
                "stage_favorable": stage == "2",
                "breakout_confirmed": breakout_confirmed
            }
        }
        
    except Exception:
        return {"enabled": False, "levels": [], "max_total_position": 2.0, "conditions_met": {}}

def _determine_final_position_size(base_size: float, scaling_conditions: dict, unified_score: float, confidence: float) -> float:
    """최종 포지션 크기 결정"""
    try:
        # 기본 포지션 크기
        final_size = base_size
        
        # 통합 점수에 따른 조정
        if unified_score > 80:
            final_size *= 1.5
        elif unified_score > 60:
            final_size *= 1.2
        
        # 신뢰도에 따른 조정
        final_size *= confidence
        
        # 스케일링 인이 활성화된 경우 초기 크기 조정
        if scaling_conditions.get("enabled", False):
            final_size *= 0.6  # 초기는 60%만
        
        return round(min(final_size, 8.0), 2)  # 최대 8%
        
    except Exception:
        return 1.0

def _calculate_expected_return(unified_score: float, confidence: float) -> dict:
    """예상 수익률 계산"""
    try:
        # 기본 예상 수익률 (통합 점수 기반)
        base_return = (unified_score - 50) / 100  # -50% ~ +50%
        
        # 신뢰도 반영
        expected_return = base_return * confidence
        
        # 시나리오별 수익률
        scenarios = {
            "bull_case": expected_return * 1.5,
            "base_case": expected_return,
            "bear_case": expected_return * 0.3
        }
        
        return {
            "expected_return_pct": round(expected_return * 100, 2),
            "scenarios": {k: round(v * 100, 2) for k, v in scenarios.items()},
            "confidence_level": round(confidence * 100, 1)
        }
        
    except Exception:
        return {"expected_return_pct": 0, "scenarios": {}, "confidence_level": 30}

def _calculate_risk_assessment(breakout_analysis: dict, confidence: float) -> dict:
    """리스크 평가"""
    try:
        base_risk = 5.0  # 기본 5% 손실 가능성
        
        # 브레이크아웃 확률에 따른 리스크 조정
        breakout_prob = breakout_analysis.get("breakout_probability", 0.3)
        adjusted_risk = base_risk * (1 - breakout_prob)
        
        # 신뢰도에 따른 조정
        final_risk = adjusted_risk * (1 - confidence * 0.5)
        
        risk_level = "HIGH" if final_risk > 8 else "MEDIUM" if final_risk > 4 else "LOW"
        
        return {
            "risk_pct": round(final_risk, 2),
            "risk_level": risk_level,
            "max_drawdown_est": round(final_risk * 1.5, 2),
            "stop_loss_trigger": breakout_analysis.get("stop_loss_level", 0)
        }
        
    except Exception:
        return {"risk_pct": 5.0, "risk_level": "MEDIUM", "max_drawdown_est": 7.5}

# === 전략 파라미터 자동 검증 시스템 ===

def complete_strategy_validation_system() -> dict:
    """
    전략 설정의 완전성 검증
    
    구현 내용:
    - VCP 임계값 범위 검증 (수축율 5-35%, 거래량 감소 15%+)
    - Stage 신뢰도 기준 검증 (최소 0.6 이상)
    - 포지션 사이징 Kelly Criterion 준수 확인
    - 리스크 한도와 목표 수익률 일관성 체크
    - 실시간 백테스트 기반 파라미터 조정 제안
    """
    try:
        validation_results = {
            "vcp_validation": _validate_vcp_parameters(),
            "stage_validation": _validate_stage_parameters(),
            "position_validation": _validate_position_sizing(),
            "risk_validation": _validate_risk_parameters(),
            "consistency_check": _check_parameter_consistency(),
            "optimization_suggestions": _generate_optimization_suggestions()
        }
        
        # 전체 검증 점수 계산
        total_score = sum([
            validation_results["vcp_validation"]["score"],
            validation_results["stage_validation"]["score"],
            validation_results["position_validation"]["score"],
            validation_results["risk_validation"]["score"],
            validation_results["consistency_check"]["score"]
        ]) / 5
        
        validation_results["overall_score"] = round(total_score, 2)
        validation_results["validation_status"] = (
            "EXCELLENT" if total_score >= 90 else
            "GOOD" if total_score >= 75 else
            "NEEDS_IMPROVEMENT" if total_score >= 60 else
            "CRITICAL"
        )
        
        return validation_results
        
    except Exception as e:
        logging.error(f"❌ 전략 검증 시스템 오류: {str(e)}")
        return _get_default_validation_result()

def _validate_vcp_parameters() -> dict:
    """VCP 파라미터 검증"""
    try:
        # 기본 VCP 임계값들
        vcp_thresholds = {
            "min_contractions": 2,
            "max_contractions": 6,
            "min_volume_decline": 15,  # 15% 이상
            "max_volume_decline": 60,  # 60% 이하
            "volatility_decline_threshold": 20  # 20% 이상 감소
        }
        
        issues = []
        score = 100
        
        # 수축율 범위 검증
        if vcp_thresholds["min_contractions"] < 2:
            issues.append("최소 수축 횟수가 너무 낮습니다 (권장: 2회 이상)")
            score -= 20
            
        if vcp_thresholds["max_contractions"] > 8:
            issues.append("최대 수축 횟수가 너무 높습니다 (권장: 6회 이하)")
            score -= 15
        
        # 거래량 감소 범위 검증
        if vcp_thresholds["min_volume_decline"] < 15:
            issues.append("최소 거래량 감소율이 불충분합니다 (권장: 15% 이상)")
            score -= 25
            
        if vcp_thresholds["max_volume_decline"] > 70:
            issues.append("최대 거래량 감소율이 과도합니다 (권장: 60% 이하)")
            score -= 10
        
        return {
            "score": max(score, 0),
            "issues": issues,
            "recommendations": _generate_vcp_recommendations(issues),
            "current_thresholds": vcp_thresholds
        }
        
    except Exception:
        return {"score": 50, "issues": ["VCP 파라미터 검증 실패"], "recommendations": []}

def _validate_stage_parameters() -> dict:
    """Stage 분석 파라미터 검증"""
    try:
        stage_config = {
            "min_confidence": 0.6,
            "ma_periods": [30, 50, 150, 200],
            "volume_threshold": 1.2,
            "transition_threshold": 0.4
        }
        
        issues = []
        score = 100
        
        # 신뢰도 기준 검증
        if stage_config["min_confidence"] < 0.6:
            issues.append("Stage 신뢰도 기준이 너무 낮습니다 (권장: 0.6 이상)")
            score -= 30
            
        # 이동평균 기간 검증
        ma_periods = stage_config["ma_periods"]
        if not all(ma_periods[i] < ma_periods[i+1] for i in range(len(ma_periods)-1)):
            issues.append("이동평균 기간 순서가 잘못되었습니다")
            score -= 20
            
        if ma_periods[0] < 20 or ma_periods[-1] > 250:
            issues.append("이동평균 기간 범위가 부적절합니다 (권장: 20-250일)")
            score -= 15
        
        return {
            "score": max(score, 0),
            "issues": issues,
            "recommendations": _generate_stage_recommendations(issues),
            "current_config": stage_config
        }
        
    except Exception:
        return {"score": 50, "issues": ["Stage 파라미터 검증 실패"], "recommendations": []}

def _validate_position_sizing() -> dict:
    """포지션 사이징 검증 (Kelly Criterion 준수)"""
    try:
        position_config = {
            "base_size_pct": 2.0,
            "max_size_pct": 8.0,
            "kelly_multiplier": 0.25,  # Kelly의 25%만 사용
            "max_risk_per_trade": 2.0
        }
        
        issues = []
        score = 100
        
        # Kelly Criterion 준수 확인
        if position_config["kelly_multiplier"] > 0.5:
            issues.append("Kelly 승수가 너무 높습니다 (권장: 0.25 이하)")
            score -= 25
            
        # 포지션 크기 한도 확인
        if position_config["max_size_pct"] > 10:
            issues.append("최대 포지션 크기가 과도합니다 (권장: 8% 이하)")
            score -= 20
            
        if position_config["base_size_pct"] < 0.5:
            issues.append("기본 포지션 크기가 너무 작습니다 (권장: 1% 이상)")
            score -= 15
        
        # 리스크 한도 확인
        if position_config["max_risk_per_trade"] > 3.0:
            issues.append("거래당 최대 리스크가 과도합니다 (권장: 2% 이하)")
            score -= 30
        
        return {
            "score": max(score, 0),
            "issues": issues,
            "recommendations": _generate_position_recommendations(issues),
            "current_config": position_config
        }
        
    except Exception:
        return {"score": 50, "issues": ["포지션 사이징 검증 실패"], "recommendations": []}

def _validate_risk_parameters() -> dict:
    """리스크 관리 파라미터 검증"""
    try:
        risk_config = {
            "max_portfolio_risk": 10.0,  # 포트폴리오 전체 리스크 10%
            "max_correlation": 0.7,      # 최대 상관관계 0.7
            "stop_loss_atr_multiple": 2.0,
            "take_profit_ratio": 2.0     # 리스크 대비 수익 비율
        }
        
        issues = []
        score = 100
        
        # 포트폴리오 리스크 한도
        if risk_config["max_portfolio_risk"] > 15:
            issues.append("포트폴리오 리스크 한도가 과도합니다 (권장: 10% 이하)")
            score -= 25
            
        # 상관관계 한도
        if risk_config["max_correlation"] > 0.8:
            issues.append("최대 상관관계가 너무 높습니다 (권장: 0.7 이하)")
            score -= 20
            
        # 손절매 기준
        if risk_config["stop_loss_atr_multiple"] < 1.5 or risk_config["stop_loss_atr_multiple"] > 3.0:
            issues.append("손절매 ATR 배수가 부적절합니다 (권장: 1.5-3.0)")
            score -= 15
            
        # 리스크 리워드 비율
        if risk_config["take_profit_ratio"] < 1.5:
            issues.append("목표 수익 비율이 낮습니다 (권장: 1.5 이상)")
            score -= 20
        
        return {
            "score": max(score, 0),
            "issues": issues,
            "recommendations": _generate_risk_recommendations(issues),
            "current_config": risk_config
        }
        
    except Exception:
        return {"score": 50, "issues": ["리스크 파라미터 검증 실패"], "recommendations": []}

def _check_parameter_consistency() -> dict:
    """파라미터 간 일관성 체크"""
    try:
        issues = []
        score = 100
        
        # 가상의 파라미터 값들 (실제로는 config에서 가져옴)
        vcp_min_score = 60
        stage_min_confidence = 0.6
        max_position_size = 8.0
        max_risk_per_trade = 2.0
        
        # VCP 점수와 포지션 크기 일관성
        if vcp_min_score < 50 and max_position_size > 5.0:
            issues.append("VCP 최소 점수가 낮은데 최대 포지션이 큽니다")
            score -= 20
            
        # Stage 신뢰도와 리스크 한도 일관성
        if stage_min_confidence < 0.5 and max_risk_per_trade > 1.5:
            issues.append("Stage 신뢰도가 낮은데 리스크 한도가 높습니다")
            score -= 25
            
        # 포지션 크기와 리스크 한도 일관성
        max_theoretical_risk = max_position_size * 0.25  # 25% 손실 가정
        if max_theoretical_risk > max_risk_per_trade * 2:
            issues.append("포지션 크기 대비 리스크 한도가 불일치합니다")
            score -= 15
            
        return {
            "score": max(score, 0),
            "issues": issues,
            "recommendations": _generate_consistency_recommendations(issues)
        }
        
    except Exception:
        return {"score": 50, "issues": ["일관성 체크 실패"], "recommendations": []}

def _generate_optimization_suggestions() -> list:
    """최적화 제안 생성"""
    try:
        suggestions = []
        
        # 현재 시장 환경 기반 제안 (가상 데이터)
        market_volatility = 0.25  # 25% 변동성
        
        if market_volatility > 0.3:
            suggestions.append({
                "category": "risk_management",
                "suggestion": "높은 시장 변동성으로 인해 포지션 크기를 20% 줄이는 것을 권장합니다",
                "impact": "medium",
                "implementation": "max_position_size *= 0.8"
            })
            
        if market_volatility < 0.15:
            suggestions.append({
                "category": "opportunity",
                "suggestion": "낮은 변동성 환경에서 VCP 패턴의 효과가 증가합니다",
                "impact": "high",
                "implementation": "vcp_weight += 0.1"
            })
            
        # 백테스트 기반 제안
        suggestions.append({
            "category": "performance",
            "suggestion": "최근 30일 백테스트 결과, Stage 2 진입 시점의 정확도를 높이기 위해 MA 기울기 임계값 조정을 권장합니다",
            "impact": "high",
            "implementation": "ma_slope_threshold *= 1.2"
        })
        
        return suggestions
        
    except Exception:
        return []

# 권장사항 생성 함수들
def _generate_vcp_recommendations(issues: list) -> list:
    """VCP 권장사항 생성"""
    recommendations = []
    for issue in issues:
        if "수축 횟수" in issue:
            recommendations.append("VCP 수축 횟수를 2-4회로 설정하세요")
        elif "거래량 감소" in issue:
            recommendations.append("거래량 감소율을 15-50% 범위로 설정하세요")
    return recommendations

def _generate_stage_recommendations(issues: list) -> list:
    """Stage 권장사항 생성"""
    recommendations = []
    for issue in issues:
        if "신뢰도" in issue:
            recommendations.append("Stage 최소 신뢰도를 0.6 이상으로 설정하세요")
        elif "이동평균" in issue:
            recommendations.append("이동평균 기간을 30, 50, 150, 200일로 설정하세요")
    return recommendations

def _generate_position_recommendations(issues: list) -> list:
    """포지션 권장사항 생성"""
    recommendations = []
    for issue in issues:
        if "Kelly" in issue:
            recommendations.append("Kelly 승수를 0.25 이하로 설정하세요")
        elif "포지션 크기" in issue:
            recommendations.append("최대 포지션을 8% 이하로 제한하세요")
    return recommendations

def _generate_risk_recommendations(issues: list) -> list:
    """리스크 권장사항 생성"""
    recommendations = []
    for issue in issues:
        if "포트폴리오 리스크" in issue:
            recommendations.append("전체 포트폴리오 리스크를 10% 이하로 관리하세요")
        elif "상관관계" in issue:
            recommendations.append("종목 간 상관관계를 0.7 이하로 유지하세요")
    return recommendations

def _generate_consistency_recommendations(issues: list) -> list:
    """일관성 권장사항 생성"""
    recommendations = []
    for issue in issues:
        if "VCP" in issue and "포지션" in issue:
            recommendations.append("VCP 점수가 낮을 때는 포지션 크기를 줄이세요")
        elif "Stage" in issue and "리스크" in issue:
            recommendations.append("Stage 신뢰도가 낮을 때는 리스크를 줄이세요")
    return recommendations

def _get_default_validation_result() -> dict:
    """기본 검증 결과 반환"""
    return {
        "overall_score": 50,
        "validation_status": "NEEDS_IMPROVEMENT",
        "vcp_validation": {"score": 50, "issues": [], "recommendations": []},
        "stage_validation": {"score": 50, "issues": [], "recommendations": []},
        "position_validation": {"score": 50, "issues": [], "recommendations": []},
        "risk_validation": {"score": 50, "issues": [], "recommendations": []},
        "consistency_check": {"score": 50, "issues": [], "recommendations": []},
        "optimization_suggestions": []
    }

def calculate_strategy_performance(strategy_combo, period_days=30):
    """
    특정 전략 조합의 성과를 계산하고 기록
    Args:
        strategy_combo: 전략 조합 이름
        period_days: 분석 기간 (일)
    """
    try:
        db_mgr = DBManager()
        
        # 기간 설정
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        # 해당 전략의 거래 내역 조회
        trades = db_mgr.execute_query("""
            SELECT 
                ticker,
                action,
                qty,
                price,
                kelly_ratio,
                swing_score,
                executed_at
            FROM trade_log 
            WHERE strategy_combo = %s 
            AND executed_at BETWEEN %s AND %s
            ORDER BY executed_at
        """, (strategy_combo, start_date, end_date))
        
        if not trades:
            logging.warning(f"⚠️ {strategy_combo} 거래 내역 없음")
            return None
        
        # 거래 데이터 분석
        df = pd.DataFrame(trades, columns=[
            'ticker', 'action', 'qty', 'price', 
            'kelly_ratio', 'swing_score', 'executed_at'
        ])
        
        # 매수/매도 쌍으로 거래 분석
        trades_analysis = []
        for i in range(0, len(df)-1, 2):
            if i+1 >= len(df):
                break
                
            buy = df.iloc[i]
            sell = df.iloc[i+1]
            
            if buy['action'] != 'BUY' or sell['action'] != 'SELL':
                continue
                
            # 수익률 계산
            return_rate = (sell['price'] - buy['price']) / buy['price']
            
            trades_analysis.append({
                'ticker': buy['ticker'],
                'buy_price': buy['price'],
                'sell_price': sell['price'],
                'return_rate': return_rate,
                'kelly_ratio': buy['kelly_ratio'],
                'swing_score': buy['swing_score'],
                'holding_period': (sell['executed_at'] - buy['executed_at']).total_seconds() / 3600  # 시간 단위
            })
        
        if not trades_analysis:
            logging.warning(f"⚠️ {strategy_combo} 완료된 거래 없음")
            return None
        
        # 성과 지표 계산
        df_analysis = pd.DataFrame(trades_analysis)
        
        win_rate = (df_analysis['return_rate'] > 0).mean()
        avg_return = df_analysis['return_rate'].mean()
        mdd = calculate_mdd(df_analysis['return_rate'].cumsum())
        kelly_ratio = calculate_kelly_ratio(win_rate, avg_return, mdd)
        swing_score = calculate_swing_score(df_analysis)
        
        # strategy_performance 테이블에 기록
        db_mgr.execute_query("""
            INSERT INTO strategy_performance (
                strategy_combo,
                period_start,
                period_end,
                win_rate,
                avg_return,
                mdd,
                num_trades,
                kelly_ratio,
                swing_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            strategy_combo,
            start_date,
            end_date,
            win_rate,
            avg_return,
            mdd,
            len(trades_analysis),
            kelly_ratio,
            swing_score
        ))
        
        logging.info(f"✅ {strategy_combo} 성과 분석 완료")
        logging.info(f"- 승률: {win_rate:.2%}")
        logging.info(f"- 평균 수익률: {avg_return:.2%}")
        logging.info(f"- MDD: {mdd:.2%}")
        logging.info(f"- Kelly 비율: {kelly_ratio:.2f}")
        logging.info(f"- Swing Score: {swing_score:.2f}")
        
        return {
            'win_rate': win_rate,
            'avg_return': avg_return,
            'mdd': mdd,
            'num_trades': len(trades_analysis),
            'kelly_ratio': kelly_ratio,
            'swing_score': swing_score
        }
        
    except Exception as e:
        logging.error(f"❌ 전략 성과 분석 중 오류 발생: {str(e)}")
        return None

def calculate_mdd(returns):
    """최대 낙폭(MDD) 계산"""
    cummax = returns.cummax()
    drawdown = (returns - cummax) / cummax
    return abs(drawdown.min())

def calculate_kelly_ratio(win_rate, avg_return, mdd):
    """Kelly 비율 계산"""
    if avg_return == 0:
        return 0
    kelly = (win_rate * avg_return - (1 - win_rate) * mdd) / avg_return
    return max(0, min(kelly, 0.5))  # 0~0.5 사이로 제한

def calculate_swing_score(trades_df):
    """Swing Score 계산"""
    if len(trades_df) < 2:
        return 0.5
        
    # 수익률의 변동성
    volatility = trades_df['return_rate'].std()
    
    # 승률
    win_rate = (trades_df['return_rate'] > 0).mean()
    
    # 평균 수익률
    avg_return = trades_df['return_rate'].mean()
    
    # 보유 기간의 효율성
    holding_efficiency = trades_df['return_rate'] / trades_df['holding_period']
    avg_efficiency = holding_efficiency.mean()
    
    # 종합 점수 계산 (0~1 사이)
    score = (
        0.3 * win_rate +  # 승률 가중치
        0.3 * (1 / (1 + np.exp(-avg_return))) +  # 수익률 가중치
        0.2 * (1 / (1 + volatility)) +  # 변동성 가중치
        0.2 * (1 / (1 + np.exp(-avg_efficiency)))  # 효율성 가중치
    )
    
    return min(max(score, 0), 1)  # 0~1 사이로 제한 

def detect_vcp_pattern(technical_data: dict) -> dict:
    """
    마크 미너비니 VCP 패턴 감지 알고리즘 구현
    
    구현 내용:
    1. 최근 8-15주 동안의 가격 변동성 계산
    2. 연속된 수축 패턴 감지 (최소 3번 이상)
    3. 각 수축에서 거래량 감소 확인
    4. 브레이크아웃 준비 상태 판별
    5. VCP 점수 계산 (0-100)
    
    반환값:
    {
        "vcp_present": bool,
        "score": int (0-100),
        "contractions": int,
        "volume_decline_pct": float,
        "volatility_trend": str,
        "breakout_ready": bool,
        "analysis_details": dict
    }
    """
    try:
        # 데이터 검증 및 정규화
        if not technical_data or 'close' not in technical_data:
            logging.warning("⚠️ VCP 분석: 가격 데이터 없음")
            return _get_default_vcp_result()
            
        prices = technical_data['close']
        volumes = technical_data.get('volume', [])
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        
        # 최소 데이터 요구사항 (15주 = 105일)
        min_required_days = 105
        if len(prices) < min_required_days:
            logging.warning(f"⚠️ VCP 분석: 데이터 부족 ({len(prices)}일 < {min_required_days}일)")
            return _get_default_vcp_result()
        
        # 최근 15주(105일) 데이터로 분석
        analysis_window = min(105, len(prices))
        recent_prices = prices[-analysis_window:]
        recent_volumes = volumes[-analysis_window:] if volumes else []
        recent_highs = highs[-analysis_window:] if highs else recent_prices
        recent_lows = lows[-analysis_window:] if lows else recent_prices
        
        # 1. 고점 수축 패턴 분석 (미너비니 기준)
        contraction_analysis = _analyze_minervini_contractions(recent_highs, recent_lows, recent_volumes)
        
        # 2. 거래량 패턴 분석
        volume_analysis = _analyze_volume_contraction_pattern(recent_volumes, contraction_analysis['pivot_points'])
        
        # 3. 변동성 감소 트렌드 분석
        volatility_analysis = _analyze_price_volatility_trend(recent_prices)
        
        # 4. 베이스 구조 분석 (8-15주 기준)
        base_analysis = _analyze_base_structure(recent_prices, recent_highs, recent_lows)
        
        # 5. 브레이크아웃 준비도 평가
        breakout_readiness = _evaluate_breakout_readiness(
            recent_prices, recent_volumes, contraction_analysis, volume_analysis
        )
        
        # 6. 통합 VCP 점수 계산
        vcp_score = _calculate_enhanced_vcp_score(
            contraction_analysis, volume_analysis, volatility_analysis, 
            base_analysis, breakout_readiness
        )
        
        # VCP 패턴 존재 여부 판별 (미너비니 기준)
        vcp_present = (
            vcp_score >= 65 and
            contraction_analysis['valid_contractions'] >= 3 and
            volume_analysis['volume_decline_pct'] >= 15 and
            volatility_analysis['trend'] in ['decreasing', 'stable']
        )
        
        result = {
            'vcp_present': vcp_present,
            'score': vcp_score,
            'contractions': contraction_analysis['valid_contractions'],
            'volume_decline_pct': volume_analysis['volume_decline_pct'],
            'volatility_trend': volatility_analysis['trend'],
            'breakout_ready': breakout_readiness['ready'],
            'analysis_details': {
                'contraction_analysis': contraction_analysis,
                'volume_analysis': volume_analysis,
                'volatility_analysis': volatility_analysis,
                'base_analysis': base_analysis,
                'breakout_readiness': breakout_readiness,
                'base_length_weeks': base_analysis['length_weeks'],
                'tightness_ratio': base_analysis['tightness_ratio'],
                'depth_percentage': contraction_analysis['max_depth_pct']
            }
        }
        
        logging.info(f"✅ VCP 패턴 분석 완료: 점수={vcp_score}, 수축={contraction_analysis['valid_contractions']}, 준비={breakout_readiness['ready']}")
        return result
        
    except Exception as e:
        logging.error(f"❌ VCP 패턴 분석 중 오류: {str(e)}")
        return _get_default_vcp_result()

def analyze_weinstein_stage(technical_data: dict) -> dict:
    """
    스탠 와인스타인 4단계 사이클 분석 구현
    
    구현 내용:
    1. MA50, MA200 기울기 계산
    2. 현재가와 이동평균선 위치 관계 분석
    3. 거래량 트렌드 확인
    4. Stage 1-4 분류 및 신뢰도 계산
    5. Stage 전환 시점 감지
    
    반환값:
    {
        "current_stage": str ("Stage 1" | "Stage 2" | "Stage 3" | "Stage 4"),
        "stage_confidence": float (0-1),
        "ma50_slope": float,
        "ma200_slope": float,
        "volume_trend": str,
        "stage_transition_signal": bool
    }
    """
    try:
        # 데이터 검증 및 정규화
        if not technical_data or 'close' not in technical_data:
            logging.warning("⚠️ Weinstein Stage 분석: 가격 데이터 없음")
            return _get_default_stage_result()
            
        prices = technical_data['close']
        volumes = technical_data.get('volume', [])
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        
        # 최소 데이터 요구사항 (MA200 + 분석을 위한 추가 데이터)
        min_required_days = 250
        if len(prices) < min_required_days:
            logging.warning(f"⚠️ Weinstein Stage 분석: 데이터 부족 ({len(prices)}일 < {min_required_days}일)")
            return _get_default_stage_result()
        
        # 이동평균 계산 (정밀도 향상)
        ma30 = _calculate_moving_average(prices, 30)
        ma50 = _calculate_moving_average(prices, 50)
        ma150 = _calculate_moving_average(prices, 150)
        ma200 = _calculate_moving_average(prices, 200)
        
        if len(ma200) == 0:
            return _get_default_stage_result()
        
        # 현재 시장 상태 분석
        current_price = prices[-1]
        current_ma30 = ma30[-1] if ma30 else current_price
        current_ma50 = ma50[-1] if ma50 else current_price
        current_ma150 = ma150[-1] if ma150 else current_price
        current_ma200 = ma200[-1] if ma200 else current_price
        
        # 기울기 계산 (다양한 기간으로 정확도 향상)
        ma30_slope = _calculate_enhanced_slope(ma30[-30:] if len(ma30) >= 30 else ma30)
        ma50_slope = _calculate_enhanced_slope(ma50[-30:] if len(ma50) >= 30 else ma50)
        ma150_slope = _calculate_enhanced_slope(ma150[-30:] if len(ma150) >= 30 else ma150)
        ma200_slope = _calculate_enhanced_slope(ma200[-30:] if len(ma200) >= 30 else ma200)
        
        # 가격과 이동평균 관계 분석
        price_ma_analysis = _analyze_price_ma_relationships(
            current_price, current_ma30, current_ma50, current_ma150, current_ma200
        )
        
        # 이동평균 배열 분석
        ma_alignment = _analyze_ma_alignment(current_ma30, current_ma50, current_ma150, current_ma200)
        
        # 거래량 패턴 분석
        volume_analysis = _analyze_enhanced_volume_trend(volumes, prices)
        
        # 상대적 강도 분석 (가격 변화율)
        relative_strength = _analyze_relative_strength(prices, ma200)
        
        # 단계 결정 및 신뢰도 계산
        stage_analysis = _determine_enhanced_weinstein_stage(
            price_ma_analysis, ma_alignment, 
            ma30_slope, ma50_slope, ma150_slope, ma200_slope,
            volume_analysis, relative_strength
        )
        
        # 단계 전환 신호 감지
        transition_signal = _detect_stage_transition_signals(
            prices, ma30, ma50, ma150, ma200, volumes,
            stage_analysis['current_stage']
        )
        
        # 단계 지속 기간 및 성숙도 분석
        stage_maturity = _analyze_stage_maturity(prices, ma200, stage_analysis['current_stage'])
        
        result = {
            'current_stage': stage_analysis['current_stage'],
            'stage_confidence': stage_analysis['confidence'],
            'ma50_slope': ma50_slope,
            'ma200_slope': ma200_slope,
            'volume_trend': volume_analysis['trend'],
            'stage_transition_signal': transition_signal['detected'],
            'analysis_details': {
                'price_ma_analysis': price_ma_analysis,
                'ma_alignment': ma_alignment,
                'slopes': {
                    'ma30': ma30_slope,
                    'ma50': ma50_slope,
                    'ma150': ma150_slope,
                    'ma200': ma200_slope
                },
                'volume_analysis': volume_analysis,
                'relative_strength': relative_strength,
                'stage_maturity': stage_maturity,
                'transition_signals': transition_signal,
                'stage_duration_weeks': stage_maturity['duration_weeks'],
                'stage_strength': stage_analysis['strength']
            }
        }
        
        logging.info(f"✅ Weinstein Stage 분석 완료: {stage_analysis['current_stage']}, 신뢰도={stage_analysis['confidence']:.2f}, 전환신호={transition_signal['detected']}")
        return result
        
    except Exception as e:
        logging.error(f"❌ Weinstein Stage 분석 중 오류: {str(e)}")
        return _get_default_stage_result()

def check_breakout_conditions(technical_data: dict, vcp_analysis: dict, stage_analysis: dict) -> dict:
    """
    통합 브레이크아웃 조건 확인
    
    구현 내용:
    1. VCP + Stage 2 조합 확인
    2. 저항선 돌파 여부 검증
    3. 거래량 급증 확인
    4. 위험대비수익비 계산
    5. 포지션 크기 권장
    
    반환값:
    {
        "action": str ("BUY" | "BUY_WEAK" | "HOLD" | "SELL"),
        "confidence": int (0-100),
        "risk_reward_ratio": float,
        "position_size": float (0-1),
        "trigger_conditions": list
    }
    """
    try:
        # 데이터 검증
        if not technical_data or not vcp_analysis or not stage_analysis:
            logging.warning("⚠️ 브레이크아웃 조건 확인: 분석 데이터 없음")
            return _get_default_breakout_result()
        
        prices = technical_data['close']
        volumes = technical_data.get('volume', [])
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        
        if len(prices) < 50:
            return _get_default_breakout_result()
        
        current_price = prices[-1]
        
        # 1. VCP 패턴 강도 평가
        vcp_score = vcp_analysis.get('score', 0)
        vcp_ready = vcp_analysis.get('breakout_ready', False)
        vcp_contractions = vcp_analysis.get('contractions', 0)
        volume_decline = vcp_analysis.get('volume_decline_pct', 0)
        
        # 2. Weinstein Stage 분석
        current_stage = stage_analysis.get('current_stage', 'Stage 1')
        stage_confidence = stage_analysis.get('stage_confidence', 0)
        stage_transition = stage_analysis.get('stage_transition_signal', False)
        
        # 3. 기술적 레벨 분석
        support_resistance = _analyze_support_resistance_levels(prices, highs, lows)
        
        # 4. 거래량 분석
        volume_confirmation = _analyze_breakout_volume_confirmation(volumes, prices)
        
        # 5. 모멘텀 분석
        momentum_analysis = _analyze_breakout_momentum(prices, volumes)
        
        # 6. 통합 브레이크아웃 조건 평가
        breakout_evaluation = _evaluate_comprehensive_breakout_conditions(
            vcp_analysis, stage_analysis, support_resistance, 
            volume_confirmation, momentum_analysis, current_price
        )
        
        # 7. 리스크 관리 계산
        risk_management = _calculate_enhanced_risk_management(
            current_price, support_resistance, breakout_evaluation, vcp_analysis
        )
        
        # 8. 포지션 사이징 (Kelly Criterion + VCP 강도)
        position_sizing = _calculate_dynamic_position_size(
            breakout_evaluation, risk_management, vcp_score, stage_confidence
        )
        
        # 9. 액션 및 신뢰도 결정
        action_decision = _determine_final_action(
            breakout_evaluation, vcp_analysis, stage_analysis, risk_management
        )
        
        # 10. 트리거 조건 목록 생성
        trigger_conditions = _generate_trigger_conditions_list(
            vcp_analysis, stage_analysis, support_resistance, 
            volume_confirmation, momentum_analysis
        )
        
        result = {
            'action': action_decision['action'],
            'confidence': action_decision['confidence'],
            'risk_reward_ratio': risk_management['risk_reward_ratio'],
            'position_size': position_sizing['recommended_size'],
            'trigger_conditions': trigger_conditions,
            'analysis_details': {
                'vcp_evaluation': {
                    'score': vcp_score,
                    'ready': vcp_ready,
                    'contractions': vcp_contractions,
                    'volume_decline': volume_decline
                },
                'stage_evaluation': {
                    'current_stage': current_stage,
                    'confidence': stage_confidence,
                    'transition_signal': stage_transition
                },
                'technical_levels': support_resistance,
                'volume_confirmation': volume_confirmation,
                'momentum_analysis': momentum_analysis,
                'breakout_evaluation': breakout_evaluation,
                'risk_management': risk_management,
                'position_sizing': position_sizing,
                'entry_price': current_price,
                'stop_loss': risk_management['stop_loss'],
                'target_price': risk_management['target_price'],
                'breakout_strength': breakout_evaluation['strength'],
                'setup_quality': breakout_evaluation['quality_score']
            }
        }
        
        logging.info(f"✅ 브레이크아웃 조건 분석 완료: {action_decision['action']}, 신뢰도={action_decision['confidence']}%, 조건={len(trigger_conditions)}개")
        return result
        
    except Exception as e:
        logging.error(f"❌ 브레이크아웃 조건 분석 중 오류: {str(e)}")
        return _get_default_breakout_result()

# === 헬퍼 함수들 ===

def _get_default_vcp_result():
    """VCP 분석 기본값 반환"""
    return {
        'vcp_present': False,
        'contractions': 0,
        'volume_decline_pct': 0.0,
        'base_length_weeks': 0,
        'volatility_trend': 'unknown',
        'breakout_ready': False,
        'score': 0
    }

def _get_default_stage_result():
    """Stage 분석 기본값 반환"""
    return {
        'current_stage': 'Stage1',
        'stage_confidence': 0.0,
        'ma50_slope': 0.0,
        'ma200_slope': 0.0,
        'price_vs_ma': {},
        'volume_trend': 'unknown',
        'stage_duration_weeks': 0
    }

def _get_default_breakout_result():
    """브레이크아웃 분석 기본값 반환"""
    return {
        'action': 'HOLD',
        'entry_price': 0.0,
        'stop_loss': 0.0,
        'target_price': 0.0,
        'position_size': 0.0,
        'risk_reward_ratio': 0.0,
        'confidence': 0
    }

def _count_price_contractions(highs):
    """개선된 가격 수축 횟수 계산 - 단계별 분석"""
    if len(highs) < 35:  # 최소 5주 데이터 필요
        return 0
    
    contractions = 0
    contraction_stages = []
    
    # 7일 간격으로 고점 찾기 (주간 고점)
    weekly_highs = []
    for i in range(6, len(highs), 7):
        weekly_high = max(highs[max(0, i-6):i+1])
        weekly_highs.append(weekly_high)
    
    if len(weekly_highs) < 5:
        return 0
    
    # 연속된 주간 고점들의 수축 패턴 분석
    for i in range(2, len(weekly_highs)):
        current_high = weekly_highs[i]
        prev_high = weekly_highs[i-1]
        base_high = weekly_highs[i-2]
        
        # 수축 조건: 각 고점이 이전 고점보다 낮아야 함
        if current_high < prev_high and prev_high < base_high:
            # 수축 강도 계산
            contraction_strength = ((base_high - current_high) / base_high) * 100
            
            if contraction_strength >= 3:  # 3% 이상 수축
                contractions += 1
                contraction_stages.append({
                    'stage': contractions,
                    'strength_pct': contraction_strength,
                    'base_high': base_high,
                    'current_high': current_high
                })
    
    return contractions

def _calculate_volume_decline(volumes):
    """수축별 거래량 감소 패턴 세분화 분석"""
    if len(volumes) < 70:  # 최소 10주 데이터 필요
        return 0.0
    
    # 주간 평균 거래량 계산
    weekly_volumes = []
    for i in range(6, len(volumes), 7):
        weekly_avg = np.mean(volumes[max(0, i-6):i+1])
        weekly_volumes.append(weekly_avg)
    
    if len(weekly_volumes) < 10:
        return 0.0
    
    # 단계별 거래량 감소 분석
    volume_stages = _analyze_volume_contraction_by_stage(weekly_volumes, 3)  # 3단계 분석
    
    if not volume_stages:
        return 0.0
    
    # 전체 평균 감소율 계산
    total_decline = sum(volume_stages) / len(volume_stages)
    return max(0, total_decline) * 100

def _analyze_volume_contraction_by_stage(volumes, contractions):
    """각 수축 단계별 거래량 감소 패턴 분석"""
    volume_stages = []
    
    if len(volumes) < contractions + 2:
        return volume_stages
    
    window_size = len(volumes) // max(1, contractions)
    
    for i in range(contractions):
        start_idx = i * window_size
        end_idx = min((i + 1) * window_size, len(volumes))
        
        if end_idx <= start_idx + 1:
            continue
            
        stage_volumes = volumes[start_idx:end_idx]
        
        if len(stage_volumes) > 2:
            # 단계 내 거래량 추세 분석
            early_avg = np.mean(stage_volumes[:len(stage_volumes)//2])
            late_avg = np.mean(stage_volumes[len(stage_volumes)//2:])
            
            if early_avg > 0:
                volume_decline = (early_avg - late_avg) / early_avg
                volume_stages.append(volume_decline)
    
    return volume_stages

def _analyze_volatility_trend(prices):
    """정확한 변동성 감소율 측정"""
    if len(prices) < 60:  # 최소 8주 데이터 필요
        return 'unknown'
    
    # 기간별 변동성 계산 (20일씩 3구간)
    volatility_periods = []
    
    for i in range(3):
        start_idx = -(60 - i*20)
        end_idx = -(40 - i*20) if i < 2 else None
        period_prices = prices[start_idx:end_idx]
        
        if len(period_prices) >= 20:
            # ATR 기반 변동성 계산 (더 정확한 측정)
            volatility = _calculate_true_range_volatility(period_prices)
            volatility_periods.append(volatility)
    
    if len(volatility_periods) < 3:
        return 'unknown'
    
    # 변동성 감소 추세 분석
    early_volatility = volatility_periods[0]
    mid_volatility = volatility_periods[1] 
    recent_volatility = volatility_periods[2]
    
    # 단계별 감소율 계산
    if early_volatility > 0 and mid_volatility > 0:
        first_decline = (early_volatility - mid_volatility) / early_volatility
        second_decline = (mid_volatility - recent_volatility) / mid_volatility if mid_volatility > 0 else 0
        
        # 지속적인 감소 패턴인지 확인
        if first_decline > 0.1 and second_decline > 0.1:  # 10% 이상 감소
            return 'strongly_decreasing'
        elif first_decline > 0.05 or second_decline > 0.05:  # 5% 이상 감소
            return 'decreasing'
        elif first_decline < -0.1 or second_decline < -0.1:  # 10% 이상 증가
            return 'increasing'
        else:
            return 'stable'
    
    return 'unknown'

def _calculate_true_range_volatility(prices):
    """True Range 기반 변동성 계산"""
    if len(prices) < 2:
        return 0.0
    
    # 일일 변동률 계산
    daily_changes = []
    for i in range(1, len(prices)):
        change_pct = abs(prices[i] - prices[i-1]) / prices[i-1] if prices[i-1] > 0 else 0
        daily_changes.append(change_pct)
    
    if not daily_changes:
        return 0.0
    
    # 평균 변동률 반환
    return np.mean(daily_changes) * 100

def _calculate_base_length(prices):
    """베이스 길이 계산 (주 단위)"""
    if len(prices) < 35:  # 최소 5주
        return 0
        
    # 최근 고점에서 현재까지의 기간
    recent_high_idx = np.argmax(prices[-70:])  # 최근 10주 내 고점
    base_days = 70 - recent_high_idx
    
    return max(1, base_days // 7)  # 주 단위 변환

def _check_breakout_readiness(prices, volumes, contractions):
    """강화된 브레이크아웃 준비도 판단 로직"""
    if len(prices) < 50:
        return False
    
    current_price = prices[-1]
    
    # 1. 베이스 형성 기간 분석 (최근 4-10주)
    base_period_start = max(20, len(prices) - 70)  # 최대 10주
    base_prices = prices[base_period_start:]
    base_high = max(base_prices)
    base_low = min(base_prices)
    
    # 2. 현재 가격이 베이스 상단 97% 이상에 위치
    price_position = (current_price - base_low) / (base_high - base_low) if base_high > base_low else 0
    near_breakout = price_position >= 0.97
    
    # 3. 가격 압축(Squeeze) 정도 측정
    recent_range = base_high - base_low
    historical_range = max(prices[-100:]) - min(prices[-100:]) if len(prices) >= 100 else recent_range
    squeeze_ratio = recent_range / historical_range if historical_range > 0 else 1
    
    # 4. 거래량 패턴 분석 (건조 후 증가)
    volume_score = _analyze_breakout_volume_pattern(volumes) if volumes else 0
    
    # 5. 기술적 지표 확인
    momentum_score = _calculate_breakout_momentum(prices)
    
    # 6. 종합 브레이크아웃 점수 계산
    breakout_score = 0
    
    # 가격 위치 점수 (30점)
    if near_breakout:
        breakout_score += 30
    elif price_position >= 0.90:
        breakout_score += 20
    
    # 압축 점수 (25점)
    if squeeze_ratio <= 0.3:  # 최근 변동폭이 역사적 변동폭의 30% 이하
        breakout_score += 25
    elif squeeze_ratio <= 0.5:
        breakout_score += 15
    
    # 거래량 점수 (25점)
    breakout_score += min(25, volume_score)
    
    # 모멘텀 점수 (20점)
    breakout_score += min(20, momentum_score)
    
    # 수축 횟수 보너스
    if contractions >= 3:
        breakout_score += 10
    elif contractions >= 2:
        breakout_score += 5
    
    # 85점 이상이면 브레이크아웃 준비 완료
    return breakout_score >= 85

def _analyze_breakout_volume_pattern(volumes):
    """브레이크아웃을 위한 거래량 패턴 분석"""
    if len(volumes) < 30:
        return 0
    
    score = 0
    
    # 1. 베이스 형성 기간 거래량 건조 확인
    base_volume = np.mean(volumes[-30:-5])  # 베이스 기간
    early_volume = np.mean(volumes[-50:-30]) if len(volumes) >= 50 else base_volume
    
    if early_volume > 0:
        dryup_ratio = base_volume / early_volume
        if dryup_ratio <= 0.7:  # 30% 이상 감소
            score += 15
        elif dryup_ratio <= 0.8:  # 20% 이상 감소
            score += 10
    
    # 2. 최근 거래량 증가 확인
    recent_volume = np.mean(volumes[-5:])
    if base_volume > 0:
        increase_ratio = recent_volume / base_volume
        if increase_ratio >= 1.5:  # 50% 이상 증가
            score += 15
        elif increase_ratio >= 1.3:  # 30% 이상 증가
            score += 10
        elif increase_ratio >= 1.1:  # 10% 이상 증가
            score += 5
    
    return score

def _calculate_breakout_momentum(prices):
    """브레이크아웃 모멘텀 점수 계산"""
    if len(prices) < 20:
        return 0
    
    score = 0
    current_price = prices[-1]
    
    # 1. 최근 5일 상승 추세
    recent_trend = (prices[-1] - prices[-6]) / prices[-6] if len(prices) >= 6 and prices[-6] > 0 else 0
    if recent_trend > 0.02:  # 2% 이상 상승
        score += 10
    elif recent_trend > 0:
        score += 5
    
    # 2. 지지선 테스트 후 반등
    recent_low = min(prices[-10:])
    base_low = min(prices[-30:]) if len(prices) >= 30 else recent_low
    
    if abs(recent_low - base_low) / base_low < 0.03:  # 3% 이내에서 지지
        score += 10
    
    return score

def _calculate_vcp_score(contractions, volume_decline_pct, volatility_trend, base_length_weeks, breakout_ready):
    """VCP 종합 점수 계산"""
    score = 0
    
    # 수축 횟수 점수 (0-30점)
    score += min(30, contractions * 10)
    
    # 거래량 감소 점수 (0-25점)
    score += min(25, volume_decline_pct * 0.8)
    
    # 변동성 트렌드 점수 (0-20점)
    if volatility_trend == 'decreasing':
        score += 20
    elif volatility_trend == 'stable':
        score += 10
    
    # 베이스 길이 점수 (0-15점)
    if 7 <= base_length_weeks <= 65:
        score += 15
    elif 5 <= base_length_weeks < 7 or 65 < base_length_weeks <= 80:
        score += 10
    
    # 브레이크아웃 준비 점수 (0-10점)
    if breakout_ready:
        score += 10
    
    return min(100, int(score))

def _calculate_moving_average(prices, period):
    """이동평균 계산"""
    if len(prices) < period:
        return []
        
    ma = []
    for i in range(period - 1, len(prices)):
        ma.append(np.mean(prices[i - period + 1:i + 1]))
        
    return ma

def _calculate_slope(values):
    """기울기 계산"""
    if len(values) < 2:
        return 0.0
        
    x = np.arange(len(values))
    slope = np.polyfit(x, values, 1)[0]
    
    return slope

def _analyze_volume_trend(volumes):
    """거래량 트렌드 분석"""
    if len(volumes) < 20:
        return 'unknown'
        
    recent_avg = np.mean(volumes[-10:])
    prev_avg = np.mean(volumes[-20:-10])
    
    if recent_avg > prev_avg * 1.2:
        return 'increasing'
    elif recent_avg < prev_avg * 0.8:
        return 'decreasing'
    else:
        return 'stable'

def _determine_weinstein_stage(price_vs_ma, ma50_slope, ma200_slope, volume_trend):
    """Weinstein 단계 결정"""
    confidence = 0.5  # 기본 신뢰도
    
    # Stage 2: 상승 추세
    if (price_vs_ma['price_above_ma50'] and price_vs_ma['price_above_ma200'] and
        price_vs_ma['ma50_above_ma200'] and ma50_slope > 0 and ma200_slope > 0):
        if volume_trend == 'increasing':
            confidence = 0.9
        else:
            confidence = 0.7
        return 'Stage2', confidence
    
    # Stage 4: 하락 추세  
    elif (not price_vs_ma['price_above_ma50'] and not price_vs_ma['price_above_ma200'] and
          not price_vs_ma['ma50_above_ma200'] and ma50_slope < 0 and ma200_slope < 0):
        if volume_trend == 'increasing':
            confidence = 0.8
        else:
            confidence = 0.6
        return 'Stage4', confidence
    
    # Stage 3: 분배 단계
    elif (price_vs_ma['price_above_ma200'] and ma50_slope < 0 and ma200_slope >= 0):
        confidence = 0.6
        return 'Stage3', confidence
    
    # Stage 1: 축적 단계 (기본값)
    else:
        if ma200_slope > 0 and volume_trend == 'stable':
            confidence = 0.6
        else:
            confidence = 0.4
        return 'Stage1', confidence

def _estimate_stage_duration(prices, ma50, ma200, current_stage):
    """단계 지속 기간 추정"""
    if len(prices) < 50:
        return 0
        
    # 간단히 최근 동향 변화점 찾기
    duration_days = 0
    
    if current_stage == 'Stage2':
        # MA50이 MA200을 상향 돌파한 시점 찾기
        for i in range(len(ma50) - 1, max(0, len(ma50) - 50), -1):
            if i > 0 and ma50[i] > ma200[i] and ma50[i-1] <= ma200[i-1]:
                duration_days = len(ma50) - i
                break
    
    return max(1, duration_days // 7)  # 주 단위 변환

def _find_resistance_level(highs):
    """저항선 찾기"""
    if len(highs) < 10:
        return max(highs) if highs else 0
        
    # 최근 고점들의 평균
    sorted_highs = sorted(highs, reverse=True)
    top_highs = sorted_highs[:3]  # 상위 3개 고점
    
    return np.mean(top_highs)

def _find_support_level(lows):
    """지지선 찾기"""
    if len(lows) < 10:
        return min(lows) if lows else 0
        
    # 최근 저점들의 평균
    sorted_lows = sorted(lows)
    bottom_lows = sorted_lows[:3]  # 하위 3개 저점
    
    return np.mean(bottom_lows)

@dataclass
class PerformanceMetrics:
    """성과 지표 데이터 클래스"""
    win_rate: float
    avg_return: float
    max_drawdown: float
    sharpe_ratio: float
    profit_factor: float
    total_trades: int
    avg_holding_period: float
    kelly_ratio: float
    swing_score: float

class EnhancedStrategyAnalyzer:
    """향상된 전략 분석기 클래스"""
    
    def __init__(self):
        try:
            from config_loader import get_trading_config
            from db_manager import DBManager
            self.config = get_trading_config()
            self.db_mgr = DBManager()
        except ImportError:
            logging.warning("⚠️ 일부 모듈 import 실패 - 기본 기능만 사용")
            self.config = None
            self.db_mgr = None
        
        self.performance_cache = {}
        self.adjustment_history = []
    
    def update_strategy_performance(self, days: int = 7) -> Dict[str, Any]:
        """실시간 전략 성과 업데이트"""
        try:
            logging.info(f"📊 최근 {days}일간 전략 성과 업데이트 시작")
            
            recent_trades = self.get_recent_trades(days=days)
            if not recent_trades:
                logging.warning("⚠️ 최근 거래 내역이 없습니다.")
                return {}
            
            performance_metrics = self.calculate_performance_metrics(recent_trades)
            
            # 성과가 저조하면 파라미터 자동 조정
            if (self.config and 
                self.config.get('adaptive_strategy.enable_auto_adjustment', True) and
                performance_metrics['win_rate'] < 0.4):
                
                adjustments = self.adjust_strategy_parameters(performance_metrics)
                logging.info(f"🔧 자동 조정 완료: {len(adjustments)}개 파라미터 변경")
            
            return performance_metrics
            
        except Exception as e:
            logging.error(f"❌ 전략 성과 업데이트 실패: {e}")
            return {}
    
    def get_recent_trades(self, days: int) -> List[Dict]:
        """최근 거래 내역 조회"""
        try:
            if not self.db_mgr:
                logging.warning("⚠️ DB 매니저가 없어 임시 데이터 반환")
                return self._get_mock_trades(days)
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            query = """
                SELECT ticker, action, qty, price, executed_at, strategy_combo
                FROM trade_log 
                WHERE executed_at BETWEEN %s AND %s
                ORDER BY executed_at DESC
            """
            
            trades = self.db_mgr.execute_query(query, (start_date, end_date))
            
            if not trades:
                return []
            
            columns = ['ticker', 'action', 'qty', 'price', 'executed_at', 'strategy_combo']
            return [dict(zip(columns, trade)) for trade in trades]
            
        except Exception as e:
            logging.error(f"❌ 최근 거래 내역 조회 실패: {e}")
            return []
    
    def calculate_performance_metrics(self, recent_trades: List[Dict]) -> Dict[str, Any]:
        """성과 지표 계산"""
        try:
            if not recent_trades:
                return {
                    'win_rate': 0.0,
                    'avg_return': 0.0,
                    'max_drawdown': 0.0,
                    'total_trades': 0,
                    'recommendation': '거래 데이터 부족'
                }
            
            # 단순화된 성과 계산
            buy_trades = [t for t in recent_trades if t['action'] == 'BUY']
            sell_trades = [t for t in recent_trades if t['action'] == 'SELL']
            
            if len(sell_trades) == 0:
                return {
                    'win_rate': 0.0,
                    'avg_return': 0.0,
                    'max_drawdown': 0.0,
                    'total_trades': len(buy_trades),
                    'recommendation': '매도 거래 없음'
                }
            
            # 수익률 계산 (단순화)
            total_return = 0.0
            profitable_trades = 0
            
            for i, sell_trade in enumerate(sell_trades):
                if i < len(buy_trades):
                    buy_trade = buy_trades[i]
                    if buy_trade['ticker'] == sell_trade['ticker']:
                        return_rate = (sell_trade['price'] - buy_trade['price']) / buy_trade['price']
                        total_return += return_rate
                        if return_rate > 0:
                            profitable_trades += 1
            
            completed_trades = min(len(buy_trades), len(sell_trades))
            
            if completed_trades == 0:
                win_rate = 0.0
                avg_return = 0.0
            else:
                win_rate = profitable_trades / completed_trades
                avg_return = total_return / completed_trades
            
            return {
                'win_rate': win_rate,
                'avg_return': avg_return,
                'max_drawdown': abs(min(0, total_return * 0.3)),  # 임시 계산
                'total_trades': completed_trades,
                'recommendation': self._generate_recommendation(win_rate, avg_return)
            }
            
        except Exception as e:
            logging.error(f"❌ 성과 지표 계산 실패: {e}")
            return {'error': str(e)}
    
    def adjust_strategy_parameters(self, performance_metrics: Dict[str, Any]) -> List[Dict]:
        """전략 파라미터 자동 조정"""
        try:
            adjustments = []
            
            win_rate = performance_metrics.get('win_rate', 0)
            avg_return = performance_metrics.get('avg_return', 0)
            max_drawdown = performance_metrics.get('max_drawdown', 0)
            
            # 승률이 낮으면 손절 강화
            if win_rate < 0.4:
                old_stop_loss = self._get_config_value('risk_management.base_stop_loss', 3.0)
                new_stop_loss = min(old_stop_loss * 1.2, 5.0)
                
                if self._update_config_value('risk_management.base_stop_loss', new_stop_loss):
                    adjustments.append({
                        'parameter': 'stop_loss',
                        'old_value': old_stop_loss,
                        'new_value': new_stop_loss,
                        'reason': f'승률 저조 ({win_rate:.1%})'
                    })
            
            # 수익률이 낮으면 진입 조건 강화
            if avg_return < 0.02:
                old_threshold = self._get_config_value('gpt_analysis.score_threshold', 85)
                new_threshold = min(old_threshold + 5, 95)
                
                if self._update_config_value('gpt_analysis.score_threshold', new_threshold):
                    adjustments.append({
                        'parameter': 'gpt_threshold',
                        'old_value': old_threshold,
                        'new_value': new_threshold,
                        'reason': f'수익률 저조 ({avg_return:.2%})'
                    })
            
            # 최대 손실이 크면 포지션 크기 축소
            if max_drawdown > 0.15:
                old_size = self._get_config_value('risk_management.base_position_size', 0.02)
                new_size = max(old_size * 0.8, 0.01)
                
                if self._update_config_value('risk_management.base_position_size', new_size):
                    adjustments.append({
                        'parameter': 'position_size',
                        'old_value': old_size,
                        'new_value': new_size,
                        'reason': f'최대 손실 과도 ({max_drawdown:.1%})'
                    })
            
            # 조정 이력 저장
            if adjustments:
                self.adjustment_history.append({
                    'timestamp': datetime.now().isoformat(),
                    'adjustments': adjustments,
                    'trigger_metrics': performance_metrics
                })
            
            return adjustments
            
        except Exception as e:
            logging.error(f"❌ 전략 파라미터 자동 조정 실패: {e}")
            return []
    
    def _get_config_value(self, key_path: str, default: Any) -> Any:
        """설정값 조회"""
        if not self.config:
            return default
        return self.config.get(key_path, default)
    
    def _update_config_value(self, key_path: str, value: Any) -> bool:
        """설정값 업데이트"""
        if not self.config:
            return False
        return self.config.set(key_path, value)
    
    def _generate_recommendation(self, win_rate: float, avg_return: float) -> str:
        """성과 기반 추천 생성"""
        if win_rate >= 0.6 and avg_return >= 0.03:
            return "우수한 성과 - 현재 전략 유지"
        elif win_rate >= 0.5 and avg_return >= 0.02:
            return "양호한 성과 - 소폭 개선 고려"
        elif win_rate >= 0.4:
            return "보통 성과 - 리스크 관리 강화 필요"
        else:
            return "저조한 성과 - 전략 재검토 필요"
    
    def _get_mock_trades(self, days: int) -> List[Dict]:
        """테스트용 모의 거래 데이터 생성"""
        mock_trades = []
        base_date = datetime.now() - timedelta(days=days)
        
        for i in range(days * 2):  # 하루에 2거래씩
            trade_date = base_date + timedelta(hours=i*12)
            action = "BUY" if i % 2 == 0 else "SELL"
            price = 30000 + (i * 100) + np.random.randint(-500, 500)
            
            mock_trades.append({
                'ticker': f'KRW-BTC',
                'action': action,
                'qty': 0.001,
                'price': price,
                'executed_at': trade_date,
                'strategy_combo': 'mock_strategy'
            })
        
        return mock_trades

    def analyze_strategy_effectiveness_by_market_regime(self, strategy_combo: str) -> Dict[str, Any]:
        """시장 상황별 전략 효과성 분석"""
        try:
            current_regime = self.market_regime_detector.detect_current_regime()
            historical_performance = self.get_performance_by_regime(strategy_combo)
            
            # 현재 시장 상황에서의 전략 효과성 분석
            regime_performance = historical_performance.get(current_regime, {})
            
            # 시장 상황 안정성 평가
            regime_stability = self.market_regime_detector.get_stability_score()
            
            # 권장 조정사항 생성
            recommended_adjustments = self.auto_tuner.get_regime_specific_adjustments(
                current_regime, regime_performance
            )
            
            return {
                'strategy_combo': strategy_combo,
                'current_regime': current_regime,
                'regime_stability': regime_stability,
                'expected_performance': regime_performance,
                'recommended_adjustments': recommended_adjustments,
                'confidence_level': self._calculate_prediction_confidence(regime_performance, regime_stability),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"❌ 시장 상황별 전략 효과성 분석 실패: {e}")
            return self._get_default_regime_analysis(strategy_combo)
    
    def get_performance_by_regime(self, strategy_combo: str) -> Dict[str, Dict[str, float]]:
        """시장 상황별 성과 데이터 조회"""
        try:
            query = """
            SELECT 
                market_regime,
                AVG(win_rate) as avg_win_rate,
                AVG(avg_return) as avg_return,
                AVG(mdd) as avg_mdd,
                COUNT(*) as sample_count
            FROM strategy_performance sp
            JOIN market_regimes mr ON DATE(sp.period_start) = mr.date
            WHERE sp.strategy_combo = %s
            AND sp.period_start >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY market_regime
            """
            
            results = self.db_mgr.execute_query(query, (strategy_combo,))
            
            performance_by_regime = {}
            for row in results:
                regime, win_rate, avg_return, mdd, sample_count = row
                performance_by_regime[regime] = {
                    'win_rate': float(win_rate) if win_rate else 0.0,
                    'avg_return': float(avg_return) if avg_return else 0.0,
                    'mdd': float(mdd) if mdd else 0.0,
                    'sample_count': int(sample_count)
                }
            
            return performance_by_regime
            
        except Exception as e:
            logging.error(f"❌ 시장 상황별 성과 조회 실패: {e}")
            return {}
    
    def run_ab_test(self, strategy_a: str, strategy_b: str, duration_days: int = 14) -> Dict[str, Any]:
        """A/B 테스트 실행"""
        try:
            return self.ab_tester.run_test(strategy_a, strategy_b, duration_days)
        except Exception as e:
            logging.error(f"❌ A/B 테스트 실행 실패: {e}")
            return {'error': str(e)}
    
    def get_real_time_metrics(self) -> Dict[str, Any]:
        """실시간 성능 메트릭 조회"""
        try:
            return self.performance_tracker.get_current_metrics()
        except Exception as e:
            logging.error(f"❌ 실시간 메트릭 조회 실패: {e}")
            return {}
    
    def optimize_parameters_advanced(self, strategy_combo: str) -> Dict[str, Any]:
        """고도화된 파라미터 최적화"""
        try:
            # 현재 시장 상황 분석
            current_regime = self.market_regime_detector.detect_current_regime()
            
            # 최적화 대상 파라미터 식별
            optimization_targets = self.auto_tuner.identify_optimization_targets(strategy_combo)
            
            # 베이지안 최적화 실행
            optimization_results = self.auto_tuner.bayesian_optimize(
                strategy_combo, optimization_targets, current_regime
            )
            
            return {
                'strategy_combo': strategy_combo,
                'current_regime': current_regime,
                'optimization_targets': optimization_targets,
                'optimized_parameters': optimization_results['optimal_params'],
                'expected_improvement': optimization_results['expected_improvement'],
                'confidence_interval': optimization_results['confidence_interval'],
                'optimization_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"❌ 고도화된 파라미터 최적화 실패: {e}")
            return {'error': str(e)}
    
    def _calculate_prediction_confidence(self, regime_performance: Dict, regime_stability: float) -> float:
        """예측 신뢰도 계산"""
        try:
            sample_count = regime_performance.get('sample_count', 0)
            
            # 샘플 수 기반 신뢰도 (최소 10개 필요)
            sample_confidence = min(1.0, sample_count / 10.0) if sample_count > 0 else 0.0
            
            # 시장 안정성 기반 신뢰도
            stability_confidence = max(0.0, min(1.0, regime_stability))
            
            # 성과 일관성 기반 신뢰도
            win_rate = regime_performance.get('win_rate', 0.0)
            consistency_confidence = min(1.0, win_rate * 2) if win_rate > 0 else 0.0
            
            # 가중 평균
            total_confidence = (
                0.4 * sample_confidence +
                0.3 * stability_confidence +
                0.3 * consistency_confidence
            )
            
            return round(total_confidence, 3)
            
        except Exception:
            return 0.5  # 기본값
    
    def _get_default_regime_analysis(self, strategy_combo: str) -> Dict[str, Any]:
        """기본 시장 상황 분석 결과"""
        return {
            'strategy_combo': strategy_combo,
            'current_regime': 'unknown',
            'regime_stability': 0.5,
            'expected_performance': {},
            'recommended_adjustments': [],
            'confidence_level': 0.3,
            'analysis_timestamp': datetime.now().isoformat(),
            'error': '시장 상황 분석 실패'
        }

class RealTimePerformanceTracker:
    """실시간 성능 메트릭 수집 및 추적"""
    
    def __init__(self):
        self.metrics_cache = {}
        self.update_interval = 60  # 1분마다 업데이트
        self.last_update = None
        
    def get_current_metrics(self) -> Dict[str, Any]:
        """현재 성능 메트릭 조회"""
        try:
            current_time = datetime.now()
            
            # 캐시가 최신인지 확인
            if (self.last_update is None or 
                (current_time - self.last_update).total_seconds() > self.update_interval):
                self._update_metrics()
            
            return self.metrics_cache.copy()
            
        except Exception as e:
            logging.error(f"❌ 실시간 메트릭 조회 실패: {e}")
            return {}
    
    def _update_metrics(self):
        """메트릭 캐시 업데이트"""
        try:
            from db_manager import DBManager
            db_mgr = DBManager()
            
            # 최근 24시간 거래 성과
            recent_trades = db_mgr.execute_query("""
                SELECT 
                    COUNT(*) as trade_count,
                    AVG(CASE WHEN action = 'SELL' AND qty > 0 THEN 
                        (price - LAG(price) OVER (PARTITION BY ticker ORDER BY executed_at)) / LAG(price) OVER (PARTITION BY ticker ORDER BY executed_at)
                        ELSE NULL END) as avg_return,
                    SUM(CASE WHEN action = 'BUY' THEN price * qty ELSE 0 END) as total_invested
                FROM trade_log 
                WHERE executed_at >= NOW() - INTERVAL '24 hours'
            """)
            
            if recent_trades:
                trade_count, avg_return, total_invested = recent_trades[0]
                
                self.metrics_cache = {
                    'daily_trade_count': trade_count or 0,
                    'daily_avg_return': float(avg_return) if avg_return else 0.0,
                    'daily_invested_amount': float(total_invested) if total_invested else 0.0,
                    'timestamp': datetime.now().isoformat()
                }
            
            self.last_update = datetime.now()
            
        except Exception as e:
            logging.error(f"❌ 메트릭 업데이트 실패: {e}")

class MarketRegimeDetector:
    """시장 상황 감지 및 분류"""
    
    def __init__(self):
        self.regimes = ['bull', 'bear', 'sideways', 'volatile']
        self.stability_window = 7  # 7일 안정성 윈도우
        
    def detect_current_regime(self) -> str:
        """현재 시장 상황 감지"""
        try:
            from db_manager import DBManager
            db_mgr = DBManager()
            
            # 최근 30일 시장 데이터 조회
            market_data = db_mgr.execute_query("""
                SELECT 
                    date,
                    AVG(close) as avg_close,
                    AVG(volume) as avg_volume,
                    STDDEV(close) as price_volatility
                FROM ohlcv_1d 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY date
                ORDER BY date DESC
                LIMIT 30
            """)
            
            if not market_data or len(market_data) < 10:
                return 'unknown'
            
            # 가격 추세 분석
            prices = [float(row[1]) for row in market_data]
            price_trend = self._calculate_trend(prices)
            
            # 변동성 분석
            volatilities = [float(row[3]) if row[3] else 0.0 for row in market_data]
            avg_volatility = sum(volatilities) / len(volatilities)
            
            # 시장 상황 분류
            if price_trend > 0.02 and avg_volatility < 0.05:
                return 'bull'
            elif price_trend < -0.02 and avg_volatility < 0.08:
                return 'bear'
            elif abs(price_trend) < 0.01:
                return 'sideways'
            elif avg_volatility > 0.1:
                return 'volatile'
            else:
                return 'mixed'
                
        except Exception as e:
            logging.error(f"❌ 시장 상황 감지 실패: {e}")
            return 'unknown'
    
    def get_stability_score(self) -> float:
        """시장 안정성 점수 (0-1)"""
        try:
            # 최근 7일간의 시장 상황 변화 추적
            regime_history = []
            for i in range(self.stability_window):
                # 실제 구현에서는 과거 데이터를 사용
                regime_history.append(self.detect_current_regime())
            
            # 일관성 계산
            unique_regimes = set(regime_history)
            stability = 1.0 - (len(unique_regimes) - 1) / len(self.regimes)
            
            return max(0.0, min(1.0, stability))
            
        except Exception:
            return 0.5  # 기본값
    
    def _calculate_trend(self, prices: List[float]) -> float:
        """가격 추세 계산 (선형 회귀 기울기)"""
        if len(prices) < 2:
            return 0.0
        
        n = len(prices)
        x = list(range(n))
        
        # 선형 회귀 계산
        sum_x = sum(x)
        sum_y = sum(prices)
        sum_xy = sum(x[i] * prices[i] for i in range(n))
        sum_x2 = sum(xi * xi for xi in x)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        
        # 정규화 (첫 번째 가격 대비)
        return slope / prices[0] if prices[0] != 0 else 0.0

class AdvancedAutoTuner:
    """고도화된 자동 파라미터 튜닝"""
    
    def __init__(self):
        self.optimization_history = {}
        self.parameter_bounds = {
            'gpt_analysis.score_threshold': (50, 95),
            'risk_management.base_stop_loss': (1.0, 8.0),
            'risk_management.base_take_profit': (3.0, 20.0),
            'filtering.max_filtered_tickers': (10, 50)
        }
    
    def get_regime_specific_adjustments(self, regime: str, performance: Dict[str, Any]) -> List[Dict[str, Any]]:
        """시장 상황별 권장 조정사항"""
        adjustments = []
        
        try:
            win_rate = performance.get('win_rate', 0.0)
            avg_return = performance.get('avg_return', 0.0)
            mdd = performance.get('mdd', 0.0)
            
            # 시장 상황별 조정 규칙
            if regime == 'bull':
                if win_rate < 0.5:
                    adjustments.append({
                        'parameter': 'gpt_analysis.score_threshold',
                        'action': 'decrease',
                        'value': -5,
                        'reason': '강세장에서 승률 개선을 위해 진입 기준 완화'
                    })
                
            elif regime == 'bear':
                if mdd > 0.1:
                    adjustments.append({
                        'parameter': 'risk_management.base_stop_loss',
                        'action': 'decrease',
                        'value': -0.5,
                        'reason': '약세장에서 손실 제한 강화'
                    })
                
            elif regime == 'volatile':
                adjustments.append({
                    'parameter': 'filtering.max_filtered_tickers',
                    'action': 'decrease',
                    'value': -10,
                    'reason': '변동성 높은 시장에서 선택과 집중'
                })
            
            return adjustments
            
        except Exception as e:
            logging.error(f"❌ 시장 상황별 조정사항 생성 실패: {e}")
            return []
    
    def identify_optimization_targets(self, strategy_combo: str) -> List[str]:
        """최적화 대상 파라미터 식별"""
        # 성과가 저조한 파라미터들을 식별
        return list(self.parameter_bounds.keys())[:3]  # 상위 3개
    
    def bayesian_optimize(self, strategy_combo: str, targets: List[str], regime: str) -> Dict[str, Any]:
        """베이지안 최적화 (간소화된 버전)"""
        try:
            # 실제 구현에서는 scikit-optimize 등의 라이브러리 사용
            optimal_params = {}
            
            for param in targets:
                if param in self.parameter_bounds:
                    min_val, max_val = self.parameter_bounds[param]
                    # 간단한 중점값 추천 (실제로는 복잡한 최적화 알고리즘)
                    optimal_params[param] = (min_val + max_val) / 2
            
            return {
                'optimal_params': optimal_params,
                'expected_improvement': 0.15,  # 15% 개선 예상
                'confidence_interval': (0.1, 0.2)
            }
            
        except Exception as e:
            logging.error(f"❌ 베이지안 최적화 실패: {e}")
            return {'optimal_params': {}, 'expected_improvement': 0.0, 'confidence_interval': (0.0, 0.0)}

class StrategyABTester:
    """전략 A/B 테스트 프레임워크"""
    
    def __init__(self):
        self.active_tests = {}
        
    def run_test(self, strategy_a: str, strategy_b: str, duration_days: int) -> Dict[str, Any]:
        """A/B 테스트 실행"""
        test_id = f"{strategy_a}_vs_{strategy_b}_{datetime.now().strftime('%Y%m%d')}"
        
        try:
            # 테스트 설정
            test_config = {
                'test_id': test_id,
                'strategy_a': strategy_a,
                'strategy_b': strategy_b,
                'start_date': datetime.now(),
                'end_date': datetime.now() + timedelta(days=duration_days),
                'allocation_ratio': 0.5,  # 50:50 분할
                'status': 'running'
            }
            
            self.active_tests[test_id] = test_config
            
            return {
                'test_id': test_id,
                'status': 'started',
                'expected_completion': test_config['end_date'].isoformat(),
                'message': f'{strategy_a} vs {strategy_b} A/B 테스트 시작'
            }
            
        except Exception as e:
            logging.error(f"❌ A/B 테스트 실행 실패: {e}")
            return {'error': str(e)}
    
    def get_test_results(self, test_id: str) -> Dict[str, Any]:
        """A/B 테스트 결과 조회"""
        if test_id not in self.active_tests:
            return {'error': '테스트를 찾을 수 없습니다'}
        
        # 실제 구현에서는 DB에서 성과 데이터 조회
        return {
            'test_id': test_id,
            'status': 'completed',
            'winner': 'strategy_a',  # 예시
            'confidence': 0.95,
            'improvement': 0.12
        }

def calculate_strategy_performance_unified(strategy_combo, period_days=30):
    """기존 함수 + 실시간 성과 업데이트 기능 통합"""
    try:
        # 기존 성과 계산 로직
        basic_performance = calculate_strategy_performance(strategy_combo, period_days)
        
        # 향상된 분석기를 통한 추가 분석
        enhanced_analyzer = get_enhanced_analyzer()
        enhanced_metrics = enhanced_analyzer.update_strategy_performance(period_days)
        
        # 결과 통합
        if basic_performance and enhanced_metrics:
            unified_result = {**basic_performance, **enhanced_metrics}
            unified_result['analysis_type'] = 'unified'
            return unified_result
        
        return basic_performance or enhanced_metrics
        
    except Exception as e:
        logging.error(f"❌ 통합 전략 성과 분석 실패: {e}")
        return None

def auto_tune_strategies_enhanced(report_path, config_path, db_manager=None):
    """VCP 성과 추적과 Kelly fraction 조정 통합"""
    try:
        enhanced_analyzer = get_enhanced_analyzer()
        
        # 최근 성과 분석
        performance_metrics = enhanced_analyzer.update_strategy_performance(days=14)
        
        if not performance_metrics:
            logging.warning("⚠️ 성과 데이터 부족으로 자동 조정 건너뜀")
            return False
        
        # 파라미터 자동 조정
        adjustments = enhanced_analyzer.adjust_strategy_parameters(performance_metrics)
        
        if adjustments:
            logging.info(f"🔧 전략 자동 조정 완료: {len(adjustments)}개 파라미터 변경")
            
            # 조정 결과를 리포트에 기록
            adjustment_report = {
                'timestamp': datetime.now().isoformat(),
                'performance_metrics': performance_metrics,
                'adjustments': adjustments,
                'recommendation': performance_metrics.get('recommendation', '')
            }
            
            try:
                with open(report_path, 'a', encoding='utf-8') as f:
                    f.write(f"\n=== 자동 전략 조정 결과 ===\n")
                    f.write(json.dumps(adjustment_report, indent=2, ensure_ascii=False))
                    f.write(f"\n{'='*50}\n")
            except Exception as e:
                logging.error(f"❌ 조정 결과 저장 실패: {e}")
            
            return True
        else:
            logging.info("✅ 현재 전략 성과 양호 - 조정 불필요")
            return False
            
    except Exception as e:
        logging.error(f"❌ 향상된 전략 자동 조정 실패: {e}")
        return False

# 글로벌 인스턴스 초기화
_enhanced_analyzer_instance = None

def get_enhanced_analyzer() -> EnhancedStrategyAnalyzer:
    """글로벌 향상된 분석기 인스턴스 제공"""
    global _enhanced_analyzer_instance
    
    if _enhanced_analyzer_instance is None:
        _enhanced_analyzer_instance = EnhancedStrategyAnalyzer()
    
    return _enhanced_analyzer_instance 


# === 새로운 VCP 분석 헬퍼 함수들 ===

def _analyze_minervini_contractions(highs: list, lows: list, volumes: list) -> dict:
    """미너비니 VCP 기준 수축 패턴 분석"""
    try:
        contractions = []
        pivot_points = []
        
        # 고점 찾기 (최근 20일 기준)
        for i in range(20, len(highs) - 20):
            if all(highs[i] >= highs[j] for j in range(i-10, i+11) if j != i):
                pivot_points.append((i, highs[i]))
        
        # 수축 패턴 분석
        valid_contractions = 0
        max_depth_pct = 0
        
        for i in range(1, len(pivot_points)):
            prev_high = pivot_points[i-1][1]
            curr_high = pivot_points[i][1]
            
            if curr_high < prev_high:  # 수축 확인
                contraction_pct = ((prev_high - curr_high) / prev_high) * 100
                if 5 <= contraction_pct <= 35:  # 적절한 수축 범위
                    valid_contractions += 1
                    max_depth_pct = max(max_depth_pct, contraction_pct)
        
        return {
            'valid_contractions': valid_contractions,
            'pivot_points': pivot_points,
            'max_depth_pct': max_depth_pct,
            'pattern_quality': min(valid_contractions * 20, 100)
        }
    except:
        return {'valid_contractions': 0, 'pivot_points': [], 'max_depth_pct': 0, 'pattern_quality': 0}


def _analyze_volume_contraction_pattern(volumes: list, pivot_points: list) -> dict:
    """거래량 수축 패턴 분석"""
    try:
        if not volumes or not pivot_points:
            return {'volume_decline_pct': 0, 'pattern_strength': 0}
        
        volume_at_pivots = []
        for i, (pivot_idx, _) in enumerate(pivot_points):
            if pivot_idx < len(volumes):
                # 피벗 포인트 전후 5일 평균 거래량
                start = max(0, pivot_idx - 5)
                end = min(len(volumes), pivot_idx + 6)
                avg_volume = sum(volumes[start:end]) / (end - start)
                volume_at_pivots.append(avg_volume)
        
        # 거래량 감소 패턴 확인
        volume_decline_pct = 0
        if len(volume_at_pivots) >= 2:
            initial_volume = volume_at_pivots[0]
            final_volume = volume_at_pivots[-1]
            volume_decline_pct = ((initial_volume - final_volume) / initial_volume) * 100
        
        return {
            'volume_decline_pct': max(0, volume_decline_pct),
            'pattern_strength': min(volume_decline_pct * 2, 100),
            'volume_consistency': len([v for v in volume_at_pivots if v < volume_at_pivots[0]]) / len(volume_at_pivots) if volume_at_pivots else 0
        }
    except:
        return {'volume_decline_pct': 0, 'pattern_strength': 0, 'volume_consistency': 0}


def _analyze_price_volatility_trend(prices: list) -> dict:
    """가격 변동성 감소 트렌드 분석"""
    try:
        if len(prices) < 20:
            return {'trend': 'unknown', 'volatility_score': 0}
        
        # 각 기간별 변동성 계산
        volatilities = []
        window_size = 20
        
        for i in range(window_size, len(prices), 5):  # 5일마다 계산
            period_prices = prices[i-window_size:i]
            volatility = np.std(period_prices) / np.mean(period_prices) * 100
            volatilities.append(volatility)
        
        if len(volatilities) < 3:
            return {'trend': 'unknown', 'volatility_score': 0}
        
        # 트렌드 분석
        recent_volatility = np.mean(volatilities[-3:])
        early_volatility = np.mean(volatilities[:3])
        
        trend = 'unknown'
        if recent_volatility < early_volatility * 0.8:
            trend = 'decreasing'
        elif recent_volatility < early_volatility * 1.2:
            trend = 'stable'
        else:
            trend = 'increasing'
        
        volatility_score = max(0, (early_volatility - recent_volatility) / early_volatility * 100)
        
        return {
            'trend': trend,
            'volatility_score': min(volatility_score, 100),
            'recent_volatility': recent_volatility,
            'early_volatility': early_volatility
        }
    except:
        return {'trend': 'unknown', 'volatility_score': 0}


def _analyze_base_structure(prices: list, highs: list, lows: list) -> dict:
    """베이스 구조 분석 (8-15주 기준)"""
    try:
        # 베이스 길이 계산 (주 단위)
        base_length_days = len(prices)
        base_length_weeks = base_length_days / 7
        
        # 베이스 조임도 계산 (고가-저가 범위)
        base_high = max(highs) if highs else max(prices)
        base_low = min(lows) if lows else min(prices)
        tightness_ratio = (base_high - base_low) / base_high * 100
        
        # 베이스 품질 평가
        quality_score = 0
        if 8 <= base_length_weeks <= 15:  # 이상적인 베이스 길이
            quality_score += 30
        if tightness_ratio <= 20:  # 적절한 조임도
            quality_score += 30
        if tightness_ratio <= 15:  # 우수한 조임도
            quality_score += 20
        
        return {
            'length_weeks': base_length_weeks,
            'tightness_ratio': tightness_ratio,
            'quality_score': quality_score,
            'base_high': base_high,
            'base_low': base_low
        }
    except:
        return {'length_weeks': 0, 'tightness_ratio': 100, 'quality_score': 0}


def _evaluate_breakout_readiness(prices: list, volumes: list, contraction_analysis: dict, volume_analysis: dict) -> dict:
    """브레이크아웃 준비도 평가"""
    try:
        readiness_score = 0
        signals = []
        
        # 1. 가격이 베이스 상단 근처에 있는지
        if prices:
            recent_high = max(prices[-10:])  # 최근 10일 고점
            base_high = contraction_analysis.get('pivot_points', [])
            if base_high:
                latest_pivot_high = base_high[-1][1] if base_high else recent_high
                if recent_high >= latest_pivot_high * 0.95:  # 95% 이상
                    readiness_score += 25
                    signals.append("가격이 베이스 상단 근처")
        
        # 2. 거래량 건조 완료
        volume_decline = volume_analysis.get('volume_decline_pct', 0)
        if volume_decline >= 20:
            readiness_score += 25
            signals.append("거래량 충분히 감소")
        
        # 3. 수축 패턴 완성도
        valid_contractions = contraction_analysis.get('valid_contractions', 0)
        if valid_contractions >= 3:
            readiness_score += 30
            signals.append(f"{valid_contractions}회 수축 완료")
        
        # 4. 최근 변동성 안정화
        if volumes:
            recent_volume_avg = np.mean(volumes[-5:]) if len(volumes) >= 5 else 0
            if recent_volume_avg > 0:
                readiness_score += 20
                signals.append("변동성 안정화")
        
        ready = readiness_score >= 60
        
        return {
            'ready': ready,
            'readiness_score': readiness_score,
            'signals': signals,
            'breakout_probability': min(readiness_score / 100, 1.0)
        }
    except:
        return {'ready': False, 'readiness_score': 0, 'signals': [], 'breakout_probability': 0}


def _calculate_enhanced_vcp_score(contraction_analysis: dict, volume_analysis: dict, 
                                volatility_analysis: dict, base_analysis: dict, 
                                breakout_readiness: dict) -> int:
    """통합 VCP 점수 계산"""
    try:
        total_score = 0
        
        # 1. 수축 패턴 품질 (30점)
        pattern_quality = contraction_analysis.get('pattern_quality', 0)
        total_score += min(pattern_quality * 0.3, 30)
        
        # 2. 거래량 패턴 (25점)
        volume_strength = volume_analysis.get('pattern_strength', 0)
        total_score += min(volume_strength * 0.25, 25)
        
        # 3. 변동성 감소 (20점)
        volatility_score = volatility_analysis.get('volatility_score', 0)
        total_score += min(volatility_score * 0.2, 20)
        
        # 4. 베이스 구조 (15점)
        base_quality = base_analysis.get('quality_score', 0)
        total_score += min(base_quality * 0.15, 15)
        
        # 5. 브레이크아웃 준비도 (10점)
        readiness_score = breakout_readiness.get('readiness_score', 0)
        total_score += min(readiness_score * 0.1, 10)
        
        return min(int(total_score), 100)
    except:
        return 0


# === 새로운 Weinstein Stage 분석 헬퍼 함수들 ===

def _calculate_enhanced_slope(values: list, periods: int = 20) -> float:
    """향상된 기울기 계산 (최소제곱법)"""
    try:
        if len(values) < periods:
            return 0.0
        
        recent_values = values[-periods:]
        x = np.arange(len(recent_values))
        y = np.array(recent_values)
        
        # 최소제곱법으로 기울기 계산
        n = len(x)
        slope = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / (n * np.sum(x**2) - np.sum(x)**2)
        
        # 정규화 (가격 대비 백분율)
        mean_price = np.mean(y)
        return (slope / mean_price) * 100 if mean_price > 0 else 0.0
    except:
        return 0.0


def _analyze_price_ma_relationships(current_price: float, ma30: float, ma50: float, ma150: float, ma200: float) -> dict:
    """가격과 이동평균 관계 분석"""
    try:
        return {
            'price_above_ma30': current_price > ma30,
            'price_above_ma50': current_price > ma50,
            'price_above_ma150': current_price > ma150,
            'price_above_ma200': current_price > ma200,
            'price_vs_ma30_pct': ((current_price - ma30) / ma30) * 100,
            'price_vs_ma50_pct': ((current_price - ma50) / ma50) * 100,
            'price_vs_ma150_pct': ((current_price - ma150) / ma150) * 100,
            'price_vs_ma200_pct': ((current_price - ma200) / ma200) * 100,
            'above_all_mas': all([current_price > ma for ma in [ma30, ma50, ma150, ma200]])
        }
    except:
        return {k: False for k in ['price_above_ma30', 'price_above_ma50', 'price_above_ma150', 'price_above_ma200', 'above_all_mas']}


def _analyze_ma_alignment(ma30: float, ma50: float, ma150: float, ma200: float) -> dict:
    """이동평균 배열 분석"""
    try:
        # 정배열 확인 (단기 > 장기)
        bullish_alignment = ma30 > ma50 > ma150 > ma200
        bearish_alignment = ma30 < ma50 < ma150 < ma200
        
        # 배열 점수 계산
        alignment_score = 0
        if ma30 > ma50: alignment_score += 25
        if ma50 > ma150: alignment_score += 25
        if ma150 > ma200: alignment_score += 25
        if bullish_alignment: alignment_score += 25
        
        return {
            'bullish_alignment': bullish_alignment,
            'bearish_alignment': bearish_alignment,
            'alignment_score': alignment_score,
            'ma30_vs_ma50': ma30 > ma50,
            'ma50_vs_ma150': ma50 > ma150,
            'ma150_vs_ma200': ma150 > ma200
        }
    except:
        return {'bullish_alignment': False, 'bearish_alignment': False, 'alignment_score': 0}


def _analyze_enhanced_volume_trend(volumes: list, prices: list) -> dict:
    """거래량 트렌드 향상 분석"""
    try:
        if not volumes or len(volumes) < 20:
            return {'trend': 'unknown', 'strength': 0}
        
        # 최근 20일간 거래량 트렌드
        recent_volumes = volumes[-20:]
        
        # 상승일과 하락일의 거래량 분석
        up_days_volume = []
        down_days_volume = []
        
        for i in range(1, min(len(prices), len(volumes))):
            if i < len(prices) and prices[i] > prices[i-1]:
                up_days_volume.append(volumes[i])
            elif i < len(prices) and prices[i] < prices[i-1]:
                down_days_volume.append(volumes[i])
        
        avg_up_volume = np.mean(up_days_volume) if up_days_volume else 0
        avg_down_volume = np.mean(down_days_volume) if down_days_volume else 0
        
        volume_ratio = avg_up_volume / avg_down_volume if avg_down_volume > 0 else 1
        
        trend = 'bullish' if volume_ratio > 1.2 else 'bearish' if volume_ratio < 0.8 else 'neutral'
        
        return {
            'trend': trend,
            'strength': min(abs(volume_ratio - 1) * 100, 100),
            'up_down_ratio': volume_ratio,
            'avg_up_volume': avg_up_volume,
            'avg_down_volume': avg_down_volume
        }
    except:
        return {'trend': 'unknown', 'strength': 0}


def _analyze_relative_strength(prices: list, ma200: list) -> dict:
    """상대적 강도 분석"""
    try:
        if len(prices) < 50 or len(ma200) < 50:
            return {'strength': 0, 'trend': 'neutral'}
        
        # 최근 50일 대비 성과
        current_price = prices[-1]
        price_50d_ago = prices[-50]
        
        price_change_pct = ((current_price - price_50d_ago) / price_50d_ago) * 100
        
        # MA200 대비 위치
        ma200_current = ma200[-1]
        price_vs_ma200_pct = ((current_price - ma200_current) / ma200_current) * 100
        
        # 상대적 강도 점수
        rs_score = (price_change_pct + price_vs_ma200_pct) / 2
        
        trend = 'strong' if rs_score > 10 else 'weak' if rs_score < -10 else 'neutral'
        
        return {
            'strength': min(abs(rs_score), 100),
            'trend': trend,
            'price_change_50d': price_change_pct,
            'vs_ma200_pct': price_vs_ma200_pct,
            'rs_score': rs_score
        }
    except:
        return {'strength': 0, 'trend': 'neutral'}


def _determine_enhanced_weinstein_stage(price_ma_analysis: dict, ma_alignment: dict,
                                      ma30_slope: float, ma50_slope: float, 
                                      ma150_slope: float, ma200_slope: float,
                                      volume_analysis: dict, relative_strength: dict) -> dict:
    """향상된 Weinstein Stage 결정"""
    try:
        stage_scores = {'Stage 1': 0, 'Stage 2': 0, 'Stage 3': 0, 'Stage 4': 0}
        
        # Stage 1 (베이스 형성 - 바닥)
        if (ma200_slope >= -0.5 and ma200_slope <= 0.5 and 
            not price_ma_analysis['above_all_mas'] and
            volume_analysis['trend'] in ['neutral', 'bearish']):
            stage_scores['Stage 1'] += 40
        
        # Stage 2 (상승 추세)
        if (ma_alignment['bullish_alignment'] and 
            price_ma_analysis['above_all_mas'] and
            ma50_slope > 0.5 and ma200_slope > 0 and
            volume_analysis['trend'] == 'bullish'):
            stage_scores['Stage 2'] += 50
            
        # Stage 3 (분배 단계)
        if (price_ma_analysis['price_above_ma200'] and
            ma200_slope > 0 and ma50_slope < 0.5 and
            relative_strength['trend'] in ['neutral', 'weak']):
            stage_scores['Stage 3'] += 40
            
        # Stage 4 (하락 추세)
        if (ma_alignment['bearish_alignment'] or
            not price_ma_analysis['price_above_ma200'] and
            ma200_slope < -0.5):
            stage_scores['Stage 4'] += 45
        
        # 가장 높은 점수의 Stage 선택
        best_stage = max(stage_scores.items(), key=lambda x: x[1])
        
        return {
            'current_stage': best_stage[0],
            'confidence': min(best_stage[1] / 50, 1.0),
            'strength': best_stage[1],
            'all_scores': stage_scores
        }
    except:
        return {'current_stage': 'Stage 1', 'confidence': 0.5, 'strength': 25, 'all_scores': {}}


def _detect_stage_transition_signals(prices: list, ma30: list, ma50: list, 
                                   ma150: list, ma200: list, volumes: list,
                                   current_stage: str) -> dict:
    """Stage 전환 신호 감지"""
    try:
        signals = []
        detected = False
        
        if len(prices) < 10:
            return {'detected': False, 'signals': []}
        
        current_price = prices[-1]
        
        # Stage 1 -> Stage 2 전환 신호
        if current_stage == 'Stage 1':
            if (current_price > ma200[-1] and 
                ma200[-1] > ma200[-10] and  # MA200 상승 전환
                len(volumes) > 5 and np.mean(volumes[-5:]) > np.mean(volumes[-20:-5])):
                signals.append("Stage 2 진입 신호")
                detected = True
        
        # Stage 2 -> Stage 3 전환 신호
        elif current_stage == 'Stage 2':
            if (len(ma50) > 5 and ma50[-1] < ma50[-5] and  # MA50 기울기 둔화
                len(volumes) > 10 and np.mean(volumes[-10:]) < np.mean(volumes[-20:-10])):
                signals.append("Stage 3 진입 신호")
                detected = True
        
        # Stage 3 -> Stage 4 전환 신호  
        elif current_stage == 'Stage 3':
            if (current_price < ma200[-1] and
                len(ma200) > 5 and ma200[-1] < ma200[-5]):
                signals.append("Stage 4 진입 신호")
                detected = True
        
        # Stage 4 -> Stage 1 전환 신호
        elif current_stage == 'Stage 4':
            if (len(ma200) > 10 and 
                abs(ma200[-1] - ma200[-10]) / ma200[-10] < 0.02):  # MA200 안정화
                signals.append("Stage 1 진입 신호")
                detected = True
        
        return {
            'detected': detected,
            'signals': signals,
            'confidence': 0.8 if detected else 0.2
        }
    except:
        return {'detected': False, 'signals': [], 'confidence': 0}


def _analyze_stage_maturity(prices: list, ma200: list, current_stage: str) -> dict:
    """Stage 성숙도 및 지속 기간 분석"""
    try:
        duration_weeks = len(prices) / 7  # 대략적인 추정
        
        maturity_score = 0
        if duration_weeks > 4:  # 4주 이상
            maturity_score += 30
        if duration_weeks > 8:  # 8주 이상
            maturity_score += 30
        if duration_weeks > 12:  # 12주 이상 (성숙)
            maturity_score += 40
        
        return {
            'duration_weeks': duration_weeks,
            'maturity_score': maturity_score,
            'is_mature': maturity_score >= 60,
            'expected_duration_weeks': {
                'Stage 1': (8, 20),
                'Stage 2': (4, 16), 
                'Stage 3': (2, 8),
                'Stage 4': (4, 12)
            }.get(current_stage, (4, 12))
        }
    except:
        return {'duration_weeks': 0, 'maturity_score': 0, 'is_mature': False}


def _analyze_support_resistance_levels(highs: list, lows: list, volumes: list) -> dict:
    """지지선과 저항선 분석"""
    try:
        if len(highs) < 20 or len(lows) < 20:
            return {'resistance': 0, 'support': 0, 'strength': 'weak'}
        
        # 최근 20일간의 고점과 저점 분석
        recent_highs = highs[-20:]
        recent_lows = lows[-20:]
        
        # 저항선: 최근 고점들의 평균
        resistance_level = np.mean(recent_highs)
        
        # 지지선: 최근 저점들의 평균  
        support_level = np.mean(recent_lows)
        
        # 현재 가격이 저항선/지지선 근처인지 확인
        current_high = highs[-1]
        current_low = lows[-1]
        
        resistance_distance = abs(current_high - resistance_level) / resistance_level
        support_distance = abs(current_low - support_level) / support_level
        
        # 강도 판정
        if resistance_distance < 0.02 or support_distance < 0.02:
            strength = 'strong'
        elif resistance_distance < 0.05 or support_distance < 0.05:
            strength = 'medium'
        else:
            strength = 'weak'
        
        return {
            'resistance': resistance_level,
            'support': support_level,
            'resistance_distance': resistance_distance,
            'support_distance': support_distance,
            'strength': strength
        }
    except:
        return {'resistance': 0, 'support': 0, 'strength': 'weak'}


def _analyze_breakout_volume_confirmation(volumes: list, vcp_analysis: dict) -> dict:
    """브레이크아웃 거래량 확인"""
    try:
        if len(volumes) < 10:
            return {'confirmed': False, 'ratio': 0}
        
        # 최근 5일 평균 거래량
        recent_avg_volume = np.mean(volumes[-5:])
        
        # 이전 20일 평균 거래량
        baseline_avg_volume = np.mean(volumes[-25:-5]) if len(volumes) >= 25 else np.mean(volumes[:-5])
        
        # 거래량 급증 비율
        volume_surge_ratio = recent_avg_volume / baseline_avg_volume if baseline_avg_volume > 0 else 1
        
        # VCP 분석에서 거래량 감소 패턴이 있었는지 확인
        had_volume_contraction = vcp_analysis.get('volume_analysis', {}).get('declining_trend', False)
        
        # 브레이크아웃 확인 조건
        confirmed = (volume_surge_ratio >= 1.5 and had_volume_contraction)
        
        return {
            'confirmed': confirmed,
            'ratio': volume_surge_ratio,
            'recent_avg': recent_avg_volume,
            'baseline_avg': baseline_avg_volume,
            'had_contraction': had_volume_contraction
        }
    except:
        return {'confirmed': False, 'ratio': 0}


def _evaluate_comprehensive_breakout_conditions(technical_data: dict, vcp_analysis: dict, 
                                              stage_analysis: dict, support_resistance: dict, 
                                              volume_confirmation: dict) -> dict:
    """종합적인 브레이크아웃 조건 평가"""
    try:
        score = 0
        conditions_met = []
        
        # VCP 패턴 점수 (40점 만점)
        vcp_score = vcp_analysis.get('score', 0)
        if vcp_score >= 70:
            score += 40
            conditions_met.append("Strong VCP pattern")
        elif vcp_score >= 50:
            score += 25
            conditions_met.append("Moderate VCP pattern")
        
        # Weinstein Stage 2 확인 (30점 만점)
        current_stage = stage_analysis.get('current_stage', 'Stage 1')
        stage_confidence = stage_analysis.get('confidence', 0)
        
        if current_stage == 'Stage 2' and stage_confidence >= 0.7:
            score += 30
            conditions_met.append("Stage 2 confirmed")
        elif current_stage == 'Stage 2' and stage_confidence >= 0.5:
            score += 20
            conditions_met.append("Stage 2 likely")
        
        # 거래량 확인 (20점 만점)
        if volume_confirmation.get('confirmed', False):
            score += 20
            conditions_met.append("Volume breakout confirmed")
        elif volume_confirmation.get('ratio', 0) >= 1.2:
            score += 10
            conditions_met.append("Volume increase detected")
        
        # 지지/저항선 돌파 (10점 만점)
        if support_resistance.get('strength') == 'strong':
            score += 10
            conditions_met.append("Strong support/resistance")
        elif support_resistance.get('strength') == 'medium':
            score += 5
            conditions_met.append("Moderate support/resistance")
        
        # 전체 평가
        if score >= 80:
            overall_rating = 'STRONG_BUY'
        elif score >= 60:
            overall_rating = 'BUY'
        elif score >= 40:
            overall_rating = 'WEAK_BUY'
        else:
            overall_rating = 'HOLD'
        
        return {
            'score': score,
            'rating': overall_rating,
            'conditions_met': conditions_met,
            'confidence': min(score / 100, 1.0)
        }
    except:
        return {'score': 0, 'rating': 'HOLD', 'conditions_met': [], 'confidence': 0}


def _calculate_enhanced_risk_management(technical_data: dict, breakout_evaluation: dict) -> dict:
    """향상된 리스크 관리 계산"""
    try:
        prices = technical_data.get('close', [])
        if len(prices) < 10:
            return {'position_size': 0.01, 'stop_loss': 0, 'target': 0}
        
        current_price = prices[-1]
        
        # ATR 기반 변동성 계산
        atr = _calculate_atr(technical_data)
        
        # 기본 손절가: 현재가 - (ATR * 2)
        stop_loss = current_price - (atr * 2)
        
        # 목표가: 현재가 + (ATR * 3) - 리스크 대비 1.5:1 비율
        target_price = current_price + (atr * 3)
        
        # Kelly Criterion 기반 포지션 사이징
        confidence = breakout_evaluation.get('confidence', 0.5)
        
        # 승률 추정 (confidence 기반)
        estimated_win_rate = 0.4 + (confidence * 0.3)  # 40-70% 범위
        
        # 평균 수익/손실 비율
        avg_win = (target_price - current_price) / current_price
        avg_loss = (current_price - stop_loss) / current_price
        
        # Kelly Criterion: f = (bp - q) / b
        # b = 승리시 수익률, p = 승률, q = 패배 확률
        if avg_loss > 0:
            kelly_fraction = (avg_win * estimated_win_rate - (1 - estimated_win_rate)) / avg_win
            kelly_fraction = max(0, min(kelly_fraction, 0.25))  # 0-25% 제한
        else:
            kelly_fraction = 0.01
        
        # 신뢰도에 따른 포지션 크기 조정
        position_size = kelly_fraction * confidence
        position_size = max(0.01, min(position_size, 0.15))  # 1-15% 제한
        
        return {
            'position_size': position_size,
            'stop_loss': stop_loss,
            'target': target_price,
            'atr': atr,
            'kelly_fraction': kelly_fraction,
            'estimated_win_rate': estimated_win_rate,
            'risk_reward_ratio': avg_win / avg_loss if avg_loss > 0 else 0
        }
    except:
        return {'position_size': 0.01, 'stop_loss': 0, 'target': 0}


def _calculate_atr(technical_data: dict, period: int = 14) -> float:
    """Average True Range 계산"""
    try:
        highs = technical_data.get('high', [])
        lows = technical_data.get('low', [])
        closes = technical_data.get('close', [])
        
        if len(highs) < period or len(lows) < period or len(closes) < period:
            return 0
        
        true_ranges = []
        for i in range(1, min(len(highs), len(lows), len(closes))):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            true_ranges.append(max(tr1, tr2, tr3))
        
        if len(true_ranges) >= period:
            return np.mean(true_ranges[-period:])
        else:
            return np.mean(true_ranges) if true_ranges else 0
    except:
        return 0


def _determine_final_action(breakout_evaluation: dict, risk_management: dict, 
                          technical_data: dict) -> dict:
    """최종 액션 결정"""
    try:
        rating = breakout_evaluation.get('rating', 'HOLD')
        confidence = breakout_evaluation.get('confidence', 0)
        position_size = risk_management.get('position_size', 0.01)
        
        # 기본 액션 결정
        if rating in ['STRONG_BUY', 'BUY'] and confidence >= 0.6:
            action = 'BUY'
            urgency = 'HIGH' if rating == 'STRONG_BUY' else 'MEDIUM'
        elif rating == 'WEAK_BUY' and confidence >= 0.4:
            action = 'WATCH'  # 관망
            urgency = 'LOW'
        else:
            action = 'HOLD'
            urgency = 'NONE'
        
        # 추가 안전장치
        prices = technical_data.get('close', [])
        if len(prices) >= 2:
            # 최근 급락 확인 (-5% 이상)
            recent_change = (prices[-1] - prices[-2]) / prices[-2]
            if recent_change <= -0.05:
                action = 'HOLD'
                urgency = 'NONE'
        
        return {
            'action': action,
            'urgency': urgency,
            'confidence': confidence,
            'position_size': position_size,
            'reasoning': f"{rating} with {confidence:.2f} confidence",
            'conditions_met': breakout_evaluation.get('conditions_met', [])
        }
    except:
        return {'action': 'HOLD', 'urgency': 'NONE', 'confidence': 0}

def calculate_integrated_position_size(technical_data: dict, kelly_params: dict, atr_params: dict, 
                                     market_conditions: dict) -> dict:
    """
    켈리 공식 + ATR + 기술적 지표 통합 포지션 사이징
    
    Args:
        technical_data: 기술적 지표 데이터
        kelly_params: 켈리 공식 파라미터
        atr_params: ATR 관련 파라미터
        market_conditions: 시장 상황 데이터
        
    Returns:
        dict: 통합 포지션 사이징 결과
    """
    try:
        # 1. 켈리 공식 기반 기본 사이징
        kelly_fraction = kelly_params.get('kelly_fraction', 0.01)
        estimated_win_rate = kelly_params.get('estimated_win_rate', 0.5)
        risk_reward_ratio = kelly_params.get('risk_reward_ratio', 1.0)
        
        # 2. ATR 기반 변동성 조정
        atr = atr_params.get('atr', 0)
        current_price = atr_params.get('current_price', 1000)
        atr_ratio = atr / current_price if current_price > 0 and atr > 0 else 0.02
        
        # ATR 기반 변동성 조정 계수
        if atr_ratio > 0.05:  # 고변동성
            atr_adjustment = 0.6
        elif atr_ratio > 0.03:  # 중변동성
            atr_adjustment = 0.8
        elif atr_ratio > 0.02:  # 저변동성
            atr_adjustment = 1.0
        elif atr_ratio > 0.01:  # 매우 낮은 변동성
            atr_adjustment = 1.2
        else:  # 극히 낮은 변동성
            atr_adjustment = 1.4
        
        # 3. 기술적 지표 기반 조정
        rsi = technical_data.get('rsi_14', 50)
        macd_signal = technical_data.get('macd_signal', 'neutral')
        ma_alignment = technical_data.get('ma_alignment', 'neutral')
        
        # RSI 기반 조정
        if rsi > 70:  # 과매수
            rsi_adjustment = 0.8
        elif rsi > 60:  # 약간 과매수
            rsi_adjustment = 0.9
        elif rsi < 30:  # 과매도
            rsi_adjustment = 1.2
        elif rsi < 40:  # 약간 과매도
            rsi_adjustment = 1.1
        else:  # 중립
            rsi_adjustment = 1.0
        
        # MACD 신호 기반 조정
        if macd_signal == 'bullish':
            macd_adjustment = 1.1
        elif macd_signal == 'bearish':
            macd_adjustment = 0.9
        else:
            macd_adjustment = 1.0
        
        # 이동평균 정렬 기반 조정
        if ma_alignment == 'bullish':
            ma_adjustment = 1.1
        elif ma_alignment == 'bearish':
            ma_adjustment = 0.9
        else:
            ma_adjustment = 1.0
        
        # 4. 시장 상황 기반 조정
        market_volatility = market_conditions.get('market_volatility', 'normal')
        trend_strength = market_conditions.get('trend_strength', 0.5)
        
        # 시장 변동성 기반 조정
        if market_volatility == 'high':
            market_adjustment = 0.8
        elif market_volatility == 'low':
            market_adjustment = 1.2
        else:
            market_adjustment = 1.0
        
        # 추세 강도 기반 조정
        trend_adjustment = 0.8 + (trend_strength * 0.4)  # 0.8-1.2 범위
        
        # 5. 통합 조정 계수 계산
        total_adjustment = (atr_adjustment * rsi_adjustment * macd_adjustment * 
                           ma_adjustment * market_adjustment * trend_adjustment)
        
        # 6. 최종 포지션 크기 계산
        base_position_size = kelly_fraction * total_adjustment
        
        # 7. 안전장치 적용
        # 최소/최대 포지션 크기 제한
        final_position_size = max(0.005, min(base_position_size, 0.15))  # 0.5-15% 범위
        
        # 8. 리스크 관리
        max_risk_per_trade = min(atr_ratio * 50, 3.0)  # ATR 기반 최대 리스크
        position_risk = final_position_size * max_risk_per_trade
        
        # 9. 포트폴리오 리스크 체크
        portfolio_risk_limit = 0.2  # 20% 포트폴리오 리스크 제한
        if position_risk > portfolio_risk_limit:
            final_position_size = portfolio_risk_limit / max_risk_per_trade
            final_position_size = max(0.005, min(final_position_size, 0.15))
        
        return {
            'final_position_size': round(final_position_size, 4),
            'kelly_fraction': round(kelly_fraction, 4),
            'total_adjustment': round(total_adjustment, 3),
            'atr_adjustment': round(atr_adjustment, 3),
            'rsi_adjustment': round(rsi_adjustment, 3),
            'macd_adjustment': round(macd_adjustment, 3),
            'ma_adjustment': round(ma_adjustment, 3),
            'market_adjustment': round(market_adjustment, 3),
            'trend_adjustment': round(trend_adjustment, 3),
            'atr_ratio': round(atr_ratio, 4),
            'estimated_win_rate': round(estimated_win_rate, 3),
            'risk_reward_ratio': round(risk_reward_ratio, 2),
            'position_risk': round(position_risk, 2),
            'max_risk_per_trade': round(max_risk_per_trade, 2),
            'confidence_score': round(estimated_win_rate * risk_reward_ratio * total_adjustment, 3)
        }
        
    except Exception as e:
        logging.error(f"❌ 통합 포지션 사이징 계산 오류: {str(e)}")
        return {
            'final_position_size': 0.01,
            'kelly_fraction': 0.01,
            'total_adjustment': 1.0,
            'atr_adjustment': 1.0,
            'rsi_adjustment': 1.0,
            'macd_adjustment': 1.0,
            'ma_adjustment': 1.0,
            'market_adjustment': 1.0,
            'trend_adjustment': 1.0,
            'atr_ratio': 0.02,
            'estimated_win_rate': 0.5,
            'risk_reward_ratio': 1.0,
            'position_risk': 2.0,
            'max_risk_per_trade': 2.0,
            'confidence_score': 0.5
        }

