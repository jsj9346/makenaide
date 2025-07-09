#!/usr/bin/env python3
"""
하이브리드 필터링 성능 모니터링 시스템

주요 기능:
- 필터링 속도 및 효율성 측정
- 데이터 품질 점수 추적
- 가중치 효과성 분석
- 주간/월간 리포트 생성
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from pathlib import Path

from utils import setup_logger, safe_strftime

# 로거 설정
logger = setup_logger()

def safe_json_serialize(data):
    """numpy 타입을 안전하게 JSON 직렬화 가능한 타입으로 변환"""
    if isinstance(data, dict):
        return {k: safe_json_serialize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [safe_json_serialize(item) for item in data]
    elif isinstance(data, (np.int64, np.int32, np.int_)):
        return int(data)
    elif isinstance(data, (np.float64, np.float32, np.float_)):
        return float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif hasattr(data, 'item'):  # numpy scalar
        return data.item()
    else:
        return data

class HybridFilteringMonitor:
    """
    하이브리드 필터링 성능을 실시간 모니터링하는 클래스
    
    기능:
    - 필터링 속도 측정
    - 데이터 품질 점수 추적
    - 가중치 효과성 분석
    """
    
    def __init__(self, metrics_dir: str = "metrics"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(exist_ok=True)
        
        self.filtering_metrics = []
        self.data_quality_history = []
        self.weight_effectiveness = {}
        
        # 메트릭스 파일 경로
        self.metrics_file = self.metrics_dir / "filtering_metrics.json"
        self.quality_file = self.metrics_dir / "data_quality.json"
        self.weights_file = self.metrics_dir / "weight_effectiveness.json"
        
        # 기존 데이터 로드
        self._load_existing_metrics()
        
    def _load_existing_metrics(self):
        """기존 메트릭스 데이터를 로드합니다."""
        try:
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.filtering_metrics = data.get('metrics', [])
                    
            if self.quality_file.exists():
                with open(self.quality_file, 'r', encoding='utf-8') as f:
                    self.data_quality_history = json.load(f)
                    
            if self.weights_file.exists():
                with open(self.weights_file, 'r', encoding='utf-8') as f:
                    self.weight_effectiveness = json.load(f)
                    
            logger.info(f"📊 기존 메트릭스 로드 완료: {len(self.filtering_metrics)}개 세션")
            
        except Exception as e:
            logger.warning(f"⚠️ 기존 메트릭스 로드 실패: {e}")
    
    def _save_metrics(self):
        """메트릭스 데이터를 파일에 저장합니다."""
        try:
            # 필터링 메트릭스 저장 (안전한 JSON 직렬화 적용)
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(safe_json_serialize({
                    'metrics': self.filtering_metrics,
                    'last_updated': datetime.now().isoformat()
                }), f, indent=2, ensure_ascii=False)
            
            # 데이터 품질 히스토리 저장 (안전한 JSON 직렬화 적용)
            with open(self.quality_file, 'w', encoding='utf-8') as f:
                json.dump(safe_json_serialize(self.data_quality_history), f, indent=2, ensure_ascii=False)
            
            # 가중치 효과성 저장 (안전한 JSON 직렬화 적용)
            with open(self.weights_file, 'w', encoding='utf-8') as f:
                json.dump(safe_json_serialize(self.weight_effectiveness), f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"❌ 메트릭스 저장 실패: {e}")
        
    def record_filtering_session(self, session_data: Dict):
        """
        필터링 세션 데이터를 기록하고 분석합니다.
        
        Args:
            session_data (dict): {
                'total_tickers': int,
                'filtered_tickers': int, 
                'processing_time': float,
                'hybrid_mode_count': int,
                'static_only_count': int,
                'data_quality_score': float,
                'static_weight': float,
                'dynamic_weight': float,
                'filter_config': dict
            }
        """
        # 타임스탬프 추가
        session_data['timestamp'] = datetime.now().isoformat()
        session_data['date'] = safe_strftime(datetime.now(), '%Y-%m-%d')
        
        # 메트릭스 계산
        efficiency = session_data['filtered_tickers'] / max(session_data['total_tickers'], 1)
        speed = session_data['total_tickers'] / max(session_data['processing_time'], 0.001)
        
        session_data['efficiency'] = efficiency
        session_data['speed'] = speed
        
        # 메트릭스 리스트에 추가
        self.filtering_metrics.append(session_data)
        
        # 데이터 품질 히스토리 업데이트
        quality_entry = {
            'timestamp': session_data['timestamp'],
            'score': session_data['data_quality_score'],
            'hybrid_ratio': session_data['hybrid_mode_count'] / max(session_data['total_tickers'], 1)
        }
        self.data_quality_history.append(quality_entry)
        
        # 가중치 효과성 분석
        weight_key = f"{session_data['static_weight']:.1f}_{session_data['dynamic_weight']:.1f}"
        if weight_key not in self.weight_effectiveness:
            self.weight_effectiveness[weight_key] = []
        
        self.weight_effectiveness[weight_key].append({
            'timestamp': session_data['timestamp'],
            'efficiency': efficiency,
            'quality': session_data['data_quality_score'],
            'filtered_count': session_data['filtered_tickers']
        })
        
        # 로깅
        logger.info(f"📊 필터링 세션 완료:")
        logger.info(f"   - 효율성: {efficiency:.2%}")
        logger.info(f"   - 처리속도: {speed:.1f} 티커/초")
        logger.info(f"   - 데이터 품질: {session_data['data_quality_score']:.2f}")
        logger.info(f"   - 하이브리드 비율: {quality_entry['hybrid_ratio']:.1%}")
        
        # 메트릭스 저장
        self._save_metrics()
        
        return {
            'efficiency': efficiency,
            'speed': speed,
            'quality_score': session_data['data_quality_score']
        }
    
    def _calculate_hybrid_ratio(self) -> float:
        """하이브리드 모드 사용 비율을 계산합니다."""
        if not self.filtering_metrics:
            return 0.0
            
        total_hybrid = sum(m['hybrid_mode_count'] for m in self.filtering_metrics)
        total_tickers = sum(m['total_tickers'] for m in self.filtering_metrics)
        
        return total_hybrid / max(total_tickers, 1)
    
    def _calculate_avg_quality(self) -> float:
        """평균 데이터 품질을 계산합니다."""
        if not self.data_quality_history:
            return 0.0
            
        return np.mean([q['score'] for q in self.data_quality_history])
    
    def _generate_recommendations(self) -> str:
        """성능 분석 기반 권장사항을 생성합니다."""
        recommendations = []
        
        if not self.filtering_metrics:
            return "분석을 위한 데이터가 부족합니다."
        
        # 최근 7일 데이터 분석
        recent_data = self._get_recent_metrics(days=7)
        
        if recent_data:
            avg_efficiency = np.mean([m['efficiency'] for m in recent_data])
            avg_quality = np.mean([m['data_quality_score'] for m in recent_data])
            avg_speed = np.mean([m['speed'] for m in recent_data])
            
            # 효율성 권장사항
            if avg_efficiency < 0.1:
                recommendations.append("• 필터링 효율성이 낮습니다. 필터 조건을 완화해보세요.")
            elif avg_efficiency > 0.5:
                recommendations.append("• 필터링이 너무 관대할 수 있습니다. 조건을 강화해보세요.")
            
            # 품질 권장사항
            if avg_quality < 0.7:
                recommendations.append("• 데이터 품질이 낮습니다. 데이터 소스를 점검하세요.")
            
            # 속도 권장사항  
            if avg_speed < 5.0:
                recommendations.append("• 처리 속도가 느립니다. 쿼리 최적화나 인덱스 추가를 고려하세요.")
            
            # 가중치 최적화 권장사항
            best_weights = self._find_optimal_weights()
            if best_weights:
                recommendations.append(f"• 최적 가중치: 정적 {best_weights['static']:.1f}, 동적 {best_weights['dynamic']:.1f}")
        
        return "\n".join(recommendations) if recommendations else "현재 성능이 양호합니다."
    
    def _get_recent_metrics(self, days: int = 7) -> List[Dict]:
        """최근 N일간의 메트릭스를 반환합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_metrics = []
        for metric in self.filtering_metrics:
            try:
                metric_date = datetime.fromisoformat(metric['timestamp'])
                if metric_date >= cutoff_date:
                    recent_metrics.append(metric)
            except:
                continue
                
        return recent_metrics
    
    def _find_optimal_weights(self) -> Optional[Dict]:
        """가장 효과적인 가중치 조합을 찾습니다."""
        if not self.weight_effectiveness:
            return None
        
        best_score = 0
        best_weights = None
        
        for weight_key, results in self.weight_effectiveness.items():
            if len(results) < 3:  # 최소 3회 이상 사용된 가중치만 고려
                continue
                
            avg_efficiency = np.mean([r['efficiency'] for r in results])
            avg_quality = np.mean([r['quality'] for r in results])
            
            # 종합 점수 (효율성 70% + 품질 30%)
            combined_score = avg_efficiency * 0.7 + avg_quality * 0.3
            
            if combined_score > best_score:
                best_score = combined_score
                static_weight, dynamic_weight = map(float, weight_key.split('_'))
                best_weights = {
                    'static': static_weight,
                    'dynamic': dynamic_weight,
                    'score': combined_score
                }
        
        return best_weights
    
    def generate_weekly_report(self) -> str:
        """주간 성능 리포트를 생성합니다."""
        if not self.filtering_metrics:
            return "리포트 생성을 위한 데이터가 부족합니다."
        
        # 최근 7일 데이터
        recent_metrics = self._get_recent_metrics(days=7)
        
        if not recent_metrics:
            return "최근 7일간 실행된 필터링 세션이 없습니다."
        
        # 성능 통계 계산
        total_sessions = len(recent_metrics)
        avg_efficiency = np.mean([m['efficiency'] for m in recent_metrics])
        avg_speed = np.mean([m['speed'] for m in recent_metrics])
        avg_quality = np.mean([m['data_quality_score'] for m in recent_metrics])
        
        total_tickers_processed = sum(m['total_tickers'] for m in recent_metrics)
        total_tickers_filtered = sum(m['filtered_tickers'] for m in recent_metrics)
        
        hybrid_ratio = self._calculate_hybrid_ratio()
        
        # 트렌드 분석
        if len(recent_metrics) >= 2:
            early_metrics = recent_metrics[:len(recent_metrics)//2]
            late_metrics = recent_metrics[len(recent_metrics)//2:]
            
            early_avg_efficiency = np.mean([m['efficiency'] for m in early_metrics])
            late_avg_efficiency = np.mean([m['efficiency'] for m in late_metrics])
            
            efficiency_trend = "↗️ 상승" if late_avg_efficiency > early_avg_efficiency else "↘️ 하락"
        else:
            efficiency_trend = "📊 데이터 부족"
        
        report = f"""
📈 하이브리드 필터링 주간 성능 리포트
=======================================
기간: {safe_strftime(datetime.now() - timedelta(days=7))} ~ {safe_strftime(datetime.now())}

📊 기본 통계
- 실행 횟수: {total_sessions}회
- 처리된 총 티커 수: {total_tickers_processed:,}개
- 선별된 총 티커 수: {total_tickers_filtered:,}개

🎯 성능 지표  
- 평균 효율성: {avg_efficiency:.2%} {efficiency_trend}
- 평균 처리속도: {avg_speed:.1f} 티커/초
- 하이브리드 모드 비율: {hybrid_ratio:.1%}
- 데이터 품질 평균: {avg_quality:.2f}/1.0

💡 권장사항
{self._generate_recommendations()}

🔍 가중치 분석
{self._generate_weight_analysis()}
        """
        
        # 리포트 파일로 저장
        report_file = self.metrics_dir / f"weekly_report_{safe_strftime(datetime.now(), '%Y%m%d')}.txt"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"📄 주간 리포트 저장: {report_file}")
        except Exception as e:
            logger.error(f"❌ 리포트 저장 실패: {e}")
        
        return report
    
    def _generate_weight_analysis(self) -> str:
        """가중치 효과성 분석 결과를 생성합니다."""
        if not self.weight_effectiveness:
            return "가중치 분석을 위한 데이터가 부족합니다."
        
        analysis = []
        
        for weight_key, results in self.weight_effectiveness.items():
            if len(results) < 2:
                continue
                
            static_weight, dynamic_weight = map(float, weight_key.split('_'))
            avg_efficiency = np.mean([r['efficiency'] for r in results])
            avg_quality = np.mean([r['quality'] for r in results])
            usage_count = len(results)
            
            analysis.append(f"정적 {static_weight:.1f}/동적 {dynamic_weight:.1f}: "
                          f"효율성 {avg_efficiency:.2%}, 품질 {avg_quality:.2f} ({usage_count}회)")
        
        if not analysis:
            return "가중치 분석 결과가 없습니다."
        
        return "\n".join(analysis)
    
    def record_backtest_session(self, backtest_metrics: Dict):
        """백테스트 세션 데이터를 기록합니다."""
        try:
            backtest_metrics['timestamp'] = datetime.now().isoformat()
            backtest_metrics['date'] = safe_strftime(datetime.now(), '%Y-%m-%d')
            
            # 백테스트 전용 메트릭스 파일
            backtest_file = self.metrics_dir / "backtest_sessions.json"
            
            # 기존 백테스트 세션 로드
            backtest_sessions = []
            if backtest_file.exists():
                with open(backtest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    backtest_sessions = data.get('sessions', [])
            
            # 새 세션 추가
            backtest_sessions.append(backtest_metrics)
            
            # 백테스트 세션 저장
            with open(backtest_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'sessions': backtest_sessions,
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📊 백테스트 세션 기록 완료:")
            logger.info(f"   - 전략 수: {backtest_metrics.get('strategy_count', 0)}")
            logger.info(f"   - 테스트 조합: {backtest_metrics.get('total_combos_tested', 0)}")
            logger.info(f"   - 하이브리드 조합: {backtest_metrics.get('hybrid_combos_tested', 0)}")
            
        except Exception as e:
            logger.error(f"❌ 백테스트 세션 기록 실패: {e}")

    def _analyze_backtest_performance(self) -> Dict:
        """백테스트 성능을 분석합니다."""
        try:
            backtest_file = self.metrics_dir / "backtest_sessions.json"
            if not backtest_file.exists():
                return {'status': 'no_data'}
            
            with open(backtest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                sessions = data.get('sessions', [])
            
            if not sessions:
                return {'status': 'no_sessions'}
            
            # 최근 7일 세션 분석
            recent_sessions = [
                s for s in sessions 
                if datetime.fromisoformat(s['timestamp']) > datetime.now() - timedelta(days=7)
            ]
            
            return {
                'total_sessions': len(sessions),
                'recent_sessions': len(recent_sessions),
                'avg_strategy_count': np.mean([s.get('strategy_count', 0) for s in recent_sessions]) if recent_sessions else 0,
                'avg_hybrid_ratio': np.mean([
                    s.get('hybrid_combos_tested', 0) / max(s.get('total_combos_tested', 1), 1) 
                    for s in recent_sessions
                ]) if recent_sessions else 0,
                'status': 'active'
            }
            
        except Exception as e:
            logger.error(f"❌ 백테스트 성능 분석 실패: {e}")
            return {'status': 'error', 'error': str(e)}

    def get_backtest_summary(self) -> Dict:
        """백테스트 요약 정보를 반환합니다."""
        return self._analyze_backtest_performance()

    def get_performance_summary(self) -> Dict:
        """현재 성능 요약을 반환합니다."""
        recent_metrics = self._get_recent_metrics(days=7)
        
        if not recent_metrics:
            return {
                'status': 'NO_DATA',
                'message': '최근 데이터가 없습니다.'
            }
        
        avg_efficiency = np.mean([m['efficiency'] for m in recent_metrics])
        avg_speed = np.mean([m['speed'] for m in recent_metrics])
        avg_quality = np.mean([m['data_quality_score'] for m in recent_metrics])
        
        # 성능 상태 판정
        if avg_efficiency >= 0.15 and avg_quality >= 0.8 and avg_speed >= 10:
            status = 'EXCELLENT'
        elif avg_efficiency >= 0.1 and avg_quality >= 0.7 and avg_speed >= 5:
            status = 'GOOD'
        elif avg_efficiency >= 0.05 and avg_quality >= 0.6 and avg_speed >= 2:
            status = 'FAIR'
        else:
            status = 'POOR'
        
        return {
            'status': status,
            'efficiency': avg_efficiency,
            'speed': avg_speed,
            'quality': avg_quality,
            'sessions_count': len(recent_metrics),
            'optimal_weights': self._find_optimal_weights()
        }

# 전역 모니터 인스턴스
_monitor_instance = None

def get_performance_monitor() -> HybridFilteringMonitor:
    """성능 모니터 싱글톤 인스턴스를 반환합니다."""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = HybridFilteringMonitor()
    return _monitor_instance

if __name__ == "__main__":
    # 테스트 코드
    monitor = get_performance_monitor()
    
    # 샘플 세션 데이터 기록
    sample_session = {
        'total_tickers': 200,
        'filtered_tickers': 25,
        'processing_time': 8.5,
        'hybrid_mode_count': 180,
        'static_only_count': 20,
        'data_quality_score': 0.85,
        'static_weight': 0.6,
        'dynamic_weight': 0.4,
        'filter_config': {'enable_hybrid': True}
    }
    
    monitor.record_filtering_session(sample_session)
    
    # 리포트 생성
    report = monitor.generate_weekly_report()
    print(report) 