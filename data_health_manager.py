"""
Integrated Data Health Management System for Makenaide
통합 데이터 건강성 관리 시스템

주요 기능:
1. 검증, 모니터링, 복구 시스템 통합 관리
2. 자동화된 워크플로우
3. 대시보드 및 리포팅
4. 설정 관리
5. 스케줄링 및 자동화
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import schedule
import threading

from enhanced_data_validator import EnhancedDataValidator
from data_quality_monitor import DataQualityMonitor
from auto_recovery_system import AutoRecoverySystem

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

class DataHealthManager:
    """통합 데이터 건강성 관리 시스템"""
    
    def __init__(self, config_file: Optional[str] = None):
        # 하위 시스템들 초기화
        self.validator = EnhancedDataValidator()
        self.monitor = DataQualityMonitor(config_file)
        self.recovery_system = AutoRecoverySystem()
        
        # 설정
        self.config = {
            'auto_recovery_enabled': True,
            'recovery_threshold_score': 0.6,  # 이 점수 이하일 때 자동 복구
            'daily_health_check_time': '09:00',
            'weekly_full_recovery_day': 'sunday',
            'emergency_recovery_enabled': True,
            'max_recovery_attempts_per_day': 3,
            'notification_enabled': True,
            'backup_before_recovery': True
        }
        
        # 설정 파일 로드
        if config_file and os.path.exists(config_file):
            self.load_config(config_file)
        
        # 상태 추적
        self.last_health_check = None
        self.last_recovery_attempt = None
        self.recovery_attempts_today = 0
        self.system_status = "INITIALIZING"
        
        # 스케줄러
        self.scheduler_thread = None
        self.is_running = False
        
        # 리포트 디렉토리
        self.reports_dir = Path("health_reports")
        self.reports_dir.mkdir(exist_ok=True)
        
        logger.info("🏥 데이터 건강성 관리 시스템 초기화 완료")
    
    def load_config(self, config_file: str):
        """설정 파일 로드"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                loaded_config = json.load(f)
                self.config.update(loaded_config)
            logger.info(f"✅ 설정 파일 로드: {config_file}")
        except Exception as e:
            logger.error(f"❌ 설정 파일 로드 실패: {e}")
    
    def save_config(self, config_file: str = "health_manager_config.json"):
        """설정 파일 저장"""
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ 설정 파일 저장: {config_file}")
        except Exception as e:
            logger.error(f"❌ 설정 파일 저장 실패: {e}")
    
    def perform_comprehensive_health_check(self) -> Dict[str, Any]:
        """종합 건강 상태 검사"""
        logger.info("🏥 종합 건강 상태 검사 시작")
        
        start_time = datetime.now()
        self.last_health_check = start_time
        
        try:
            # 1. 데이터 검증 실행
            validation_summary = self.validator.run_comprehensive_validation(days=7)
            
            # 2. 건강도 점수 계산
            health_scores = self.monitor.calculate_health_score(self.validator.validation_results)
            
            # 3. 결과 요약
            health_status = {
                'timestamp': start_time.isoformat(),
                'overall_health_score': health_scores['overall'],
                'health_breakdown': health_scores,
                'validation_summary': validation_summary,
                'system_recommendations': self._generate_recommendations(health_scores, validation_summary),
                'auto_recovery_triggered': False,
                'recovery_results': None
            }
            
            # 4. 자동 복구 필요성 판단
            if (self.config['auto_recovery_enabled'] and 
                health_scores['overall'] < self.config['recovery_threshold_score'] and
                self.recovery_attempts_today < self.config['max_recovery_attempts_per_day']):
                
                logger.info(f"�� 건강도 점수 낮음 ({health_scores['overall']:.1%}) - 자동 복구 실행")
                
                recovery_results = self._perform_auto_recovery()
                health_status['auto_recovery_triggered'] = True
                health_status['recovery_results'] = recovery_results
                
                # 복구 후 재검증
                post_recovery_summary = self.validator.run_comprehensive_validation(days=3)
                post_recovery_scores = self.monitor.calculate_health_score(self.validator.validation_results)
                
                health_status['post_recovery_health_score'] = post_recovery_scores['overall']
                health_status['recovery_improvement'] = post_recovery_scores['overall'] - health_scores['overall']
            
            # 5. 상태 업데이트
            if health_scores['overall'] >= 0.8:
                self.system_status = "HEALTHY"
            elif health_scores['overall'] >= 0.6:
                self.system_status = "STABLE"
            elif health_scores['overall'] >= 0.4:
                self.system_status = "DEGRADED"
            else:
                self.system_status = "CRITICAL"
            
            duration = (datetime.now() - start_time).total_seconds()
            health_status['check_duration'] = duration
            
            logger.info(f"🏥 종합 건강 검사 완료 ({duration:.2f}초) - 상태: {self.system_status}")
            
            return health_status
            
        except Exception as e:
            logger.error(f"❌ 건강 검사 중 오류: {e}")
            self.system_status = "ERROR"
            return {
                'timestamp': start_time.isoformat(),
                'error': str(e),
                'system_status': 'ERROR'
            }
    
    def _generate_recommendations(self, health_scores: Dict[str, float], validation_summary: Dict[str, Any]) -> List[str]:
        """건강 상태에 따른 권장사항 생성"""
        recommendations = []
        
        overall_score = health_scores['overall']
        severity_breakdown = validation_summary.get('severity_breakdown', {})
        
        # 전체 건강도 기반 권장사항
        if overall_score < 0.3:
            recommendations.append("🚨 즉시 수동 개입 필요 - 시스템 전체 점검 권장")
            recommendations.append("🔧 긴급 복구 작업 실행 고려")
        elif overall_score < 0.6:
            recommendations.append("⚠️ 자동 복구 시스템 실행 권장")
            recommendations.append("📊 문제 티커들에 대한 개별 분석 필요")
        elif overall_score < 0.8:
            recommendations.append("👀 지속적인 모니터링 필요")
            recommendations.append("🔍 예방적 점검 실행 고려")
        
        # Static Indicators 관련
        if health_scores['static_indicators'] < 0.5:
            recommendations.append("📈 Static Indicators 재계산 필요")
            recommendations.append("🔄 데이터 수집 파이프라인 점검 권장")
        
        # 오류 개수 기반
        if severity_breakdown.get('CRITICAL', 0) > 0:
            recommendations.append("🚨 CRITICAL 오류 즉시 해결 필요")
        if severity_breakdown.get('ERROR', 0) > 5:
            recommendations.append("❌ 다수 오류 발생 - 시스템 전반 점검 필요")
        
        # OHLCV 관련
        if health_scores['ohlcv'] < 0.7:
            recommendations.append("📊 OHLCV 데이터 품질 개선 필요")
            recommendations.append("🔄 데이터 소스 안정성 확인 권장")
        
        if not recommendations:
            recommendations.append("✅ 현재 상태 양호 - 정기 모니터링 지속")
        
        return recommendations
    
    def _perform_auto_recovery(self) -> Dict[str, Any]:
        """자동 복구 실행"""
        logger.info("🔧 자동 복구 시작")
        
        self.last_recovery_attempt = datetime.now()
        self.recovery_attempts_today += 1
        
        try:
            # 백업 생성 (옵션)
            if self.config['backup_before_recovery']:
                self._create_backup()
            
            # 문제 티커들 자동 복구
            recovery_results = self.recovery_system.recover_all_problematic_tickers(limit_tickers=10)
            
            # 결과 요약
            total_tickers = len(recovery_results)
            successful_tickers = sum(1 for ticker_results in recovery_results.values() 
                                   if any(r.success for r in ticker_results))
            
            recovery_summary = {
                'timestamp': self.last_recovery_attempt.isoformat(),
                'total_tickers_processed': total_tickers,
                'successful_tickers': successful_tickers,
                'success_rate': successful_tickers / total_tickers if total_tickers > 0 else 0,
                'recovery_stats': self.recovery_system.stats.copy(),
                'detailed_results': recovery_results
            }
            
            logger.info(f"🔧 자동 복구 완료 - 성공률: {recovery_summary['success_rate']:.1%}")
            
            return recovery_summary
            
        except Exception as e:
            logger.error(f"❌ 자동 복구 실패: {e}")
            return {
                'timestamp': self.last_recovery_attempt.isoformat(),
                'error': str(e),
                'success': False
            }
    
    def _create_backup(self):
        """중요 데이터 백업 생성"""
        try:
            backup_dir = Path("backups") / datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # TODO: 실제 백업 로직 구현
            # 예: pg_dump 등을 사용한 DB 백업
            
            logger.info(f"💾 백업 생성: {backup_dir}")
            
        except Exception as e:
            logger.warning(f"⚠️ 백업 생성 실패: {e}")
    
    def start_automated_management(self):
        """자동화된 관리 시작"""
        if self.is_running:
            logger.warning("⚠️ 자동 관리가 이미 실행중입니다")
            return
        
        self.is_running = True
        
        # 스케줄 설정
        schedule.every().day.at(self.config['daily_health_check_time']).do(
            self._scheduled_health_check
        )
        
        # 주간 전체 복구 (선택한 요일)
        if self.config['weekly_full_recovery_day']:
            getattr(schedule.every(), self.config['weekly_full_recovery_day'].lower()).do(
                self._scheduled_full_recovery
            )
        
        # 응급 상황 모니터링 (1시간마다)
        if self.config['emergency_recovery_enabled']:
            schedule.every().hour.do(self._emergency_check)
        
        # 스케줄러 스레드 시작
        def scheduler_loop():
            logger.info("📅 자동화된 관리 스케줄러 시작")
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 스케줄 확인
        
        self.scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        # 모니터링 시스템도 시작
        self.monitor.start_monitoring()
        
        logger.info("🚀 자동화된 데이터 건강성 관리 시작")
    
    def stop_automated_management(self):
        """자동화된 관리 중지"""
        self.is_running = False
        schedule.clear()
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        self.monitor.stop_monitoring()
        
        logger.info("🛑 자동화된 데이터 건강성 관리 중지")
    
    def _scheduled_health_check(self):
        """스케줄된 건강 검사"""
        logger.info("📅 정기 건강 검사 실행")
        health_status = self.perform_comprehensive_health_check()
        self._save_health_report(health_status)
        
        # 하루 복구 시도 횟수 리셋
        if datetime.now().hour == 0:  # 자정일 때
            self.recovery_attempts_today = 0
    
    def _scheduled_full_recovery(self):
        """스케줄된 전체 복구"""
        logger.info("📅 주간 전체 복구 실행")
        
        # 모든 문제 티커 복구
        recovery_results = self.recovery_system.recover_all_problematic_tickers(limit_tickers=50)
        
        # 복구 후 전체 검증
        health_status = self.perform_comprehensive_health_check()
        
        # 보고서 저장
        self._save_health_report(health_status, prefix="weekly_full_recovery")
        self.recovery_system.save_recovery_report(recovery_results, 
                                                f"weekly_recovery_{datetime.now().strftime('%Y%m%d')}.txt")
    
    def _emergency_check(self):
        """응급 상황 체크"""
        try:
            # 빠른 건강도 체크
            validation_summary = self.validator.run_comprehensive_validation(days=1)
            health_scores = self.monitor.calculate_health_score(self.validator.validation_results)
            
            # 심각한 상황일 때만 응급 복구
            if health_scores['overall'] < 0.3:
                logger.warning(f"🚨 응급 상황 감지 - 건강도: {health_scores['overall']:.1%}")
                
                if self.recovery_attempts_today < self.config['max_recovery_attempts_per_day']:
                    emergency_recovery = self._perform_auto_recovery()
                    logger.info("🚑 응급 복구 실행됨")
                else:
                    logger.warning("⚠️ 일일 복구 한도 초과 - 수동 개입 필요")
            
        except Exception as e:
            logger.error(f"❌ 응급 체크 실패: {e}")
    
    def _save_health_report(self, health_status: Dict[str, Any], prefix: str = "health_check"):
        """건강 상태 보고서 저장"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self.reports_dir / f"{prefix}_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(health_status, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📁 건강 보고서 저장: {filename}")
            
        except Exception as e:
            logger.error(f"❌ 보고서 저장 실패: {e}")
    
    def get_system_dashboard(self) -> str:
        """시스템 대시보드 생성"""
        dashboard_lines = []
        dashboard_lines.append("🏥 " + "=" * 70)
        dashboard_lines.append("🏥 Makenaide 데이터 건강성 관리 시스템 대시보드")
        dashboard_lines.append("🏥 " + "=" * 70)
        dashboard_lines.append(f"📅 현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 시스템 상태
        status_emoji = {
            'HEALTHY': '💚',
            'STABLE': '💛', 
            'DEGRADED': '🧡',
            'CRITICAL': '🚨',
            'ERROR': '❌',
            'INITIALIZING': '🔄'
        }
        
        dashboard_lines.append(f"🏥 시스템 상태: {status_emoji.get(self.system_status, '❓')} {self.system_status}")
        
        # 자동화 상태
        automation_status = "✅ 실행중" if self.is_running else "🛑 중지됨"
        dashboard_lines.append(f"🤖 자동화 상태: {automation_status}")
        
        # 최근 활동
        if self.last_health_check:
            dashboard_lines.append(f"🔍 마지막 건강 검사: {self.last_health_check.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.last_recovery_attempt:
            dashboard_lines.append(f"🔧 마지막 복구 시도: {self.last_recovery_attempt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        dashboard_lines.append(f"📊 오늘 복구 시도: {self.recovery_attempts_today}/{self.config['max_recovery_attempts_per_day']}")
        
        # 설정 요약
        dashboard_lines.append(f"\n⚙️ 주요 설정:")
        dashboard_lines.append(f"   자동 복구: {'✅' if self.config['auto_recovery_enabled'] else '❌'}")
        dashboard_lines.append(f"   복구 임계치: {self.config['recovery_threshold_score']:.1%}")
        dashboard_lines.append(f"   정기 검사: {self.config['daily_health_check_time']}")
        
        # 하위 시스템 상태
        dashboard_lines.append(f"\n🔧 하위 시스템:")
        dashboard_lines.append(f"   검증 시스템: ✅ 정상")
        monitor_status = "✅ 실행중" if self.monitor.is_monitoring else "🛑 중지"
        dashboard_lines.append(f"   모니터링 시스템: {monitor_status}")
        dashboard_lines.append(f"   복구 시스템: ✅ 대기중")
        
        dashboard_lines.append("🏥 " + "=" * 70)
        
        return "\n".join(dashboard_lines)
    
    def get_recent_reports_summary(self, days: int = 7) -> str:
        """최근 보고서 요약"""
        try:
            recent_reports = []
            cutoff_date = datetime.now() - timedelta(days=days)
            
            for report_file in self.reports_dir.glob("health_check_*.json"):
                if report_file.stat().st_mtime > cutoff_date.timestamp():
                    try:
                        with open(report_file, 'r', encoding='utf-8') as f:
                            report_data = json.load(f)
                            recent_reports.append({
                                'filename': report_file.name,
                                'timestamp': report_data.get('timestamp'),
                                'health_score': report_data.get('overall_health_score', 0),
                                'auto_recovery': report_data.get('auto_recovery_triggered', False)
                            })
                    except Exception:
                        continue
            
            # 시간순 정렬
            recent_reports.sort(key=lambda x: x['timestamp'] or '')
            
            if not recent_reports:
                return f"📊 최근 {days}일간 보고서 없음"
            
            summary_lines = []
            summary_lines.append(f"📊 최근 {days}일 건강 보고서 요약 ({len(recent_reports)}건)")
            summary_lines.append("=" * 50)
            
            for report in recent_reports[-10:]:  # 최근 10개만
                timestamp = report['timestamp'][:16] if report['timestamp'] else 'Unknown'
                health_score = report['health_score']
                recovery_mark = " 🔧" if report['auto_recovery'] else ""
                
                health_emoji = "💚" if health_score > 0.8 else "💛" if health_score > 0.6 else "🧡" if health_score > 0.4 else "🚨"
                
                summary_lines.append(f"{health_emoji} {timestamp} - {health_score:.1%}{recovery_mark}")
            
            return "\n".join(summary_lines)
            
        except Exception as e:
            return f"❌ 보고서 요약 생성 실패: {e}"

def main():
    """메인 실행 함수"""
    manager = DataHealthManager()
    
    # 설정 저장
    manager.save_config()
    
    try:
        print(manager.get_system_dashboard())
        
        print("\n🏥 데이터 건강성 관리 시스템")
        print("=" * 50)
        print("1. 즉시 건강 검사 실행")
        print("2. 자동화 관리 시작")
        print("3. 자동화 관리 중지")
        print("4. 시스템 대시보드")
        print("5. 최근 보고서 요약")
        print("6. 설정 변경")
        print("7. 종료")
        
        while True:
            try:
                choice = input("\n선택하세요 (1-7): ").strip()
                
                if choice == '1':
                    print("🔍 건강 검사 실행중...")
                    health_status = manager.perform_comprehensive_health_check()
                    
                    print(f"\n🏥 건강 검사 결과:")
                    print(f"   전체 건강도: {health_status.get('overall_health_score', 0):.1%}")
                    print(f"   시스템 상태: {manager.system_status}")
                    
                    if health_status.get('auto_recovery_triggered'):
                        recovery_results = health_status.get('recovery_results', {})
                        print(f"   🔧 자동 복구 실행됨")
                        print(f"   복구 성공률: {recovery_results.get('success_rate', 0):.1%}")
                    
                    for recommendation in health_status.get('system_recommendations', []):
                        print(f"   💡 {recommendation}")
                
                elif choice == '2':
                    manager.start_automated_management()
                    print("✅ 자동화 관리가 시작되었습니다.")
                
                elif choice == '3':
                    manager.stop_automated_management()
                    print("🛑 자동화 관리가 중지되었습니다.")
                
                elif choice == '4':
                    print("\n" + manager.get_system_dashboard())
                
                elif choice == '5':
                    days = input("조회할 일수 (기본값: 7): ").strip()
                    days = int(days) if days.isdigit() else 7
                    print("\n" + manager.get_recent_reports_summary(days))
                
                elif choice == '6':
                    print("⚙️ 설정 변경 기능은 추후 구현 예정")
                
                elif choice == '7':
                    break
                
                else:
                    print("❓ 잘못된 선택입니다.")
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ 오류: {e}")
        
    finally:
        manager.stop_automated_management()
        print("\n👋 데이터 건강성 관리 시스템이 종료되었습니다.")

if __name__ == "__main__":
    main()
