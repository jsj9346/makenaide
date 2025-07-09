#!/usr/bin/env python3
"""
하이브리드 필터링 실시간 알림 시스템

주요 기능:
- 성능 저하 실시간 감지
- 데이터 품질 이상 알림
- 필터링 효율성 모니터링
- 다채널 알림 발송 (로그, 이메일, 슬랙 등)
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import logging

# 선택적 import (이메일/슬랙 기능은 필요시에만)
try:
    import smtplib
    import requests
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

from utils import setup_logger, safe_strftime

# 로거 설정
logger = setup_logger()

class FilteringAlertSystem:
    """
    하이브리드 필터링 과정에서 중요한 이벤트를 실시간 알림하는 시스템
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._load_default_config()
        
        # 알림 임계값 설정
        self.alert_thresholds = {
            'data_quality_drop': self.config.get('data_quality_threshold', 0.7),
            'filtering_rate_drop': self.config.get('filtering_rate_threshold', 0.05),
            'processing_time_spike': self.config.get('processing_time_threshold', 30.0),
            'efficiency_drop': self.config.get('efficiency_threshold', 0.08),
            'error_rate_spike': self.config.get('error_rate_threshold', 0.1)
        }
        
        # 알림 채널 설정
        self.notification_channels = {
            'log': self.config.get('enable_log_alerts', True),
            'email': self.config.get('enable_email_alerts', False),
            'slack': self.config.get('enable_slack_alerts', False),
            'file': self.config.get('enable_file_alerts', True)
        }
        
        # 알림 히스토리
        self.alert_history = []
        self.alert_history_file = Path("alerts") / "alert_history.json"
        self.alert_history_file.parent.mkdir(exist_ok=True)
        
        # 중복 알림 방지를 위한 쿨다운 (분 단위)
        self.cooldown_minutes = self.config.get('alert_cooldown_minutes', 30)
        self.last_alerts = {}
        
        # 기존 알림 히스토리 로드
        self._load_alert_history()
        
    def _load_default_config(self) -> Dict:
        """기본 설정을 로드합니다."""
        return {
            'data_quality_threshold': 0.7,
            'filtering_rate_threshold': 0.05,
            'processing_time_threshold': 30.0,
            'efficiency_threshold': 0.08,
            'error_rate_threshold': 0.1,
            'enable_log_alerts': True,
            'enable_email_alerts': False,
            'enable_slack_alerts': False,
            'enable_file_alerts': True,
            'alert_cooldown_minutes': 30,
            'email_smtp_server': 'smtp.gmail.com',
            'email_smtp_port': 587,
            'email_sender': '',
            'email_password': '',
            'email_recipients': [],
            'slack_webhook_url': ''
        }
    
    def _load_alert_history(self):
        """기존 알림 히스토리를 로드합니다."""
        try:
            if self.alert_history_file.exists():
                with open(self.alert_history_file, 'r', encoding='utf-8') as f:
                    self.alert_history = json.load(f)
                logger.info(f"📚 알림 히스토리 로드: {len(self.alert_history)}개 기록")
        except Exception as e:
            logger.warning(f"⚠️ 알림 히스토리 로드 실패: {e}")
            self.alert_history = []
    
    def _save_alert_history(self):
        """알림 히스토리를 저장합니다."""
        try:
            with open(self.alert_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.alert_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"❌ 알림 히스토리 저장 실패: {e}")
    
    def check_and_send_alerts(self, session_metrics: Dict) -> List[Dict]:
        """
        세션 메트릭스를 바탕으로 알림 필요성을 판단하고 발송합니다.
        
        Args:
            session_metrics (dict): {
                'total_tickers': int,
                'filtered_tickers': int,
                'processing_time': float,
                'data_quality_score': float,
                'efficiency': float,
                'error_count': int,
                'timestamp': str
            }
        
        Returns:
            List[Dict]: 발송된 알림 목록
        """
        alerts = []
        current_time = datetime.now()
        
        try:
            # 1. 데이터 품질 저하 검사
            if session_metrics.get('data_quality_score', 1.0) < self.alert_thresholds['data_quality_drop']:
                alert = {
                    'type': 'DATA_QUALITY_DROP',
                    'message': f"데이터 품질 저하 감지: {session_metrics['data_quality_score']:.2f}",
                    'severity': 'HIGH',
                    'value': session_metrics['data_quality_score'],
                    'threshold': self.alert_thresholds['data_quality_drop']
                }
                alerts.append(alert)
            
            # 2. 필터링 효율성 저하 검사
            efficiency = session_metrics.get('efficiency', 0)
            if efficiency < self.alert_thresholds['efficiency_drop']:
                alert = {
                    'type': 'LOW_EFFICIENCY',
                    'message': f"필터링 효율성 저하: {efficiency:.2%}",
                    'severity': 'MEDIUM',
                    'value': efficiency,
                    'threshold': self.alert_thresholds['efficiency_drop']
                }
                alerts.append(alert)
            
            # 3. 필터링 비율 급감 검사
            if session_metrics.get('total_tickers', 0) > 0:
                filtering_rate = session_metrics['filtered_tickers'] / session_metrics['total_tickers']
                if filtering_rate < self.alert_thresholds['filtering_rate_drop']:
                    alert = {
                        'type': 'LOW_FILTERING_RATE',
                        'message': f"필터링 비율 급감: {filtering_rate:.2%}",
                        'severity': 'MEDIUM',
                        'value': filtering_rate,
                        'threshold': self.alert_thresholds['filtering_rate_drop']
                    }
                    alerts.append(alert)
            
            # 4. 처리 속도 저하 검사
            processing_time = session_metrics.get('processing_time', 0)
            if processing_time > self.alert_thresholds['processing_time_spike']:
                alert = {
                    'type': 'PROCESSING_DELAY',
                    'message': f"처리 시간 급증: {processing_time:.1f}초",
                    'severity': 'LOW',
                    'value': processing_time,
                    'threshold': self.alert_thresholds['processing_time_spike']
                }
                alerts.append(alert)
            
            # 5. 오류율 급증 검사
            error_count = session_metrics.get('error_count', 0)
            total_operations = session_metrics.get('total_tickers', 1)
            error_rate = error_count / total_operations
            if error_rate > self.alert_thresholds['error_rate_spike']:
                alert = {
                    'type': 'HIGH_ERROR_RATE',
                    'message': f"오류율 급증: {error_rate:.1%} ({error_count}/{total_operations})",
                    'severity': 'HIGH',
                    'value': error_rate,
                    'threshold': self.alert_thresholds['error_rate_spike']
                }
                alerts.append(alert)
            
            # 6. 알림 발송 (쿨다운 확인)
            sent_alerts = []
            for alert in alerts:
                if self._should_send_alert(alert, current_time):
                    self._send_alert(alert, session_metrics)
                    sent_alerts.append(alert)
                    
                    # 쿨다운 갱신
                    self.last_alerts[alert['type']] = current_time
            
            # 7. 히스토리 업데이트
            for alert in sent_alerts:
                alert['timestamp'] = current_time.isoformat()
                alert['session_id'] = session_metrics.get('session_id', 'unknown')
                self.alert_history.append(alert)
            
            if sent_alerts:
                self._save_alert_history()
            
            return sent_alerts
            
        except Exception as e:
            logger.error(f"❌ 알림 검사 중 오류: {e}")
            return []
    
    def _should_send_alert(self, alert: Dict, current_time: datetime) -> bool:
        """쿨다운을 고려하여 알림 발송 여부를 결정합니다."""
        alert_type = alert['type']
        
        if alert_type not in self.last_alerts:
            return True
        
        last_alert_time = self.last_alerts[alert_type]
        time_diff = current_time - last_alert_time
        
        return time_diff.total_seconds() > (self.cooldown_minutes * 60)
    
    def _send_alert(self, alert: Dict, session_metrics: Dict):
        """실제 알림을 발송합니다."""
        try:
            # 로그 알림
            if self.notification_channels['log']:
                self._send_log_alert(alert)
            
            # 파일 알림
            if self.notification_channels['file']:
                self._send_file_alert(alert, session_metrics)
            
            # 이메일 알림
            if self.notification_channels['email']:
                self._send_email_alert(alert, session_metrics)
            
            # 슬랙 알림
            if self.notification_channels['slack']:
                self._send_slack_alert(alert, session_metrics)
                
        except Exception as e:
            logger.error(f"❌ 알림 발송 실패: {e}")
    
    def _send_log_alert(self, alert: Dict):
        """로그 알림을 발송합니다."""
        severity_emoji = {
            'HIGH': '🚨',
            'MEDIUM': '⚠️',
            'LOW': '💡'
        }
        
        emoji = severity_emoji.get(alert['severity'], '📢')
        logger.warning(f"{emoji} [{alert['severity']}] {alert['type']}: {alert['message']}")
    
    def _send_file_alert(self, alert: Dict, session_metrics: Dict):
        """파일 알림을 저장합니다."""
        try:
            alert_file = Path("alerts") / f"alert_{safe_strftime(datetime.now(), '%Y%m%d')}.txt"
            
            alert_text = f"""
[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {alert['severity']} 알림
유형: {alert['type']}
메시지: {alert['message']}
값: {alert.get('value', 'N/A')}
임계값: {alert.get('threshold', 'N/A')}
세션 정보: {json.dumps(session_metrics, ensure_ascii=False, indent=2, default=str)}
{"="*50}
"""
            
            with open(alert_file, 'a', encoding='utf-8') as f:
                f.write(alert_text)
                
        except Exception as e:
            logger.error(f"❌ 파일 알림 저장 실패: {e}")
    
    def _send_email_alert(self, alert: Dict, session_metrics: Dict):
        """이메일 알림을 발송합니다."""
        try:
            if not EMAIL_AVAILABLE:
                logger.warning("이메일 기능을 사용할 수 없습니다 (의존성 누락)")
                return
                
            if not self.config.get('email_sender') or not self.config.get('email_recipients'):
                return
            
            # 이메일 내용 구성
            subject = f"[MakenaideBot] {alert['severity']} 알림: {alert['type']}"
            
            body = f"""
하이브리드 필터링 시스템에서 알림이 발생했습니다.

📊 알림 정보
- 유형: {alert['type']}
- 심각도: {alert['severity']}
- 메시지: {alert['message']}
- 현재 값: {alert.get('value', 'N/A')}
- 임계값: {alert.get('threshold', 'N/A')}
- 발생 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📈 세션 메트릭스
- 전체 티커 수: {session_metrics.get('total_tickers', 'N/A')}
- 필터링된 티커 수: {session_metrics.get('filtered_tickers', 'N/A')}
- 처리 시간: {session_metrics.get('processing_time', 'N/A')}초
- 데이터 품질 점수: {session_metrics.get('data_quality_score', 'N/A')}
- 효율성: {session_metrics.get('efficiency', 0):.2%}

시스템을 점검해주세요.

Best regards,
MakenaideBot Alert System
            """
            
            # 이메일 발송
            msg = MimeMultipart()
            msg['From'] = self.config['email_sender']
            msg['To'] = ', '.join(self.config['email_recipients'])
            msg['Subject'] = subject
            
            msg.attach(MimeText(body, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(self.config['email_smtp_server'], self.config['email_smtp_port'])
            server.starttls()
            server.login(self.config['email_sender'], self.config['email_password'])
            
            text = msg.as_string()
            server.sendmail(self.config['email_sender'], self.config['email_recipients'], text)
            server.quit()
            
            logger.info(f"📧 이메일 알림 발송 완료: {alert['type']}")
            
        except Exception as e:
            logger.error(f"❌ 이메일 알림 발송 실패: {e}")
    
    def _send_slack_alert(self, alert: Dict, session_metrics: Dict):
        """슬랙 알림을 발송합니다."""
        try:
            if not EMAIL_AVAILABLE:
                logger.warning("슬랙 기능을 사용할 수 없습니다 (의존성 누락)")
                return
                
            webhook_url = self.config.get('slack_webhook_url')
            if not webhook_url:
                return
            
            # 심각도별 색상 설정
            color_map = {
                'HIGH': '#FF0000',     # 빨간색
                'MEDIUM': '#FFA500',   # 주황색
                'LOW': '#FFFF00'       # 노란색
            }
            
            # 슬랙 메시지 구성
            slack_data = {
                "text": f"🚨 MakenaideBot 알림: {alert['type']}",
                "attachments": [
                    {
                        "color": color_map.get(alert['severity'], '#808080'),
                        "fields": [
                            {
                                "title": "알림 유형",
                                "value": alert['type'],
                                "short": True
                            },
                            {
                                "title": "심각도",
                                "value": alert['severity'],
                                "short": True
                            },
                            {
                                "title": "메시지",
                                "value": alert['message'],
                                "short": False
                            },
                            {
                                "title": "현재 값",
                                "value": str(alert.get('value', 'N/A')),
                                "short": True
                            },
                            {
                                "title": "임계값",
                                "value": str(alert.get('threshold', 'N/A')),
                                "short": True
                            },
                            {
                                "title": "처리 시간",
                                "value": f"{session_metrics.get('processing_time', 'N/A')}초",
                                "short": True
                            },
                            {
                                "title": "데이터 품질",
                                "value": f"{session_metrics.get('data_quality_score', 'N/A')}",
                                "short": True
                            }
                        ],
                        "timestamp": int(datetime.now().timestamp())
                    }
                ]
            }
            
            response = requests.post(webhook_url, json=slack_data)
            
            if response.status_code == 200:
                logger.info(f"💬 슬랙 알림 발송 완료: {alert['type']}")
            else:
                logger.error(f"❌ 슬랙 알림 발송 실패: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ 슬랙 알림 발송 실패: {e}")
    
    def get_alert_summary(self, days: int = 7) -> Dict:
        """최근 N일간의 알림 요약을 반환합니다."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            recent_alerts = []
            for alert in self.alert_history:
                try:
                    alert_time = datetime.fromisoformat(alert['timestamp'])
                    if alert_time >= cutoff_date:
                        recent_alerts.append(alert)
                except:
                    continue
            
            # 알림 유형별 집계
            alert_counts = {}
            severity_counts = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
            
            for alert in recent_alerts:
                alert_type = alert['type']
                severity = alert['severity']
                
                alert_counts[alert_type] = alert_counts.get(alert_type, 0) + 1
                severity_counts[severity] += 1
            
            # 가장 빈번한 알림 유형
            most_frequent_alert = max(alert_counts.items(), key=lambda x: x[1]) if alert_counts else None
            
            summary = {
                'period_days': days,
                'total_alerts': len(recent_alerts),
                'alert_types': alert_counts,
                'severity_breakdown': severity_counts,
                'most_frequent_alert': {
                    'type': most_frequent_alert[0] if most_frequent_alert else None,
                    'count': most_frequent_alert[1] if most_frequent_alert else 0
                },
                'alerts_per_day': len(recent_alerts) / days,
                'recent_alerts': recent_alerts[-10:] if recent_alerts else []  # 최근 10개
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 알림 요약 생성 실패: {e}")
            return {'error': str(e)}
    
    def update_thresholds(self, new_thresholds: Dict):
        """알림 임계값을 업데이트합니다."""
        try:
            self.alert_thresholds.update(new_thresholds)
            logger.info(f"✅ 알림 임계값 업데이트 완료: {new_thresholds}")
        except Exception as e:
            logger.error(f"❌ 알림 임계값 업데이트 실패: {e}")
    
    def enable_channel(self, channel: str, enabled: bool = True):
        """알림 채널을 활성화/비활성화합니다."""
        if channel in self.notification_channels:
            self.notification_channels[channel] = enabled
            status = "활성화" if enabled else "비활성화"
            logger.info(f"✅ {channel} 알림 채널 {status}")
        else:
            logger.warning(f"⚠️ 알 수 없는 알림 채널: {channel}")

# 전역 알림 시스템 인스턴스
_alert_system_instance = None

def get_alert_system(config: Optional[Dict] = None) -> FilteringAlertSystem:
    """알림 시스템 싱글톤 인스턴스를 반환합니다."""
    global _alert_system_instance
    if _alert_system_instance is None:
        _alert_system_instance = FilteringAlertSystem(config)
    return _alert_system_instance

if __name__ == "__main__":
    # 테스트 코드
    alert_system = get_alert_system()
    
    # 샘플 세션 메트릭스
    sample_metrics = {
        'total_tickers': 200,
        'filtered_tickers': 5,  # 낮은 필터링 비율
        'processing_time': 35.0,  # 높은 처리 시간
        'data_quality_score': 0.6,  # 낮은 품질 점수
        'efficiency': 0.025,  # 낮은 효율성
        'error_count': 15,  # 높은 오류 수
        'session_id': 'test_session_001'
    }
    
    # 알림 테스트
    alerts = alert_system.check_and_send_alerts(sample_metrics)
    
    print(f"📊 발송된 알림 수: {len(alerts)}")
    for alert in alerts:
        print(f"   - {alert['type']}: {alert['message']}")
    
    # 알림 요약 테스트
    summary = alert_system.get_alert_summary(days=7)
    print(f"\n📈 최근 7일 알림 요약:")
    print(f"   - 총 알림 수: {summary['total_alerts']}")
    print(f"   - 일평균 알림: {summary['alerts_per_day']:.1f}")
    print(f"   - 가장 빈번한 알림: {summary['most_frequent_alert']['type']}") 