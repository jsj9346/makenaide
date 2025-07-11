"""
실시간 모니터링 시스템
켈리공식 기반 거래의 실시간 성능 추적 및 파라미터 튜닝
"""

import pandas as pd
import numpy as np
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import threading
from pathlib import Path
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TradeRecord:
    """거래 기록"""
    timestamp: datetime
    ticker: str
    action: str  # BUY, SELL
    price: float
    size: float
    capital: float
    profit: Optional[float] = None
    return_pct: Optional[float] = None
    reason: str = ""
    kelly_fraction: float = 0.0
    atr_multiplier: float = 0.0

@dataclass
class PerformanceMetrics:
    """성과 지표"""
    timestamp: datetime
    total_return: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    current_capital: float
    initial_capital: float

class RealTimeMonitor:
    """실시간 모니터링 시스템"""
    
    def __init__(self, db_path: str = "realtime_monitor.db"):
        self.db_path = db_path
        self.initial_capital = 1000000
        self.current_capital = self.initial_capital
        self.trades: List[TradeRecord] = []
        self.performance_history: List[PerformanceMetrics] = []
        self.monitoring_active = False
        self.monitor_thread = None
        
        # 데이터베이스 초기화
        self._init_database()
        
        # 모니터링 설정
        self.monitoring_interval = 60  # 60초마다 체크
        self.alert_thresholds = {
            'max_drawdown': 0.05,  # 5% 최대 낙폭
            'win_rate': 0.4,       # 40% 최소 승률
            'profit_factor': 1.2   # 1.2 최소 수익 팩터
        }
    
    def _init_database(self):
        """데이터베이스 초기화"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 거래 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    action TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    capital REAL NOT NULL,
                    profit REAL,
                    return_pct REAL,
                    reason TEXT,
                    kelly_fraction REAL,
                    atr_multiplier REAL
                )
            """)
            
            # 성과 지표 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    total_return REAL NOT NULL,
                    win_rate REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    sharpe_ratio REAL NOT NULL,
                    total_trades INTEGER NOT NULL,
                    winning_trades INTEGER NOT NULL,
                    losing_trades INTEGER NOT NULL,
                    current_capital REAL NOT NULL,
                    initial_capital REAL NOT NULL
                )
            """)
            
            # 알림 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    severity TEXT NOT NULL
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("✅ 실시간 모니터링 데이터베이스 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
    
    def start_monitoring(self):
        """실시간 모니터링 시작"""
        if self.monitoring_active:
            logger.warning("⚠️ 모니터링이 이미 실행 중입니다")
            return
        
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("🚀 실시간 모니터링 시작")
    
    def stop_monitoring(self):
        """실시간 모니터링 중지"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("🛑 실시간 모니터링 중지")
    
    def _monitoring_loop(self):
        """모니터링 루프"""
        while self.monitoring_active:
            try:
                # 성과 지표 계산
                metrics = self._calculate_current_metrics()
                self.performance_history.append(metrics)
                
                # 데이터베이스에 저장
                self._save_performance_metrics(metrics)
                
                # 알림 체크
                self._check_alerts(metrics)
                
                # 로그 출력
                self._log_performance_summary(metrics)
                
                # 대기
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"❌ 모니터링 루프 오류: {e}")
                time.sleep(self.monitoring_interval)
    
    def record_trade(self, trade: TradeRecord):
        """거래 기록"""
        try:
            self.trades.append(trade)
            
            # 자본 업데이트
            if trade.action == 'BUY':
                self.current_capital -= trade.price * trade.size
            elif trade.action == 'SELL':
                self.current_capital += trade.price * trade.size
                if trade.profit:
                    self.current_capital += trade.profit
            
            # 데이터베이스에 저장
            self._save_trade_record(trade)
            
            logger.info(f"📝 거래 기록: {trade.ticker} {trade.action} @ {trade.price:,.0f}원")
            
        except Exception as e:
            logger.error(f"❌ 거래 기록 실패: {e}")
    
    def _calculate_current_metrics(self) -> PerformanceMetrics:
        """현재 성과 지표 계산"""
        try:
            if not self.trades:
                return PerformanceMetrics(
                    timestamp=datetime.now(),
                    total_return=0.0,
                    win_rate=0.0,
                    profit_factor=0.0,
                    max_drawdown=0.0,
                    sharpe_ratio=0.0,
                    total_trades=0,
                    winning_trades=0,
                    losing_trades=0,
                    current_capital=self.current_capital,
                    initial_capital=self.initial_capital
                )
            
            # 기본 지표
            total_return = ((self.current_capital - self.initial_capital) / self.initial_capital) * 100
            
            # 거래 분석
            sell_trades = [t for t in self.trades if t.action == 'SELL']
            winning_trades = [t for t in sell_trades if t.profit and t.profit > 0]
            losing_trades = [t for t in sell_trades if t.profit and t.profit <= 0]
            
            win_rate = (len(winning_trades) / len(sell_trades)) * 100 if sell_trades else 0
            
            # 수익/손실 분석
            total_profit = sum(t.profit for t in winning_trades if t.profit)
            total_loss = abs(sum(t.profit for t in losing_trades if t.profit))
            profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
            
            # 최대 낙폭 계산
            max_drawdown = self._calculate_max_drawdown()
            
            # 샤프 비율 계산
            sharpe_ratio = self._calculate_sharpe_ratio()
            
            return PerformanceMetrics(
                timestamp=datetime.now(),
                total_return=round(total_return, 2),
                win_rate=round(win_rate, 2),
                profit_factor=round(profit_factor, 2),
                max_drawdown=round(max_drawdown, 2),
                sharpe_ratio=round(sharpe_ratio, 2),
                total_trades=len(sell_trades),
                winning_trades=len(winning_trades),
                losing_trades=len(losing_trades),
                current_capital=self.current_capital,
                initial_capital=self.initial_capital
            )
            
        except Exception as e:
            logger.error(f"❌ 성과 지표 계산 실패: {e}")
            return PerformanceMetrics(
                timestamp=datetime.now(),
                total_return=0.0,
                win_rate=0.0,
                profit_factor=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                current_capital=self.current_capital,
                initial_capital=self.initial_capital
            )
    
    def _calculate_max_drawdown(self) -> float:
        """최대 낙폭 계산"""
        try:
            if not self.performance_history:
                return 0.0
            
            capital_values = [p.current_capital for p in self.performance_history]
            peak = self.initial_capital
            max_drawdown = 0.0
            
            for value in capital_values:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak * 100
                max_drawdown = max(max_drawdown, drawdown)
            
            return max_drawdown
            
        except Exception as e:
            logger.error(f"❌ 최대 낙폭 계산 실패: {e}")
            return 0.0
    
    def _calculate_sharpe_ratio(self) -> float:
        """샤프 비율 계산"""
        try:
            if len(self.performance_history) < 2:
                return 0.0
            
            returns = []
            for i in range(1, len(self.performance_history)):
                prev_capital = self.performance_history[i-1].current_capital
                curr_capital = self.performance_history[i].current_capital
                ret = (curr_capital - prev_capital) / prev_capital
                returns.append(ret)
            
            if not returns:
                return 0.0
            
            avg_return = np.mean(returns)
            std_return = np.std(returns)
            
            if std_return == 0:
                return 0.0
            
            # 연율화된 샤프 비율
            sharpe_ratio = (avg_return / std_return) * np.sqrt(252 * 24 * 60 / self.monitoring_interval)
            return sharpe_ratio
            
        except Exception as e:
            logger.error(f"❌ 샤프 비율 계산 실패: {e}")
            return 0.0
    
    def _check_alerts(self, metrics: PerformanceMetrics):
        """알림 체크"""
        try:
            alerts = []
            
            # 최대 낙폭 알림
            if metrics.max_drawdown > self.alert_thresholds['max_drawdown'] * 100:
                alerts.append({
                    'type': 'max_drawdown',
                    'message': f"최대 낙폭 경고: {metrics.max_drawdown:.2f}%",
                    'severity': 'high'
                })
            
            # 승률 알림
            if metrics.win_rate < self.alert_thresholds['win_rate'] * 100:
                alerts.append({
                    'type': 'win_rate',
                    'message': f"승률 경고: {metrics.win_rate:.2f}%",
                    'severity': 'medium'
                })
            
            # 수익 팩터 알림
            if metrics.profit_factor < self.alert_thresholds['profit_factor']:
                alerts.append({
                    'type': 'profit_factor',
                    'message': f"수익 팩터 경고: {metrics.profit_factor:.2f}",
                    'severity': 'medium'
                })
            
            # 알림 저장
            for alert in alerts:
                self._save_alert(alert)
                logger.warning(f"⚠️ {alert['message']}")
            
        except Exception as e:
            logger.error(f"❌ 알림 체크 실패: {e}")
    
    def _log_performance_summary(self, metrics: PerformanceMetrics):
        """성과 요약 로그"""
        logger.info(f"📊 실시간 성과 요약:")
        logger.info(f"   - 총 수익률: {metrics.total_return:.2f}%")
        logger.info(f"   - 승률: {metrics.win_rate:.2f}%")
        logger.info(f"   - 수익 팩터: {metrics.profit_factor:.2f}")
        logger.info(f"   - 최대 낙폭: {metrics.max_drawdown:.2f}%")
        logger.info(f"   - 샤프 비율: {metrics.sharpe_ratio:.2f}")
        logger.info(f"   - 총 거래: {metrics.total_trades}회")
    
    def _save_trade_record(self, trade: TradeRecord):
        """거래 기록 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO trades (timestamp, ticker, action, price, size, capital, 
                                  profit, return_pct, reason, kelly_fraction, atr_multiplier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.timestamp.isoformat(),
                trade.ticker,
                trade.action,
                trade.price,
                trade.size,
                trade.capital,
                trade.profit,
                trade.return_pct,
                trade.reason,
                trade.kelly_fraction,
                trade.atr_multiplier
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ 거래 기록 저장 실패: {e}")
    
    def _save_performance_metrics(self, metrics: PerformanceMetrics):
        """성과 지표 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO performance (timestamp, total_return, win_rate, profit_factor,
                                       max_drawdown, sharpe_ratio, total_trades, 
                                       winning_trades, losing_trades, current_capital, initial_capital)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metrics.timestamp.isoformat(),
                metrics.total_return,
                metrics.win_rate,
                metrics.profit_factor,
                metrics.max_drawdown,
                metrics.sharpe_ratio,
                metrics.total_trades,
                metrics.winning_trades,
                metrics.losing_trades,
                metrics.current_capital,
                metrics.initial_capital
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ 성과 지표 저장 실패: {e}")
    
    def _save_alert(self, alert: Dict):
        """알림 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO alerts (timestamp, alert_type, message, severity)
                VALUES (?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                alert['type'],
                alert['message'],
                alert['severity']
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ 알림 저장 실패: {e}")
    
    def generate_performance_report(self, output_path: str = "realtime_performance_report.html") -> str:
        """성과 리포트 생성"""
        try:
            if not self.performance_history:
                return "성과 데이터가 없습니다."
            
            # HTML 리포트 생성
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>실시간 성과 리포트</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric-card {{ background-color: #fff; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .metric-label {{ color: #666; margin-top: 5px; }}
        .positive {{ color: #28a745; }}
        .negative {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>실시간 성과 리포트</h1>
        <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="metrics">
"""
            
            # 최신 성과 지표
            latest = self.performance_history[-1]
            
            metrics_data = [
                ("총 수익률", f"{latest.total_return:.2f}%", "positive" if latest.total_return > 0 else "negative"),
                ("승률", f"{latest.win_rate:.2f}%", "positive" if latest.win_rate > 50 else "warning"),
                ("수익 팩터", f"{latest.profit_factor:.2f}", "positive" if latest.profit_factor > 1 else "negative"),
                ("최대 낙폭", f"{latest.max_drawdown:.2f}%", "negative" if latest.max_drawdown > 5 else "positive"),
                ("샤프 비율", f"{latest.sharpe_ratio:.2f}", "positive" if latest.sharpe_ratio > 1 else "warning"),
                ("총 거래", f"{latest.total_trades}회", "positive"),
                ("현재 자본", f"{latest.current_capital:,.0f}원", "positive"),
                ("초기 자본", f"{latest.initial_capital:,.0f}원", "positive")
            ]
            
            for label, value, color_class in metrics_data:
                html_content += f"""
        <div class="metric-card">
            <div class="metric-value {color_class}">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
"""
            
            html_content += """
    </div>
    
    <h2>최근 거래 내역</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <thead>
            <tr style="background-color: #f0f0f0;">
                <th style="padding: 10px; text-align: left; border: 1px solid #ddd;">시간</th>
                <th style="padding: 10px; text-align: left; border: 1px solid #ddd;">티커</th>
                <th style="padding: 10px; text-align: left; border: 1px solid #ddd;">액션</th>
                <th style="padding: 10px; text-align: right; border: 1px solid #ddd;">가격</th>
                <th style="padding: 10px; text-align: right; border: 1px solid #ddd;">수익률</th>
            </tr>
        </thead>
        <tbody>
"""
            
            # 최근 거래 내역
            recent_trades = self.trades[-10:] if len(self.trades) > 10 else self.trades
            for trade in reversed(recent_trades):
                if trade.action == 'SELL' and trade.return_pct is not None:
                    color_class = "positive" if trade.return_pct > 0 else "negative"
                    return_pct_str = f"{trade.return_pct:.2f}%"
                else:
                    color_class = ""
                    return_pct_str = "-"
                
                html_content += f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">{trade.timestamp.strftime('%Y-%m-%d %H:%M')}</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{trade.ticker}</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{trade.action}</td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd;">{trade.price:,.0f}원</td>
                <td style="padding: 10px; text-align: right; border: 1px solid #ddd; color: {color_class};">{return_pct_str}</td>
            </tr>
"""
            
            html_content += """
        </tbody>
    </table>
</body>
</html>
"""
            
            # 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"📄 성과 리포트 생성 완료: {output_path}")
            return html_content
            
        except Exception as e:
            logger.error(f"❌ 성과 리포트 생성 실패: {e}")
            return ""

def main():
    """메인 실행 함수"""
    try:
        # 실시간 모니터링 시스템 초기화
        monitor = RealTimeMonitor()
        
        # 모니터링 시작
        monitor.start_monitoring()
        
        # 샘플 거래 기록 (테스트용)
        time.sleep(2)
        
        # 매수 거래 기록
        buy_trade = TradeRecord(
            timestamp=datetime.now(),
            ticker="KRW-BTC",
            action="BUY",
            price=50000000,
            size=0.01,
            capital=950000,
            kelly_fraction=0.5,
            atr_multiplier=1.5,
            reason="켈리공식 매수 신호"
        )
        monitor.record_trade(buy_trade)
        
        time.sleep(2)
        
        # 매도 거래 기록
        sell_trade = TradeRecord(
            timestamp=datetime.now(),
            ticker="KRW-BTC",
            action="SELL",
            price=52000000,
            size=0.01,
            capital=1020000,
            profit=200000,
            return_pct=4.0,
            kelly_fraction=0.5,
            atr_multiplier=1.5,
            reason="켈리공식 매도 신호"
        )
        monitor.record_trade(sell_trade)
        
        # 성과 리포트 생성
        time.sleep(2)
        report = monitor.generate_performance_report()
        
        # 모니터링 중지
        monitor.stop_monitoring()
        
        print("✅ 실시간 모니터링 테스트 완료")
        
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")

if __name__ == "__main__":
    main() 