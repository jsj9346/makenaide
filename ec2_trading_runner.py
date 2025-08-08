#!/usr/bin/env python3
"""
EC2에서 실행될 메인 거래 실행기
DynamoDB에서 거래 파라미터를 읽고 실제 업비트 거래 실행
"""

import os
import sys
import json
import time
import logging
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# 로컬 모듈 import
from upbit_trading_module import UpbitAPIClient, TradingRiskManager, TradingExecutor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/makenaide/trading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TradingRunner:
    """
    EC2에서 실행되는 메인 거래 실행기
    """
    
    def __init__(self):
        self.trading_executor = TradingExecutor()
        self.upbit_client = self.trading_executor.upbit_client
        self.risk_manager = self.trading_executor.risk_manager
        self.running = True
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        logger.info("🚀 Makenaide Trading Runner initialized")
    
    def signal_handler(self, signum, frame):
        """
        시그널 핸들러 (SIGTERM, SIGINT)
        """
        logger.info(f"Received signal {signum} - shutting down gracefully")
        self.running = False
    
    def get_pending_trading_signals(self) -> List[Dict]:
        """
        DynamoDB에서 대기 중인 거래 신호 조회
        """
        try:
            table = self.trading_executor.params_table
            
            # 최근 24시간 내 pending 상태인 거래 신호들
            cutoff_time = (datetime.utcnow() - timedelta(hours=24)).date().isoformat()
            
            response = table.scan(
                FilterExpression='#ts >= :cutoff AND #status = :status',
                ExpressionAttributeNames={
                    '#ts': 'timestamp',
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':cutoff': cutoff_time,
                    ':status': 'pending'
                }
            )
            
            signals = response['Items']
            logger.info(f"📊 Found {len(signals)} pending trading signals")
            
            return signals
            
        except Exception as e:
            logger.error(f"❌ Error getting pending signals: {str(e)}")
            return []
    
    def update_signal_status(self, signal_id: str, status: str, result: Dict = None):
        """
        거래 신호 상태 업데이트
        """
        try:
            table = self.trading_executor.params_table
            
            update_expression = "SET #status = :status, updated_at = :updated_at"
            expression_values = {
                ':status': status,
                ':updated_at': datetime.utcnow().isoformat()
            }
            expression_names = {'#status': 'status'}
            
            if result:
                update_expression += ", execution_result = :result"
                expression_values[':result'] = result
            
            table.update_item(
                Key={
                    'signal_id': signal_id,
                    'timestamp': datetime.utcnow().date().isoformat()
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names
            )
            
            logger.info(f"✅ Updated signal {signal_id} status to {status}")
            
        except Exception as e:
            logger.error(f"❌ Error updating signal status: {str(e)}")
    
    def execute_pending_signals(self) -> int:
        """
        대기 중인 거래 신호들 실행
        """
        try:
            pending_signals = self.get_pending_trading_signals()
            
            if not pending_signals:
                logger.info("📝 No pending trading signals found")
                return 0
            
            executed_count = 0
            
            for signal in pending_signals:
                if not self.running:
                    logger.info("🛑 Shutdown requested - stopping signal processing")
                    break
                
                signal_id = signal['signal_id']
                logger.info(f"🎯 Processing signal: {signal_id}")
                
                # 상태를 processing으로 변경
                self.update_signal_status(signal_id, 'processing')
                
                try:
                    # 거래 실행
                    signal_data = {
                        'action': signal.get('action'),
                        'tickers': signal.get('tickers', []),
                        'signal_strength': signal.get('signal_strength', 'medium'),
                        'signal_id': signal_id
                    }
                    
                    execution_result = self.trading_executor.execute_trading_signal(signal_data)
                    
                    if execution_result.get('status') != 'error':
                        # 성공 처리
                        self.update_signal_status(signal_id, 'completed', execution_result)
                        executed_count += 1
                        
                        logger.info(f"✅ Signal {signal_id} executed successfully")
                        
                        # 거래 간 간격 (API 제한 고려)
                        if len(pending_signals) > 1:
                            time.sleep(2)
                    else:
                        # 실패 처리
                        self.update_signal_status(signal_id, 'failed', execution_result)
                        logger.error(f"❌ Signal {signal_id} execution failed")
                    
                except Exception as signal_error:
                    logger.error(f"❌ Error executing signal {signal_id}: {str(signal_error)}")
                    self.update_signal_status(signal_id, 'failed', {
                        'error': str(signal_error),
                        'timestamp': datetime.utcnow().isoformat()
                    })
            
            logger.info(f"🎉 Processed {executed_count}/{len(pending_signals)} signals successfully")
            return executed_count
            
        except Exception as e:
            logger.error(f"❌ Error executing pending signals: {str(e)}")
            return 0
    
    def run_health_checks(self) -> bool:
        """
        시스템 상태 확인
        """
        try:
            logger.info("🔍 Running health checks...")
            
            # 1. 업비트 API 연결 확인
            accounts = self.upbit_client.get_accounts()
            if not accounts:
                logger.error("❌ Upbit API connection failed")
                return False
            
            logger.info(f"✅ Upbit API connected - {len(accounts)} accounts")
            
            # 2. 잔고 확인
            total_balance = self.risk_manager.get_total_balance(self.upbit_client)
            if total_balance <= 0:
                logger.warning("⚠️  Zero balance detected")
            else:
                logger.info(f"💰 Total balance: {total_balance:,.0f} KRW")
            
            # 3. DynamoDB 연결 확인
            try:
                self.trading_executor.params_table.scan(Limit=1)
                logger.info("✅ DynamoDB connection verified")
            except Exception as db_error:
                logger.error(f"❌ DynamoDB connection failed: {str(db_error)}")
                return False
            
            # 4. 일일 거래 한도 확인
            daily_count = self.risk_manager.get_daily_trade_count()
            logger.info(f"📊 Daily trades: {daily_count}/{self.risk_manager.max_daily_trades}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Health check failed: {str(e)}")
            return False
    
    def run_monitoring_loop(self, check_interval: int = 60):
        """
        메인 모니터링 루프
        """
        logger.info(f"🔄 Starting monitoring loop (check every {check_interval}s)")
        
        # 초기 상태 확인
        if not self.run_health_checks():
            logger.error("❌ Initial health check failed - exiting")
            return False
        
        consecutive_errors = 0
        max_errors = 5
        
        while self.running:
            try:
                # 대기 중인 거래 신호 처리
                executed_count = self.execute_pending_signals()
                
                if executed_count > 0:
                    logger.info(f"✅ Executed {executed_count} trading signals")
                    consecutive_errors = 0  # 성공 시 에러 카운터 리셋
                
                # 다음 체크까지 대기 (중간에 시그널 받으면 즉시 종료)
                for _ in range(check_interval):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"❌ Error in monitoring loop: {str(e)} ({consecutive_errors}/{max_errors})")
                
                if consecutive_errors >= max_errors:
                    logger.error("❌ Too many consecutive errors - shutting down")
                    self.running = False
                    break
                
                # 에러 발생 시 잠시 대기
                time.sleep(10)
        
        logger.info("🛑 Monitoring loop ended")
        return True
    
    def run_single_check(self):
        """
        단일 체크 실행 (테스트용)
        """
        logger.info("🔍 Running single check...")
        
        if not self.run_health_checks():
            return False
        
        executed_count = self.execute_pending_signals()
        logger.info(f"✅ Single check completed - executed {executed_count} signals")
        
        return True

def main():
    """
    메인 실행 함수
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Makenaide Trading Runner')
    parser.add_argument('--mode', choices=['loop', 'single'], default='loop',
                       help='Execution mode: loop (continuous) or single (one-time)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Check interval in seconds (for loop mode)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - health checks only')
    
    args = parser.parse_args()
    
    print("🚀 Makenaide Trading Runner")
    print("=" * 50)
    print(f"Mode: {args.mode}")
    print(f"Interval: {args.interval}s")
    print(f"Test mode: {args.test}")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 50)
    
    try:
        runner = TradingRunner()
        
        if args.test:
            # 테스트 모드 - 상태 확인만
            result = runner.run_health_checks()
            print(f"\n{'✅ Health check passed' if result else '❌ Health check failed'}")
            return 0 if result else 1
            
        elif args.mode == 'single':
            # 단일 실행 모드
            result = runner.run_single_check()
            return 0 if result else 1
            
        else:
            # 연속 실행 모드
            result = runner.run_monitoring_loop(args.interval)
            return 0 if result else 1
            
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())