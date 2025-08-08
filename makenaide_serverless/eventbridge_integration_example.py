
# Lambda 함수에서 다음 Phase를 트리거하는 EventBridge 이벤트 발송 예제

import boto3
import json
from datetime import datetime

def send_phase_completed_event(phase_number, status, data=None):
    """
    Phase 완료 이벤트를 EventBridge로 전송합니다.
    
    Args:
        phase_number (int): 완료된 Phase 번호 (0-6)
        status (str): 'success' 또는 'failure'
        data (dict): 추가 데이터 (선택사항)
    """
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
    detail = {
        "phase": str(phase_number),
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data or {}
    }
    
    try:
        response = events_client.put_events(
            Entries=[
                {
                    'Source': 'makenaide',
                    'DetailType': 'Phase Completed',
                    'Detail': json.dumps(detail),
                    'Time': datetime.utcnow()
                }
            ]
        )
        
        print(f"✅ Event sent: Phase {phase_number} {status}")
        return response
        
    except Exception as e:
        print(f"❌ Error sending event: {e}")
        return None

def send_trading_signal_event(action, tickers, signal_strength="high"):
    """
    거래 신호 이벤트를 EventBridge로 전송합니다.
    
    Args:
        action (str): 'buy' 또는 'sell'
        tickers (list): 거래 대상 티커들
        signal_strength (str): 신호 강도 ('high', 'medium', 'low')
    """
    events_client = boto3.client('events', region_name='ap-northeast-2')
    
    detail = {
        "action": action,
        "tickers": tickers,
        "signal_strength": signal_strength,
        "timestamp": datetime.utcnow().isoformat(),
        "generated_by": "Phase5-ConditionCheck"
    }
    
    try:
        response = events_client.put_events(
            Entries=[
                {
                    'Source': 'makenaide',
                    'DetailType': 'Trading Signal',
                    'Detail': json.dumps(detail),
                    'Time': datetime.utcnow()
                }
            ]
        )
        
        print(f"✅ Trading signal sent: {action} {len(tickers)} tickers")
        return response
        
    except Exception as e:
        print(f"❌ Error sending trading signal: {e}")
        return None

# Lambda 함수에서 사용 예시
def lambda_handler(event, context):
    try:
        # Phase 로직 실행
        # ...
        
        # Phase 완료 시 다음 Phase 트리거
        send_phase_completed_event(
            phase_number=1,  # 현재 Phase 번호
            status="success",
            data={"processed_tickers": 105, "filtered_count": 23}
        )
        
        # 거래 신호 발생 시 (Phase 5에서)
        if trading_candidates:
            send_trading_signal_event(
                action="buy",
                tickers=["KRW-BTC", "KRW-ETH"],
                signal_strength="high"
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Phase completed successfully')
        }
        
    except Exception as e:
        # 실패 시 실패 이벤트 전송
        send_phase_completed_event(
            phase_number=1,
            status="failure",
            data={"error": str(e)}
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
