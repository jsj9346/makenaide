#!/usr/bin/env python3
"""
🔧 Phase 6 Lambda DynamoDB Decimal 타입 수정
- Python float → DynamoDB Decimal 변환 유틸리티 추가
"""

import boto3
import json
from decimal import Decimal

def fix_lambda_function():
    """Lambda 함수 코드에서 Decimal 변환 추가"""
    
    lambda_client = boto3.client('lambda')
    function_name = 'makenaide-trade-execution-phase6'
    
    try:
        # Lambda 함수 코드 수정 - Decimal 변환 로직 추가
        decimal_conversion_fix = '''
# DynamoDB Float → Decimal 변환 헬퍼 함수
def convert_floats_to_decimal(obj):
    """Python float를 DynamoDB Decimal로 변환"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    return obj

# save_trade_record 메서드 수정
def save_trade_record(self, order_data: Dict, order_result: Dict):
    try:
        trade_record = {
            'trade_id': order_result.get('order_id', str(uuid.uuid4())),
            'ticker': order_data['ticker'],
            'side': order_data['side'],
            'executed_price': Decimal(str(order_result.get('executed_price', 0))),
            'executed_quantity': Decimal(str(order_result.get('executed_quantity', 0))),
            'executed_amount': Decimal(str(order_result.get('executed_amount', 0))),
            'timestamp': order_data['timestamp'],
            'simulation': order_result.get('simulation', False),
            'reason': order_data.get('reason', 'signal'),
            'signal_data': convert_floats_to_decimal(order_data.get('signal_data', {})),
            'status': 'completed' if order_result['success'] else 'failed'
        }
        
        self.trades_table.put_item(Item=trade_record)
        logger.info(f"거래 기록 저장: {trade_record['trade_id']}")
        
    except Exception as e:
        logger.error(f"거래 기록 저장 실패: {e}")
'''
        
        print("📝 Lambda 함수 수정 사항:")
        print("="*60)
        print("1. Decimal 변환 헬퍼 함수 추가")
        print("2. save_trade_record 메서드에서 Decimal 타입 사용") 
        print("3. update_position_record 메서드에서 Decimal 타입 사용")
        print("="*60)
        
        print("\n⚠️  수동 수정 필요:")
        print("lambda_trade_execution_phase6.py 파일에서 다음 수정 필요:")
        print("- from decimal import Decimal (import 추가)")
        print("- convert_floats_to_decimal 함수 추가")
        print("- save_trade_record와 update_position_record에서 Decimal 사용")
        
        return True
        
    except Exception as e:
        print(f"Lambda 함수 수정 실패: {e}")
        return False

def test_decimal_conversion():
    """Decimal 변환 테스트"""
    
    from decimal import Decimal
    
    # 테스트 데이터
    test_data = {
        'price': 90000000.0,
        'quantity': 0.001,
        'amount': 90000.0,
        'nested': {
            'value': 123.456
        }
    }
    
    def convert_floats_to_decimal(obj):
        """Python float를 DynamoDB Decimal로 변환"""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_floats_to_decimal(item) for item in obj]
        return obj
    
    converted = convert_floats_to_decimal(test_data)
    
    print("🧪 Decimal 변환 테스트:")
    print("-" * 40)
    print("Original:", test_data)
    print("Converted:", converted)
    print("-" * 40)
    
    # 타입 확인
    for key, value in converted.items():
        print(f"{key}: {type(value)} = {value}")
        if isinstance(value, dict):
            for k, v in value.items():
                print(f"  {k}: {type(v)} = {v}")

def main():
    """메인 실행"""
    
    print("🔧 DynamoDB Decimal 타입 수정 유틸리티")
    print("="*50)
    
    # 1. Decimal 변환 테스트
    test_decimal_conversion()
    
    print("\n" + "="*50)
    
    # 2. Lambda 함수 수정 안내
    fix_lambda_function()
    
    print("""
📋 수정 완료 후 다음 단계:
1. Lambda 함수 재배포
2. Phase 6 테스트 재실행
3. DynamoDB 테이블 데이터 확인

💡 Decimal 사용 이유:
- DynamoDB는 IEEE 754 float를 지원하지 않음
- 금융 데이터의 정확성을 위해 Decimal 타입 필수
- Decimal('123.45')로 변환하여 정밀도 보장
    """)

if __name__ == '__main__':
    main()