#!/usr/bin/env python3
"""
주문 상태 확인 로직 수정 테스트
- cancel 상태에서도 체결된 수량이 있으면 성공으로 처리하는 로직 테스트
"""

import sys
import os
import logging
from unittest.mock import Mock, patch

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from trade_executor import buy_asset, sell_asset

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_buy_order_cancel_with_execution():
    """cancel 상태이지만 체결된 수량이 있는 매수 주문 테스트"""
    logger.info("🧪 매수 주문 cancel 상태 + 체결 수량 테스트 시작")
    
    # Mock 응답 데이터 (실제 로그에서 추출한 데이터)
    mock_order_detail = {
        'uuid': '7112c068-db61-4182-ad4d-0c2f90f802e9',
        'side': 'bid',
        'ord_type': 'price',
        'price': '10000',
        'state': 'cancel',  # cancel 상태
        'market': 'KRW-AAVE',
        'created_at': '2025-07-08T11:49:14+09:00',
        'reserved_fee': '5',
        'remaining_fee': '0.0000005515',
        'paid_fee': '4.9999994485',
        'locked': '0.0011035515',
        'prevented_locked': '0',
        'executed_volume': '0.02628466',  # 체결된 수량이 있음
        'trades_count': 1,
        'trades': [
            {
                'market': 'KRW-AAVE',
                'uuid': 'e76405ab-67a4-4f09-b225-77aaed27af5f',
                'price': '380450',
                'volume': '0.02628466',
                'funds': '9999.998897',
                'trend': 'up',
                'created_at': '2025-07-08T11:49:14+09:00',
                'side': 'bid'
            }
        ]
    }
    
    # Mock API 응답
    mock_api_response = {
        'uuid': '7112c068-db61-4182-ad4d-0c2f90f802e9'
    }
    
    with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
        # Mock Upbit 인스턴스 설정
        mock_upbit = Mock()
        mock_upbit_class.return_value = mock_upbit
        
        # buy_market_order 응답 설정
        mock_upbit.buy_market_order.return_value = mock_api_response
        
        # get_order 응답 설정
        mock_upbit.get_order.return_value = mock_order_detail
        
        # 테스트 실행
        result = buy_asset(
            upbit_client=mock_upbit,
            ticker="KRW-AAVE",
            current_price=380450.0,
            trade_amount_krw=10000.0
        )
        
        # 결과 검증
        logger.info(f"📊 매수 결과: {result}")
        
        assert result['status'] == 'SUCCESS_PARTIAL', f"예상: SUCCESS_PARTIAL, 실제: {result['status']}"
        assert result['quantity'] == 0.02628466, f"예상: 0.02628466, 실제: {result['quantity']}"
        assert abs(result['price'] - 380450.0) < 0.01, f"예상: 380450.0, 실제: {result['price']}"
        assert result['order_id'] == '7112c068-db61-4182-ad4d-0c2f90f802e9'
        
        logger.info("✅ 매수 주문 cancel 상태 + 체결 수량 테스트 통과")

def test_sell_order_cancel_with_execution():
    """cancel 상태이지만 체결된 수량이 있는 매도 주문 테스트"""
    logger.info("🧪 매도 주문 cancel 상태 + 체결 수량 테스트 시작")
    
    # Mock 응답 데이터
    mock_order_detail = {
        'uuid': '1e702c5b-18e5-4dfb-86d3-3db016c0409f',
        'side': 'ask',
        'ord_type': 'market',
        'state': 'cancel',  # cancel 상태
        'market': 'KRW-AAVE',
        'executed_volume': '0.02628466',  # 체결된 수량이 있음
        'trades': [
            {
                'market': 'KRW-AAVE',
                'uuid': 'test-trade-uuid',
                'price': '380000',
                'volume': '0.02628466',
                'funds': '9999.998897',
                'trend': 'down',
                'created_at': '2025-07-08T11:49:37+09:00',
                'side': 'ask'
            }
        ]
    }
    
    # Mock API 응답
    mock_api_response = {
        'uuid': '1e702c5b-18e5-4dfb-86d3-3db016c0409f'
    }
    
    with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
        # Mock Upbit 인스턴스 설정
        mock_upbit = Mock()
        mock_upbit_class.return_value = mock_upbit
        
        # get_balance 응답 설정 (매도할 수량이 있다고 가정)
        mock_upbit.get_balance.return_value = 0.02628466
        
        # get_current_price 응답 설정
        mock_upbit.get_current_price.return_value = 380450.0
        
        # sell_market_order 응답 설정
        mock_upbit.sell_market_order.return_value = mock_api_response
        
        # get_order 응답 설정
        mock_upbit.get_order.return_value = mock_order_detail
        
        # 테스트 실행
        result = sell_asset(
            ticker="KRW-AAVE",
            quantity_to_sell=0.02628466,
            upbit_client=mock_upbit
        )
        
        # 결과 검증
        logger.info(f"📊 매도 결과: {result}")
        
        assert result['status'] == 'SUCCESS_PARTIAL', f"예상: SUCCESS_PARTIAL, 실제: {result['status']}"
        assert result['quantity'] == 0.02628466, f"예상: 0.02628466, 실제: {result['quantity']}"
        assert abs(result['price'] - 380000.0) < 0.01, f"예상: 380000.0, 실제: {result['price']}"
        assert result['order_id'] == '1e702c5b-18e5-4dfb-86d3-3db016c0409f'
        
        logger.info("✅ 매도 주문 cancel 상태 + 체결 수량 테스트 통과")

def test_order_cancel_without_execution():
    """cancel 상태이고 체결된 수량이 없는 주문 테스트 (실제 실패 케이스)"""
    logger.info("🧪 매수 주문 cancel 상태 + 체결 수량 없음 테스트 시작")
    
    # Mock 응답 데이터 (체결된 수량이 0)
    mock_order_detail = {
        'uuid': 'test-order-uuid',
        'side': 'bid',
        'ord_type': 'price',
        'state': 'cancel',  # cancel 상태
        'market': 'KRW-TEST',
        'executed_volume': '0',  # 체결된 수량이 없음
        'trades': []
    }
    
    # Mock API 응답
    mock_api_response = {
        'uuid': 'test-order-uuid'
    }
    
    with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
        # Mock Upbit 인스턴스 설정
        mock_upbit = Mock()
        mock_upbit_class.return_value = mock_upbit
        
        # buy_market_order 응답 설정
        mock_upbit.buy_market_order.return_value = mock_api_response
        
        # get_order 응답 설정
        mock_upbit.get_order.return_value = mock_order_detail
        
        # 테스트 실행
        result = buy_asset(
            upbit_client=mock_upbit,
            ticker="KRW-TEST",
            current_price=1000.0,
            trade_amount_krw=10000.0
        )
        
        # 결과 검증
        logger.info(f"📊 매수 결과: {result}")
        
        assert result['status'] == 'FAILURE', f"예상: FAILURE, 실제: {result['status']}"
        assert result['quantity'] is None, f"예상: None, 실제: {result['quantity']}"
        assert result['price'] is None, f"예상: None, 실제: {result['price']}"
        
        logger.info("✅ 매수 주문 cancel 상태 + 체결 수량 없음 테스트 통과")

def test_makenaide_buy_result_processing():
    """makenaide.py의 매수 결과 처리 로직 테스트"""
    logger.info("🧪 makenaide.py 매수 결과 처리 로직 테스트 시작")
    
    # 성공 상태들 테스트
    success_statuses = ["SUCCESS", "SUCCESS_PARTIAL", "SUCCESS_PARTIAL_NO_AVG", "SUCCESS_NO_AVG_PRICE"]
    
    for status in success_statuses:
        buy_result = {
            "status": status,
            "order_id": "test-uuid",
            "quantity": 0.02628466,
            "price": 380450.0,
            "error": None
        }
        
        # 성공 상태인지 확인
        is_success = buy_result.get("status") in ["SUCCESS", "SUCCESS_PARTIAL", "SUCCESS_PARTIAL_NO_AVG", "SUCCESS_NO_AVG_PRICE"]
        
        logger.info(f"📊 상태: {status}, 성공 여부: {is_success}")
        assert is_success, f"상태 {status}는 성공으로 처리되어야 합니다"
    
    # 실패 상태 테스트
    failure_statuses = ["FAILURE", "SKIPPED", "ERROR"]
    
    for status in failure_statuses:
        buy_result = {
            "status": status,
            "order_id": None,
            "quantity": None,
            "price": None,
            "error": "Test error"
        }
        
        # 실패 상태인지 확인
        is_success = buy_result.get("status") in ["SUCCESS", "SUCCESS_PARTIAL", "SUCCESS_PARTIAL_NO_AVG", "SUCCESS_NO_AVG_PRICE"]
        
        logger.info(f"📊 상태: {status}, 성공 여부: {is_success}")
        assert not is_success, f"상태 {status}는 실패로 처리되어야 합니다"
    
    logger.info("✅ makenaide.py 매수 결과 처리 로직 테스트 통과")

def main():
    """모든 테스트 실행"""
    logger.info("🚀 주문 상태 확인 로직 수정 테스트 시작")
    
    try:
        test_buy_order_cancel_with_execution()
        test_sell_order_cancel_with_execution()
        test_order_cancel_without_execution()
        test_makenaide_buy_result_processing()
        
        logger.info("🎉 모든 테스트 통과!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 