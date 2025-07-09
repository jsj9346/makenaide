# #!/usr/bin/env python3
# """
# 주문 상태 확인 로직 수정 테스트
# - cancel 상태에서도 체결된 수량이 있으면 성공으로 처리하는 로직 테스트
# """
# 
# import sys
# import os
# import logging
# from unittest.mock import Mock, patch
# 
# # 프로젝트 루트를 Python 경로에 추가
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# 
# from trade_executor import buy_asset, sell_asset
# 
# # 로깅 설정
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
# 
# def test_buy_order_cancel_with_execution():
#     """cancel 상태이지만 체결된 수량이 있는 매수 주문 테스트"""
#     logger.info("🧪 매수 주문 cancel 상태 + 체결 수량 테스트 시작")
#     
#     # Mock 응답 데이터 (실제 로그에서 추출한 데이터)
#     mock_order_detail = {
#         'uuid': '7112c068-db61-4182-ad4d-0c2f90f802e9',
#         'side': 'bid',
#         'ord_type': 'price',
#         'price': '10000',
#         'state': 'cancel',  # cancel 상태
#         'market': 'KRW-AAVE',
#         'created_at': '2025-07-08T11:49:14+09:00',
#         'reserved_fee': '5',
#         'remaining_fee': '0.0000005515',
#         'paid_fee': '4.9999994485',
#         'locked': '0.0011035515',
#         'prevented_locked': '0',
#         'executed_volume': '0.02628466',  # 체결된 수량이 있음
#         'trades_count': 1,
#         'trades': [
#             {
#                 'market': 'KRW-AAVE',
#                 'uuid': 'e76405ab-67a4-4f09-b225-77aaed27af5f',
#                 'price': '380450',
#                 'volume': '0.02628466',
#                 'funds': '9999.998897',
#                 'trend': 'up',
#                 'created_at': '2025-07-08T11:49:14+09:00',
#                 'side': 'bid'
#             }
#         ]
#     }
#     
#     # Mock API 응답
#     mock_api_response = {
#         'uuid': '7112c068-db61-4182-ad4d-0c2f90f802e9'
#     }
#     
#     with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
#         # Mock Upbit 인스턴스 설정
#         mock_upbit = Mock()
#         mock_upbit_class.return_value = mock_upbit
#         
#         # buy_market_order 응답 설정
#         mock_upbit.buy_market_order.return_value = mock_api_response
#         
#         # get_order 응답 설정
#         mock_upbit.get_order.return_value = mock_order_detail
#         
#         # 테스트 실행
#         result = buy_asset(
#             upbit_client=mock_upbit,
#             ticker="KRW-AAVE",
#             current_price=380450.0,
#             trade_amount_krw=10000.0
#         )
#         
#         # 결과 검증
#         logger.info(f"📊 매수 결과: {result}")
#         
#         assert result['status'] == 'SUCCESS_PARTIAL', f"예상: SUCCESS_PARTIAL, 실제: {result['status']}"
#         assert result['quantity'] == 0.02628466, f"예상: 0.02628466, 실제: {result['quantity']}"
#         assert abs(result['price'] - 380450.0) < 0.01, f"예상: 380450.0, 실제: {result['price']}"
#         assert result['order_id'] == '7112c068-db61-4182-ad4d-0c2f90f802e9'
#         
#         logger.info("✅ 매수 주문 cancel 상태 + 체결 수량 테스트 통과")
# 
# def test_sell_order_cancel_with_execution():
#     """cancel 상태이지만 체결된 수량이 있는 매도 주문 테스트"""
#     logger.info("🧪 매도 주문 cancel 상태 + 체결 수량 테스트 시작")
#     
#     # Mock 응답 데이터
#     mock_order_detail = {
#         'uuid': '1e702c5b-18e5-4dfb-86d3-3db016c0409f',
#         'side': 'ask',
#         'ord_type': 'market',
#         'state': 'cancel',  # cancel 상태
#         'market': 'KRW-AAVE',
#         'executed_volume': '0.02628466',  # 체결된 수량이 있음
#         'trades': [
#             {
#                 'market': 'KRW-AAVE',
#                 'uuid': 'test-trade-uuid',
#                 'price': '380000',
#                 'volume': '0.02628466',
#                 'funds': '9999.998897',
#                 'trend': 'down',
#                 'created_at': '2025-07-08T11:49:37+09:00',
#                 'side': 'ask'
#             }
#         ]
#     }
#     
#     # Mock API 응답
#     mock_api_response = {
#         'uuid': '1e702c5b-18e5-4dfb-86d3-3db016c0409f'
#     }
#     
#     with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
#         # Mock Upbit 인스턴스 설정
#         mock_upbit = Mock()
#         mock_upbit_class.return_value = mock_upbit
#         
#         # get_balance 응답 설정 (매도할 수량이 있다고 가정)
#         mock_upbit.get_balance.return_value = 0.02628466
#         
#         # get_current_price 응답 설정
#         mock_upbit.get_current_price.return_value = 380450.0
#         
#         # sell_market_order 응답 설정
#         mock_upbit.sell_market_order.return_value = mock_api_response
#         
#         # get_order 응답 설정
#         mock_upbit.get_order.return_value = mock_order_detail
#         
#         # 테스트 실행
#         result = sell_asset(
#             ticker="KRW-AAVE",
#             quantity_to_sell=0.02628466,
#             upbit_client=mock_upbit
#         )
#         
#         # 결과 검증
#         logger.info(f"📊 매도 결과: {result}")
#         
#         assert result['status'] == 'SUCCESS_PARTIAL', f"예상: SUCCESS_PARTIAL, 실제: {result['status']}"
#         assert result['quantity'] == 0.02628466, f"예상: 0.02628466, 실제: {result['quantity']}"
#         assert abs(result['price'] - 380000.0) < 0.01, f"예상: 380000.0, 실제: {result['price']}"
#         assert result['order_id'] == '1e702c5b-18e5-4dfb-86d3-3db016c0409f'
#         
#         logger.info("✅ 매도 주문 cancel 상태 + 체결 수량 테스트 통과")
# 
# def test_order_cancel_without_execution():
#     """cancel 상태이고 체결된 수량이 없는 주문 테스트 (실제 실패 케이스)"""
#     logger.info("🧪 매수 주문 cancel 상태 + 체결 수량 없음 테스트 시작")
#     
#     # Mock 응답 데이터 (체결된 수량이 0)
#     mock_order_detail = {
#         'uuid': 'test-order-uuid',
#         'side': 'bid',
#         'ord_type': 'price',
#         'state': 'cancel',  # cancel 상태
#         'market': 'KRW-TEST',
#         'executed_volume': '0',  # 체결된 수량이 없음
#         'trades': []
#     }
#     
#     # Mock API 응답
#     mock_api_response = {
#         'uuid': 'test-order-uuid'
#     }
#     
#     with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
#         # Mock Upbit 인스턴스 설정
#         mock_upbit = Mock()
#         mock_upbit_class.return_value = mock_upbit
#         
#         # buy_market_order 응답 설정
#         mock_upbit.buy_market_order.return_value = mock_api_response
#         
#         # get_order 응답 설정
#         mock_upbit.get_order.return_value = mock_order_detail
#         
#         # 테스트 실행
#         result = buy_asset(
#             upbit_client=mock_upbit,
#             ticker="KRW-TEST",
#             current_price=1000.0,
#             trade_amount_krw=10000.0
#         )
#         
#         # 결과 검증
#         logger.info(f"📊 매수 결과: {result}")
#         
#         assert result['status'] == 'FAILURE', f"예상: FAILURE, 실제: {result['status']}"
#         assert result['quantity'] is None, f"예상: None, 실제: {result['quantity']}"
#         assert result['price'] is None, f"예상: None, 실제: {result['price']}"
#         
#         logger.info("✅ 매수 주문 cancel 상태 + 체결 수량 없음 테스트 통과")
# 
# def test_makenaide_buy_result_processing():
#     """Makenaide 봇의 매수 결과 처리 테스트"""
#     logger.info("🧪 Makenaide 매수 결과 처리 테스트 시작")
#     
#     # Mock 응답 데이터
#     mock_order_detail = {
#         'uuid': 'test-order-uuid',
#         'side': 'bid',
#         'ord_type': 'price',
#         'state': 'cancel',
#         'market': 'KRW-BTC',
#         'executed_volume': '0.001',
#         'trades': [
#             {
#                 'market': 'KRW-BTC',
#                 'uuid': 'test-trade-uuid',
#                 'price': '50000000',
#                 'volume': '0.001',
#                 'funds': '50000',
#                 'trend': 'up',
#                 'created_at': '2025-07-08T11:49:14+09:00',
#                 'side': 'bid'
#             }
#         ]
#     }
#     
#     # Mock API 응답
#     mock_api_response = {
#         'uuid': 'test-order-uuid'
#     }
#     
#     with patch('trade_executor.pyupbit.Upbit') as mock_upbit_class:
#         # Mock Upbit 인스턴스 설정
#         mock_upbit = Mock()
#         mock_upbit_class.return_value = mock_upbit
#         
#         # buy_market_order 응답 설정
#         mock_upbit.buy_market_order.return_value = mock_api_response
#         
#         # get_order 응답 설정
#         mock_upbit.get_order.return_value = mock_order_detail
#         
#         # 테스트 실행
#         result = buy_asset(
#             upbit_client=mock_upbit,
#             ticker="KRW-BTC",
#             current_price=50000000.0,
#             trade_amount_krw=50000.0
#         )
#         
#         # 결과 검증
#         logger.info(f"📊 매수 결과: {result}")
#         
#         assert result['status'] == 'SUCCESS_PARTIAL', f"예상: SUCCESS_PARTIAL, 실제: {result['status']}"
#         assert result['quantity'] == 0.001, f"예상: 0.001, 실제: {result['quantity']}"
#         assert abs(result['price'] - 50000000.0) < 0.01, f"예상: 50000000.0, 실제: {result['price']}"
#         
#         logger.info("✅ Makenaide 매수 결과 처리 테스트 통과")
# 
# def main():
#     """메인 테스트 실행 함수"""
#     logger.info("🚀 주문 상태 확인 로직 수정 테스트 시작")
#     
#     try:
#         # 각 테스트 함수 실행
#         test_buy_order_cancel_with_execution()
#         test_sell_order_cancel_with_execution()
#         test_order_cancel_without_execution()
#         test_makenaide_buy_result_processing()
#         
#         logger.info("✅ 모든 테스트가 성공적으로 완료되었습니다!")
#         
#     except Exception as e:
#         logger.error(f"❌ 테스트 중 오류 발생: {e}")
#         raise
# 
# 
# if __name__ == "__main__":
#     main() 