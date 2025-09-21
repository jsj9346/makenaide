#!/usr/bin/env python3
"""
의존성 및 Import 호환성 검증
trading_engine.py의 모든 import가 makenaide.py 환경에서 작동하는지 테스트
"""

import sys
import importlib
import traceback
from typing import List, Dict, Tuple

class DependencyChecker:
    """의존성 검증 도구"""

    def __init__(self):
        self.import_results = {}

    def extract_imports_from_trading_engine(self) -> List[str]:
        """trading_engine.py에서 사용하는 import 목록 추출"""

        # trading_engine.py의 실제 import 문들 (파일 분석 결과)
        imports_to_check = [
            # 표준 라이브러리
            'datetime',
            'logging',
            'sqlite3',
            'time',
            'json',
            'argparse',

            # 데이터 처리
            'dataclasses',
            'enum',

            # 타입 힌트
            'typing',

            # 외부 라이브러리
            'pyupbit',

            # 프로젝트 내부 모듈
            'config'  # makenaide.py에서 사용하는 config
        ]

        return imports_to_check

    def test_single_import(self, module_name: str) -> Tuple[bool, str]:
        """단일 모듈 import 테스트"""

        try:
            # 표준 라이브러리나 외부 패키지
            if module_name in ['datetime', 'logging', 'sqlite3', 'time', 'json',
                              'argparse', 'dataclasses', 'enum', 'typing']:
                importlib.import_module(module_name)
                return True, "표준 라이브러리 - OK"

            elif module_name == 'pyupbit':
                import pyupbit
                # 간단한 기능 테스트
                tickers = pyupbit.get_tickers(fiat="KRW")
                if tickers and len(tickers) > 0:
                    return True, f"pyupbit - OK (종목 {len(tickers)}개 확인)"
                else:
                    return False, "pyupbit - 종목 조회 실패"

            elif module_name == 'config':
                # config 모듈이 makenaide.py 환경에서 사용 가능한지 확인
                # 실제로는 makenaide.py에서 config를 어떻게 로드하는지 확인해야 함
                try:
                    import config
                    return True, "config 모듈 - OK"
                except ImportError:
                    # config.py 파일이 있는지 확인
                    import os
                    if os.path.exists('config.py'):
                        return True, "config.py 파일 존재 - OK"
                    elif os.path.exists('config.json'):
                        return True, "config.json 파일 존재 - OK"
                    else:
                        return False, "config 모듈/파일 없음"

            else:
                importlib.import_module(module_name)
                return True, "기타 모듈 - OK"

        except ImportError as e:
            return False, f"ImportError: {str(e)}"
        except Exception as e:
            return False, f"기타 오류: {str(e)}"

    def test_trading_engine_import(self) -> Tuple[bool, str]:
        """trading_engine.py 자체를 import 할 수 있는지 테스트"""

        try:
            # 현재 디렉토리를 Python path에 추가
            if '.' not in sys.path:
                sys.path.insert(0, '.')

            # trading_engine 모듈 import 시도
            import trading_engine

            # 주요 클래스들이 정상적으로 import 되는지 확인
            required_classes = [
                'LocalTradingEngine',
                'TradingConfig',
                'OrderStatus',
                'TradeResult'
            ]

            missing_classes = []
            for class_name in required_classes:
                if not hasattr(trading_engine, class_name):
                    missing_classes.append(class_name)

            if missing_classes:
                return False, f"클래스 누락: {', '.join(missing_classes)}"

            # TradingConfig 인스턴스 생성 테스트
            config = trading_engine.TradingConfig()
            if not hasattr(config, 'take_profit_percent'):
                return False, "TradingConfig 속성 누락"

            return True, "trading_engine 모듈 완전 import 성공"

        except ImportError as e:
            return False, f"trading_engine ImportError: {str(e)}"
        except Exception as e:
            return False, f"trading_engine 기타 오류: {str(e)}"

    def test_makenaide_integration(self) -> Tuple[bool, str]:
        """makenaide.py에서 trading_engine import가 정상 작동하는지 테스트"""

        try:
            # makenaide.py에서 사용하는 import 문 시뮬레이션
            from trading_engine import LocalTradingEngine, TradingConfig, OrderStatus, TradeResult

            # 간단한 객체 생성 테스트
            config = TradingConfig(take_profit_percent=0)
            engine = LocalTradingEngine(config, dry_run=True)

            # 기본 메서드들이 존재하는지 확인
            required_methods = [
                'get_current_positions',
                'process_portfolio_management',
                'execute_buy_order',
                'execute_sell_order'
            ]

            missing_methods = []
            for method_name in required_methods:
                if not hasattr(engine, method_name):
                    missing_methods.append(method_name)

            if missing_methods:
                return False, f"필수 메서드 누락: {', '.join(missing_methods)}"

            return True, "makenaide.py 통합 import 성공"

        except ImportError as e:
            return False, f"통합 ImportError: {str(e)}"
        except Exception as e:
            return False, f"통합 기타 오류: {str(e)}"

    def run_dependency_check(self) -> bool:
        """전체 의존성 검사 실행"""

        print("=" * 80)
        print("🔍 의존성 및 Import 호환성 검증")
        print("=" * 80)

        # 1. 개별 모듈 import 테스트
        print("\n📦 개별 모듈 의존성 검사:")

        imports_to_check = self.extract_imports_from_trading_engine()
        all_passed = True

        for module_name in imports_to_check:
            success, message = self.test_single_import(module_name)
            status_icon = "✅" if success else "❌"
            print(f"   {status_icon} {module_name:15} - {message}")

            self.import_results[module_name] = {'success': success, 'message': message}

            if not success:
                all_passed = False

        # 2. trading_engine 모듈 자체 import 테스트
        print(f"\n🔧 trading_engine.py 모듈 Import 테스트:")

        te_success, te_message = self.test_trading_engine_import()
        status_icon = "✅" if te_success else "❌"
        print(f"   {status_icon} trading_engine - {te_message}")

        if not te_success:
            all_passed = False

        # 3. makenaide.py 통합 테스트
        print(f"\n🔗 makenaide.py 통합 Import 테스트:")

        integration_success, integration_message = self.test_makenaide_integration()
        status_icon = "✅" if integration_success else "❌"
        print(f"   {status_icon} makenaide 통합 - {integration_message}")

        if not integration_success:
            all_passed = False

        # 4. 결과 요약
        print(f"\n" + "=" * 80)
        print("📊 의존성 검사 결과")
        print("=" * 80)

        if all_passed:
            print("🎉 모든 의존성 검사 통과!")
            print("   trading_engine.py가 makenaide.py 환경에서 정상 작동할 것으로 예상됩니다.")
            return True
        else:
            failed_modules = [name for name, result in self.import_results.items()
                            if not result['success']]

            print(f"🚨 의존성 오류 발견:")
            print(f"   실패한 모듈: {len(failed_modules)}개")

            for module_name in failed_modules:
                result = self.import_results[module_name]
                print(f"   ❌ {module_name}: {result['message']}")

            if not te_success:
                print(f"   ❌ trading_engine: {te_message}")

            if not integration_success:
                print(f"   ❌ makenaide 통합: {integration_message}")

            print(f"\n💡 해결 방법:")
            print(f"   1. 누락된 패키지 설치: pip install [패키지명]")
            print(f"   2. Python path 설정 확인")
            print(f"   3. trading_engine.py 파일 경로 확인")

            return False

if __name__ == "__main__":
    checker = DependencyChecker()
    success = checker.run_dependency_check()
    sys.exit(0 if success else 1)