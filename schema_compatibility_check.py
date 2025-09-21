#!/usr/bin/env python3
"""
SQLite 스키마 호환성 검증 도구
trading_engine.py가 기대하는 테이블/컬럼과 실제 makenaide_local.db 스키마를 비교
"""

import sqlite3
import sys
from typing import Dict, List, Set, Tuple

class SchemaCompatibilityChecker:
    def __init__(self, db_path: str = 'makenaide_local.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def get_existing_tables(self) -> Dict[str, List[str]]:
        """실제 DB의 테이블과 컬럼 구조 조회"""
        tables = {}

        # 모든 테이블 목록 조회
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = [row[0] for row in self.cursor.fetchall()]

        # 각 테이블의 컬럼 정보 조회
        for table_name in table_names:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in self.cursor.fetchall()]  # row[1]은 컬럼명
            tables[table_name] = columns

        return tables

    def get_required_schema(self) -> Dict[str, List[str]]:
        """trading_engine.py가 기대하는 스키마 정의"""
        return {
            # trades 테이블 - INSERT 쿼리에서 추출
            'trades': [
                'ticker',
                'order_type',
                'status',
                'order_id',
                'quantity',
                'price',
                'amount_krw',
                'fee',
                'error_message',
                'timestamp',
                'created_at'  # SELECT에서 사용
            ],

            # technical_analysis 테이블 - SELECT 쿼리에서 추출
            'technical_analysis': [
                'atr',
                'supertrend',
                'macd_histogram',
                'support_level',
                'adx'
            ],

            # gpt_analysis 테이블 - SELECT 쿼리에서 추출
            'gpt_analysis': [
                'analysis_result'
            ]
        }

    def check_table_compatibility(self, required_tables: Dict[str, List[str]],
                                existing_tables: Dict[str, List[str]]) -> Dict[str, Dict]:
        """테이블별 호환성 검사"""

        compatibility_report = {}

        for table_name, required_columns in required_tables.items():
            report = {
                'exists': table_name in existing_tables,
                'missing_columns': [],
                'extra_columns': [],
                'status': 'OK'
            }

            if not report['exists']:
                report['status'] = 'CRITICAL'
                report['error'] = f"테이블 '{table_name}'이 존재하지 않음"
            else:
                existing_columns = existing_tables[table_name]
                existing_set = set(existing_columns)
                required_set = set(required_columns)

                # 누락된 컬럼
                report['missing_columns'] = list(required_set - existing_set)

                # 추가 컬럼 (문제없음, 정보용)
                report['extra_columns'] = list(existing_set - required_set)

                if report['missing_columns']:
                    report['status'] = 'CRITICAL'
                    report['error'] = f"필수 컬럼 누락: {', '.join(report['missing_columns'])}"
                elif len(report['extra_columns']) > 10:
                    report['status'] = 'WARNING'
                    report['warning'] = "매우 많은 추가 컬럼이 있음"

            compatibility_report[table_name] = report

        return compatibility_report

    def generate_schema_fix_sql(self, compatibility_report: Dict[str, Dict]) -> List[str]:
        """스키마 수정을 위한 SQL 문 생성"""
        fix_sqls = []

        for table_name, report in compatibility_report.items():
            if not report['exists']:
                # 테이블이 없는 경우 - 기본 스키마로 생성
                if table_name == 'trades':
                    sql = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    order_id TEXT,
    quantity REAL,
    price REAL,
    amount_krw REAL,
    fee REAL,
    error_message TEXT,
    timestamp TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
                elif table_name == 'technical_analysis':
                    sql = """
-- technical_analysis 테이블은 이미 존재해야 함
-- 누락된 컬럼들을 추가해야 함
"""
                elif table_name == 'gpt_analysis':
                    sql = """
-- gpt_analysis 테이블은 이미 존재해야 함
-- 누락된 컬럼들을 추가해야 함
"""

                fix_sqls.append(sql.strip())

            elif report['missing_columns']:
                # 누락된 컬럼 추가
                for column in report['missing_columns']:
                    if table_name == 'trades':
                        if column == 'amount_krw':
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} REAL;"
                        elif column == 'fee':
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} REAL;"
                        elif column in ['ticker', 'order_type', 'status', 'order_id', 'error_message']:
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"
                        elif column in ['quantity', 'price']:
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} REAL;"
                        elif column == 'timestamp':
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"
                        else:
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"

                    elif table_name == 'technical_analysis':
                        if column in ['atr', 'adx']:
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} REAL;"
                        else:
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"

                    elif table_name == 'gpt_analysis':
                        sql = f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"

                    fix_sqls.append(sql)

        return fix_sqls

    def run_compatibility_check(self) -> bool:
        """전체 호환성 검사 실행"""

        print("=" * 80)
        print("🔍 SQLite 스키마 호환성 검증")
        print("=" * 80)

        # 1. 기존 스키마 조회
        print("\n📋 현재 데이터베이스 스키마 조회 중...")
        existing_tables = self.get_existing_tables()
        print(f"   발견된 테이블: {len(existing_tables)}개")

        # 2. 요구사항 정의
        print("\n📝 trading_engine.py 요구사항 분석 중...")
        required_tables = self.get_required_schema()
        print(f"   필요한 테이블: {len(required_tables)}개")

        # 3. 호환성 검사
        print("\n🔍 호환성 검사 실행 중...")
        compatibility_report = self.check_table_compatibility(required_tables, existing_tables)

        # 4. 결과 출력
        print("\n" + "=" * 80)
        print("📊 호환성 검사 결과")
        print("=" * 80)

        critical_issues = 0
        warning_issues = 0

        for table_name, report in compatibility_report.items():
            status_icon = "✅" if report['status'] == 'OK' else "⚠️" if report['status'] == 'WARNING' else "🔴"
            print(f"\n{status_icon} 테이블: {table_name}")
            print(f"   존재 여부: {'✅ 존재' if report['exists'] else '❌ 없음'}")

            if report['missing_columns']:
                print(f"   누락된 컬럼: {', '.join(report['missing_columns'])}")
                critical_issues += len(report['missing_columns'])

            if report['extra_columns'] and len(report['extra_columns']) <= 5:
                print(f"   추가 컬럼: {', '.join(report['extra_columns'][:5])}{'...' if len(report['extra_columns']) > 5 else ''}")
            elif len(report['extra_columns']) > 5:
                print(f"   추가 컬럼: {len(report['extra_columns'])}개 (정상)")

            if 'error' in report:
                print(f"   🚨 오류: {report['error']}")
                critical_issues += 1

            if 'warning' in report:
                print(f"   ⚠️ 경고: {report['warning']}")
                warning_issues += 1

        # 5. 수정 사항 제안
        if critical_issues > 0:
            print(f"\n🚨 CRITICAL 이슈: {critical_issues}개 발견")
            print("   다음 SQL로 수정 가능:")

            fix_sqls = self.generate_schema_fix_sql(compatibility_report)
            for i, sql in enumerate(fix_sqls, 1):
                print(f"\n   {i}. {sql}")

            # SQL 파일로 저장
            with open('schema_fix.sql', 'w') as f:
                f.write('\n\n'.join(fix_sqls))
            print(f"\n💾 수정 SQL이 'schema_fix.sql' 파일에 저장되었습니다.")

            return False

        elif warning_issues > 0:
            print(f"\n⚠️ WARNING 이슈: {warning_issues}개 발견 (운영에는 문제없음)")
            return True

        else:
            print(f"\n🎉 모든 스키마 호환성 검사 통과!")
            print("   trading_engine.py가 정상적으로 작동할 것으로 예상됩니다.")
            return True

    def close(self):
        """DB 연결 종료"""
        self.conn.close()

if __name__ == "__main__":
    checker = SchemaCompatibilityChecker()
    try:
        success = checker.run_compatibility_check()
        sys.exit(0 if success else 1)
    finally:
        checker.close()