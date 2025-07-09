#!/usr/bin/env python3
"""
=== PostgreSQL 데이터베이스 완전 삭제 스크립트 ===

이 스크립트는 Makenaide 프로젝트의 모든 PostgreSQL 테이블, 인덱스, 시퀀스를 안전하게 삭제합니다.

주요 기능:
- 모든 테이블 및 백업 테이블 삭제
- 의존성 순서를 고려한 안전한 삭제
- 삭제 전 백업 생성 옵션
- 상세한 로깅 및 진행 상황 표시
- 다중 안전장치 (확인 프롬프트)

사용법:
    python drop_all_schemas.py [--backup] [--force]
    
옵션:
    --backup: 삭제 전 SQL 덤프 백업 생성
    --force: 확인 프롬프트 생략 (자동화용)
"""

import psycopg2
from psycopg2 import sql
import os
import sys
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
import subprocess
from typing import List, Tuple, Dict, Optional

# 로깅 설정
# 로그 디렉토리 생성
log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{log_dir}/drop_schemas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseDropper:
    """PostgreSQL 데이터베이스 스키마 완전 삭제 클래스"""
    
    # 삭제 대상 테이블 목록 (의존성 순서 고려)
    TABLES_TO_DROP = [
        # 의존성이 있는 테이블들을 먼저 삭제
        'performance_summary',
        'trade_log',
        'strategy_performance', 
        'trend_analysis_log',
        'trend_analysis',
        'portfolio_history',
        'trailing_stops',
        'market_data_4h',
        'market_data',
        'ohlcv_4h',
        'ohlcv',
        'static_indicators',
        'tickers',
    ]
    
    def __init__(self):
        """환경변수 로드 및 데이터베이스 연결 정보 설정"""
        load_dotenv()
        
        self.db_config = {
            'host': os.getenv("PG_HOST"),
            'port': os.getenv("PG_PORT"),
            'dbname': os.getenv("PG_DATABASE"),
            'user': os.getenv("PG_USER"),
            'password': os.getenv("PG_PASSWORD")
        }
        
        # 필수 환경변수 검증
        missing_vars = [k for k, v in self.db_config.items() if not v]
        if missing_vars:
            raise ValueError(f"다음 환경변수가 누락되었습니다: {missing_vars}")
        
        self.conn = None
        self.backup_file = None
    
    def connect_database(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            logger.info("✅ 데이터베이스 연결 성공")
            return True
        except Exception as e:
            logger.error(f"❌ 데이터베이스 연결 실패: {e}")
            return False
    
    def get_current_schema_info(self) -> Dict[str, any]:
        """현재 스키마 상태 조사"""
        logger.info("📊 현재 데이터베이스 스키마 상태 조사 중...")
        
        schema_info = {
            'tables': {},
            'indexes': [],
            'sequences': [],
            'total_size': 0,
            'backup_tables': []
        }
        
        try:
            with self.conn.cursor() as cur:
                # 테이블 목록 및 레코드 수 조회
                cur.execute("""
                    SELECT 
                        table_name,
                        pg_total_relation_size(quote_ident(table_name)::regclass) as size_bytes
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """)
                
                tables = cur.fetchall()
                total_records = 0
                
                for table_name, size_bytes in tables:
                    # 각 테이블의 레코드 수 조회
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                        record_count = cur.fetchone()[0]
                        total_records += record_count
                    except:
                        record_count = 0
                    
                    schema_info['tables'][table_name] = {
                        'records': record_count,
                        'size_bytes': size_bytes or 0
                    }
                    
                    # 백업 테이블 식별
                    if '_backup_' in table_name or table_name.endswith('_backup'):
                        schema_info['backup_tables'].append(table_name)
                
                # 인덱스 목록 조회
                cur.execute("""
                    SELECT indexname, tablename 
                    FROM pg_indexes 
                    WHERE schemaname = 'public'
                    AND indexname NOT LIKE '%_pkey';
                """)
                schema_info['indexes'] = cur.fetchall()
                
                # 시퀀스 목록 조회
                cur.execute("""
                    SELECT sequence_name 
                    FROM information_schema.sequences 
                    WHERE sequence_schema = 'public';
                """)
                schema_info['sequences'] = [row[0] for row in cur.fetchall()]
                
                # 전체 데이터베이스 크기
                cur.execute(f"SELECT pg_database_size('{self.db_config['dbname']}')")
                schema_info['total_size'] = cur.fetchone()[0]
                
                logger.info(f"📋 발견된 테이블: {len(schema_info['tables'])}개")
                logger.info(f"📋 총 레코드 수: {total_records:,}개")
                logger.info(f"📋 인덱스: {len(schema_info['indexes'])}개")
                logger.info(f"📋 시퀀스: {len(schema_info['sequences'])}개")
                logger.info(f"📋 백업 테이블: {len(schema_info['backup_tables'])}개")
                logger.info(f"📋 전체 DB 크기: {schema_info['total_size'] / 1024 / 1024:.2f} MB")
                
        except Exception as e:
            logger.error(f"❌ 스키마 정보 조회 실패: {e}")
            
        return schema_info
    
    def print_schema_summary(self, schema_info: Dict[str, any]):
        """스키마 요약 정보 출력"""
        print("\n" + "="*60)
        print("📊 현재 데이터베이스 상태")
        print("="*60)
        
        if schema_info['tables']:
            print("\n📋 테이블별 상세 정보:")
            for table_name, info in sorted(schema_info['tables'].items()):
                size_mb = info['size_bytes'] / 1024 / 1024
                print(f"  • {table_name:<25} | {info['records']:>8,} 레코드 | {size_mb:>6.2f} MB")
        
        if schema_info['backup_tables']:
            print(f"\n🗃️  백업 테이블 ({len(schema_info['backup_tables'])}개):")
            for table in schema_info['backup_tables']:
                print(f"  • {table}")
        
        if schema_info['indexes']:
            print(f"\n📇 인덱스 ({len(schema_info['indexes'])}개)")
        
        if schema_info['sequences']:
            print(f"\n🔢 시퀀스 ({len(schema_info['sequences'])}개)")
        
        print(f"\n💾 전체 데이터베이스 크기: {schema_info['total_size'] / 1024 / 1024:.2f} MB")
        print("="*60)
    
    # def create_backup(self) -> bool:
    #     """전체 데이터베이스 SQL 덤프 백업 생성"""
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     self.backup_file = f"db_backup_before_drop_{timestamp}.sql"
        
    #     logger.info(f"💾 데이터베이스 백업 생성 중: {self.backup_file}")
        
    #     try:
    #         # pg_dump 명령어 구성
    #         cmd = [
    #             'pg_dump',
    #             '-h', self.db_config['host'],
    #             '-p', str(self.db_config['port']),
    #             '-U', self.db_config['user'],
    #             '-d', self.db_config['dbname'],
    #             '--no-password',
    #             '--verbose',
    #             '--clean',
    #             '--create',
    #             '--file', self.backup_file
    #         ]
            
    #         # 환경변수로 패스워드 설정
    #         env = os.environ.copy()
    #         env['PGPASSWORD'] = self.db_config['password']
            
    #         # 백업 실행
    #         result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
    #         if result.returncode == 0:
    #             # 백업 파일 크기 확인
    #             backup_size = os.path.getsize(self.backup_file) / 1024 / 1024
    #             logger.info(f"✅ 백업 완료: {self.backup_file} ({backup_size:.2f} MB)")
    #             return True
    #         else:
    #             logger.error(f"❌ 백업 실패: {result.stderr}")
    #             return False
                
    #     except FileNotFoundError:
    #         logger.error("❌ pg_dump 명령어를 찾을 수 없습니다. PostgreSQL 클라이언트가 설치되어 있는지 확인하세요.")
    #         return False
    #     except Exception as e:
    #         logger.error(f"❌ 백업 생성 중 오류: {e}")
    #         return False
    
    def get_user_confirmation(self, force: bool = False, truncate_only: bool = False) -> bool:
        """사용자 확인 입력 받기"""
        if force:
            logger.warning("🔥 --force 옵션으로 인해 확인 프롬프트를 생략합니다.")
            return True
        
        print("\n" + "⚠️ " * 20)
        if truncate_only:
            print("🚨 경고: 모든 테이블의 데이터가 삭제됩니다!")
            print("🚨 테이블 구조는 유지되지만 모든 데이터가 완전히 삭제됩니다.")
            confirm_text = "YES TRUNCATE ALL"
        else:
            print("🚨 경고: 이 작업은 되돌릴 수 없습니다!")
            print("🚨 모든 테이블, 데이터, 인덱스, 시퀀스가 완전히 삭제됩니다.")
            confirm_text = "y"
        print("⚠️ " * 20)
        
        print(f"\n계속하려면 정확히 '{confirm_text}'을 입력하세요:")
        print("(취소하려면 다른 값을 입력하거나 Ctrl+C를 누르세요)")
        
        try:
            user_input = input("\n입력: ").strip()
            if user_input == confirm_text:
                logger.info("✅ 사용자 확인 완료")
                return True
            else:
                logger.info("❌ 사용자가 취소했습니다.")
                return False
        except KeyboardInterrupt:
            logger.info("\n❌ 사용자가 중단했습니다.")
            return False
    
    def drop_all_tables(self, schema_info: Dict[str, any]) -> bool:
        """모든 테이블 삭제"""
        logger.info("🗑️  테이블 삭제 시작...")
        
        success_count = 0
        failed_tables = []
        
        try:
            with self.conn.cursor() as cur:
                # 1. 외래키 제약 조건 먼저 제거
                logger.info("🔗 외래키 제약 조건 제거 중...")
                self._drop_foreign_key_constraints(cur)
                
                # 2. 모든 테이블을 CASCADE 옵션으로 강제 삭제
                all_tables = list(schema_info['tables'].keys())
                logger.info(f"📋 삭제 대상 테이블: {len(all_tables)}개")
                
                for table in all_tables:
                    if self._drop_single_table(cur, table, cascade=True):
                        success_count += 1
                        logger.info(f"  ✅ 테이블 삭제됨: {table}")
                    else:
                        failed_tables.append(table)
                        logger.warning(f"  ❌ 테이블 삭제 실패: {table}")
                
                # 3. 삭제 후 남은 테이블 재확인 및 재삭제 시도
                remaining_tables = self._get_remaining_tables(cur)
                if remaining_tables:
                    logger.warning(f"🔄 남은 테이블 재삭제 시도: {remaining_tables}")
                    for table in remaining_tables:
                        if self._drop_single_table(cur, table, cascade=True):
                            success_count += 1
                            logger.info(f"  ✅ 재삭제 성공: {table}")
                        else:
                            failed_tables.append(table)
                            logger.error(f"  ❌ 재삭제 실패: {table}")
                
                # 4. 최종 강제 삭제 시도 (모든 의존성 무시)
                final_remaining = self._get_remaining_tables(cur)
                if final_remaining:
                    logger.warning(f"🔥 최종 강제 삭제 시도: {final_remaining}")
                    try:
                        # 모든 테이블을 한 번에 CASCADE로 삭제
                        tables_list = ', '.join(final_remaining)
                        cur.execute(f"DROP TABLE IF EXISTS {tables_list} CASCADE")
                        logger.info(f"  ✅ 최종 강제 삭제 성공: {len(final_remaining)}개 테이블")
                        success_count += len(final_remaining)
                    except Exception as e:
                        logger.error(f"  ❌ 최종 강제 삭제 실패: {e}")
                        failed_tables.extend(final_remaining)
                
                logger.info(f"✅ 테이블 삭제 완료: {success_count}개 성공")
                
                if failed_tables:
                    logger.warning(f"⚠️  삭제 실패한 테이블: {failed_tables}")
                    return len(failed_tables) == 0
                
                return True
                
        except Exception as e:
            logger.error(f"❌ 테이블 삭제 중 오류: {e}")
            return False
    
    def _drop_foreign_key_constraints(self, cursor):
        """모든 외래키 제약 조건 제거"""
        try:
            # 모든 외래키 제약 조건 조회
            cursor.execute("""
                SELECT 
                    tc.table_name, 
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public';
            """)
            
            foreign_keys = cursor.fetchall()
            
            for table_name, constraint_name in foreign_keys:
                try:
                    drop_fk_sql = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}"
                    cursor.execute(drop_fk_sql)
                    logger.info(f"  🔗 외래키 제약 조건 제거: {table_name}.{constraint_name}")
                except Exception as e:
                    logger.warning(f"  ⚠️  외래키 제약 조건 제거 실패: {table_name}.{constraint_name} - {e}")
                    
        except Exception as e:
            logger.warning(f"⚠️  외래키 제약 조건 조회/제거 중 오류: {e}")
    
    def _get_remaining_tables(self, cursor) -> List[str]:
        """남은 테이블 목록 조회"""
        try:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ 남은 테이블 조회 실패: {e}")
            return []
    
    def _drop_single_table(self, cursor, table_name: str, cascade: bool = False) -> bool:
        """단일 테이블 삭제"""
        try:
            cascade_sql = " CASCADE" if cascade else ""
            sql_query = f"DROP TABLE IF EXISTS {table_name}{cascade_sql}"
            
            cursor.execute(sql_query)
            return True
            
        except Exception as e:
            logger.error(f"  ❌ 테이블 삭제 실패 ({table_name}): {e}")
            return False
    
    def truncate_all_tables(self, schema_info: Dict[str, any]) -> bool:
        """모든 테이블의 데이터만 삭제 (구조 유지)"""
        logger.info("🗑️  테이블 데이터 삭제 시작 (구조 유지)...")
        
        success_count = 0
        failed_tables = []
        all_tables = list(schema_info['tables'].keys())
        
        try:
            with self.conn.cursor() as cur:
                # 1. 외래키 제약 조건이 있는 테이블들 먼저 처리
                logger.info("🔗 외래키 제약 조건 고려하여 순서 결정...")
                
                # 2. 모든 테이블 TRUNCATE (CASCADE 옵션으로)
                for table in all_tables:
                    try:
                        # RESTART IDENTITY로 시퀀스도 초기화, CASCADE로 의존성 해결
                        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                        success_count += 1
                        logger.info(f"  ✅ 테이블 데이터 삭제됨: {table}")
                    except Exception as e:
                        failed_tables.append(table)
                        logger.warning(f"  ❌ 테이블 데이터 삭제 실패: {table} - {e}")
                
                logger.info(f"✅ 테이블 데이터 삭제 완료: {success_count}개 성공")
                
                if failed_tables:
                    logger.warning(f"⚠️  데이터 삭제 실패한 테이블: {failed_tables}")
                
                return len(failed_tables) == 0
                
        except Exception as e:
            logger.error(f"❌ 테이블 데이터 삭제 중 오류: {e}")
            return False
    
    def drop_indexes_and_sequences(self, schema_info: Dict[str, any]) -> bool:
        """인덱스 및 시퀀스 삭제"""
        logger.info("🗑️  인덱스 및 시퀀스 삭제 중...")
        
        try:
            with self.conn.cursor() as cur:
                # 인덱스 삭제
                for index_name, table_name in schema_info['indexes']:
                    try:
                        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
                        logger.info(f"  ✅ 인덱스 삭제됨: {index_name}")
                    except Exception as e:
                        logger.warning(f"  ⚠️  인덱스 삭제 실패 ({index_name}): {e}")
                
                # 시퀀스 삭제
                for sequence_name in schema_info['sequences']:
                    try:
                        cur.execute(f"DROP SEQUENCE IF EXISTS {sequence_name}")
                        logger.info(f"  ✅ 시퀀스 삭제됨: {sequence_name}")
                    except Exception as e:
                        logger.warning(f"  ⚠️  시퀀스 삭제 실패 ({sequence_name}): {e}")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ 인덱스/시퀀스 삭제 중 오류: {e}")
            return False
    
    def verify_cleanup(self) -> Dict[str, any]:
        """삭제 후 상태 검증"""
        logger.info("🔍 삭제 후 상태 검증 중...")
        
        verification_result = {
            'remaining_tables': [],
            'remaining_indexes': [],
            'remaining_sequences': [],
            'current_size': 0,
            'cleanup_success': True
        }
        
        try:
            with self.conn.cursor() as cur:
                # 남은 테이블 확인
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                """)
                verification_result['remaining_tables'] = [row[0] for row in cur.fetchall()]
                
                # 남은 인덱스 확인
                cur.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE schemaname = 'public'
                    AND indexname NOT LIKE '%_pkey'
                """)
                verification_result['remaining_indexes'] = [row[0] for row in cur.fetchall()]
                
                # 남은 시퀀스 확인
                cur.execute("""
                    SELECT sequence_name 
                    FROM information_schema.sequences 
                    WHERE sequence_schema = 'public'
                """)
                verification_result['remaining_sequences'] = [row[0] for row in cur.fetchall()]
                
                # 현재 데이터베이스 크기
                cur.execute(f"SELECT pg_database_size('{self.db_config['dbname']}')")
                verification_result['current_size'] = cur.fetchone()[0]
                
                # 성공 여부 판단
                if (verification_result['remaining_tables'] or 
                    verification_result['remaining_indexes'] or 
                    verification_result['remaining_sequences']):
                    verification_result['cleanup_success'] = False
                
        except Exception as e:
            logger.error(f"❌ 검증 중 오류: {e}")
            verification_result['cleanup_success'] = False
        
        return verification_result
    
    def print_final_report(self, before_info: Dict[str, any], after_info: Dict[str, any]):
        """최종 리포트 출력"""
        print("\n" + "="*60)
        print("📊 삭제 작업 완료 리포트")
        print("="*60)
        
        # 삭제 전후 비교
        before_tables = len(before_info['tables'])
        after_tables = len(after_info['remaining_tables'])
        before_size = before_info['total_size'] / 1024 / 1024
        after_size = after_info['current_size'] / 1024 / 1024
        space_freed = before_size - after_size
        
        print(f"\n📋 테이블:")
        print(f"  • 삭제 전: {before_tables}개")
        print(f"  • 삭제 후: {after_tables}개")
        print(f"  • 삭제됨: {before_tables - after_tables}개")
        
        print(f"\n💾 데이터베이스 크기:")
        print(f"  • 삭제 전: {before_size:.2f} MB")
        print(f"  • 삭제 후: {after_size:.2f} MB")
        print(f"  • 확보 공간: {space_freed:.2f} MB")
        
        if after_info['remaining_tables']:
            print(f"\n⚠️  남은 테이블 ({len(after_info['remaining_tables'])}개):")
            for table in after_info['remaining_tables']:
                print(f"  • {table}")
        
        if after_info['remaining_indexes']:
            print(f"\n⚠️  남은 인덱스 ({len(after_info['remaining_indexes'])}개):")
            for index in after_info['remaining_indexes']:
                print(f"  • {index}")
        
        if after_info['remaining_sequences']:
            print(f"\n⚠️  남은 시퀀스 ({len(after_info['remaining_sequences'])}개):")
            for sequence in after_info['remaining_sequences']:
                print(f"  • {sequence}")
        
        # 백업 파일 정보
        if self.backup_file and os.path.exists(self.backup_file):
            backup_size = os.path.getsize(self.backup_file) / 1024 / 1024
            print(f"\n💾 백업 파일: {self.backup_file} ({backup_size:.2f} MB)")
        
        # 최종 상태
        if after_info['cleanup_success']:
            print(f"\n✅ 스키마 삭제 완료!")
        else:
            print(f"\n⚠️  일부 객체가 남아있습니다. 수동으로 확인이 필요합니다.")
        
        print("="*60)
    
    def close_connection(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 데이터베이스 연결 종료")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="PostgreSQL 데이터베이스 스키마 완전 삭제")
    parser.add_argument('--backup', action='store_true', help='삭제 전 백업 생성')
    parser.add_argument('--force', action='store_true', help='확인 프롬프트 생략')
    parser.add_argument('--truncate-only', action='store_true', help='테이블 구조는 유지하고 데이터만 삭제 (TRUNCATE)')
    
    args = parser.parse_args()
    
    dropper = DatabaseDropper()
    
    try:
        # 1. 데이터베이스 연결 테스트
        logger.info("🚀 PostgreSQL 스키마 삭제 스크립트 시작")
        if not dropper.connect_database():
            sys.exit(1)
        
        # 2. 현재 스키마 상태 조사
        before_info = dropper.get_current_schema_info()
        dropper.print_schema_summary(before_info)
        
        # 3. 삭제할 객체가 없으면 종료
        if not before_info['tables']:
            logger.info("✅ 삭제할 테이블이 없습니다.")
            sys.exit(0)
        
        # 4. 사용자 확인
        if not dropper.get_user_confirmation(args.force, args.truncate_only):
            sys.exit(0)
        
        # 5. 선택적 백업 생성
        if args.backup:
            if not dropper.create_backup():
                logger.error("❌ 백업 생성에 실패했습니다. 계속하시겠습니까? (y/N)")
                if input().lower() != 'y':
                    sys.exit(1)
        
        # 6. 삭제 작업 실행
        if args.truncate_only:
            logger.info("🗑️  데이터 삭제 시작 (테이블 구조 유지)...")
            dropper.truncate_all_tables(before_info)
        else:
            logger.info("🗑️  데이터베이스 스키마 완전 삭제 시작...")
            
            # 테이블 삭제
            if not dropper.drop_all_tables(before_info):
                logger.error("❌ 테이블 삭제 중 오류가 발생했습니다.")
            
            # 인덱스 및 시퀀스 삭제
            dropper.drop_indexes_and_sequences(before_info)
        
        # 7. 결과 검증 및 리포트
        after_info = dropper.verify_cleanup()
        dropper.print_final_report(before_info, after_info)
        
        if after_info['cleanup_success']:
            logger.info("✅ 모든 스키마 삭제가 성공적으로 완료되었습니다.")
            sys.exit(0)
        else:
            logger.warning("⚠️  일부 객체가 남아있습니다.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n❌ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        sys.exit(1)
    finally:
        dropper.close_connection()


if __name__ == "__main__":
    main() 