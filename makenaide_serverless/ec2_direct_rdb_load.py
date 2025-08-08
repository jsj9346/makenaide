#!/usr/bin/env python3
"""
EC2에서 직접 실행하는 RDB 적재 스크립트
Lambda 호환성 문제를 우회하여 직접 EC2에서 데이터 적재
"""

import boto3
import json
import subprocess
import time
from datetime import datetime

def deploy_ec2_rdb_script():
    """EC2에서 실행할 RDB 적재 스크립트 생성"""
    
    # EC2에서 실행할 Python 스크립트
    ec2_script = '''#!/usr/bin/env python3
"""
EC2에서 실행되는 Makenaide RDB 적재 스크립트
"""

import requests
import psycopg2
import os
import json
from datetime import datetime

def get_upbit_tickers():
    """Upbit 티커 목록 조회"""
    try:
        url = "https://api.upbit.com/v1/market/all"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        krw_tickers = [
            market['market'] for market in data 
            if market['market'].startswith('KRW-')
        ]
        
        print(f"✅ Upbit에서 {len(krw_tickers)}개 티커 조회 완료")
        return krw_tickers
        
    except Exception as e:
        print(f"❌ Upbit API 오류: {e}")
        return []

def load_tickers_to_db(tickers):
    """티커를 RDB에 적재"""
    try:
        # 환경변수에서 DB 정보 가져오기
        conn = psycopg2.connect(
            host=os.environ.get('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
            port=int(os.environ.get('PG_PORT', 5432)),
            database=os.environ.get('PG_DATABASE', 'makenaide'),
            user=os.environ.get('PG_USER', 'makenaide_user'),
            password=os.environ.get('PG_PASSWORD', 'your_secure_password_123!'),
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # 기존 티커 조회
        cursor.execute("SELECT ticker FROM tickers")
        existing_tickers = set(row[0] for row in cursor.fetchall())
        
        # 신규 티커만 삽입
        new_tickers = set(tickers) - existing_tickers
        inserted_count = 0
        
        print(f"📊 기존 티커: {len(existing_tickers)}개")
        print(f"📊 신규 티커: {len(new_tickers)}개")
        
        for ticker in new_tickers:
            try:
                cursor.execute("""
                    INSERT INTO tickers (ticker, created_at, is_active) 
                    VALUES (%s, CURRENT_TIMESTAMP, true)
                """, (ticker,))
                inserted_count += 1
                print(f"   ✅ {ticker} 삽입 성공")
            except Exception as e:
                print(f"   ❌ {ticker} 삽입 실패: {e}")
        
        conn.commit()
        
        # 최종 상태 확인
        cursor.execute("SELECT COUNT(*) FROM tickers")
        total_tickers = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
        active_tickers = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        result = {
            'success': True,
            'upbit_tickers': len(tickers),
            'existing_tickers': len(existing_tickers),
            'new_tickers_inserted': inserted_count,
            'total_tickers_in_db': total_tickers,
            'active_tickers_in_db': active_tickers
        }
        
        print(f"🎉 RDB 적재 완료!")
        print(f"   - 신규 삽입: {inserted_count}개")
        print(f"   - 전체 DB 티커: {total_tickers}개")
        print(f"   - 활성 티커: {active_tickers}개")
        
        return result
        
    except Exception as e:
        print(f"❌ DB 적재 실패: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide EC2 RDB 적재 시작")
    print("=" * 50)
    
    start_time = datetime.now()
    
    # 1. Upbit 티커 조회
    tickers = get_upbit_tickers()
    if not tickers:
        print("❌ Upbit 티커 조회 실패")
        return
    
    # 2. RDB에 적재
    result = load_tickers_to_db(tickers)
    
    execution_time = (datetime.now() - start_time).total_seconds()
    
    # 3. 결과 출력
    print("\\n" + "=" * 50)
    print(f"🎯 실행 결과 (실행시간: {execution_time:.2f}초)")
    
    if result['success']:
        print("✅ RDB 적재 성공!")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("❌ RDB 적재 실패")
        print(f"   오류: {result.get('error', 'Unknown')}")

if __name__ == "__main__":
    main()
'''
    
    # 스크립트를 파일로 저장
    with open('ec2_rdb_loader.py', 'w', encoding='utf-8') as f:
        f.write(ec2_script.strip())
    
    print("✅ EC2 RDB 적재 스크립트 생성 완료: ec2_rdb_loader.py")
    return True

def upload_script_to_ec2():
    """EC2에 스크립트 업로드"""
    
    try:
        print("📤 EC2에 스크립트 업로드 중...")
        
        ec2_ip = "52.78.186.226"
        pem_key = "/Users/13ruce/aws/makenaide-key.pem"
        
        # scp로 파일 업로드
        scp_command = [
            'scp', '-i', pem_key,
            '-o', 'StrictHostKeyChecking=no',
            'ec2_rdb_loader.py',
            f'ec2-user@{ec2_ip}:~/ec2_rdb_loader.py'
        ]
        
        result = subprocess.run(scp_command, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ EC2 스크립트 업로드 완료")
            return True
        else:
            print(f"❌ EC2 업로드 실패: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ EC2 업로드 오류: {e}")
        return False

def execute_script_on_ec2():
    """EC2에서 스크립트 실행"""
    
    try:
        print("🚀 EC2에서 RDB 적재 스크립트 실행 중...")
        
        ec2_ip = "52.78.186.226"
        pem_key = "/Users/13ruce/aws/makenaide-key.pem"
        
        # SSH로 스크립트 실행
        ssh_command = [
            'ssh', '-i', pem_key,
            '-o', 'StrictHostKeyChecking=no',
            f'ec2-user@{ec2_ip}',
            'cd ~ && python3 ec2_rdb_loader.py'
        ]
        
        result = subprocess.run(ssh_command, capture_output=True, text=True)
        
        print("📊 EC2 실행 결과:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ EC2 실행 오류:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("✅ EC2 RDB 적재 성공!")
            return True
        else:
            print("❌ EC2 RDB 적재 실패")
            return False
            
    except Exception as e:
        print(f"❌ EC2 실행 오류: {e}")
        return False

def verify_rdb_data():
    """RDB 데이터 검증"""
    
    try:
        print("🔍 RDB 데이터 검증 중...")
        
        # 로컬에서 DB 상태 확인
        subprocess.run(['python', 'check_db_status_direct.py'])
        
    except Exception as e:
        print(f"❌ RDB 검증 오류: {e}")

if __name__ == "__main__":
    print("🚀 EC2 직접 RDB 적재 실행")
    print("=" * 60)
    
    success = True
    
    # 1. EC2 스크립트 생성
    if deploy_ec2_rdb_script():
        print("✅ 1단계: EC2 스크립트 생성 완료")
        
        # 2. EC2에 업로드
        if upload_script_to_ec2():
            print("✅ 2단계: EC2 업로드 완료")
            
            # 3. EC2에서 실행
            if execute_script_on_ec2():
                print("✅ 3단계: EC2 실행 완료")
                
                # 4. 데이터 검증
                verify_rdb_data()
                
                print("\n🎉 **EC2 직접 RDB 적재 완전 성공!**")
            else:
                print("❌ 3단계: EC2 실행 실패")
                success = False
        else:
            print("❌ 2단계: EC2 업로드 실패")
            success = False
    else:
        print("❌ 1단계: EC2 스크립트 생성 실패")
        success = False
    
    print("\n" + "=" * 60)
    if success:
        print("🎯 **모든 작업 완료! RDB 적재 성공!**")
    else:
        print("🎯 작업 완료 (일부 실패)") 