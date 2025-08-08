#!/usr/bin/env python3
"""
CloudWatch 로그 보존 기간 최적화 스크립트
- 14일 → 7일로 변경
- 무제한 → 7일로 설정
"""
import subprocess
import json
import time
from datetime import datetime

def get_log_groups():
    """최적화 대상 로그 그룹 조회"""
    cmd = [
        'aws', 'logs', 'describe-log-groups',
        '--query', 'logGroups[?contains(logGroupName, `makenaide`) || contains(logGroupName, `lambda`)]',
        '--output', 'json'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return json.loads(result.stdout)

def update_retention_policy(log_group_name, retention_days=7):
    """로그 그룹의 보존 정책 업데이트"""
    cmd = [
        'aws', 'logs', 'put-retention-policy',
        '--log-group-name', log_group_name,
        '--retention-in-days', str(retention_days)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0

def main():
    print("🚀 CloudWatch 로그 보존 기간 최적화 시작")
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 로그 그룹 조회
    log_groups = get_log_groups()
    
    groups_to_update = []
    groups_no_retention = []
    
    # 최적화 대상 분류
    for group in log_groups:
        group_name = group.get('logGroupName', '')
        retention_days = group.get('retentionInDays')
        
        if retention_days == 14:
            groups_to_update.append(group_name)
        elif retention_days is None:
            groups_no_retention.append(group_name)
    
    print(f"\n📋 최적화 계획:")
    print(f"• 14일 → 7일 변경 대상: {len(groups_to_update)}개")
    print(f"• 무제한 → 7일 설정 대상: {len(groups_no_retention)}개")
    print(f"• 총 변경 대상: {len(groups_to_update) + len(groups_no_retention)}개")
    
    # 사용자 확인
    response = input("\n⚠️  보존 기간을 변경하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("❌ 작업 취소됨")
        return
    
    print("\n🔄 보존 정책 업데이트 시작...")
    
    success_count = 0
    failed_count = 0
    
    # 14일 → 7일 변경
    for group_name in groups_to_update:
        print(f"  • {group_name}: ", end="")
        if update_retention_policy(group_name, 7):
            print("✅ 성공 (14일 → 7일)")
            success_count += 1
        else:
            print("❌ 실패")
            failed_count += 1
        time.sleep(0.1)  # API 제한 방지
    
    # 무제한 → 7일 설정
    for group_name in groups_no_retention:
        print(f"  • {group_name}: ", end="")
        if update_retention_policy(group_name, 7):
            print("✅ 성공 (무제한 → 7일)")
            success_count += 1
        else:
            print("❌ 실패")
            failed_count += 1
        time.sleep(0.1)  # API 제한 방지
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("📊 최적화 결과:")
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {failed_count}개")
    print(f"완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success_count > 0:
        print("\n💰 예상 효과:")
        print("• 로그 보존 비용 50% 절감 (장기적 효과)")
        print("• 로그 관리 간소화")
        print("• 규정 준수 (7일은 충분한 분석 기간)")
        
        # 자동화 스크립트 생성
        create_automation_script()
    
    print("\n✅ CloudWatch 로그 최적화 완료!")

def create_automation_script():
    """향후 자동화를 위한 스크립트 생성"""
    script_content = '''#!/bin/bash
# CloudWatch 로그 보존 정책 자동 설정 스크립트
# 새로운 로그 그룹이 생성될 때 자동으로 7일 보존 정책 적용

set -e

echo "🔍 Makenaide 관련 로그 그룹 검색..."

# 보존 정책이 없거나 14일인 로그 그룹 조회
aws logs describe-log-groups \\
    --query 'logGroups[?contains(logGroupName, `makenaide`) || contains(logGroupName, `lambda`)].[logGroupName, retentionInDays]' \\
    --output text | while read -r group_name retention_days; do
    
    if [ "$retention_days" == "None" ] || [ "$retention_days" == "14" ]; then
        echo "📝 업데이트 중: $group_name"
        aws logs put-retention-policy \\
            --log-group-name "$group_name" \\
            --retention-in-days 7
        echo "✅ 완료: $group_name (7일 보존 설정)"
    fi
done

echo "✅ 모든 로그 그룹 보존 정책 최적화 완료!"
'''
    
    with open('cloudwatch_retention_automation.sh', 'w') as f:
        f.write(script_content)
    
    subprocess.run(['chmod', '+x', 'cloudwatch_retention_automation.sh'])
    print("\n📄 자동화 스크립트 생성됨: cloudwatch_retention_automation.sh")

if __name__ == '__main__':
    main()