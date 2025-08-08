#!/usr/bin/env python3
"""
CloudWatch 로그 분석 및 비용 계산 스크립트
"""
import json
import subprocess
import sys

def analyze_logs():
    # AWS CLI로 로그 그룹 정보 가져오기
    cmd = [
        'aws', 'logs', 'describe-log-groups',
        '--query', 'logGroups[?contains(logGroupName, `makenaide`) || contains(logGroupName, `lambda`)]',
        '--output', 'json'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    
    total_size_bytes = 0
    total_groups = 0
    groups_14_days = []
    groups_no_retention = []
    groups_7_days = []
    
    for group in data:
        group_name = group.get('logGroupName', '')
        retention_days = group.get('retentionInDays')
        size_bytes = group.get('storedBytes', 0)
        
        total_size_bytes += size_bytes
        total_groups += 1
        
        if retention_days == 14:
            groups_14_days.append({
                'name': group_name,
                'size_mb': size_bytes / (1024 * 1024)
            })
        elif retention_days == 7:
            groups_7_days.append(group_name)
        elif retention_days is None:
            groups_no_retention.append({
                'name': group_name,
                'size_mb': size_bytes / (1024 * 1024)
            })
    
    total_size_mb = total_size_bytes / (1024 * 1024)
    
    print('📊 CloudWatch 로그 현황 분석')
    print('=' * 60)
    print(f'총 로그 그룹 수: {total_groups}개')
    print(f'총 저장 용량: {total_size_mb:.2f} MB')
    print()
    
    print('📋 보존 기간별 분류:')
    print(f'• 14일 보존: {len(groups_14_days)}개')
    print(f'• 7일 보존: {len(groups_7_days)}개')
    print(f'• 무제한 보존: {len(groups_no_retention)}개')
    print()
    
    # 최적화 대상 로그 그룹 출력
    print('🎯 최적화 대상 로그 그룹 (14일 → 7일):')
    for group in groups_14_days:
        print(f"  • {group['name']} ({group['size_mb']:.2f} MB)")
    
    print()
    print('⚠️  보존 정책 미설정 그룹 (무제한 → 7일):')
    for group in groups_no_retention:
        print(f"  • {group['name']} ({group['size_mb']:.2f} MB)")
    
    # 비용 계산 (CloudWatch Logs 비용: $0.50/GB/월)
    cost_per_gb_month = 0.50
    current_monthly_cost = (total_size_mb / 1024) * cost_per_gb_month
    
    # 14일에서 7일로 변경시 50% 절감 예상
    optimized_monthly_cost = current_monthly_cost * 0.5
    monthly_savings = current_monthly_cost - optimized_monthly_cost
    
    print()
    print('💰 비용 분석:')
    print(f'현재 월간 로그 비용 (추정): ${current_monthly_cost:.2f}')
    print(f'7일 보존시 예상 비용: ${optimized_monthly_cost:.2f}')
    print(f'월간 절약액: ${monthly_savings:.2f}')
    print(f'연간 절약액: ${monthly_savings * 12:.2f}')
    print()
    print(f'⭐ ROI: {(monthly_savings * 12 / 50) * 100:.0f}% (투자비용 $50 기준)')
    
    return groups_14_days, groups_no_retention

if __name__ == '__main__':
    analyze_logs()