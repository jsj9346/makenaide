#!/bin/bash
# CloudWatch 로그 보존 정책 자동 설정 스크립트
# 새로운 로그 그룹이 생성될 때 자동으로 7일 보존 정책 적용

set -e

echo "🔍 Makenaide 관련 로그 그룹 검색..."

# 보존 정책이 없거나 14일인 로그 그룹 조회
aws logs describe-log-groups \
    --query 'logGroups[?contains(logGroupName, `makenaide`) || contains(logGroupName, `lambda`)].[logGroupName, retentionInDays]' \
    --output text | while read -r group_name retention_days; do
    
    if [ "$retention_days" == "None" ] || [ "$retention_days" == "14" ]; then
        echo "📝 업데이트 중: $group_name"
        aws logs put-retention-policy \
            --log-group-name "$group_name" \
            --retention-in-days 7
        echo "✅ 완료: $group_name (7일 보존 설정)"
    fi
done

echo "✅ 모든 로그 그룹 보존 정책 최적화 완료!"
