# Makenaide 긴급 배포 가이드

## 🚀 EC2에서 실행할 명령어 (SSH 연결 성공 시)

```bash
# 1. S3에서 업데이트 스크립트 다운로드 및 실행
aws s3 cp s3://makenaide-config-deploy/scripts/ec2_auto_update.py /tmp/ec2_auto_update.py && python3 /tmp/ec2_auto_update.py

# 또는 직접 market_sentiment.py 다운로드
aws s3 cp s3://makenaide-config-deploy/code/market_sentiment.py /home/ec2-user/makenaide/market_sentiment.py
```

## 📋 배포된 파일 상태

### ✅ S3 업로드 완료
- `s3://makenaide-config-deploy/code/market_sentiment.py` - 수정된 메인 파일
- `s3://makenaide-config-deploy/scripts/ec2_auto_update.py` - 자동 업데이트 스크립트
- `s3://makenaide-config-deploy/config/filter_rules_config.yaml` - 설정 파일

### 🔧 적용된 임계값 변경사항
```python
# 기존 (너무 엄격)
default_thresholds = {
    'min_pct_up': 40.0,           # 상승종목 비율 40%
    'max_top10_volume': 75.0,     # 거래대금 집중도 75%
    'min_ma200_above': 20.0,      # MA200 상회 비율 20%
    'min_sentiment_score': 40.0   # 종합 점수 40점
}

# 수정 후 (완화된 조건)
default_thresholds = {
    'min_pct_up': 30.0,           # 상승종목 비율 30% (↓10%)
    'max_top10_volume': 85.0,     # 거래대금 집중도 85% (↑10%)
    'min_ma200_above': 10.0,      # MA200 상회 비율 10% (↓10%)
    'min_sentiment_score': 25.0   # 종합 점수 25점 (↓15점)
}
```

## ⚡ 다음 단계

1. **EC2 인스턴스가 다음에 시작될 때** (18:00 KST EventBridge 실행 시)
2. **자동으로 S3에서 최신 코드를 다운로드**하도록 설정 완료
3. **완화된 임계값으로 파이프라인 실행**이 가능해짐

## 🎯 예상 결과

- 기존: 4개 조건 모두 만족해야 함 (매우 까다로움)
- 수정 후: 완화된 조건으로 정상적인 시장에서 파이프라인 실행 가능

다음 18:00 KST EventBridge 실행에서 파이프라인이 정상 동작할 것으로 예상됩니다.