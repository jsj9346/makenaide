#!/bin/bash

# EC2 자동 종료 모니터링 스크립트 (로컬 실행용)
# 작성일: 2025-07-18

# 설정
INSTANCE_ID="i-082bf343089af62d3"
REGION="ap-northeast-2"
EC2_IP="52.78.186.226"
PEM_KEY="/Users/13ruce/aws/makenaide-key.pem"
SHUTDOWN_SIGNAL_FILE="/tmp/shutdown_request"

echo "========================================="
echo "🔍 EC2 자동 종료 모니터링 시작"
echo "📍 인스턴스 ID: $INSTANCE_ID"
echo "📍 EC2 IP: $EC2_IP"
echo "📍 리전: $REGION"
echo "========================================="

# 무한 루프로 종료 신호 모니터링
while true; do
    echo "🔍 $(date): EC2 종료 신호 확인 중..."
    
    # EC2에서 종료 신호 파일 확인
    if ssh -i "$PEM_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ec2-user@"$EC2_IP" "test -f $SHUTDOWN_SIGNAL_FILE" 2>/dev/null; then
        echo "🚨 $(date): 종료 신호 감지됨!"
        
        # 신호 파일 내용 확인
        SIGNAL_CONTENT=$(ssh -i "$PEM_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ec2-user@"$EC2_IP" "cat $SHUTDOWN_SIGNAL_FILE" 2>/dev/null)
        echo "📋 종료 신호 내용: $SIGNAL_CONTENT"
        
        # Elastic IP 상태 확인
        echo "🔍 Elastic IP 상태 확인 중..."
        ELASTIC_IP_INFO=$(aws ec2 describe-addresses --region "$REGION" --filters "Name=instance-id,Values=$INSTANCE_ID" --query 'Addresses[0].{AllocationId:AllocationId,PublicIp:PublicIp}' --output json 2>/dev/null)
        
        if [ "$ELASTIC_IP_INFO" != "null" ] && [ "$ELASTIC_IP_INFO" != "" ]; then
            echo "✅ Elastic IP 연결 확인: $ELASTIC_IP_INFO"
        else
            echo "⚠️ Elastic IP가 연결되어 있지 않음"
        fi
        
        # EC2 인스턴스 종료 실행
        echo "🔌 $(date): EC2 인스턴스 종료 실행..."
        
        # 종료 전 최종 확인
        INSTANCE_STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
        echo "📊 현재 인스턴스 상태: $INSTANCE_STATE"
        
        if [ "$INSTANCE_STATE" = "running" ]; then
            # 실제 종료 명령 실행
            echo "⚡ EC2 인스턴스 종료 명령 전송..."
            aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$REGION"
            
            if [ $? -eq 0 ]; then
                echo "✅ $(date): EC2 인스턴스 종료 명령 성공"
                
                # 종료 상태 모니터링
                echo "⏳ 인스턴스 종료 완료까지 대기 중..."
                aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID" --region "$REGION"
                
                echo "🔌 $(date): EC2 인스턴스 종료 완료"
                
                # Elastic IP 상태 재확인
                echo "🔍 종료 후 Elastic IP 상태 재확인..."
                FINAL_EIP_INFO=$(aws ec2 describe-addresses --region "$REGION" --filters "Name=instance-id,Values=$INSTANCE_ID" --query 'Addresses[0].{AllocationId:AllocationId,PublicIp:PublicIp}' --output json 2>/dev/null)
                
                if [ "$FINAL_EIP_INFO" = "null" ]; then
                    echo "✅ Elastic IP가 성공적으로 해제되어 고정 IP 유지됨"
                else
                    echo "⚠️ Elastic IP 상태 확인 필요: $FINAL_EIP_INFO"
                fi
                
                break
            else
                echo "❌ $(date): EC2 인스턴스 종료 명령 실패"
                exit 1
            fi
        else
            echo "⚠️ 인스턴스가 이미 실행 중이 아닙니다. 현재 상태: $INSTANCE_STATE"
            break
        fi
        
    else
        # SSH 연결 실패 또는 신호 파일 없음
        SSH_RESULT=$?
        if [ $SSH_RESULT -ne 0 ]; then
            echo "⚠️ $(date): EC2 연결 실패 (인스턴스가 이미 종료되었을 수 있음)"
            # 인스턴스 상태 직접 확인
            CURRENT_STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
            echo "📊 인스턴스 상태: $CURRENT_STATE"
            
            if [ "$CURRENT_STATE" = "stopped" ] || [ "$CURRENT_STATE" = "stopping" ]; then
                echo "✅ 인스턴스가 이미 종료 상태입니다"
                break
            fi
        fi
    fi
    
    # 30초 대기 후 다시 확인
    sleep 30
done

echo "========================================="
echo "🏁 EC2 자동 종료 모니터링 완료: $(date)"
echo "=========================================" 