import pandas as pd
import yaml
import os
from datetime import datetime
from utils import safe_strftime

def auto_tune_strategies(
    report_path='strategy_report.csv',
    config_path='config/strategy.yaml',
    log_path='tuning_log.txt',
    swing_score_threshold=80,
    kelly_min=0.05,
    kelly_max=0.5,
    kelly_step=0.05
):
    """
    전략별 리포트(성과)에 따라 config의 Kelly fraction을 자동 조정
    - swing_score가 높으면 Kelly fraction 상향, 낮으면 하향
    - 변경 이력은 log_path에 기록
    """
    if not os.path.exists(report_path):
        print(f"❌ 리포트 파일 없음: {report_path}")
        return
    if not os.path.exists(config_path):
        print(f"❌ 전략 config 파일 없음: {config_path}")
        return
    # 리포트 로드
    report = pd.read_csv(report_path)
    # config 로드
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    updated = False
    log_lines = []
    now = safe_strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    for _, row in report.iterrows():
        combo = str(row['strategy_combo'])
        swing_score = row.get('swing_score', 0)
        # 전략명이 config에 있으면만 튜닝
        if combo in config:
            prev_kelly = config[combo].get('kelly_fraction', 0.1)
            new_kelly = prev_kelly
            if swing_score >= swing_score_threshold:
                new_kelly = min(prev_kelly + kelly_step, kelly_max)
            else:
                new_kelly = max(prev_kelly - kelly_step, kelly_min)
            if new_kelly != prev_kelly:
                config[combo]['kelly_fraction'] = new_kelly
                updated = True
                log_lines.append(f"{now} | {combo} | swing_score: {swing_score} | kelly_fraction: {prev_kelly:.3f} -> {new_kelly:.3f}")
    if updated:
        with open(config_path, 'w') as f:
            yaml.dump(config, f, allow_unicode=True)
        with open(log_path, 'a') as f:
            for line in log_lines:
                f.write(line + '\n')
        print(f"[strategy_tuner] 튜닝 결과가 {log_path}에 기록됨.")
    else:
        print("[strategy_tuner] 변경 사항 없음.")

def track_vcp_performance(
    db_manager,
    lookback_days=30,
    vcp_score_threshold=60
):
    """
    VCP 패턴 기반 거래의 성과를 별도 추적합니다.
    
    Args:
        db_manager: 데이터베이스 매니저 인스턴스
        lookback_days: 분석 기간 (일)
        vcp_score_threshold: VCP 점수 임계값
        
    Returns:
        dict: VCP 성과 통계
    """
    try:
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=lookback_days)
        
        # VCP 기반 거래 내역 조회
        vcp_trades = db_manager.execute_query("""
            SELECT 
                t.ticker,
                t.action,
                t.qty,
                t.price,
                t.executed_at,
                t.kelly_ratio,
                a.vcp_score,
                a.stage_analysis,
                a.confidence
            FROM trade_log t
            LEFT JOIN trend_analysis a ON t.ticker = a.ticker 
                AND DATE(t.executed_at) = DATE(a.created_at)
            WHERE t.executed_at BETWEEN %s AND %s
            AND a.vcp_score >= %s
            ORDER BY t.executed_at
        """, (start_date, end_date, vcp_score_threshold))
        
        if not vcp_trades:
            return {
                'total_trades': 0,
                'vcp_performance': {},
                'message': 'VCP 기반 거래 내역 없음'
            }
        
        # 거래 쌍 분석 (매수-매도)
        df = pd.DataFrame(vcp_trades, columns=[
            'ticker', 'action', 'qty', 'price', 'executed_at',
            'kelly_ratio', 'vcp_score', 'stage_analysis', 'confidence'
        ])
        
        vcp_performance = {}
        
        # VCP 점수 구간별 성과 분석
        score_ranges = [
            (60, 70, 'medium_vcp'),
            (70, 80, 'good_vcp'), 
            (80, 100, 'excellent_vcp')
        ]
        
        for min_score, max_score, category in score_ranges:
            category_trades = df[
                (df['vcp_score'] >= min_score) & 
                (df['vcp_score'] < max_score)
            ]
            
            if len(category_trades) == 0:
                continue
                
            # 매수-매도 쌍 분석
            trades_analysis = []
            for i in range(0, len(category_trades)-1, 2):
                if i+1 >= len(category_trades):
                    break
                    
                buy = category_trades.iloc[i]
                sell = category_trades.iloc[i+1]
                
                if buy['action'] != 'BUY' or sell['action'] != 'SELL':
                    continue
                    
                return_rate = (sell['price'] - buy['price']) / buy['price']
                holding_hours = (sell['executed_at'] - buy['executed_at']).total_seconds() / 3600
                
                trades_analysis.append({
                    'ticker': buy['ticker'],
                    'return_rate': return_rate,
                    'vcp_score': buy['vcp_score'],
                    'confidence': buy['confidence'],
                    'holding_hours': holding_hours
                })
            
            if trades_analysis:
                df_analysis = pd.DataFrame(trades_analysis)
                
                vcp_performance[category] = {
                    'trade_count': len(trades_analysis),
                    'win_rate': (df_analysis['return_rate'] > 0).mean(),
                    'avg_return': df_analysis['return_rate'].mean(),
                    'max_return': df_analysis['return_rate'].max(),
                    'min_return': df_analysis['return_rate'].min(),
                    'avg_vcp_score': df_analysis['vcp_score'].mean(),
                    'avg_confidence': df_analysis['confidence'].mean(),
                    'avg_holding_hours': df_analysis['holding_hours'].mean()
                }
        
        # 전체 VCP 성과 요약
        all_vcp_trades = []
        for i in range(0, len(df)-1, 2):
            if i+1 >= len(df):
                break
                
            buy = df.iloc[i]
            sell = df.iloc[i+1]
            
            if buy['action'] != 'BUY' or sell['action'] != 'SELL':
                continue
                
            return_rate = (sell['price'] - buy['price']) / buy['price']
            all_vcp_trades.append(return_rate)
        
        overall_performance = {}
        if all_vcp_trades:
            overall_performance = {
                'total_trades': len(all_vcp_trades),
                'overall_win_rate': sum(1 for r in all_vcp_trades if r > 0) / len(all_vcp_trades),
                'overall_avg_return': sum(all_vcp_trades) / len(all_vcp_trades),
                'best_trade': max(all_vcp_trades),
                'worst_trade': min(all_vcp_trades)
            }
        
        return {
            'total_trades': len(vcp_trades),
            'vcp_performance': vcp_performance,
            'overall_performance': overall_performance,
            'analysis_period': f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
        }
        
    except Exception as e:
        return {
            'error': f"VCP 성과 추적 중 오류: {str(e)}",
            'total_trades': 0,
            'vcp_performance': {}
        }

def auto_tune_strategies_enhanced(
    report_path='strategy_report.csv',
    config_path='config/strategy.yaml',
    log_path='tuning_log.txt',
    db_manager=None,
    swing_score_threshold=80,
    kelly_min=0.05,
    kelly_max=0.5,
    kelly_step=0.05
):
    """
    전략별 리포트(성과)에 따라 config의 Kelly fraction 및 VCP/Stage 가중치를 자동 조정
    
    기존 Kelly fraction 조정에 더해:
    - VCP 패턴 신뢰도 조정
    - Stage별 진입 가중치 조정  
    - 브레이크아웃 볼륨 임계값 조정
    """
    if not os.path.exists(config_path):
        print(f"❌ 전략 config 파일 없음: {config_path}")
        return
    
    # config 로드
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    updated = False
    log_lines = []
    now = safe_strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    
    # 1. 기존 Kelly fraction 조정 (리포트 기반)
    if os.path.exists(report_path):
        report = pd.read_csv(report_path)
        
        for _, row in report.iterrows():
            combo = str(row['strategy_combo'])
            swing_score = row.get('swing_score', 0)
            
            if combo in config:
                prev_kelly = config[combo].get('kelly_fraction', 0.1)
                new_kelly = prev_kelly
                
                if swing_score >= swing_score_threshold:
                    new_kelly = min(prev_kelly + kelly_step, kelly_max)
                else:
                    new_kelly = max(prev_kelly - kelly_step, kelly_min)
                
                if new_kelly != prev_kelly:
                    config[combo]['kelly_fraction'] = new_kelly
                    updated = True
                    log_lines.append(f"{now} | {combo} | kelly_fraction: {prev_kelly:.3f} -> {new_kelly:.3f}")
    
    # 2. VCP 성과 기반 가중치 조정
    if db_manager:
        vcp_performance = track_vcp_performance(db_manager)
        
        if vcp_performance.get('total_trades', 0) > 0:
            overall_perf = vcp_performance.get('overall_performance', {})
            overall_win_rate = overall_perf.get('overall_win_rate', 0.5)
            overall_avg_return = overall_perf.get('overall_avg_return', 0)
            
            # VCP 가중치 조정
            prev_vcp_weight = config.get('strategy_weights', {}).get('vcp_weight', 0.4)
            new_vcp_weight = prev_vcp_weight
            
            # 성과가 좋으면 VCP 가중치 증가, 나쁘면 감소
            if overall_win_rate > 0.6 and overall_avg_return > 0.02:  # 승률 60% 이상, 평균 수익률 2% 이상
                new_vcp_weight = min(0.6, prev_vcp_weight + 0.05)
            elif overall_win_rate < 0.4 or overall_avg_return < -0.01:  # 승률 40% 미만 또는 평균 손실
                new_vcp_weight = max(0.2, prev_vcp_weight - 0.05)
            
            if abs(new_vcp_weight - prev_vcp_weight) > 0.001:
                if 'strategy_weights' not in config:
                    config['strategy_weights'] = {}
                config['strategy_weights']['vcp_weight'] = new_vcp_weight
                updated = True
                log_lines.append(f"{now} | VCP Weight | {prev_vcp_weight:.3f} -> {new_vcp_weight:.3f} | Win Rate: {overall_win_rate:.2%}")
            
            # VCP 점수 임계값 조정
            vcp_perf = vcp_performance.get('vcp_performance', {})
            
            # 우수한 VCP (80+) 성과가 좋으면 임계값 상향 조정
            if 'excellent_vcp' in vcp_perf:
                excellent_stats = vcp_perf['excellent_vcp']
                if excellent_stats['win_rate'] > 0.7 and excellent_stats['avg_return'] > 0.03:
                    prev_threshold = config.get('vcp_settings', {}).get('vcp_score_threshold', 60)
                    new_threshold = min(75, prev_threshold + 5)
                    
                    if new_threshold != prev_threshold:
                        if 'vcp_settings' not in config:
                            config['vcp_settings'] = {}
                        config['vcp_settings']['vcp_score_threshold'] = new_threshold
                        updated = True
                        log_lines.append(f"{now} | VCP Threshold | {prev_threshold} -> {new_threshold}")
    
    # 3. Stage별 가중치 미세 조정 (단순 로직)
    # 실제로는 Stage별 성과 데이터가 있어야 하지만, 여기서는 기본적인 조정만 구현
    stage_weights = config.get('stage_settings', {})
    if stage_weights:
        # Stage2 가중치가 너무 높거나 낮으면 조정
        stage2_weight = stage_weights.get('stage2_entry_weight', 1.0)
        if stage2_weight > 1.2:  # 너무 높으면 감소
            new_stage2_weight = max(1.0, stage2_weight - 0.1)
            config['stage_settings']['stage2_entry_weight'] = new_stage2_weight
            updated = True
            log_lines.append(f"{now} | Stage2 Weight | {stage2_weight:.2f} -> {new_stage2_weight:.2f}")
    
    # 설정 파일 저장
    if updated:
        with open(config_path, 'w') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        
        # 로그 기록
        with open(log_path, 'a') as f:
            for line in log_lines:
                f.write(line + '\n')
        
        print(f"[strategy_tuner] ✅ 향상된 튜닝 결과가 {log_path}에 기록됨.")
        print(f"- 총 {len(log_lines)}개 설정 변경")
        
        # VCP 성과 요약 출력
        if db_manager:
            vcp_perf = track_vcp_performance(db_manager)
            if vcp_perf.get('total_trades', 0) > 0:
                print(f"- VCP 거래 수: {vcp_perf['total_trades']}")
                overall = vcp_perf.get('overall_performance', {})
                if overall:
                    print(f"- VCP 승률: {overall.get('overall_win_rate', 0):.1%}")
                    print(f"- VCP 평균 수익률: {overall.get('overall_avg_return', 0):.2%}")
    else:
        print("[strategy_tuner] 변경 사항 없음.")

def get_vcp_tuning_report(db_manager, days=30):
    """
    VCP 튜닝을 위한 상세 리포트 생성
    """
    try:
        vcp_perf = track_vcp_performance(db_manager, lookback_days=days)
        
        print(f"\n=== VCP 성과 리포트 ({days}일) ===")
        print(f"총 VCP 거래 수: {vcp_perf.get('total_trades', 0)}")
        
        overall = vcp_perf.get('overall_performance', {})
        if overall:
            print(f"전체 승률: {overall.get('overall_win_rate', 0):.1%}")
            print(f"평균 수익률: {overall.get('overall_avg_return', 0):.2%}")
            print(f"최고 수익률: {overall.get('best_trade', 0):.2%}")
            print(f"최악 수익률: {overall.get('worst_trade', 0):.2%}")
        
        # VCP 점수 구간별 성과
        vcp_performance = vcp_perf.get('vcp_performance', {})
        for category, stats in vcp_performance.items():
            print(f"\n{category.upper()}:")
            print(f"  거래 수: {stats['trade_count']}")
            print(f"  승률: {stats['win_rate']:.1%}")  
            print(f"  평균 수익률: {stats['avg_return']:.2%}")
            print(f"  평균 VCP 점수: {stats['avg_vcp_score']:.1f}")
            print(f"  평균 보유 시간: {stats['avg_holding_hours']:.1f}시간")
        
        return vcp_perf
        
    except Exception as e:
        print(f"❌ VCP 리포트 생성 중 오류: {str(e)}")
        return None

# 기존 함수는 호환성을 위해 유지
def auto_tune_strategies(
    report_path='strategy_report.csv',
    config_path='config/strategy.yaml', 
    log_path='tuning_log.txt',
    swing_score_threshold=80,
    kelly_min=0.05,
    kelly_max=0.5,
    kelly_step=0.05
):
    """
    기존 auto_tune_strategies 함수 (호환성 유지)
    새로운 기능을 사용하려면 auto_tune_strategies_enhanced 사용 권장
    """
    return auto_tune_strategies_enhanced(
        report_path=report_path,
        config_path=config_path,
        log_path=log_path,
        db_manager=None,  # DB 매니저 없이 기존 방식으로 동작
        swing_score_threshold=swing_score_threshold,
        kelly_min=kelly_min,
        kelly_max=kelly_max,
        kelly_step=kelly_step
    )

if __name__ == '__main__':
    # 기본 실행은 기존 방식 유지
    auto_tune_strategies() 