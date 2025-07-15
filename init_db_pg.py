import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import logging

def create_disclaimer_table():
    """Disclaimer 동의 테이블을 생성합니다."""
    
    disclaimer_table = """
        CREATE TABLE IF NOT EXISTS disclaimer_agreements (
            id SERIAL PRIMARY KEY,
            agreement_version VARCHAR(10) NOT NULL,
            agreed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            agreed_by VARCHAR(100) DEFAULT 'system',
            ip_address INET,
            user_agent TEXT,
            agreement_text_hash VARCHAR(64),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """
    
    disclaimer_indexes = [
        """
        CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_version 
        ON disclaimer_agreements(agreement_version)
        """,
        
        """
        CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_active 
        ON disclaimer_agreements(is_active)
        """
    ]
    
    return disclaimer_table, disclaimer_indexes

def create_backtest_tables():
    """백테스트 전용 테이블들을 생성합니다."""
    
    backtest_tables = [
        # 백테스트 전용 OHLCV 테이블
        """
        CREATE TABLE IF NOT EXISTS backtest_ohlcv (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(ticker, date)
        )
        """,
        
        # 백테스트 세션 관리 테이블
        """
        CREATE TABLE IF NOT EXISTS backtest_sessions (
            session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(100) NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            data_snapshot_date TIMESTAMP DEFAULT NOW(),
            description TEXT,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT NOW()
        )
        """,
        
        # 백테스트 인덱스들
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_ohlcv_ticker_date 
        ON backtest_ohlcv(ticker, date DESC)
        """,
        
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_sessions_created_at 
        ON backtest_sessions(created_at)
        """,
        
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_sessions_status 
        ON backtest_sessions(status)
        """
    ]
    
    return backtest_tables

def execute_sql_safely(cur, sql_query, description):
    """SQL 쿼리를 안전하게 실행합니다."""
    try:
        cur.execute(sql_query)
        logging.info(f"✅ {description} 성공")
    except Exception as e:
        logging.error(f"❌ {description} 실패: {e}")
        raise

def create_tables():
    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()

    cur.execute("""
    --DROP TABLE IF EXISTS performance_summary, trade_log, strategy_performance, trend_analysis, trend_analysis_log, market_data, market_data_4h, tickers, ohlcv, ohlcv_4h, portfolio_history CASCADE;


    CREATE TABLE IF NOT EXISTS performance_summary (
        id SERIAL PRIMARY KEY,
        period_start DATE,
        period_end DATE,
        initial_cash REAL,
        final_valuation REAL,
        net_profit REAL,
        win_rate REAL,
        profit_factor REAL,
        max_drawdown REAL,
        num_trades INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS trade_log (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        action TEXT NOT NULL,
        qty REAL,
        price REAL,
        buy_price REAL,  -- 추가: 매수 가격
        score REAL,      -- 추가: GPT 점수
        confidence REAL, -- 추가: GPT 신뢰도
        trade_amount_krw REAL, -- 추가: 거래 금액
        bought_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 추가: 매수 시간
        status TEXT,     -- 추가: 거래 상태
        error_msg TEXT,  -- 추가: 오류 메시지
        kelly_ratio REAL,
        swing_score REAL,
        strategy_combo TEXT,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS strategy_performance (
        id SERIAL PRIMARY KEY,
        strategy_combo TEXT NOT NULL,
        period_start DATE,
        period_end DATE,
        win_rate REAL,
        avg_return REAL,
        mdd REAL,
        num_trades INTEGER,
        kelly_ratio REAL,
        swing_score REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS trend_analysis (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        score INTEGER,
        confidence REAL,
        action TEXT,
        market_phase TEXT,
        pattern TEXT,
        reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS trend_analysis_log (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        action TEXT,
        confidence INTEGER,
        time_window TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS trade_history (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        trade_datetime TIMESTAMP NOT NULL,
        order_type TEXT NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        order_id TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        gpt_confidence REAL,
        gpt_summary TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DROP TABLE IF EXISTS market_data_4h;
    CREATE TABLE market_data_4h (
        ticker text PRIMARY KEY,
        price real,
        -- 이동평균선 지표
        ma_10 real,
        ma_20 real,
        ma_50 real,                 -- ✅ 추가: 마켓타이밍 필터용
        ma_200 real,                -- ✅ 추가: 장기 추세 확인용
        -- RSI 지표
        rsi_14 real,                -- ✅ 수정: rsi_7 → rsi_14로 변경
        -- Stochastic 지표
        stochastic_k real,
        stochastic_d real,
        -- MACD 지표
        macd real,                  -- ✅ 추가: MACD 라인
        macds real,                 -- ✅ 추가: MACD Signal 라인
        macdh real,                 -- ✅ 추가: MACD Histogram
        -- ADX 지표
        adx real,                   -- ✅ 추가: 추세 강도
        plus_di real,               -- ✅ 추가: +DI
        minus_di real,              -- ✅ 추가: -DI
        -- 볼린저밴드 지표
        bb_upper real,
        bb_middle real,             -- ✅ 추가: 볼린저밴드 중앙선
        bb_lower real,
        -- CCI 지표
        cci real,                   -- ✅ 추가: Commodity Channel Index
        -- Supertrend 지표
        supertrend real,            -- ✅ 추가: Supertrend 값
        supertrend_signal text,     -- ✅ 추가: Supertrend 신호 ('up' 또는 'down')
        -- 피벗 포인트 지표
        pivot real,
        r1 real, r2 real, r3 real,
        s1 real, s2 real, s3 real,
        -- 피보나치 지표
        fibo_236 real, fibo_382 real,
        fibo_500 real, fibo_618 real, fibo_786 real,
        -- 메타데이터
        updated_at timestamp without time zone default CURRENT_TIMESTAMP  -- 4시간 주기로 업데이트
    );

    CREATE TABLE IF NOT EXISTS tickers (
        ticker     VARCHAR(20) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker)
    );

    CREATE TABLE IF NOT EXISTS ohlcv (
        ticker TEXT,
        date DATE,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        -- 확정된 동적 지표들만 유지
        fibo_618 REAL,
        fibo_382 REAL,
        -- supertrend_signal TEXT,      -- ❌ 제거됨 (정적 지표로 이동)
        ht_trendline REAL,
        ma_20 REAL,                  -- ✅ 추가: 20일 이동평균선
        ma_50 REAL,
        ma_200 REAL,
        bb_upper REAL,
        bb_lower REAL,
        donchian_high REAL,
        donchian_low REAL,
        macd_histogram REAL,
        rsi_14 REAL,
        volume_20ma REAL,
        volume_ratio REAL,           -- ✅ 추가: 거래량 비율
        stoch_k REAL,
        stoch_d REAL,
        cci REAL,
        PRIMARY KEY (ticker, date)
    );

    DROP TABLE IF EXISTS ohlcv_4h;
    CREATE TABLE ohlcv_4h (
        ticker TEXT,
        date TIMESTAMP,             -- ✅ 수정: datetime → date로 변경 (일관성 확보)
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (ticker, date)
    );

    CREATE TABLE IF NOT EXISTS portfolio_history (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        action TEXT NOT NULL,
        qty REAL,
        price REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS trailing_stops (
        id SERIAL PRIMARY KEY,
        ticker TEXT UNIQUE NOT NULL,
        initial_price REAL NOT NULL,
        activation_price REAL NOT NULL,
        stop_price REAL NOT NULL,
        atr_value REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS static_indicators (
        ticker TEXT PRIMARY KEY,
        -- 확정된 정적 지표들
        nvt_relative REAL,          -- 온체인 과열/저평가 판단
        volume_change_7_30 REAL,    -- 거래량 변화율
        price REAL,                 -- 현재가 또는 기준 종가
        high_60 REAL,               -- 60일 최고가
        low_60 REAL,                -- 60일 최저가 (VCP 전략용)
        pivot REAL,                 -- 당일 피벗 포인트
        s1 REAL,                    -- 지지선 1
        r1 REAL,                    -- 저항선 1
        resistance REAL,            -- 저항선 (20일 최고가)
        support REAL,               -- 지지선 (20일 최저가)
        atr REAL,                   -- ATR(14) 지표
        adx REAL,                   -- ADX(14) 지표
        supertrend_signal TEXT,     -- SuperTrend 신호 ('bull' 또는 'bear')
        rsi_14 REAL,                -- ✅ 추가: RSI(14)
        ma20 REAL,                  -- ✅ 추가: 20일 이동평균선
        volume_ratio REAL,          -- ✅ 추가: 거래량 비율
        volume REAL,                -- ✅ 추가: 거래량
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- trading_log 테이블은 trade_log로 통합됨 (중복 제거)
    -- 기존 trading_log 데이터는 마이그레이션 후 삭제됨
    
    CREATE TABLE IF NOT EXISTS manual_override_log (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(20) NOT NULL,
        detection_type VARCHAR(50) NOT NULL,  -- 'manual_buy', 'manual_sell', 'quantity_mismatch'
        expected_quantity DECIMAL(20, 8),
        actual_quantity DECIMAL(20, 8),
        quantity_diff DECIMAL(20, 8),
        description TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolved BOOLEAN DEFAULT FALSE,
        resolved_at TIMESTAMP,
        notes TEXT
    );
    """)
    
    # 기존 trend_analysis 테이블 마이그레이션 (기존 데이터와의 호환성 보장)
    try:
        # 기존 테이블 구조 확인 및 필요한 컬럼 추가
        cur.execute("ALTER TABLE trend_analysis ADD COLUMN IF NOT EXISTS id SERIAL;")
        cur.execute("ALTER TABLE trend_analysis ADD COLUMN IF NOT EXISTS score INTEGER;")
        # 기존 PRIMARY KEY 제약 조건 제거하고 새로운 id 컬럼으로 변경
        cur.execute("ALTER TABLE trend_analysis DROP CONSTRAINT IF EXISTS trend_analysis_pkey;")
        cur.execute("ALTER TABLE trend_analysis ADD PRIMARY KEY (id);")
        # ticker 컬럼에 UNIQUE 제약조건 추가 (ON CONFLICT 지원, 기존 제약조건 있을 경우 제거 후 추가)
        cur.execute("ALTER TABLE trend_analysis DROP CONSTRAINT IF EXISTS unique_trend_analysis_ticker;")
        cur.execute("ALTER TABLE trend_analysis ADD CONSTRAINT unique_trend_analysis_ticker UNIQUE (ticker);")
        # 기존에 없던 컬럼들 제거 (새 스키마와 맞추기 위해)
        cur.execute("ALTER TABLE trend_analysis DROP COLUMN IF EXISTS type;")
        cur.execute("ALTER TABLE trend_analysis DROP COLUMN IF EXISTS time_window;")
        logging.info("✅ trend_analysis 테이블 마이그레이션 완료")
    except Exception as e:
        logging.warning(f"⚠️ trend_analysis 테이블 마이그레이션 중 오류: {e}")
    
    # Ensure new static_indicators columns exist (5단계 스키마 정리 작업)
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS resistance REAL;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS support REAL;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS atr REAL;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS adx REAL;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS low_60 REAL;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS supertrend_signal TEXT;")
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS rsi_14 REAL;")  # ✅ 추가
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS ma20 REAL;")    # ✅ 추가
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS volume_ratio REAL;") # ✅ 추가
    cur.execute("ALTER TABLE static_indicators ADD COLUMN IF NOT EXISTS volume REAL;")  # ✅ 추가

    # Add new ohlcv columns (VCP 전략 강화용)
    cur.execute("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS stoch_k REAL;")
    cur.execute("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS stoch_d REAL;")
    cur.execute("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS cci REAL;")
    cur.execute("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS ma_20 REAL;")  # ✅ 추가
    cur.execute("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS volume_ratio REAL;")  # ✅ 추가
    
    # Remove duplicate ATR column from ohlcv (moved to static_indicators) - 인덱스 생성 전에 실행
    cur.execute("ALTER TABLE ohlcv DROP COLUMN IF EXISTS atr;")
    
    # Remove supertrend_signal column from ohlcv (moved to static_indicators)
    cur.execute("ALTER TABLE ohlcv DROP COLUMN IF EXISTS supertrend_signal;")
    
    # Disclaimer 테이블 생성 추가
    disclaimer_table, disclaimer_indexes = create_disclaimer_table()
    execute_sql_safely(cur, disclaimer_table, "Disclaimer 테이블 생성")
    
    for index_sql in disclaimer_indexes:
        execute_sql_safely(cur, index_sql, "Disclaimer 인덱스 생성")
    
    logging.info("✅ Disclaimer 테이블 생성 완료")
    
    # 백테스트 테이블 생성 추가
    backtest_tables = create_backtest_tables()
    for table_sql in backtest_tables:
        execute_sql_safely(cur, table_sql, "백테스트 테이블 생성")
    
    logging.info("✅ 백테스트 전용 테이블 생성 완료")
    
    # 스키마 최적화 완료 - 확정된 지표만 사용
    # static_indicators: nvt_relative, volume_change_7_30, price, high_60, low_60, pivot, s1, r1, resistance, support, atr, adx, supertrend_signal
    # ohlcv: 14개 확정 동적 지표만 유지 (fibo_618, fibo_382, ht_trendline, 
    #        ma_50, ma_200, bb_upper, bb_lower, donchian_high, donchian_low, macd_histogram, rsi_14, volume_20ma, stoch_k, stoch_d, cci)
    logging.info("✅ 최적화된 스키마 적용 완료")
    logging.info("   - static_indicators: 13개 확정 정적 지표 (ma200_slope 제거)")
    logging.info("   - ohlcv: 14개 확정 동적 지표 (supertrend_signal 제거됨)")
    
    # Ensure timestamp column exists in portfolio_history (backward compatibility)
    cur.execute("ALTER TABLE portfolio_history ADD COLUMN IF NOT EXISTS timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
    
    # Ensure is_active column exists in tickers table for filtering (근본적 해결책)
    cur.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true;")
    
    # 🔧 [2단계 작업] trade_log 테이블 스키마 업데이트 및 trading_log 통합
    try:
        # trade_log 테이블에 누락된 컬럼들 추가
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS buy_price REAL;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS score REAL;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS confidence REAL;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS trade_amount_krw REAL;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS bought_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS status TEXT;")
        cur.execute("ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS error_msg TEXT;")
        
        logging.info("✅ trade_log 테이블 스키마 업데이트 완료")
        
        # trading_log 테이블이 존재하는지 확인하고 데이터 마이그레이션
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'trading_log'
            );
        """)
        trading_log_exists = cur.fetchone()[0]
        
        if trading_log_exists:
            logging.info("🔄 trading_log 테이블 발견 - 데이터 마이그레이션 시작")
            
            # trading_log 데이터를 trade_log로 마이그레이션
            cur.execute("""
                INSERT INTO trade_log (ticker, action, price, qty, executed_at, status, strategy_combo)
                SELECT 
                    ticker, 
                    action, 
                    price::REAL, 
                    quantity::REAL, 
                    created_at, 
                    COALESCE(status, 'completed'), 
                    strategy_combo
                FROM trading_log
                WHERE NOT EXISTS (
                    SELECT 1 FROM trade_log tl 
                    WHERE tl.ticker = trading_log.ticker 
                    AND tl.executed_at = trading_log.created_at
                    AND tl.action = trading_log.action
                )
            """)
            
            migrated_count = cur.rowcount
            logging.info(f"✅ trading_log 데이터 마이그레이션 완료: {migrated_count}개 레코드")
            
            # trading_log 테이블 삭제
            cur.execute("DROP TABLE IF EXISTS trading_log CASCADE;")
            logging.info("✅ trading_log 테이블 삭제 완료")
            
        else:
            logging.info("ℹ️ trading_log 테이블이 존재하지 않음 - 마이그레이션 건너뜀")
            
    except Exception as e:
        logging.error(f"❌ trade_log 스키마 업데이트 및 마이그레이션 실패: {e}")
        raise

    # 인덱스 생성 (atr 컬럼 제거 후 실행)
    logging.info("🔧 인덱스 생성 중...")
    
    # market_data_4h 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_data_4h_ticker ON market_data_4h(ticker);
        CREATE INDEX IF NOT EXISTS idx_market_data_4h_updated_at ON market_data_4h(updated_at);
    """)
    
    # ohlcv 테이블 인덱스 (성능 최적화)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker ON ohlcv(ticker);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON ohlcv(date);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON ohlcv(ticker, date DESC);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_ma_indicators ON ohlcv(ticker, ma_50, ma_200) WHERE ma_50 IS NOT NULL AND ma_200 IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_ohlcv_technical ON ohlcv(ticker, rsi_14) WHERE rsi_14 IS NOT NULL;
    """)
    
    # ohlcv_4h 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_4h_ticker ON ohlcv_4h(ticker);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_4h_date ON ohlcv_4h(date);
    """)
    
    # trade_log 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_trade_log_ticker ON trade_log(ticker);
        CREATE INDEX IF NOT EXISTS idx_trade_log_executed_at ON trade_log(executed_at);
        CREATE INDEX IF NOT EXISTS idx_trade_log_strategy_combo ON trade_log(strategy_combo);
    """)
    
    # trend_analysis 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_trend_analysis_ticker ON trend_analysis(ticker);
        CREATE INDEX IF NOT EXISTS idx_trend_analysis_created_at ON trend_analysis(created_at);
    """)
    
    # static_indicators 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_static_indicators_ticker ON static_indicators(ticker);
        CREATE INDEX IF NOT EXISTS idx_static_indicators_updated_at ON static_indicators(updated_at);
    """)
    
    # trading_log 테이블은 통합되어 삭제됨 - 인덱스 생성 제거
    
    # manual_override_log 테이블 인덱스
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_manual_override_log_ticker ON manual_override_log(ticker);
        CREATE INDEX IF NOT EXISTS idx_manual_override_log_detected_at ON manual_override_log(detected_at);
        CREATE INDEX IF NOT EXISTS idx_manual_override_log_resolved ON manual_override_log(resolved);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("✅ 인덱스 생성 완료")
    print("✅ PostgreSQL DB 및 테이블 생성 완료")

def main():
    """메인 함수"""
    create_tables()

if __name__ == "__main__":
    main()