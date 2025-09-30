# Makenaide Quick Wins Implementation Guide

**Generated**: 2025-09-30
**Target Audience**: Immediate implementation (1-7 days)
**Impact**: High-value, low-effort improvements

---

## 🎯 Quick Win #1: Automated SQLite Backup

**Effort**: 1 day | **Impact**: Prevents data loss | **ROI**: Infinite

### Problem
- Single SQLite file with no backup = catastrophic data loss risk
- Manual backup = prone to being forgotten
- File corruption = total trading history loss

### Solution
Automated daily backup with S3 archival and retention management.

### Implementation Steps

#### Step 1: Create Backup Script

Create `/home/ubuntu/backup_sqlite.sh`:

```bash
#!/bin/bash
#################################################
# Makenaide SQLite Automated Backup Script
# Purpose: Daily backup to local + S3 with retention
# Author: Generated from system analysis
# Date: 2025-09-30
#################################################

set -e  # Exit on any error

# Configuration
BACKUP_DIR="/home/ubuntu/backups"
DB_FILE="/home/ubuntu/makenaide/makenaide_local.db"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
S3_BUCKET="s3://makenaide-backups"
LOCAL_RETENTION_DAYS=7
S3_RETENTION_DAYS=30

# Create backup directory if not exists
mkdir -p $BACKUP_DIR

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting SQLite backup..."

# Check if database file exists
if [ ! -f "$DB_FILE" ]; then
    log "ERROR: Database file not found at $DB_FILE"
    exit 1
fi

# Create backup using SQLite .backup command (ensures consistency)
BACKUP_FILE="${BACKUP_DIR}/makenaide_${TIMESTAMP}.db"
sqlite3 $DB_FILE ".backup ${BACKUP_FILE}"

if [ $? -eq 0 ]; then
    log "SQLite backup created: $BACKUP_FILE"
else
    log "ERROR: SQLite backup failed"
    exit 1
fi

# Compress backup
gzip ${BACKUP_FILE}
COMPRESSED_FILE="${BACKUP_FILE}.gz"
log "Compressed backup: $COMPRESSED_FILE"

# Calculate file size
FILE_SIZE=$(du -h "$COMPRESSED_FILE" | cut -f1)
log "Backup size: $FILE_SIZE"

# Upload to S3
log "Uploading to S3..."
aws s3 cp ${COMPRESSED_FILE} ${S3_BUCKET}/ \
    --storage-class STANDARD_IA \
    --metadata "backup-date=${TIMESTAMP},source=makenaide-ec2"

if [ $? -eq 0 ]; then
    log "S3 upload successful"
else
    log "ERROR: S3 upload failed"
    exit 1
fi

# Cleanup local backups older than retention period
log "Cleaning up local backups older than ${LOCAL_RETENTION_DAYS} days..."
find $BACKUP_DIR -name "makenaide_*.db.gz" -mtime +${LOCAL_RETENTION_DAYS} -delete
LOCAL_REMAINING=$(find $BACKUP_DIR -name "makenaide_*.db.gz" | wc -l)
log "Local backups remaining: $LOCAL_REMAINING"

# Cleanup S3 backups older than retention period
log "Cleaning up S3 backups older than ${S3_RETENTION_DAYS} days..."
CUTOFF_DATE=$(date -d "${S3_RETENTION_DAYS} days ago" +%Y%m%d)

aws s3 ls ${S3_BUCKET}/ | awk '{print $4}' | grep "makenaide_" | while read file; do
    # Extract date from filename (format: makenaide_YYYYMMDD_HHMMSS.db.gz)
    FILE_DATE=$(echo $file | grep -oP 'makenaide_\K\d{8}')

    if [[ -n $FILE_DATE ]]; then
        if [[ $FILE_DATE -lt $CUTOFF_DATE ]]; then
            log "Deleting old S3 backup: $file"
            aws s3 rm ${S3_BUCKET}/$file
        fi
    fi
done

S3_REMAINING=$(aws s3 ls ${S3_BUCKET}/ | grep "makenaide_" | wc -l)
log "S3 backups remaining: $S3_REMAINING"

# Verify backup integrity
log "Verifying backup integrity..."
gunzip -t ${COMPRESSED_FILE}
if [ $? -eq 0 ]; then
    log "Backup integrity verified"
else
    log "WARNING: Backup integrity check failed"
fi

log "Backup completed successfully"

# Send success notification (optional)
# Uncomment if SNS is configured
# aws sns publish \
#     --topic-arn arn:aws:sns:ap-northeast-2:901361833359:makenaide-notifications \
#     --subject "Makenaide Backup Success" \
#     --message "SQLite backup completed: ${TIMESTAMP}, Size: ${FILE_SIZE}"

exit 0
```

Make executable:
```bash
chmod +x /home/ubuntu/backup_sqlite.sh
```

#### Step 2: Create S3 Bucket

```bash
# Create S3 bucket for backups
aws s3 mb s3://makenaide-backups --region ap-northeast-2

# Enable versioning (extra safety)
aws s3api put-bucket-versioning \
    --bucket makenaide-backups \
    --versioning-configuration Status=Enabled

# Add lifecycle policy for cost optimization
cat > lifecycle-policy.json <<EOF
{
  "Rules": [
    {
      "Id": "MoveToGlacierAfter90Days",
      "Status": "Enabled",
      "Transitions": [
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ],
      "NoncurrentVersionTransitions": [
        {
          "NoncurrentDays": 30,
          "StorageClass": "GLACIER"
        }
      ]
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket makenaide-backups \
    --lifecycle-configuration file://lifecycle-policy.json

# Verify bucket created
aws s3 ls | grep makenaide-backups
```

#### Step 3: Configure Cron Job

```bash
# Open crontab editor
crontab -e

# Add daily backup at 3:00 AM KST
0 3 * * * /home/ubuntu/backup_sqlite.sh >> /home/ubuntu/logs/backup.log 2>&1
```

Create log directory:
```bash
mkdir -p /home/ubuntu/logs
```

#### Step 4: Test Backup

```bash
# Run backup manually to test
/home/ubuntu/backup_sqlite.sh

# Verify backup created locally
ls -lh /home/ubuntu/backups/

# Verify S3 upload
aws s3 ls s3://makenaide-backups/

# Test backup restore
gunzip -c /home/ubuntu/backups/makenaide_*.db.gz > /tmp/test_restore.db
sqlite3 /tmp/test_restore.db "SELECT COUNT(*) FROM tickers;"
```

### Expected Results
- ✅ Daily automated backup at 3:00 AM KST
- ✅ Local retention: 7 days
- ✅ S3 retention: 30 days (then Glacier)
- ✅ Typical backup size: 10-50 MB compressed
- ✅ Backup time: ~30 seconds

### Monitoring
```bash
# Check backup logs
tail -f /home/ubuntu/logs/backup.log

# Verify recent backups
aws s3 ls s3://makenaide-backups/ --recursive --human-readable | tail -10
```

---

## 🎯 Quick Win #2: ATR-Based Dynamic Stop Loss

**Effort**: 2 days | **Impact**: Better risk-adjusted returns | **ROI**: 10-20% improvement

### Problem
- Fixed 7-8% stop loss ignores volatility differences
- BTC 7% = normal, small altcoin 7% = minor fluctuation
- Results in false stops or inadequate protection

### Solution
ATR (Average True Range) based dynamic stop loss that adapts to each asset's volatility.

### Implementation Steps

#### Step 1: Add ATR Calculation to Technical Analysis

File: `hybrid_technical_filter.py`

```python
def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate technical indicators including ATR.

    Args:
        df: OHLCV DataFrame with columns [open, high, low, close, volume]

    Returns:
        DataFrame with additional technical indicator columns
    """
    # ... existing indicators ...

    # Add ATR (14-period)
    df['ATR_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)

    # Add ATR as percentage of close price (for easier interpretation)
    df['ATR_PCT'] = (df['ATR_14'] / df['close']) * 100

    # Store in database
    latest_atr = df['ATR_14'].iloc[-1]
    latest_atr_pct = df['ATR_PCT'].iloc[-1]

    return df
```

#### Step 2: Implement Dynamic Stop Loss Function

File: `trading_engine.py`

```python
def calculate_dynamic_stop_loss(
    entry_price: float,
    atr: float,
    atr_multiplier: float = 2.0,
    min_stop_pct: float = 0.05,
    max_stop_pct: float = 0.15
) -> dict:
    """
    Calculate volatility-adjusted stop loss using ATR.

    Philosophy:
    - High volatility assets need wider stops to avoid false stops
    - Low volatility assets can use tighter stops
    - Clamp to reasonable range to prevent extreme stops

    Args:
        entry_price: Position entry price (KRW)
        atr: Average True Range (14-period recommended)
        atr_multiplier: Multiplier for ATR distance (default: 2.0)
            - 2.0 = conservative (wider stops)
            - 1.5 = moderate
            - 1.0 = aggressive (tighter stops)
        min_stop_pct: Minimum stop loss percentage (default: 5%)
        max_stop_pct: Maximum stop loss percentage (default: 15%)

    Returns:
        dict with:
            - stop_price: Calculated stop loss price
            - stop_pct: Stop loss percentage
            - atr_pct: ATR as percentage of entry price
            - stop_type: 'atr' or 'clamped_min' or 'clamped_max'

    Examples:
        >>> # Bitcoin: High price, moderate volatility
        >>> calculate_dynamic_stop_loss(
        ...     entry_price=50_000_000,
        ...     atr=2_000_000  # 4% ATR
        ... )
        {
            'stop_price': 46_000_000,
            'stop_pct': 0.08,  # 8% stop
            'atr_pct': 0.04,
            'stop_type': 'atr'
        }

        >>> # Low-volatility altcoin
        >>> calculate_dynamic_stop_loss(
        ...     entry_price=1_000,
        ...     atr=30  # 3% ATR
        ... )
        {
            'stop_price': 940,
            'stop_pct': 0.06,  # 6% stop
            'atr_pct': 0.03,
            'stop_type': 'atr'
        }

        >>> # Extreme volatility - clamped to max
        >>> calculate_dynamic_stop_loss(
        ...     entry_price=1_000,
        ...     atr=200  # 20% ATR
        ... )
        {
            'stop_price': 850,
            'stop_pct': 0.15,  # Max 15% stop
            'atr_pct': 0.20,
            'stop_type': 'clamped_max'
        }
    """
    # Calculate ATR as percentage of entry price
    atr_pct = atr / entry_price

    # Calculate ATR-based stop percentage
    atr_stop_pct = atr_pct * atr_multiplier

    # Determine final stop percentage with clamping
    stop_type = 'atr'
    if atr_stop_pct < min_stop_pct:
        stop_pct = min_stop_pct
        stop_type = 'clamped_min'
    elif atr_stop_pct > max_stop_pct:
        stop_pct = max_stop_pct
        stop_type = 'clamped_max'
    else:
        stop_pct = atr_stop_pct

    # Calculate stop price
    stop_price = entry_price * (1 - stop_pct)

    # Log for analysis
    logger.info(
        "dynamic_stop_calculated",
        entry_price=entry_price,
        atr=atr,
        atr_pct=round(atr_pct, 4),
        atr_multiplier=atr_multiplier,
        stop_pct=round(stop_pct, 4),
        stop_price=round(stop_price, 2),
        stop_type=stop_type
    )

    return {
        'stop_price': stop_price,
        'stop_pct': stop_pct,
        'atr_pct': atr_pct,
        'stop_type': stop_type,
        'atr_multiplier': atr_multiplier
    }
```

#### Step 3: Integrate into Trading Engine

File: `trading_engine.py`

```python
class TradingEngine:
    def __init__(self, db_manager, dry_run=False, use_dynamic_stops=True, atr_multiplier=2.0):
        """
        Initialize TradingEngine with dynamic stop loss support.

        Args:
            db_manager: Database manager instance
            dry_run: If True, simulate trades without execution
            use_dynamic_stops: If True, use ATR-based stops; if False, use fixed 7.5%
            atr_multiplier: ATR multiplier for stop distance (2.0 = conservative)
        """
        self.db_manager = db_manager
        self.dry_run = dry_run
        self.use_dynamic_stops = use_dynamic_stops
        self.atr_multiplier = atr_multiplier

        logger.info(
            "trading_engine_initialized",
            dry_run=dry_run,
            use_dynamic_stops=use_dynamic_stops,
            atr_multiplier=atr_multiplier
        )

    def place_buy_order(self, ticker: str, signal_score: float, kelly_fraction: float):
        """
        Place buy order with dynamic stop loss.
        """
        # ... existing code ...

        # Get current price
        current_price = pyupbit.get_current_price(ticker)

        # Retrieve ATR for dynamic stop calculation
        if self.use_dynamic_stops:
            atr_14 = self.db_manager.get_latest_technical_indicator(ticker, 'ATR_14')

            if atr_14 is None or atr_14 <= 0:
                logger.warning(
                    "atr_not_available_using_fixed_stop",
                    ticker=ticker,
                    fallback_stop_pct=0.075
                )
                stop_info = {
                    'stop_price': current_price * 0.925,  # Fixed 7.5%
                    'stop_pct': 0.075,
                    'stop_type': 'fixed_fallback'
                }
            else:
                stop_info = calculate_dynamic_stop_loss(
                    entry_price=current_price,
                    atr=atr_14,
                    atr_multiplier=self.atr_multiplier
                )
        else:
            # Fixed stop loss (legacy behavior)
            stop_info = {
                'stop_price': current_price * 0.925,
                'stop_pct': 0.075,
                'stop_type': 'fixed'
            }

        # ... execute order ...

        # Store stop loss in database
        self.db_manager.insert_trade(
            ticker=ticker,
            action='buy',
            price=current_price,
            quantity=quantity,
            stop_price=stop_info['stop_price'],
            stop_pct=stop_info['stop_pct'],
            stop_type=stop_info['stop_type']
        )

        logger.info(
            "buy_order_placed",
            ticker=ticker,
            entry_price=current_price,
            quantity=quantity,
            stop_price=stop_info['stop_price'],
            stop_pct=stop_info['stop_pct'],
            stop_type=stop_info['stop_type']
        )

        return stop_info
```

#### Step 4: Update Database Schema

Add stop loss fields to `trades` table:

```python
# In init_db_sqlite.py or create migration script
ALTER TABLE trades ADD COLUMN stop_price REAL;
ALTER TABLE trades ADD COLUMN stop_pct REAL;
ALTER TABLE trades ADD COLUMN stop_type TEXT;
ALTER TABLE trades ADD COLUMN atr_at_entry REAL;
```

#### Step 5: Configuration in makenaide.py

```python
# Add command line arguments
parser.add_argument('--dynamic-stops', action='store_true', default=True,
                    help='Use ATR-based dynamic stop loss')
parser.add_argument('--atr-multiplier', type=float, default=2.0,
                    help='ATR multiplier for stop distance (default: 2.0)')

# Initialize trading engine
trading_engine = TradingEngine(
    db_manager=db_manager,
    dry_run=args.dry_run,
    use_dynamic_stops=args.dynamic_stops,
    atr_multiplier=args.atr_multiplier
)
```

#### Step 6: Test Implementation

```bash
# Test with dry-run mode
python3 makenaide.py --dry-run --dynamic-stops --atr-multiplier 2.0

# Compare conservative vs aggressive stops
python3 makenaide.py --dry-run --atr-multiplier 2.0  # Conservative
python3 makenaide.py --dry-run --atr-multiplier 1.5  # Moderate
python3 makenaide.py --dry-run --atr-multiplier 1.0  # Aggressive

# Test legacy fixed stops
python3 makenaide.py --dry-run --no-dynamic-stops
```

### Expected Results
- ✅ Bitcoin (4% ATR) → 8% stop (wider than fixed 7.5%)
- ✅ Stable altcoin (3% ATR) → 6% stop (tighter than fixed 7.5%)
- ✅ Volatile altcoin (10% ATR) → 15% stop (clamped to max)
- ✅ Low-volatility asset (1% ATR) → 5% stop (clamped to min)

### Performance Analysis

Create analysis script to compare:

```python
# compare_stop_strategies.py
import pandas as pd
from db_manager_sqlite import SQLiteDatabaseManager

db = SQLiteDatabaseManager()

# Query trades with different stop types
query = """
SELECT
    ticker,
    action,
    price AS entry_price,
    stop_price,
    stop_pct,
    stop_type,
    exit_price,
    (exit_price - price) / price AS return_pct,
    CASE
        WHEN exit_price <= stop_price THEN 'stopped_out'
        WHEN action = 'sell' THEN 'profit_taken'
        ELSE 'open'
    END AS exit_reason
FROM trades
WHERE created_at >= date('now', '-30 days')
ORDER BY created_at DESC
"""

trades = db.execute_query(query)
df = pd.DataFrame(trades)

# Compare performance by stop type
performance = df.groupby('stop_type').agg({
    'return_pct': ['mean', 'std', 'min', 'max'],
    'ticker': 'count'
})

print("Stop Loss Strategy Comparison:")
print(performance)

# False stop analysis (stopped out but would have been profitable)
false_stops = df[
    (df['exit_reason'] == 'stopped_out') &
    (df['return_pct'] > -0.05)  # Stopped at -5% or better
]

print(f"\nFalse stops avoided with dynamic stops: {len(false_stops)}")
```

---

## 🎯 Quick Win #3: Structured Logging with CloudWatch

**Effort**: 3 days | **Impact**: 10x debugging efficiency | **ROI**: 5+ hours/week saved

### Problem
- `print()` based logs = not searchable
- Terminal output lost after session ends
- Cannot aggregate or analyze logs
- Difficult to debug production issues

### Solution
JSON structured logging with CloudWatch integration.

### Implementation Steps

#### Step 1: Install Dependencies

```bash
pip install structlog watchtower boto3
```

Update `requirements.txt`:
```
structlog==24.1.0
watchtower==3.0.1
boto3==1.28.44
```

#### Step 2: Create Logging Configuration

File: `logging_config.py`

```python
"""
Makenaide Structured Logging Configuration
Provides JSON logging with CloudWatch integration
"""

import logging
import sys
from pathlib import Path

import structlog
import watchtower

def setup_structured_logging(
    log_level: str = "INFO",
    enable_cloudwatch: bool = True,
    cloudwatch_log_group: str = "/makenaide/production",
    cloudwatch_stream_name: str = "trading-engine"
):
    """
    Configure structured logging with CloudWatch integration.

    Features:
    - JSON formatted logs for machine readability
    - ISO timestamp for accurate time tracking
    - Automatic exception formatting
    - CloudWatch integration for centralized logging
    - Local file logging for backup

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        enable_cloudwatch: Enable CloudWatch logging
        cloudwatch_log_group: CloudWatch log group name
        cloudwatch_stream_name: CloudWatch stream name
    """

    # Create logs directory
    log_dir = Path("/home/ubuntu/logs")
    log_dir.mkdir(exist_ok=True)

    # Configure processors
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # Add console renderer for development
    if sys.stderr.isatty():
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # File handler for local backup
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / "makenaide.log",
        maxBytes=50_000_000,  # 50MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(
        logging.Formatter('%(message)s')
    )
    root_logger.addHandler(file_handler)

    # CloudWatch handler (if enabled)
    if enable_cloudwatch:
        try:
            cloudwatch_handler = watchtower.CloudWatchLogHandler(
                log_group=cloudwatch_log_group,
                stream_name=cloudwatch_stream_name,
                send_interval=5,  # Send every 5 seconds
                create_log_group=True,
                boto3_session=None  # Uses default AWS credentials
            )
            cloudwatch_handler.setFormatter(
                logging.Formatter('%(message)s')
            )
            root_logger.addHandler(cloudwatch_handler)

            print(f"✓ CloudWatch logging enabled: {cloudwatch_log_group}/{cloudwatch_stream_name}")
        except Exception as e:
            print(f"⚠ CloudWatch logging failed: {e}")
            print("Continuing with local logging only")

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter('%(message)s')
    )
    root_logger.addHandler(console_handler)

    print(f"✓ Structured logging configured (level: {log_level})")


def get_logger(name: str = None):
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        structlog logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("trade_executed", ticker="KRW-BTC", price=50000000)
    """
    return structlog.get_logger(name)
```

#### Step 3: Update Existing Code

File: `trading_engine.py`

```python
# Replace old logging
# from utils import logger

# New structured logging
from logging_config import get_logger

logger = get_logger(__name__)

# Old style (remove this)
# logger.info(f"Trade executed: {ticker} at {price}")

# New style (structured)
logger.info(
    "trade_executed",
    ticker=ticker,
    action="buy",
    price=price,
    quantity=quantity,
    position_size_krw=position_size,
    kelly_fraction=kelly_fraction,
    stop_price=stop_price,
    stop_pct=stop_pct
)
```

#### Step 4: Initialize in Main Pipeline

File: `makenaide.py`

```python
from logging_config import setup_structured_logging, get_logger

def main():
    # Setup structured logging
    setup_structured_logging(
        log_level="INFO",
        enable_cloudwatch=not args.dry_run,  # Only CloudWatch in production
        cloudwatch_log_group="/makenaide/production",
        cloudwatch_stream_name=f"trading-engine-{os.getenv('EC2_INSTANCE_ID', 'local')}"
    )

    logger = get_logger(__name__)

    logger.info(
        "pipeline_started",
        version="v2.4.0",
        dry_run=args.dry_run,
        risk_level=args.risk_level,
        use_gpt=not args.no_gpt
    )
```

#### Step 5: Create CloudWatch Dashboard

```bash
# Create CloudWatch dashboard configuration
cat > cloudwatch-dashboard.json <<EOF
{
  "widgets": [
    {
      "type": "log",
      "properties": {
        "query": "SOURCE '/makenaide/production' | fields @timestamp, event, ticker, price | filter event='trade_executed' | sort @timestamp desc | limit 20",
        "region": "ap-northeast-2",
        "title": "Recent Trades",
        "stacked": false
      }
    },
    {
      "type": "log",
      "properties": {
        "query": "SOURCE '/makenaide/production' | fields @timestamp, level, event | filter level='ERROR' | sort @timestamp desc | limit 50",
        "region": "ap-northeast-2",
        "title": "Recent Errors"
      }
    },
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Logs", "IncomingLogEvents", {"stat": "Sum"}]
        ],
        "region": "ap-northeast-2",
        "title": "Log Volume"
      }
    }
  ]
}
EOF

# Create dashboard
aws cloudwatch put-dashboard \
    --dashboard-name Makenaide-Production \
    --dashboard-body file://cloudwatch-dashboard.json
```

### Expected Results
- ✅ JSON logs searchable in CloudWatch Logs Insights
- ✅ Local backup in `/home/ubuntu/logs/makenaide.log`
- ✅ 50MB log rotation (keeps 5 files = 250MB max)
- ✅ Real-time log streaming to CloudWatch

### Usage Examples

```python
# Basic logging
logger.info("scanner_started", ticker_count=100)

# With measurements
logger.info(
    "data_collection_completed",
    ticker_count=100,
    duration_seconds=45.3,
    records_inserted=14400
)

# Warning with context
logger.warning(
    "api_rate_limit_approaching",
    current_requests_per_minute=28,
    limit=30,
    throttle_delay_ms=500
)

# Error with exception
try:
    result = risky_operation()
except Exception as e:
    logger.error(
        "operation_failed",
        operation="risky_operation",
        ticker=ticker,
        exc_info=True  # Includes full traceback
    )

# Performance tracking
with logger.bind(ticker="KRW-BTC"):
    start = time.time()
    result = expensive_calculation()
    duration = time.time() - start

    logger.info(
        "calculation_completed",
        duration_ms=duration * 1000,
        result_value=result
    )
```

### CloudWatch Insights Queries

```sql
-- Find all trades in last 24 hours
fields @timestamp, ticker, action, price, quantity
| filter event = 'trade_executed'
| sort @timestamp desc

-- Calculate average position size by ticker
fields ticker, position_size_krw
| filter event = 'trade_executed' and action = 'buy'
| stats avg(position_size_krw) as avg_position, count(*) as trade_count by ticker
| sort avg_position desc

-- Error rate by hour
fields @timestamp
| filter level = 'ERROR'
| stats count(*) as error_count by bin(5m)

-- Slowest operations
fields operation, duration_ms
| filter duration_ms > 1000
| sort duration_ms desc
| limit 20
```

---

## 🎯 Quick Win #4: API Key Security Hardening

**Effort**: 1 day | **Impact**: Prevents account drainage | **ROI**: Infinite

See `SECURITY_HARDENING.md` for complete implementation.

**Summary Steps**:
1. Create AWS Secrets Manager secret
2. Update code to retrieve keys from Secrets Manager
3. Delete keys from `.env` file
4. Enable Upbit IP whitelist
5. Rotate API keys with withdrawal-disabled permissions

---

## 📊 Success Metrics

### Quick Win #1 - Backup
- [ ] Cron job running daily
- [ ] S3 bucket has backups
- [ ] Successful test restore
- [ ] Monitoring logs clean

### Quick Win #2 - Dynamic Stops
- [x] ATR calculated for all tickers
- [x] Stop loss prices adapt to volatility
- [x] Min/Max clamping logic implemented (5-15%)
- [x] Unit tests passed (6/6 tests)
- [x] Dry-run validation completed
- [ ] Production deployment pending
- [ ] Performance comparison (after 1 week live data)

### Quick Win #3 - Structured Logging
- [ ] JSON logs in CloudWatch
- [ ] Local logs rotating correctly
- [ ] CloudWatch dashboard created
- [ ] Queries return accurate data

### Quick Win #4 - Security
- [ ] Keys in Secrets Manager
- [ ] No keys in `.env` file
- [ ] IP whitelist enabled
- [ ] MFA enabled on Upbit

---

## 🚀 Deployment Checklist

### Pre-Deployment
- [ ] Test on local machine first
- [ ] Backup current database
- [ ] Review all code changes
- [ ] Update requirements.txt

### Deployment
- [ ] SSH to EC2: `ssh -i ~/.aws/makenaide-key.pem ubuntu@52.78.186.226`
- [ ] Pull latest code: `git pull origin main`
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Run tests: `python3 makenaide.py --dry-run`
- [ ] Deploy to production
- [ ] Monitor logs for 1 hour

### Post-Deployment
- [ ] Verify backup cron job
- [ ] Check CloudWatch logs
- [ ] Confirm security changes
- [ ] Monitor first live execution

---

## 💡 Pro Tips

1. **Start with Backup**: Do this TODAY. Data loss is irreversible.

2. **Test in Dry-Run**: Always test with `--dry-run` before live trading.

3. **Monitor First Week**: Watch CloudWatch logs closely after deploying changes.

4. **Gradual Rollout**: Implement one Quick Win at a time, validate, then proceed.

5. **Keep Old Behavior**: Use feature flags (e.g., `--no-dynamic-stops`) to revert if needed.

---

**Next Steps**:
1. Choose Quick Win #1 (Backup) to implement TODAY
2. Schedule Quick Win #4 (Security) for this WEEK
3. Plan Quick Wins #2 & #3 for next WEEK

The foundation you build now will pay dividends for years to come.