-- ===================================================================
-- CRYPTO TRADING SYSTEM DATABASE SCHEMA
-- ===================================================================
-- CRITICAL: NO underscores in table/column names - camelCase only
-- This schema supports real-time indicator calculations and alerts
-- for 1000+ Solana tokens with 15m, 1h, 4h timeframes
-- ===================================================================

-- 1. TRACKED TOKENS (Main Registry with Soft Delete)
-- ===================================================================
CREATE TABLE trackedtokens (
    trackedtokenid BIGSERIAL PRIMARY KEY,
    tokenaddress CHAR(44) NOT NULL UNIQUE,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    pairaddress CHAR(44) NOT NULL,
    status INTEGER DEFAULT 1 CHECK (status IN (1, 2)), -- 1=active, 2=disabled
    enabledat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    disabledat TIMESTAMP WITH TIME ZONE,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    addedby VARCHAR(100), -- Track who added the token
    disabledby VARCHAR(100), -- Track who disabled it
    metadata JSONB -- Store additional info like initial price, market cap
);

-- 2. TIMEFRAME METADATA (Scheduler)
-- ===================================================================
CREATE TABLE timeframemetadata (
    id BIGSERIAL PRIMARY KEY,
    tokenaddress CHAR(44) NOT NULL,
    pairaddress CHAR(44) NOT NULL,
    timeframe CHAR(3) NOT NULL CHECK (timeframe IN ('15m', '1h', '4h')),
    nextfetchat TIMESTAMP WITH TIME ZONE NOT NULL,
    lastfetchedat TIMESTAMP WITH TIME ZONE,
    lastsuccessfullfetchat TIMESTAMP WITH TIME ZONE,
    fetchintervalseconds INTEGER DEFAULT 900,
    consecutivefailures INTEGER DEFAULT 0,
    isactive BOOLEAN DEFAULT TRUE,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tokenaddress, pairaddress, timeframe)
);

-- 3. OHLCV DATA with Validation
-- ===================================================================
CREATE TABLE ohlcvdetails (
    id BIGSERIAL PRIMARY KEY,
    timeframeid BIGINT NOT NULL REFERENCES timeframemetadata(id),
    tokenaddress CHAR(44) NOT NULL,
    pairaddress CHAR(44) NOT NULL,
    timeframe CHAR(3) NOT NULL,
    unixtime BIGINT NOT NULL,
    timebucket BIGINT NOT NULL, -- For aggregation queries
    openprice DECIMAL(20,8) NOT NULL,
    highprice DECIMAL(20,8) NOT NULL,
    lowprice DECIMAL(20,8) NOT NULL,
    closeprice DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,4) NOT NULL,
    -- Validation columns (always populated for debugging)
    vwapvalue DECIMAL(20,8),
    ema21value DECIMAL(20,8),
    ema34value DECIMAL(20,8),
    iscomplete BOOLEAN DEFAULT TRUE, -- Mark incomplete candles
    datasource VARCHAR(20) DEFAULT 'api', -- 'api' or 'aggregated'
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tokenaddress, timeframe, unixtime),
    CHECK (highprice >= lowprice),
    CHECK (highprice >= openprice AND highprice >= closeprice),
    CHECK (lowprice <= openprice AND lowprice <= closeprice)
);

-- 4. DYNAMIC INDICATOR STATES
-- ===================================================================
CREATE TABLE indicatorstates (
    tokenaddress CHAR(44),
    timeframe CHAR(3),
    indicatorkey VARCHAR(20), -- 'ema_21', 'ema_34', etc.
    currentvalue DECIMAL(20,8),
    previousvalue DECIMAL(20,8),
    candlecount INTEGER DEFAULT 0, -- Track how many candles processed
    lastupdatedunix BIGINT,
    iswarmedup BOOLEAN DEFAULT FALSE, -- TRUE when enough data for accurate calculation
    PRIMARY KEY (tokenaddress, timeframe, indicatorkey)
);

-- 5. VWAP SESSIONS
-- ===================================================================
CREATE TABLE vwapsessions (
    tokenaddress CHAR(44),
    timeframe CHAR(3),
    sessionstartunix BIGINT,
    sessionendunix BIGINT,
    cumulativepv DECIMAL(30,8),
    cumulativevolume DECIMAL(30,8),
    lastcandleunix BIGINT,
    currentvwap DECIMAL(20,8),
    highvwap DECIMAL(20,8), -- Session high VWAP
    lowvwap DECIMAL(20,8),  -- Session low VWAP
    candlecount INTEGER DEFAULT 0,
    PRIMARY KEY (tokenaddress, timeframe, sessionstartunix)
);

-- 6. FLEXIBLE INDICATOR CONFIGURATIONS
-- ===================================================================
CREATE TABLE indicatorconfigs (
    configid SERIAL PRIMARY KEY,
    tokenaddress CHAR(44), -- NULL for global configs
    timeframe CHAR(3), -- NULL for all-timeframe configs
    indicatortype VARCHAR(20) NOT NULL, -- 'ema', 'vwap', 'sma', etc.
    parameters JSONB NOT NULL, -- Flexible parameters: {"period": 21} or {"periods": [21, 34]}
    configname VARCHAR(50), -- User-friendly name like "EMA_21_34_Cross"
    description TEXT,
    isactive BOOLEAN DEFAULT TRUE,
    priority INTEGER DEFAULT 100, -- Lower = higher priority
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    createdby VARCHAR(100),
    -- Allow multiple configs per token/timeframe/type combination
    UNIQUE(tokenaddress, timeframe, indicatortype, configname)
);

-- 7. ALERT CONDITIONS
-- ===================================================================
CREATE TABLE alertconditions (
    conditionid SERIAL PRIMARY KEY,
    tokenaddress CHAR(44),
    timeframe CHAR(3),
    conditionname VARCHAR(100),
    conditiontype VARCHAR(50), -- 'ema_cross_bullish', 'complex'
    conditionjson JSONB, -- Complex condition logic
    alertmessage TEXT,
    telegramchatid VARCHAR(100),
    cooldownminutes INTEGER DEFAULT 60, -- Prevent spam
    lasttriggeredat TIMESTAMP WITH TIME ZONE,
    isactive BOOLEAN DEFAULT TRUE,
    createdat TIMESTAMP DEFAULT NOW()
);

-- 8. ALERT HISTORY (for tracking/debugging)
-- ===================================================================
CREATE TABLE alerthistory (
    alertid SERIAL PRIMARY KEY,
    tokenaddress CHAR(44),
    timeframe CHAR(3),
    conditionid INTEGER REFERENCES alertconditions(conditionid),
    triggeredat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    indicatorvalues JSONB, -- Snapshot of all indicators at trigger time
    message TEXT,
    sentstatus BOOLEAN DEFAULT FALSE,
    errordetails TEXT
);

-- 9. API CREDIT TRACKING
-- ===================================================================
CREATE TABLE apicredittracker (
    id SERIAL PRIMARY KEY,
    apiname VARCHAR(50), -- 'birdeye'
    creditsused INTEGER,
    endpoint VARCHAR(200),
    tokencount INTEGER,
    usedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dailytotal INTEGER, -- Running total for the day
    monthlytotal INTEGER -- Running total for the month
);

-- 10. AGGREGATION TRACKING (for monitoring completeness)
-- ===================================================================
CREATE TABLE aggregationtracking (
    id SERIAL PRIMARY KEY,
    tokenaddress CHAR(44),
    sourcetimeframe CHAR(3), -- '15m' for source data
    targettimeframe CHAR(3), -- '1h' or '4h' for target
    periodstartunix BIGINT,
    candlesrequired INTEGER,
    candlescollected INTEGER,
    iscomplete BOOLEAN DEFAULT FALSE,
    aggregatedat TIMESTAMP WITH TIME ZONE,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tokenaddress, sourcetimeframe, targettimeframe, periodstartunix)
);

-- ===================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ===================================================================

-- OHLCV Data Indexes
CREATE INDEX idx_ohlcv_token_time ON ohlcvdetails(tokenaddress, timeframe, unixtime DESC);
CREATE INDEX idx_ohlcv_timebucket ON ohlcvdetails(tokenaddress, timeframe, timebucket);
CREATE INDEX idx_ohlcv_incomplete ON ohlcvdetails(iscomplete) WHERE iscomplete = FALSE;
CREATE INDEX idx_ohlcv_timeframeid ON ohlcvdetails(timeframeid);

-- Scheduler Indexes
CREATE INDEX idx_timeframe_next_fetch ON timeframemetadata(nextfetchat) WHERE isactive = TRUE;
CREATE INDEX idx_timeframe_active ON timeframemetadata(tokenaddress, isactive);
CREATE INDEX idx_timeframe_failures ON timeframemetadata(consecutivefailures) WHERE consecutivefailures > 3;

-- Indicator Indexes
CREATE INDEX idx_indicator_states ON indicatorstates(tokenaddress, timeframe);
CREATE INDEX idx_indicator_warmedup ON indicatorstates(tokenaddress, timeframe, iswarmedup) WHERE iswarmedup = TRUE;

-- VWAP Indexes
CREATE INDEX idx_vwap_sessions ON vwapsessions(tokenaddress, timeframe, sessionstartunix);
CREATE INDEX idx_vwap_current ON vwapsessions(tokenaddress, timeframe) WHERE lastcandleunix IS NOT NULL;

-- Alert Indexes
CREATE INDEX idx_alert_history ON alerthistory(tokenaddress, triggeredat DESC);
CREATE INDEX idx_alert_conditions_active ON alertconditions(tokenaddress, timeframe, isactive) WHERE isactive = TRUE;
CREATE INDEX idx_alert_cooldown ON alertconditions(lasttriggeredat);

-- Token Tracking Indexes
CREATE INDEX idx_tracked_active ON trackedtokens(status) WHERE status = 1;
CREATE INDEX idx_tracked_token_address ON trackedtokens(tokenaddress);

-- Aggregation Indexes
CREATE INDEX idx_aggregation_pending ON aggregationtracking(tokenaddress, targettimeframe, iscomplete) WHERE iscomplete = FALSE;
CREATE INDEX idx_aggregation_period ON aggregationtracking(periodstartunix);

-- API Usage Indexes
CREATE INDEX idx_api_credits_daily ON apicredittracker(apiname, usedat::date);
CREATE INDEX idx_api_credits_monthly ON apicredittracker(apiname, EXTRACT(YEAR FROM usedat), EXTRACT(MONTH FROM usedat));

-- ===================================================================
-- PERFORMANCE VIEWS FOR COMMON QUERIES
-- ===================================================================

-- View for active tokens with latest candle data
CREATE VIEW activetokenslastcandle AS
SELECT 
    tt.tokenaddress,
    tt.symbol,
    tt.name,
    tt.pairaddress,
    o.timeframe,
    o.unixtime as lastcandleunix,
    o.closeprice,
    o.volume,
    o.vwapvalue,
    o.ema21value,
    o.ema34value,
    tm.nextfetchat,
    tm.consecutivefailures
FROM trackedtokens tt
JOIN timeframemetadata tm ON tt.tokenaddress = tm.tokenaddress
LEFT JOIN LATERAL (
    SELECT * FROM ohlcvdetails 
    WHERE tokenaddress = tt.tokenaddress 
    AND timeframe = tm.timeframe
    ORDER BY unixtime DESC 
    LIMIT 1
) o ON true
WHERE tt.status = 1 
AND tm.isactive = TRUE;

-- View for indicator cross detection
CREATE VIEW indicatorcrosses AS
SELECT 
    tokenaddress,
    timeframe,
    CASE 
        WHEN short.currentvalue > long.currentvalue 
         AND short.previousvalue <= long.previousvalue THEN 'bullish_cross'
        WHEN short.currentvalue < long.currentvalue 
         AND short.previousvalue >= long.previousvalue THEN 'bearish_cross'
        ELSE 'no_cross'
    END as crosstype,
    short.currentvalue as ema21current,
    short.previousvalue as ema21previous,
    long.currentvalue as ema34current,
    long.previousvalue as ema34previous,
    short.lastupdatedunix
FROM indicatorstates short
JOIN indicatorstates long ON short.tokenaddress = long.tokenaddress 
    AND short.timeframe = long.timeframe
WHERE short.indicatorkey = 'ema_21'
AND long.indicatorkey = 'ema_34'
AND short.iswarmedup = TRUE
AND long.iswarmedup = TRUE;

-- ===================================================================
-- FUNCTIONS FOR AUTOMATED MAINTENANCE
-- ===================================================================

-- Function to clean old API credit tracking data
CREATE OR REPLACE FUNCTION cleanup_old_api_credits()
RETURNS void AS $$
BEGIN
    DELETE FROM apicredittracker 
    WHERE usedat < NOW() - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- Function to reset daily/monthly credit totals
CREATE OR REPLACE FUNCTION reset_credit_totals()
RETURNS void AS $$
BEGIN
    -- Reset daily totals at midnight
    UPDATE apicredittracker 
    SET dailytotal = 0 
    WHERE usedat::date < CURRENT_DATE;
    
    -- Reset monthly totals on first day of month
    UPDATE apicredittracker 
    SET monthlytotal = 0 
    WHERE EXTRACT(MONTH FROM usedat) < EXTRACT(MONTH FROM CURRENT_DATE)
    OR EXTRACT(YEAR FROM usedat) < EXTRACT(YEAR FROM CURRENT_DATE);
END;
$$ LANGUAGE plpgsql;

-- ===================================================================
-- COMMENTS FOR DOCUMENTATION
-- ===================================================================

COMMENT ON TABLE trackedtokens IS 'Main registry of tokens being tracked with soft delete support';
COMMENT ON TABLE timeframemetadata IS 'Scheduler metadata for each token/timeframe combination';
COMMENT ON TABLE ohlcvdetails IS 'Raw and aggregated OHLCV data with validation columns';
COMMENT ON TABLE indicatorstates IS 'Current and previous indicator values for cross detection';
COMMENT ON TABLE vwapsessions IS 'VWAP accumulation data with session boundaries';
COMMENT ON TABLE indicatorconfigs IS 'User-configurable indicator parameters per token';
COMMENT ON TABLE alertconditions IS 'Alert conditions and notification settings';
COMMENT ON TABLE alerthistory IS 'Historical record of triggered alerts';
COMMENT ON TABLE apicredittracker IS 'API usage and credit consumption tracking';
COMMENT ON TABLE aggregationtracking IS 'Monitor aggregation completeness and gaps';

COMMENT ON COLUMN ohlcvdetails.timebucket IS 'Pre-calculated bucket for efficient aggregation queries';
COMMENT ON COLUMN ohlcvdetails.vwapvalue IS 'Validation column - always populated for debugging';
COMMENT ON COLUMN ohlcvdetails.ema21value IS 'Validation column - EMA21 at this candle';
COMMENT ON COLUMN ohlcvdetails.ema34value IS 'Validation column - EMA34 at this candle';
COMMENT ON COLUMN ohlcvdetails.datasource IS 'api=fetched from BirdEye, aggregated=derived from 15m data';

COMMENT ON COLUMN indicatorstates.candlecount IS 'Number of candles processed - used for initialization logic';
COMMENT ON COLUMN indicatorstates.iswarmedup IS 'TRUE when indicator has enough data for accurate calculation';

COMMENT ON COLUMN timeframemetadata.consecutivefailures IS 'Circuit breaker - stop fetching after 5 failures';
COMMENT ON COLUMN alertconditions.cooldownminutes IS 'Prevent spam - minimum time between alerts';