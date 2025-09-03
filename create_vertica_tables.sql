-- Vertica Table Creation Script for Polygon Market Data
-- Based on Polygon.io documentation for trades and quotes

-- =====================================================
-- 1. TRADES TABLE
-- =====================================================
-- Schema based on: https://polygon.io/docs/flat-files/stocks/trades

CREATE TABLE IF NOT EXISTS polygon_trades (
    -- Primary identifiers
    ticker VARCHAR(20) NOT NULL,
    id VARCHAR(50) NOT NULL,
    
    -- Timestamps (nanosecond precision)
    participant_timestamp BIGINT NOT NULL,  -- Exchange timestamp (nanoseconds)
    sip_timestamp BIGINT NOT NULL,          -- SIP timestamp (nanoseconds)
    trf_timestamp BIGINT,                   -- TRF timestamp (nanoseconds)
    
    -- Trade details
    price DECIMAL(18,6) NOT NULL,           -- Trade price
    size BIGINT NOT NULL,                   -- Trade size/volume
    
    -- Exchange and routing information
    exchange INTEGER NOT NULL,              -- Exchange ID
    tape INTEGER NOT NULL,                  -- Tape (1=A/NYSE, 2=B/ARCA, 3=C/NASDAQ)
    trf_id INTEGER,                         -- Trade Reporting Facility ID
    
    -- Trade conditions and corrections
    conditions VARCHAR(100),                -- Condition codes (comma-separated)
    correction INTEGER,                     -- Trade correction indicator
    
    -- Sequence and ordering
    sequence_number BIGINT NOT NULL,        -- Sequence number per ticker
    
    -- Metadata
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),  -- When inserted into Vertica
    kafka_offset BIGINT,                    -- Kafka message offset
    kafka_partition INTEGER,                -- Kafka partition
    
    -- Constraints
    PRIMARY KEY (ticker, id, exchange, participant_timestamp)
)
ORDER BY ticker, participant_timestamp
SEGMENTED BY HASH(ticker) ALL NODES
PARTITION BY (DATE_TRUNC('day', FROM_UNIXTIME(participant_timestamp/1000000000)));

-- Create projection for time-series queries
CREATE PROJECTION IF NOT EXISTS polygon_trades_time_proj AS 
SELECT * FROM polygon_trades 
ORDER BY participant_timestamp, ticker
SEGMENTED BY HASH(ticker) ALL NODES;

-- =====================================================
-- 2. QUOTES TABLE  
-- =====================================================
-- Schema based on: https://polygon.io/docs/flat-files/stocks/quotes

CREATE TABLE IF NOT EXISTS polygon_quotes (
    -- Primary identifiers
    ticker VARCHAR(20) NOT NULL,
    
    -- Timestamps (nanosecond precision)
    participant_timestamp BIGINT NOT NULL,  -- Exchange timestamp (nanoseconds)
    sip_timestamp BIGINT NOT NULL,          -- SIP timestamp (nanoseconds)
    trf_timestamp BIGINT,                   -- TRF timestamp (nanoseconds)
    
    -- Bid information
    bid_price DECIMAL(18,6),                -- Bid price
    bid_size BIGINT,                        -- Bid size
    bid_exchange INTEGER,                   -- Bid exchange ID
    
    -- Ask information
    ask_price DECIMAL(18,6),                -- Ask price
    ask_size BIGINT,                        -- Ask size
    ask_exchange INTEGER,                   -- Ask exchange ID
    
    -- Market information
    tape INTEGER NOT NULL,                  -- Tape (1=A/NYSE, 2=B/ARCA, 3=C/NASDAQ)
    
    -- Conditions and indicators
    conditions VARCHAR(100),                -- Condition codes (comma-separated)
    indicators VARCHAR(100),                -- Indicator codes (comma-separated)
    
    -- Sequence and ordering
    sequence_number BIGINT NOT NULL,        -- Sequence number per ticker
    
    -- Metadata
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),  -- When inserted into Vertica
    kafka_offset BIGINT,                    -- Kafka message offset
    kafka_partition INTEGER,                -- Kafka partition
    
    -- Constraints
    PRIMARY KEY (ticker, participant_timestamp, sequence_number)
)
ORDER BY ticker, participant_timestamp
SEGMENTED BY HASH(ticker) ALL NODES
PARTITION BY (DATE_TRUNC('day', FROM_UNIXTIME(participant_timestamp/1000000000)));

-- Create projection for time-series queries
CREATE PROJECTION IF NOT EXISTS polygon_quotes_time_proj AS 
SELECT * FROM polygon_quotes 
ORDER BY participant_timestamp, ticker
SEGMENTED BY HASH(ticker) ALL NODES;

-- =====================================================
-- 3. TRADING SIGNALS TABLE (Output from Microstructure UDx)
-- =====================================================

CREATE TABLE IF NOT EXISTS trading_signals (
    -- Primary identifiers
    ticker VARCHAR(20) NOT NULL,
    signal_timestamp BIGINT NOT NULL,       -- Timestamp when signal was generated
    
    -- Source trade/quote reference
    source_timestamp BIGINT NOT NULL,       -- Original market data timestamp
    source_type VARCHAR(10) NOT NULL,       -- 'TRADE' or 'QUOTE'
    
    -- Microstructure metrics (from C++ UDx)
    standardized_return DECIMAL(18,8),      -- Standardized return
    lee_mykland_stat DECIMAL(18,8),         -- Lee-Mykland jump test statistic
    bns_stat DECIMAL(18,8),                 -- BNS statistic
    trade_intensity_zscore DECIMAL(18,8),   -- Trade intensity Z-score
    acd_surprise DECIMAL(18,8),             -- ACD duration surprise
    conditional_duration DECIMAL(18,8),     -- ACD conditional duration
    
    -- Jump detection
    jump_detected BOOLEAN NOT NULL DEFAULT FALSE,
    jump_threshold DECIMAL(18,8),           -- Threshold used for jump detection
    
    -- Trading signals
    trading_signal VARCHAR(10) NOT NULL,    -- 'BUY', 'SELL', 'HOLD'
    signal_strength DECIMAL(8,4),           -- Signal strength (0-1)
    confidence_level DECIMAL(8,4),          -- Confidence level (0-1)
    
    -- Risk metrics
    volatility_estimate DECIMAL(18,8),      -- Current volatility estimate
    garch_variance DECIMAL(18,8),           -- GARCH conditional variance
    
    -- Metadata
    processing_timestamp TIMESTAMP DEFAULT NOW(),  -- When processed by UDx
    udx_version VARCHAR(20),                -- Version of microstructure UDx
    model_parameters TEXT,                  -- JSON of model parameters used
    
    -- Constraints
    PRIMARY KEY (ticker, signal_timestamp, source_timestamp)
)
ORDER BY ticker, signal_timestamp
SEGMENTED BY HASH(ticker) ALL NODES
PARTITION BY (DATE_TRUNC('hour', FROM_UNIXTIME(signal_timestamp/1000000000)));

-- =====================================================
-- 4. ENRICHED MARKET DATA VIEW
-- =====================================================
-- Combines raw market data with trading signals for MCP server

CREATE VIEW IF NOT EXISTS enriched_market_data AS
SELECT 
    -- Trade data
    t.ticker as symbol,
    t.participant_timestamp as timestamp_ns,
    t.price,
    t.size,
    t.exchange,
    'T' as event_type,
    
    -- Trading signals
    s.standardized_return,
    s.lee_mykland_stat,
    s.bns_stat,
    s.trade_intensity_zscore,
    s.acd_surprise,
    s.conditional_duration,
    s.jump_detected,
    s.trading_signal,
    s.signal_strength,
    s.confidence_level,
    s.volatility_estimate,
    
    -- Timestamps
    s.processing_timestamp as processed_at,
    t.ingestion_timestamp as ingested_at

FROM polygon_trades t
LEFT JOIN trading_signals s ON (
    t.ticker = s.ticker 
    AND t.participant_timestamp = s.source_timestamp
    AND s.source_type = 'TRADE'
)

UNION ALL

SELECT 
    -- Quote data  
    q.ticker as symbol,
    q.participant_timestamp as timestamp_ns,
    (q.bid_price + q.ask_price) / 2 as price,  -- Mid price
    q.bid_size + q.ask_size as size,           -- Total size
    COALESCE(q.bid_exchange, q.ask_exchange) as exchange,
    'Q' as event_type,
    
    -- Trading signals
    s.standardized_return,
    s.lee_mykland_stat,
    s.bns_stat,
    s.trade_intensity_zscore,
    s.acd_surprise,
    s.conditional_duration,
    s.jump_detected,
    s.trading_signal,
    s.signal_strength,
    s.confidence_level,
    s.volatility_estimate,
    
    -- Timestamps
    s.processing_timestamp as processed_at,
    q.ingestion_timestamp as ingested_at

FROM polygon_quotes q
LEFT JOIN trading_signals s ON (
    q.ticker = s.ticker 
    AND q.participant_timestamp = s.source_timestamp
    AND s.source_type = 'QUOTE'
);

-- =====================================================
-- 5. INDEXES FOR PERFORMANCE
-- =====================================================

-- Indexes for trades table
CREATE INDEX IF NOT EXISTS idx_trades_ticker_time ON polygon_trades(ticker, participant_timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_exchange ON polygon_trades(exchange);
CREATE INDEX IF NOT EXISTS idx_trades_ingestion ON polygon_trades(ingestion_timestamp);

-- Indexes for quotes table  
CREATE INDEX IF NOT EXISTS idx_quotes_ticker_time ON polygon_quotes(ticker, participant_timestamp);
CREATE INDEX IF NOT EXISTS idx_quotes_exchange ON polygon_quotes(bid_exchange, ask_exchange);
CREATE INDEX IF NOT EXISTS idx_quotes_ingestion ON polygon_quotes(ingestion_timestamp);

-- Indexes for trading signals
CREATE INDEX IF NOT EXISTS idx_signals_ticker_time ON trading_signals(ticker, signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_jump ON trading_signals(jump_detected);
CREATE INDEX IF NOT EXISTS idx_signals_trading ON trading_signals(trading_signal);
CREATE INDEX IF NOT EXISTS idx_signals_processing ON trading_signals(processing_timestamp);

-- =====================================================
-- 6. UTILITY FUNCTIONS
-- =====================================================

-- Function to convert nanosecond timestamp to readable format
CREATE OR REPLACE FUNCTION ns_to_timestamp(ns_timestamp BIGINT)
RETURN TIMESTAMP
AS BEGIN
    RETURN FROM_UNIXTIME(ns_timestamp / 1000000000);
END;

-- Function to get latest data processing status
CREATE OR REPLACE FUNCTION get_processing_status()
RETURN TABLE (
    ticker VARCHAR(20),
    latest_trade_time TIMESTAMP,
    latest_quote_time TIMESTAMP,
    latest_signal_time TIMESTAMP,
    trades_count BIGINT,
    quotes_count BIGINT,
    signals_count BIGINT,
    processing_lag_seconds DECIMAL(10,3)
)
AS BEGIN
    RETURN (
        SELECT 
            COALESCE(t.ticker, q.ticker, s.ticker) as ticker,
            ns_to_timestamp(MAX(t.participant_timestamp)) as latest_trade_time,
            ns_to_timestamp(MAX(q.participant_timestamp)) as latest_quote_time,
            MAX(s.processing_timestamp) as latest_signal_time,
            COUNT(DISTINCT t.id) as trades_count,
            COUNT(DISTINCT q.sequence_number) as quotes_count,
            COUNT(DISTINCT s.signal_timestamp) as signals_count,
            EXTRACT(EPOCH FROM (NOW() - MAX(s.processing_timestamp))) as processing_lag_seconds
        FROM polygon_trades t
        FULL OUTER JOIN polygon_quotes q ON t.ticker = q.ticker
        FULL OUTER JOIN trading_signals s ON COALESCE(t.ticker, q.ticker) = s.ticker
        WHERE COALESCE(t.ingestion_timestamp, q.ingestion_timestamp, s.processing_timestamp) > NOW() - INTERVAL '1 hour'
        GROUP BY COALESCE(t.ticker, q.ticker, s.ticker)
        ORDER BY processing_lag_seconds ASC
    );
END;

-- =====================================================
-- 7. SAMPLE QUERIES FOR TESTING
-- =====================================================

-- Test query: Recent trades for a symbol
/*
SELECT 
    ticker,
    ns_to_timestamp(participant_timestamp) as trade_time,
    price,
    size,
    exchange
FROM polygon_trades 
WHERE ticker = 'AAPL' 
  AND participant_timestamp > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour') * 1000000000
ORDER BY participant_timestamp DESC 
LIMIT 10;
*/

-- Test query: Recent quotes for a symbol
/*
SELECT 
    ticker,
    ns_to_timestamp(participant_timestamp) as quote_time,
    bid_price,
    ask_price,
    (ask_price - bid_price) as spread,
    bid_size,
    ask_size
FROM polygon_quotes 
WHERE ticker = 'AAPL' 
  AND participant_timestamp > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour') * 1000000000
ORDER BY participant_timestamp DESC 
LIMIT 10;
*/

-- Test query: Recent trading signals
/*
SELECT 
    ticker,
    ns_to_timestamp(signal_timestamp) as signal_time,
    trading_signal,
    standardized_return,
    jump_detected,
    signal_strength
FROM trading_signals 
WHERE ticker = 'AAPL' 
  AND signal_timestamp > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour') * 1000000000
ORDER BY signal_timestamp DESC 
LIMIT 10;
*/

-- Test query: Enriched market data
/*
SELECT * FROM enriched_market_data 
WHERE symbol = 'AAPL' 
  AND processed_at > NOW() - INTERVAL '1 hour'
ORDER BY timestamp_ns DESC 
LIMIT 10;
*/

-- =====================================================
-- 8. GRANTS AND PERMISSIONS
-- =====================================================

-- Grant permissions for application user
-- GRANT SELECT, INSERT, UPDATE ON polygon_trades TO kafka_user;
-- GRANT SELECT, INSERT, UPDATE ON polygon_quotes TO kafka_user;
-- GRANT SELECT, INSERT, UPDATE ON trading_signals TO microstructure_user;
-- GRANT SELECT ON enriched_market_data TO mcp_server_user;

-- =====================================================
-- NOTES:
-- =====================================================
-- 1. All timestamps are stored as BIGINT nanoseconds since Unix epoch
-- 2. Tables are partitioned by day for efficient time-series queries
-- 3. Segmentation by ticker distributes data across Vertica nodes
-- 4. Projections optimize for time-series access patterns
-- 5. The enriched_market_data view combines trades/quotes with signals
-- 6. Indexes support common query patterns for the MCP server
-- 7. Utility functions help with timestamp conversion and monitoring

