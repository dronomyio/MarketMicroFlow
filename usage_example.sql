-- Example: Using the Microstructure UDx in Vertica

-- 1. Create tables for market data
CREATE TABLE market_data (
    timestamp_ns INT,
    symbol VARCHAR(10),
    price FLOAT,
    size INT,
    exchange VARCHAR(1),
    event_type VARCHAR(1)
);

-- 2. Create table for processed signals
CREATE TABLE trading_signals (
    timestamp_ns INT,
    symbol VARCHAR(10),
    standardized_return FLOAT,
    lee_mykland_stat FLOAT,
    bns_stat FLOAT,
    trade_intensity_zscore FLOAT,
    acd_surprise FLOAT,
    conditional_duration FLOAT,
    jump_detected BOOLEAN,
    trading_signal VARCHAR(10),
    processed_at TIMESTAMP DEFAULT NOW()
);

-- 3. Process market data and insert signals
INSERT INTO trading_signals (
    timestamp_ns, symbol, standardized_return, lee_mykland_stat,
    bns_stat, trade_intensity_zscore, acd_surprise, conditional_duration,
    jump_detected, trading_signal
)
SELECT * FROM batch_microstructure(
    SELECT timestamp_ns, symbol, price, size, exchange
    FROM market_data
    WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL')
      AND timestamp_ns > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour') * 1000000000
    ORDER BY symbol, timestamp_ns
) OVER (PARTITION BY symbol ORDER BY timestamp_ns);

-- 4. Query for recent trading signals
SELECT 
    symbol,
    COUNT(*) as total_trades,
    SUM(CASE WHEN jump_detected THEN 1 ELSE 0 END) as jumps_detected,
    AVG(standardized_return) as avg_return,
    MAX(ABS(lee_mykland_stat)) as max_lm_stat,
    STRING_AGG(DISTINCT trading_signal, ',') as signals_generated
FROM trading_signals
WHERE processed_at > NOW() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY jumps_detected DESC;

-- 5. Create a view for MCP server to query
CREATE VIEW enriched_market_data AS
SELECT 
    ts.timestamp_ns,
    ts.symbol,
    md.price,
    md.size,
    ts.standardized_return,
    ts.lee_mykland_stat,
    ts.bns_stat,
    ts.trade_intensity_zscore,
    ts.acd_surprise,
    ts.jump_detected,
    ts.trading_signal,
    ts.processed_at
FROM trading_signals ts
JOIN market_data md ON ts.timestamp_ns = md.timestamp_ns AND ts.symbol = md.symbol
WHERE ts.processed_at > NOW() - INTERVAL '24 hours'
ORDER BY ts.timestamp_ns DESC;
