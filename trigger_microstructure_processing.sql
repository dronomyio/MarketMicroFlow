-- Trigger Microstructure Processing in Vertica
-- This script processes new market data and generates trading signals

-- =====================================================
-- 1. STORED PROCEDURE: Process Recent Market Data
-- =====================================================

CREATE OR REPLACE PROCEDURE process_recent_market_data(
    lookback_minutes INTEGER DEFAULT 5,
    batch_size INTEGER DEFAULT 1000
)
LANGUAGE PLvSQL AS $$
DECLARE
    trade_count INTEGER;
    quote_count INTEGER;
    signal_count INTEGER;
    processing_start TIMESTAMP;
    processing_end TIMESTAMP;
    cutoff_time BIGINT;
BEGIN
    processing_start := NOW();
    
    -- Calculate cutoff time (nanoseconds since epoch)
    cutoff_time := EXTRACT(EPOCH FROM NOW() - INTERVAL lookback_minutes||' minutes') * 1000000000;
    
    RAISE NOTICE 'Starting microstructure processing for last % minutes...', lookback_minutes;
    RAISE NOTICE 'Cutoff timestamp: % (ns)', cutoff_time;
    
    -- Count new data to process
    SELECT COUNT(*) INTO trade_count 
    FROM polygon_trades 
    WHERE participant_timestamp > cutoff_time
      AND NOT EXISTS (
          SELECT 1 FROM trading_signals s 
          WHERE s.ticker = polygon_trades.ticker 
            AND s.source_timestamp = polygon_trades.participant_timestamp
            AND s.source_type = 'TRADE'
      );
    
    SELECT COUNT(*) INTO quote_count 
    FROM polygon_quotes 
    WHERE participant_timestamp > cutoff_time
      AND NOT EXISTS (
          SELECT 1 FROM trading_signals s 
          WHERE s.ticker = polygon_quotes.ticker 
            AND s.source_timestamp = polygon_quotes.participant_timestamp
            AND s.source_type = 'QUOTE'
      );
    
    RAISE NOTICE 'Found % new trades and % new quotes to process', trade_count, quote_count;
    
    -- Process trades with microstructure UDx
    IF trade_count > 0 THEN
        RAISE NOTICE 'Processing trades with microstructure algorithms...';
        
        INSERT INTO trading_signals (
            ticker, signal_timestamp, source_timestamp, source_type,
            standardized_return, lee_mykland_stat, bns_stat, 
            trade_intensity_zscore, acd_surprise, conditional_duration,
            jump_detected, trading_signal, signal_strength, confidence_level,
            volatility_estimate, garch_variance, udx_version, model_parameters
        )
        SELECT 
            ticker,
            EXTRACT(EPOCH FROM NOW()) * 1000000000 as signal_timestamp,
            participant_timestamp as source_timestamp,
            'TRADE' as source_type,
            standardized_return,
            lee_mykland_stat,
            bns_stat,
            trade_intensity_zscore,
            acd_surprise,
            conditional_duration,
            jump_detected,
            CASE 
                WHEN jump_detected AND standardized_return > 2.0 THEN 'BUY'
                WHEN jump_detected AND standardized_return < -2.0 THEN 'SELL'
                ELSE 'HOLD'
            END as trading_signal,
            ABS(standardized_return) / 4.0 as signal_strength,  -- Normalize to 0-1
            CASE 
                WHEN ABS(lee_mykland_stat) > 3.0 THEN 0.9
                WHEN ABS(lee_mykland_stat) > 2.0 THEN 0.7
                ELSE 0.5
            END as confidence_level,
            SQRT(garch_variance) as volatility_estimate,
            garch_variance,
            'v1.0' as udx_version,
            '{"garch_omega":0.00001,"garch_alpha":0.05,"garch_beta":0.94,"jump_threshold":4.0,"window_size":100}' as model_parameters
        FROM batch_microstructure(
            SELECT 
                participant_timestamp,
                ticker,
                price,
                size,
                CAST(exchange AS VARCHAR) as exchange
            FROM polygon_trades 
            WHERE participant_timestamp > cutoff_time
              AND NOT EXISTS (
                  SELECT 1 FROM trading_signals s 
                  WHERE s.ticker = polygon_trades.ticker 
                    AND s.source_timestamp = polygon_trades.participant_timestamp
                    AND s.source_type = 'TRADE'
              )
            ORDER BY ticker, participant_timestamp
            LIMIT batch_size
        ) OVER (PARTITION BY ticker ORDER BY participant_timestamp);
        
        GET DIAGNOSTICS signal_count = ROW_COUNT;
        RAISE NOTICE 'Generated % trading signals from trades', signal_count;
    END IF;
    
    -- Process quotes with microstructure UDx (if needed)
    -- Note: Typically microstructure analysis focuses on trades, but quotes can be used for spread analysis
    
    processing_end := NOW();
    
    RAISE NOTICE 'Microstructure processing completed in % seconds', 
                 EXTRACT(EPOCH FROM processing_end - processing_start);
    
    -- Log processing statistics
    INSERT INTO processing_log (
        processing_timestamp, lookback_minutes, trades_processed, 
        quotes_processed, signals_generated, processing_duration_seconds
    ) VALUES (
        processing_start, lookback_minutes, trade_count, 
        quote_count, signal_count, 
        EXTRACT(EPOCH FROM processing_end - processing_start)
    );
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in microstructure processing: %', SQLERRM;
        ROLLBACK;
        RAISE;
END;
$$;

-- =====================================================
-- 2. PROCESSING LOG TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS processing_log (
    id IDENTITY(1,1) PRIMARY KEY,
    processing_timestamp TIMESTAMP NOT NULL,
    lookback_minutes INTEGER NOT NULL,
    trades_processed INTEGER NOT NULL,
    quotes_processed INTEGER NOT NULL,
    signals_generated INTEGER NOT NULL,
    processing_duration_seconds DECIMAL(10,3) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
)
ORDER BY processing_timestamp
SEGMENTED BY HASH(id) ALL NODES;

-- =====================================================
-- 3. AUTOMATED PROCESSING FUNCTION
-- =====================================================

CREATE OR REPLACE PROCEDURE start_continuous_processing(
    interval_seconds INTEGER DEFAULT 30,
    lookback_minutes INTEGER DEFAULT 5
)
LANGUAGE PLvSQL AS $$
DECLARE
    last_run TIMESTAMP;
    current_time TIMESTAMP;
    sleep_duration INTEGER;
BEGIN
    RAISE NOTICE 'Starting continuous microstructure processing...';
    RAISE NOTICE 'Interval: % seconds, Lookback: % minutes', interval_seconds, lookback_minutes;
    
    last_run := NOW();
    
    -- This would typically be run in a loop by an external scheduler
    -- For demonstration, we'll just show the logic
    
    LOOP
        current_time := NOW();
        
        -- Check if enough time has passed
        IF EXTRACT(EPOCH FROM current_time - last_run) >= interval_seconds THEN
            
            RAISE NOTICE 'Running microstructure processing at %', current_time;
            
            -- Process recent market data
            CALL process_recent_market_data(lookback_minutes, 1000);
            
            last_run := current_time;
            
        END IF;
        
        -- Calculate sleep duration
        sleep_duration := interval_seconds - EXTRACT(EPOCH FROM NOW() - last_run);
        IF sleep_duration > 0 THEN
            -- In real implementation, this would be handled by external scheduler
            RAISE NOTICE 'Sleeping for % seconds...', sleep_duration;
            -- PERFORM pg_sleep(sleep_duration); -- Not available in Vertica
        END IF;
        
        -- Exit condition (in real implementation, this would run indefinitely)
        EXIT;
        
    END LOOP;
    
END;
$$;

-- =====================================================
-- 4. MANUAL PROCESSING COMMANDS
-- =====================================================

-- Process last 5 minutes of data
-- CALL process_recent_market_data(5, 1000);

-- Process last hour of data  
-- CALL process_recent_market_data(60, 5000);

-- Process specific time range
/*
INSERT INTO trading_signals (
    ticker, signal_timestamp, source_timestamp, source_type,
    standardized_return, lee_mykland_stat, bns_stat, 
    trade_intensity_zscore, acd_surprise, conditional_duration,
    jump_detected, trading_signal, signal_strength, confidence_level,
    volatility_estimate, garch_variance, udx_version, model_parameters
)
SELECT 
    ticker,
    EXTRACT(EPOCH FROM NOW()) * 1000000000 as signal_timestamp,
    participant_timestamp as source_timestamp,
    'TRADE' as source_type,
    standardized_return, lee_mykland_stat, bns_stat, 
    trade_intensity_zscore, acd_surprise, conditional_duration,
    jump_detected,
    CASE 
        WHEN jump_detected AND standardized_return > 2.0 THEN 'BUY'
        WHEN jump_detected AND standardized_return < -2.0 THEN 'SELL'
        ELSE 'HOLD'
    END as trading_signal,
    ABS(standardized_return) / 4.0 as signal_strength,
    CASE 
        WHEN ABS(lee_mykland_stat) > 3.0 THEN 0.9
        WHEN ABS(lee_mykland_stat) > 2.0 THEN 0.7
        ELSE 0.5
    END as confidence_level,
    SQRT(garch_variance) as volatility_estimate,
    garch_variance,
    'v1.0' as udx_version,
    '{"garch_omega":0.00001,"garch_alpha":0.05,"garch_beta":0.94,"jump_threshold":4.0,"window_size":100}' as model_parameters
FROM batch_microstructure(
    SELECT 
        participant_timestamp, ticker, price, size, CAST(exchange AS VARCHAR) as exchange
    FROM polygon_trades 
    WHERE ticker = 'AAPL'
      AND participant_timestamp BETWEEN 
          EXTRACT(EPOCH FROM '2024-09-02 14:00:00'::TIMESTAMP) * 1000000000 AND
          EXTRACT(EPOCH FROM '2024-09-02 15:00:00'::TIMESTAMP) * 1000000000
    ORDER BY participant_timestamp
) OVER (PARTITION BY ticker ORDER BY participant_timestamp);
*/

-- =====================================================
-- 5. MONITORING QUERIES
-- =====================================================

-- Check processing status
CREATE OR REPLACE VIEW processing_status AS
SELECT 
    ticker,
    COUNT(*) as total_trades,
    MAX(ns_to_timestamp(participant_timestamp)) as latest_trade_time,
    COUNT(CASE WHEN s.ticker IS NOT NULL THEN 1 END) as processed_trades,
    ROUND(
        COUNT(CASE WHEN s.ticker IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2
    ) as processing_percentage,
    MAX(s.processing_timestamp) as latest_signal_time,
    EXTRACT(EPOCH FROM NOW() - MAX(s.processing_timestamp)) as seconds_since_last_signal
FROM polygon_trades t
LEFT JOIN trading_signals s ON (
    t.ticker = s.ticker 
    AND t.participant_timestamp = s.source_timestamp 
    AND s.source_type = 'TRADE'
)
WHERE t.ingestion_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY ticker
ORDER BY seconds_since_last_signal ASC;

-- Check recent signals
CREATE OR REPLACE VIEW recent_signals AS
SELECT 
    ticker,
    ns_to_timestamp(signal_timestamp) as signal_time,
    trading_signal,
    standardized_return,
    lee_mykland_stat,
    jump_detected,
    signal_strength,
    confidence_level,
    processing_timestamp
FROM trading_signals 
WHERE processing_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY signal_timestamp DESC;

-- Check processing performance
CREATE OR REPLACE VIEW processing_performance AS
SELECT 
    processing_timestamp,
    lookback_minutes,
    trades_processed,
    signals_generated,
    ROUND(signals_generated * 100.0 / NULLIF(trades_processed, 0), 2) as signal_rate_percent,
    processing_duration_seconds,
    ROUND(trades_processed / NULLIF(processing_duration_seconds, 0), 2) as trades_per_second
FROM processing_log 
ORDER BY processing_timestamp DESC 
LIMIT 20;

-- =====================================================
-- 6. EXAMPLE USAGE
-- =====================================================

-- View current processing status
-- SELECT * FROM processing_status;

-- View recent trading signals
-- SELECT * FROM recent_signals WHERE ticker = 'AAPL' LIMIT 10;

-- View processing performance
-- SELECT * FROM processing_performance;

-- Manual processing trigger
-- CALL process_recent_market_data(10, 2000);

-- =====================================================
-- 7. EXTERNAL SCHEDULER INTEGRATION
-- =====================================================

/*
For production use, integrate with external schedulers:

1. CRON JOB (Linux):
   # Process every 30 seconds
   * * * * * /usr/bin/vsql -h localhost -U dbadmin -d polygon_data -c "CALL process_recent_market_data(1, 1000);"
   * * * * * sleep 30; /usr/bin/vsql -h localhost -U dbadmin -d polygon_data -c "CALL process_recent_market_data(1, 1000);"

2. AIRFLOW DAG:
   from airflow import DAG
   from airflow.providers.vertica.operators.vertica import VerticaOperator
   
   dag = DAG('microstructure_processing', schedule_interval='*/30 * * * * *')
   
   process_task = VerticaOperator(
       task_id='process_microstructure',
       vertica_conn_id='vertica_default',
       sql="CALL process_recent_market_data(1, 1000);",
       dag=dag
   )

3. KUBERNETES CRONJOB:
   apiVersion: batch/v1
   kind: CronJob
   metadata:
     name: microstructure-processor
   spec:
     schedule: "*/30 * * * *"
     jobTemplate:
       spec:
         template:
           spec:
             containers:
             - name: processor
               image: vertica/vertica-ce
               command: ["/opt/vertica/bin/vsql"]
               args: ["-h", "vertica-host", "-U", "dbadmin", "-d", "polygon_data", 
                      "-c", "CALL process_recent_market_data(1, 1000);"]

4. PYTHON SCHEDULER:
   import schedule
   import time
   import vertica_python
   
   def process_microstructure():
       conn = vertica_python.connect(host='localhost', user='dbadmin', database='polygon_data')
       cursor = conn.cursor()
       cursor.execute("CALL process_recent_market_data(1, 1000);")
       conn.commit()
       conn.close()
   
   schedule.every(30).seconds.do(process_microstructure)
   
   while True:
       schedule.run_pending()
       time.sleep(1)
*/

-- =====================================================
-- NOTES:
-- =====================================================
-- 1. The stored procedure processes only new data (not already processed)
-- 2. Batch processing improves performance for large datasets
-- 3. Error handling ensures robustness in production
-- 4. Logging provides visibility into processing performance
-- 5. Views enable easy monitoring of the processing pipeline
-- 6. External schedulers provide automated, continuous processing
-- 7. The microstructure UDx function must be created first (see build_vertica_udx.sh)

