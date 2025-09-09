# Vertica Microstructure UDx - Fixed Version

This document explains the fixes applied to the MarketMicroFlow Vertica UDx to resolve compilation and runtime issues.

## Issues Fixed

### 1. Symbol Resolution Error (`vt_throw_exception`)
**Problem**: The original UDx failed to load with undefined symbol errors.
**Root Cause**: Not linking against the real `Vertica.cpp` file from the SDK.
**Solution**: Updated `Makefile_udx` to include `$(SDK_HOME)/include/Vertica.cpp` in compilation.

### 2. Varchar Handling Error (`setVarcharPtr`)
**Problem**: Transform function used incorrect `setVarchar()` method.
**Root Cause**: Wrong Vertica API method for varchar output.
**Solution**: Replaced with `getStringRef().copy()` method.

## Files Modified

### Core UDx Files
- **`vertica_microstructure_udx.cpp`** - Fixed varchar handling and added SDK comments
- **`Makefile_udx`** - NEW: Proper Makefile with Vertica.cpp linking
- **`usage_example.sql`** - Updated with installation instructions

### Key Changes in `vertica_microstructure_udx.cpp`
```cpp
// BEFORE (incorrect):
outputWriter.setVarchar(1, symbols[i]);
outputWriter.setVarchar(9, signal);

// AFTER (fixed):
outputWriter.getStringRef(1).copy(symbols[i]);
outputWriter.getStringRef(9).copy(signal);
```

### Key Changes in `Makefile_udx`
```makefile
# NEW: Proper SDK integration
SDK_HOME?=/opt/vertica/sdk
VERTICA_CPP = $(SDK_HOME)/include/Vertica.cpp

$(TARGET): $(SOURCE)
    $(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCE) $(VERTICA_CPP)
```

## Build Instructions

### Prerequisites
- Vertica SDK installed at `/opt/vertica/sdk`
- g++ compiler with C++11 support
- Make utility

### Compilation
```bash
# Use the new UDx-specific Makefile
make -f Makefile_udx

# Or for development without real SDK:
make -f Makefile_udx fallback
```

### Installation
```bash
# Option 1: Automatic installation (requires sudo)
make -f Makefile_udx install

# Option 2: Manual installation
sudo cp libmicrostructure_udx.so /opt/vertica/lib/

# Option 3: Generate SQL installation script
make -f Makefile_udx sql-install
vsql -f install_udx.sql
```

## Usage

### 1. Create Library in Vertica
```sql
CREATE OR REPLACE LIBRARY MicrostructureLib AS '/opt/vertica/lib/libmicrostructure_udx.so';
```

### 2. Create Functions
```sql
-- Scalar function for single trade analysis
CREATE OR REPLACE FUNCTION microstructure_analyze(
    symbol VARCHAR(10),
    timestamp_ns INT,
    price FLOAT,
    size INT,
    exchange INT
) RETURN ROW(
    timestamp_ns INT,
    symbol VARCHAR(10),
    standardized_return FLOAT,
    lee_mykland_stat FLOAT,
    bns_stat FLOAT,
    trade_intensity_zscore FLOAT,
    acd_surprise FLOAT,
    conditional_duration FLOAT,
    jump_detected BOOLEAN,
    trading_signal VARCHAR(10)
) AS LANGUAGE 'C++' 
NAME 'MicrostructureAnalyzerFactory' 
LIBRARY MicrostructureLib;

-- Transform function for batch processing
CREATE OR REPLACE TRANSFORM FUNCTION batch_microstructure_analyze(
    symbol VARCHAR(10),
    timestamp_ns INT,
    price FLOAT,
    size INT,
    exchange INT
) RETURN TABLE(
    timestamp_ns INT,
    symbol VARCHAR(10),
    standardized_return FLOAT,
    lee_mykland_stat FLOAT,
    bns_stat FLOAT,
    trade_intensity_zscore FLOAT,
    acd_surprise FLOAT,
    conditional_duration FLOAT,
    jump_detected BOOLEAN,
    trading_signal VARCHAR(10)
) AS LANGUAGE 'C++' 
NAME 'BatchMicrostructureAnalyzerFactory' 
LIBRARY MicrostructureLib;
```

### 3. Process Market Data
```sql
-- Example: Process AAPL trades
SELECT * FROM batch_microstructure_analyze(
    SELECT symbol, timestamp_ns, price, size, exchange
    FROM market_data
    WHERE symbol = 'AAPL'
    ORDER BY timestamp_ns
) OVER (PARTITION BY symbol ORDER BY timestamp_ns);
```

## Verification

### Check Symbol Resolution
```bash
# Check for undefined symbols
make -f Makefile_udx test-symbols

# Should show minimal undefined symbols (normal for UDx)
```

### Test in Vertica
```sql
-- Verify library loads
SELECT lib_name, lib_file_name FROM user_libraries WHERE lib_name = 'MicrostructureLib';

-- Test with sample data
SELECT microstructure_analyze('AAPL', 1234567890123456789, 150.0, 100, 1);
```

## Troubleshooting

### Common Issues

1. **"Vertica.cpp not found"**
   - Ensure Vertica SDK is installed: `ls /opt/vertica/sdk/include/Vertica.cpp`
   - Use fallback build for development: `make -f Makefile_udx fallback`

2. **"Library not found" in Vertica**
   - Check file permissions: `ls -la /opt/vertica/lib/libmicrostructure_udx.so`
   - Verify path in CREATE LIBRARY statement

3. **"Function not found" errors**
   - Check factory names match exactly in CREATE FUNCTION statements
   - Verify library was created successfully

### Debug Commands
```bash
# Check library symbols
nm -D libmicrostructure_udx.so | grep Factory

# Check Vertica logs
tail -f /opt/vertica/log/vertica.log

# Test library loading
ldd libmicrostructure_udx.so
```

## Migration from Original

If you have the original (broken) version:

1. **Stop using old library**: `DROP LIBRARY OldMicrostructureLib CASCADE;`
2. **Build new version**: `make -f Makefile_udx`
3. **Install new library**: Follow installation instructions above
4. **Update function calls**: Use new function names if changed
5. **Test thoroughly**: Run verification steps

## Production Deployment

### Recommended Steps
1. **Test in development** with fallback build
2. **Build with real SDK** using `Makefile_udx`
3. **Verify symbol resolution** with `test-symbols` target
4. **Deploy to staging** environment first
5. **Run comprehensive tests** with real market data
6. **Deploy to production** with monitoring

### Performance Notes
- The UDx processes trades in batches for efficiency
- Memory usage scales with window size (configurable)
- CPU usage depends on number of symbols processed simultaneously

## Support

For issues with this fixed version:
1. Check the troubleshooting section above
2. Verify you're using the new `Makefile_udx`
3. Ensure varchar fixes are applied (`getStringRef().copy()`)
4. Test with the provided SQL examples

## Summary of Fixes

✅ **Symbol Resolution**: Fixed by linking against `Vertica.cpp`
✅ **Varchar Handling**: Fixed by using `getStringRef().copy()`
✅ **Build Process**: New `Makefile_udx` with proper SDK integration
✅ **Documentation**: Updated usage examples and installation instructions
✅ **Testing**: Added symbol verification and troubleshooting guides

The UDx is now production-ready and should work correctly with Vertica installations that have the proper SDK.


# Build Process

# MarketMicroFlow - Comprehensive Microstructure Analysis System

A production-grade real-time market microstructure analysis system with GPU acceleration, SIMD optimization, and advanced quantitative algorithms integrated into Vertica database.

## 🚀 Features

- **Real-time Market Data Streaming**: Polygon WebSocket → Kafka → Vertica pipeline
- **GPU-Accelerated Analysis**: CUDA kernels for high-performance computing
- **SIMD Optimization**: AVX-512 vectorized operations for maximum CPU efficiency
- **Advanced Algorithms**: 
  - GARCH volatility modeling with real-time updates
  - Lee-Mykland jump detection with bipower variation
  - BNS jump statistics for robust volatility estimation
  - ACD duration modeling for trade timing analysis
- **Vertica Integration**: Native UDx functions with comprehensive C++/CUDA library

## 📋 Prerequisites

- **Vertica Database** (with SDK at `/opt/vertica/sdk`)
- **CUDA Toolkit** (optional, for GPU acceleration)
- **Go 1.24+** (for data streaming)
- **Polygon.io API Key** (for live market data)
- **Kafka** (for data pipeline)

## 🛠️ Installation

### 1. Setup Environment

```bash
# Set environment variables
export VERTICA_USER="dbadmin"
export VERTICA_DB="testdb" 
export VERTICA_PASS="testdb"
export POLYGON_API_KEY="your_actual_polygon_api_key"

# Navigate to project directory
cd MarketMicroFlow-main
```

### 2. Build Comprehensive Microstructure UDx

```bash
# Check dependencies
make -f Makefile_library_udx_fixed check-deps

# Build the comprehensive UDx with C++/CUDA library integration
make -f Makefile_library_udx_fixed UDX_SOURCE=vertica_microstructure_udx_library_fixed.cpp

# Copy to Vertica library directory
sudo cp libmicrostructure_library_udx.so /opt/vertica/lib/
```

### 3. Install UDx Functions in Vertica

```bash
# Create SQL installation script
cat > install_comprehensive_udx_correct.sql << 'EOF'
-- Install Comprehensive Microstructure Library UDx
CREATE OR REPLACE LIBRARY MicrostructureLibraryLib AS '/opt/vertica/lib/libmicrostructure_library_udx.so';

-- Advanced scalar function with full library
CREATE OR REPLACE FUNCTION comprehensive_microstructure_analyze AS LANGUAGE 'C++' 
NAME 'LibraryMicrostructureFunctionFactory' 
LIBRARY MicrostructureLibraryLib;

-- Advanced batch transform function
CREATE OR REPLACE TRANSFORM FUNCTION comprehensive_batch_microstructure AS LANGUAGE 'C++' 
NAME 'LibraryBatchMicrostructureFunctionFactory' 
LIBRARY MicrostructureLibraryLib;

SELECT 'Comprehensive C++/CUDA Microstructure Library UDx installed successfully!' as status;
EOF

# Install the functions
vsql -d testdb -f install_comprehensive_udx_correct.sql
```

### 4. Setup Market Data Tables

```bash
# Create enhanced quote_data table with all Polygon.io fields
vsql -d testdb << 'EOF'
-- Create comprehensive quote_data table
CREATE TABLE IF NOT EXISTS quote_data (
    timestamp_ns INT,
    symbol VARCHAR(10),
    bid_price FLOAT,
    bid_size INT,
    ask_price FLOAT,
    ask_size INT
);

-- Add additional Polygon.io quote fields
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS sip_timestamp BIGINT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS participant_timestamp BIGINT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS trf_timestamp BIGINT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS sequence_number BIGINT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS conditions VARCHAR(50);
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS indicators VARCHAR(50);
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS bid_exchange_id INT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS ask_exchange_id INT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS tape INT;
ALTER TABLE quote_data ADD COLUMN IF NOT EXISTS event_type VARCHAR(1) DEFAULT 'Q';

-- Verify table structure
\d quote_data;
EOF
```

### 5. Build and Run Data Streaming

```bash
# Copy Go module file
cp go_vertica.mod go.mod

# Install Go dependencies
go mod tidy

# Build the streaming application
make -f Makefile_vertica build

# Run the data streamer (during market hours)
./build/polygon-vertica-streamer
```

## 🧪 Testing

### Test Scalar Function

```sql
-- Test with single trade
SELECT comprehensive_microstructure_analyze(1757355025443, 237.74, 50, 'N');

-- Test with real market data
SELECT comprehensive_microstructure_analyze(timestamp_ns, price, size, 'N') 
FROM test_market_data 
WHERE symbol = 'AAPL' 
LIMIT 5;
```

### Test Transform Function

```sql
-- Single symbol analysis
SELECT * FROM comprehensive_batch_microstructure(timestamp_ns, symbol, price, size, 'N') 
OVER (PARTITION BY symbol ORDER BY timestamp_ns)
FROM test_market_data 
WHERE symbol = 'AAPL' 
LIMIT 10;

-- Multi-symbol analysis
SELECT * FROM comprehensive_batch_microstructure(timestamp_ns, symbol, price, size, 'N') 
OVER (PARTITION BY symbol ORDER BY timestamp_ns)
FROM test_market_data 
WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL')
ORDER BY symbol, timestamp_ns
LIMIT 20;
```

### Performance Testing

```sql
-- Check data volume
SELECT symbol, COUNT(*) as trade_count, 
       SUM(size) as total_volume,
       AVG(price) as avg_price,
       MIN(price) as low_price,
       MAX(price) as high_price
FROM test_market_data 
GROUP BY symbol 
ORDER BY total_volume DESC;

-- Test processing speed
\timing on
SELECT COUNT(*) as processed_trades
FROM comprehensive_batch_microstructure(timestamp_ns, symbol, price, size, 'N') 
OVER (PARTITION BY symbol ORDER BY timestamp_ns)
FROM test_market_data 
WHERE symbol IN ('AAPL', 'MSFT') 
LIMIT 5000;
\timing off
```

## 📊 Market Data Analysis

### Real-time Microstructure Metrics

```sql
-- Latest microstructure analysis
SELECT symbol,
       COUNT(*) as total_trades,
       AVG(standardized_return) as avg_return,
       MAX(ABS(lee_mykland_stat)) as max_lm_stat,
       SUM(CASE WHEN jump_detected THEN 1 ELSE 0 END) as jumps_detected,
       STRING_AGG(DISTINCT trading_signal, ',') as signals_generated
FROM comprehensive_batch_microstructure(timestamp_ns, symbol, price, size, 'N') 
OVER (PARTITION BY symbol ORDER BY timestamp_ns)
FROM test_market_data 
WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA')
GROUP BY symbol
ORDER BY jumps_detected DESC;
```

### Quote Analysis

```sql
-- Check quote spreads
SELECT timestamp_ns, symbol, bid_price, ask_price, 
       (ask_price - bid_price) as spread,
       (ask_price - bid_price)/bid_price * 100 as spread_pct
FROM quote_data 
WHERE symbol = 'AAPL' 
ORDER BY timestamp_ns DESC 
LIMIT 10;
```

### Combined Quote-Trade Analysis

```sql
-- Market impact analysis
SELECT t.timestamp_ns, t.symbol, t.price as trade_price, t.size,
       q.bid_price, q.ask_price,
       (t.price - (q.bid_price + q.ask_price)/2) as price_vs_mid
FROM test_market_data t
JOIN quote_data q ON t.symbol = q.symbol 
  AND ABS(t.timestamp_ns - q.timestamp_ns) < 1000000
WHERE t.symbol = 'AAPL'
ORDER BY t.timestamp_ns DESC
LIMIT 20;
```

## 🔧 Configuration

### GARCH Parameters

The UDx uses these default GARCH(1,1) parameters:
- ω (omega) = 0.00001
- α (alpha) = 0.1  
- β (beta) = 0.85

### Jump Detection Thresholds

- **Lee-Mykland threshold**: 4.6 (99.9% confidence)
- **Window size**: 100 observations
- **BNS alpha**: 0.999

### GPU Configuration

- **CUDA architectures**: compute_70, compute_75, compute_80
- **Buffer size**: 1,000,000 trades
- **Auto-detection**: Falls back to CPU if GPU unavailable

## 📈 Market Hours

**US Stock Market Hours (ET):**
- **Regular Trading**: 9:30 AM - 4:00 PM
- **Pre-market**: 4:00 AM - 9:30 AM  
- **After-hours**: 4:00 PM - 8:00 PM

**Note**: Live data streaming only works during market hours. Outside these times, the connection will be established but no trading activity will be observed.

## 🚨 Troubleshooting

### Build Issues

```bash
# Clean and rebuild
make -f Makefile_library_udx_fixed clean
make -f Makefile_library_udx_fixed UDX_SOURCE=vertica_microstructure_udx_library_fixed.cpp

# Check dependencies
make -f Makefile_library_udx_fixed check-deps
```

### Database Connection Issues

```bash
# Test Vertica connection
vsql -h localhost -p 5433 -U dbadmin -d testdb

# Check environment variables
echo $VERTICA_USER $VERTICA_DB $VERTICA_PASS
```

### Function Installation Issues

```sql
-- Check if library is loaded
SELECT lib_name, lib_file_name FROM user_libraries;

-- Check if functions exist
SELECT function_name, procedure_type FROM user_functions 
WHERE function_name LIKE '%microstructure%';
```

### Data Streaming Issues

```bash
# Check if data is being inserted
vsql -d testdb -c "SELECT COUNT(*) as total_trades, MAX(timestamp_ns) as latest_trade FROM test_market_data;"

# Monitor real-time insertions
watch -n 5 "vsql -d testdb -c 'SELECT COUNT(*) FROM test_market_data;'"
```

## 📚 Algorithm Details

### GARCH Volatility Model
- **Purpose**: Dynamic volatility estimation for standardized returns
- **Formula**: σₜ² = ω + α·rₜ₋₁² + β·σₜ₋₁²
- **Output**: Real-time volatility updates for risk management

### Lee-Mykland Jump Detection
- **Purpose**: Identify price jumps from news/events
- **Method**: Bipower variation for jump-robust volatility estimation
- **Statistic**: L(i) = |return(i)| / local_volatility(i)
- **Threshold**: 4.6 for 99.9% confidence level

### BNS Jump Detection
- **Purpose**: Alternative jump detection methodology
- **Method**: Realized variance vs bipower variation comparison
- **Formula**: (RV - BV) / BV where RV > BV indicates jumps

### ACD Duration Model
- **Purpose**: Analyze trade timing and duration surprises
- **Model**: ψᵢ = ω + α·xᵢ₋₁ + β·ψᵢ₋₁
- **Output**: Expected vs actual trade durations

## 🎯 Performance Metrics

**Expected Performance:**
- **CUDA Processing**: 10,000+ trades/second
- **SIMD Processing**: 5,000+ trades/second  
- **CPU Processing**: 1,000+ trades/second
- **Memory Usage**: ~100MB for 1M trade buffer
- **Latency**: <1ms per trade analysis

## 📄 License

This project integrates multiple components:
- MarketMicroFlow application code
- Comprehensive C++/CUDA surprise metrics library
- Vertica UDx integration layer
- Polygon.io market data integration

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Test with sample data
4. Submit pull request with performance benchmarks

## 📞 Support

For technical issues:
1. Check troubleshooting section
2. Verify all prerequisites are installed
3. Test with sample data before live trading
4. Monitor system resources during operation

---

**⚠️ Disclaimer**: This system is for educational and research purposes. Always validate results with independent calculations before using in production trading environments.


