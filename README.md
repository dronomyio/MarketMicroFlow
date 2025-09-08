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


