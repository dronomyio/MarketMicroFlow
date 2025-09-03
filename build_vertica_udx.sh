#!/bin/bash

# Build script for Vertica Microstructure UDx
# This script compiles the UDx with the microstructure surprise metrics library

set -e

echo "🔨 Building Vertica Microstructure UDx..."

# Configuration
VERTICA_SDK_PATH="/opt/vertica/sdk"
MICROSTRUCTURE_PATH="/home/ubuntu/upload/microstructure_surprise_metrics-main-2"
OUTPUT_DIR="/home/ubuntu/vertica_udx_build"
UDX_NAME="microstructure_udx"

# Create output directory
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo "📁 Working directory: $OUTPUT_DIR"

# Check if Vertica SDK exists (this would be on a real Vertica system)
if [ ! -d "$VERTICA_SDK_PATH" ]; then
    echo "⚠️  Vertica SDK not found at $VERTICA_SDK_PATH"
    echo "📝 This script is for demonstration - on a real Vertica system:"
    echo "   1. Install Vertica with SDK"
    echo "   2. Set VERTICA_SDK_PATH to actual SDK location"
    echo "   3. Run this script to build the UDx"
    echo ""
    echo "🔧 Creating mock build environment for demonstration..."
    
    # Create mock SDK structure for demonstration
    mkdir -p mock_vertica_sdk/include
    cat > mock_vertica_sdk/include/Vertica.h << 'EOF'
// Mock Vertica.h for demonstration purposes
// In real environment, this comes from Vertica SDK

#pragma once
#include <string>
#include <vector>
#include <memory>

namespace Vertica {
    class ServerInterface {};
    class BlockReader {
    public:
        bool isNull(int col) { return false; }
        long getIntRef(int col) { return 0; }
        double getFloatRef(int col) { return 0.0; }
        std::string getStringRef(int col) { return ""; }
        bool next() { return false; }
        void seek(int pos) {}
    };
    class BlockWriter {
    public:
        void setInt(int col, long val) {}
        void setFloat(int col, double val) {}
        void setVarchar(int col, const std::string& val) {}
        void setBool(int col, bool val) {}
        void setNull(int col) {}
        void next() {}
    };
    class PartitionReader : public BlockReader {};
    class PartitionWriter : public BlockWriter {};
    
    class ColumnTypes {
    public:
        void addInt() {}
        void addFloat() {}
        void addVarchar() {}
        void addBool() {}
    };
    class SizedColumnTypes : public ColumnTypes {
    public:
        size_t size() { return 0; }
        struct Type {
            bool isInt() { return true; }
            bool isFloat() { return true; }
            bool isStringType() { return true; }
        };
        Type getType() { return Type{}; }
        Type operator[](int i) { return Type{}; }
        void addInt(const std::string& name) {}
        void addFloat(const std::string& name) {}
        void addVarchar(int size, const std::string& name) {}
        void addBool(const std::string& name) {}
    };
    
    class ScalarFunction {
    public:
        virtual void processBlock(ServerInterface&, BlockReader&, BlockWriter&) = 0;
    };
    class TransformFunction {
    public:
        virtual void processPartition(ServerInterface&, PartitionReader&, PartitionWriter&) = 0;
    };
    
    class ScalarFunctionFactory {
    public:
        virtual void getPrototype(ServerInterface&, ColumnTypes&, ColumnTypes&) = 0;
        virtual void getReturnType(ServerInterface&, const SizedColumnTypes&, SizedColumnTypes&) = 0;
        virtual ScalarFunction* createScalarFunction(ServerInterface&) = 0;
    };
    class TransformFunctionFactory {
    public:
        virtual void getPrototype(ServerInterface&, ColumnTypes&, ColumnTypes&) = 0;
        virtual void getReturnType(ServerInterface&, const SizedColumnTypes&, SizedColumnTypes&) = 0;
        virtual TransformFunction* createTransformFunction(ServerInterface&) = 0;
    };
    
    struct Allocator {
        template<typename T> T* allocate() { return new T(); }
    };
}

#define vt_report_error(code, msg, ...) throw std::runtime_error(msg)
#define vt_createFuncObject(allocator) new
#define RegisterFactory(factory) static factory _factory_instance

EOF
    
    VERTICA_SDK_PATH="$OUTPUT_DIR/mock_vertica_sdk"
fi

echo "📦 Copying microstructure library..."

# Copy microstructure headers and source
cp -r "$MICROSTRUCTURE_PATH/include" .
cp "$MICROSTRUCTURE_PATH"/*.cpp . 2>/dev/null || true

# Copy our UDx source
cp /home/ubuntu/vertica_microstructure_udx.cpp .

echo "🔧 Compiling UDx..."

# Compilation flags (adjust for your system)
CXX_FLAGS="-std=c++17 -fPIC -shared -O3"
INCLUDE_FLAGS="-I$VERTICA_SDK_PATH/include -I./include"
LINK_FLAGS="-lcuda -lcudart"  # Add CUDA libraries if available

# Create Makefile
cat > Makefile << EOF
CXX = g++
CXXFLAGS = $CXX_FLAGS $INCLUDE_FLAGS
LDFLAGS = $LINK_FLAGS

TARGET = lib${UDX_NAME}.so
SOURCES = vertica_microstructure_udx.cpp

# Add microstructure library sources if they exist
MICROSTRUCTURE_SOURCES = \$(wildcard src/*.cpp)

all: \$(TARGET)

\$(TARGET): \$(SOURCES) \$(MICROSTRUCTURE_SOURCES)
	\$(CXX) \$(CXXFLAGS) -o \$@ \$^ \$(LDFLAGS)

clean:
	rm -f \$(TARGET)

install: \$(TARGET)
	@echo "📋 To install in Vertica:"
	@echo "   CREATE OR REPLACE LIBRARY MicrostructureLib AS '\$(PWD)/\$(TARGET)';"
	@echo "   CREATE FUNCTION microstructure_metrics AS LANGUAGE 'C++'"
	@echo "     NAME 'MicrostructureMetricsFactory' LIBRARY MicrostructureLib;"
	@echo "   CREATE TRANSFORM FUNCTION batch_microstructure AS LANGUAGE 'C++'"
	@echo "     NAME 'BatchMicrostructureFactory' LIBRARY MicrostructureLib;"

.PHONY: all clean install
EOF

echo "🏗️  Building shared library..."

# Try to build (may fail without actual microstructure library)
if make; then
    echo "✅ Build successful!"
    echo "📄 Generated: lib${UDX_NAME}.so"
else
    echo "⚠️  Build failed - this is expected without the full microstructure library"
    echo "📝 To complete the build on a real system:"
    echo "   1. Ensure all microstructure library dependencies are available"
    echo "   2. Compile the microstructure library first"
    echo "   3. Link against the compiled microstructure library"
fi

echo ""
echo "📋 Installation Instructions:"
echo "=============================================="
echo ""
echo "1. Copy the built library to your Vertica cluster:"
echo "   scp lib${UDX_NAME}.so dbadmin@vertica-node:/home/dbadmin/"
echo ""
echo "2. In Vertica SQL, create the library:"
echo "   CREATE OR REPLACE LIBRARY MicrostructureLib AS '/home/dbadmin/lib${UDX_NAME}.so';"
echo ""
echo "3. Create the UDx functions:"
echo "   -- Scalar function for single-row processing"
echo "   CREATE FUNCTION microstructure_metrics("
echo "     timestamp_ns INT, price FLOAT, size INT, exchange VARCHAR"
echo "   ) RETURN ROW("
echo "     timestamp_ns INT, standardized_return FLOAT, lee_mykland_stat FLOAT,"
echo "     bns_stat FLOAT, trade_intensity_zscore FLOAT, acd_surprise FLOAT,"
echo "     conditional_duration FLOAT, jump_detected BOOLEAN"
echo "   ) AS LANGUAGE 'C++' NAME 'MicrostructureMetricsFactory' LIBRARY MicrostructureLib;"
echo ""
echo "   -- Transform function for batch processing"
echo "   CREATE TRANSFORM FUNCTION batch_microstructure("
echo "     timestamp_ns INT, symbol VARCHAR, price FLOAT, size INT, exchange VARCHAR"
echo "   ) RETURN ROW("
echo "     timestamp_ns INT, symbol VARCHAR, standardized_return FLOAT,"
echo "     lee_mykland_stat FLOAT, bns_stat FLOAT, trade_intensity_zscore FLOAT,"
echo "     acd_surprise FLOAT, conditional_duration FLOAT, jump_detected BOOLEAN,"
echo "     trading_signal VARCHAR"
echo "   ) AS LANGUAGE 'C++' NAME 'BatchMicrostructureFactory' LIBRARY MicrostructureLib;"
echo ""
echo "4. Usage examples:"
echo "   -- Process market data and generate signals"
echo "   SELECT * FROM batch_microstructure("
echo "     SELECT timestamp_ns, symbol, price, size, exchange"
echo "     FROM market_data"
echo "     WHERE symbol = 'AAPL' AND date = '2024-09-02'"
echo "     ORDER BY timestamp_ns"
echo "   ) OVER (PARTITION BY symbol ORDER BY timestamp_ns);"
echo ""
echo "🎯 This creates a complete pipeline:"
echo "   Kafka → Vertica → C++ Microstructure → Trading Signals → MCP Server"

# Create usage example
cat > usage_example.sql << 'EOF'
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
EOF

echo ""
echo "📄 Created usage_example.sql with complete SQL examples"
echo "✅ Vertica UDx build script completed!"

