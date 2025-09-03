#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <cstring>
#include "surprise_metrics.h"

using namespace surprise_metrics;

// Simple CSV parser for testing
std::vector<Trade> load_test_trades() {
    std::vector<Trade> trades;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> price_dist(100.0, 0.5);
    std::poisson_distribution<> size_dist(100);
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Generate synthetic test data
    for (int i = 0; i < 10000; ++i) {
        Trade trade;
        trade.timestamp = std::chrono::nanoseconds(
            std::chrono::steady_clock::now().time_since_epoch().count() + i * 1000000
        );
        trade.price = price_dist(gen);
        trade.size = size_dist(gen);
        trade.exchange = 'N';  // NYSE
        std::memset(trade.conditions, 0, sizeof(trade.conditions));
        trades.push_back(trade);
    }
    
    return trades;
}

int main(int argc, char* argv[]) {
    std::cout << "SurpriseMetrics Runner v0.1.0\n";
    std::cout << "=============================\n\n";
    
    // Check CUDA availability
    int device_count = 0;
    cudaError_t error = cudaGetDeviceCount(&device_count);
    
    if (error != cudaSuccess) {
        std::cerr << "CUDA Error: " << cudaGetErrorString(error) << std::endl;
        device_count = 0;
    }
    
    std::cout << "CUDA Devices Found: " << device_count << std::endl;
    
    for (int i = 0; i < device_count; ++i) {
        cudaDeviceProp prop;
        cudaGetDeviceProperties(&prop, i);
        std::cout << "  Device " << i << ": " << prop.name 
                  << " (SM " << prop.major << "." << prop.minor << ")"
                  << " Memory: " << prop.totalGlobalMem / (1024*1024) << " MB\n";
    }
    
    std::cout << "\nInitializing MetricsCalculator...\n";
    
    // Initialize calculator
    MetricsCalculator calculator(device_count > 0 ? device_count : 1, 1000000);
    
    // Set parameters
    calculator.set_garch_params(0.00001, 0.05, 0.94);
    calculator.set_jump_threshold(4.0);
    calculator.set_window_size(100);
    
    std::cout << "Loading test data...\n";
    auto trades = load_test_trades();
    std::cout << "Loaded " << trades.size() << " trades\n";
    
    // Process trades
    std::cout << "Processing trades...\n";
    auto start = std::chrono::high_resolution_clock::now();
    
    calculator.process_trades(trades);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Processing completed in " << duration.count() << " ms\n";
    
    // Get metrics
    auto metrics = calculator.get_metrics();
    
    // Print summary
    int jump_count = 0;
    float max_zscore = 0.0f;
    for (const auto& m : metrics) {
        if (m.jump_detected) jump_count++;
        max_zscore = std::max(max_zscore, std::abs(m.standardized_return));
    }
    
    std::cout << "\nResults Summary:\n";
    std::cout << "  Total Metrics: " << metrics.size() << "\n";
    std::cout << "  Jumps Detected: " << jump_count << "\n";
    std::cout << "  Max Z-Score: " << max_zscore << "\n";
    std::cout << "  Throughput: " << (trades.size() * 1000.0 / duration.count()) 
              << " trades/sec\n";
    
    return 0;
}
