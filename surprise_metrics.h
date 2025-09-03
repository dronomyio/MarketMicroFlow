#pragma once
#include <vector>
#include <memory>
#include <chrono>
#include <cstdint>

namespace surprise_metrics {

using timestamp_t = std::chrono::nanoseconds;
using price_t = double;
using volume_t = uint64_t;

// Aligned memory for SIMD operations
template<typename T, std::size_t Alignment>
class aligned_allocator {
public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    
    template<typename U>
    struct rebind {
        using other = aligned_allocator<U, Alignment>;
    };
    
    aligned_allocator() noexcept = default;
    
    template<typename U>
    aligned_allocator(const aligned_allocator<U, Alignment>&) noexcept {}
    
    T* allocate(size_type n) {
        void* ptr = nullptr;
        if (posix_memalign(&ptr, Alignment, n * sizeof(T)) != 0) {
            throw std::bad_alloc();
        }
        return static_cast<T*>(ptr);
    }
    
    void deallocate(T* ptr, size_type) noexcept {
        free(ptr);
    }
};

template<typename T>
using aligned_vector = std::vector<T, aligned_allocator<T, 64>>;

struct Trade {
    timestamp_t timestamp;
    price_t price;
    volume_t size;
    char exchange;
    uint8_t conditions[4];
};

struct Quote {
    timestamp_t timestamp;
    price_t bid_price;
    price_t ask_price;
    volume_t bid_size;
    volume_t ask_size;
    char bid_exchange;
    char ask_exchange;
};

struct SurpriseMetrics {
    float standardized_return;
    float lee_mykland_stat;
    float bns_stat;
    float trade_intensity_zscore;  // Burst detection score
    float acd_surprise;            // ADD: ACD standardized duration surprise
    float conditional_duration;    // ADD: ACD conditional expected duration (optional)
    bool jump_detected;
    timestamp_t timestamp;
};
//OLD
struct SurpriseMetrics_OLD {
    float standardized_return;
    float lee_mykland_stat;
    float bns_stat;
    float trade_intensity_zscore;
    bool jump_detected;
    timestamp_t timestamp;
};

class MetricsCalculator {
public:
    MetricsCalculator(int num_gpus = 1, size_t buffer_size = 1000000);
    ~MetricsCalculator();
    
    // Main processing functions
    void process_trades(const std::vector<Trade>& trades);
    void process_quotes(const std::vector<Quote>& quotes);
    
    // Get computed metrics
    std::vector<SurpriseMetrics> get_metrics() const;
    
    // Configuration
    void set_garch_params(double omega, double alpha, double beta);
    void set_jump_threshold(double threshold);
    void set_window_size(int window);
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

// SIMD optimized functions
namespace simd {
    void compute_returns_avx512(const float* prices, float* returns, size_t n);
    void compute_realized_variance_avx512(const float* returns, float* rv, size_t n, int window);
    void compute_bipower_variation_avx512(const float* returns, float* bv, size_t n);
}

} // namespace surprise_metrics
