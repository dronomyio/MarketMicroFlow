#pragma once

#include <vector>
#include <string>
#include "surprise_metrics.h"

namespace surprise_metrics {

// Polygon data loader class
class PolygonDataLoader {
public:
    explicit PolygonDataLoader(const std::string& api_key);
    
    std::string download_trades_file(const std::string& date, const std::string& ticker);
    std::string download_quotes_file(const std::string& date, const std::string& ticker);
    
private:
    std::string api_key_;
    std::string base_url_;
    
    std::string download_file(const std::string& url);
};

// Parsing functions
std::vector<Trade> parse_polygon_trades(const std::string& csv_data);
std::vector<Quote> parse_polygon_quotes(const std::string& csv_data);

// File loading functions
std::vector<Trade> load_trades_from_file(const std::string& filename);
std::vector<Quote> load_quotes_from_file(const std::string& filename);

// Decompression
std::string decompress_gzip(const std::string& compressed_data);

} // namespace surprise_metrics
