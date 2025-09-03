package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/vertica/vertica-sql-go"
)

// MCPServer provides enriched market data from Vertica via MCP protocol
type MCPServer struct {
	db     *sql.DB
	port   string
	dbHost string
	dbPort string
	dbName string
	dbUser string
	dbPass string
}

// EnrichedMarketData represents the processed market data with microstructure signals
type EnrichedMarketData struct {
	TimestampNs          int64   `json:"timestamp_ns"`
	Symbol               string  `json:"symbol"`
	Price                float64 `json:"price"`
	Size                 int64   `json:"size"`
	StandardizedReturn   float64 `json:"standardized_return"`
	LeeMyklandStat       float64 `json:"lee_mykland_stat"`
	BnsStat              float64 `json:"bns_stat"`
	TradeIntensityZscore float64 `json:"trade_intensity_zscore"`
	AcdSurprise          float64 `json:"acd_surprise"`
	JumpDetected         bool    `json:"jump_detected"`
	TradingSignal        string  `json:"trading_signal"`
	ProcessedAt          string  `json:"processed_at"`
}

// MCPRequest represents an MCP protocol request
type MCPRequest struct {
	Method string                 `json:"method"`
	Params map[string]interface{} `json:"params"`
	ID     string                 `json:"id"`
}

// MCPResponse represents an MCP protocol response
type MCPResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  *MCPError   `json:"error,omitempty"`
	ID     string      `json:"id"`
}

// MCPError represents an MCP error
type MCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// NewMCPServer creates a new MCP server instance
func NewMCPServer() *MCPServer {
	return &MCPServer{
		port:   getEnv("MCP_PORT", "8080"),
		dbHost: getEnv("VERTICA_HOST", "localhost"),
		dbPort: getEnv("VERTICA_PORT", "5433"),
		dbName: getEnv("VERTICA_DB", "polygon_data"),
		dbUser: getEnv("VERTICA_USER", "dbadmin"),
		dbPass: getEnv("VERTICA_PASS", ""),
	}
}

// Connect establishes connection to Vertica database
func (mcp *MCPServer) Connect() error {
	connStr := fmt.Sprintf("vertica://%s:%s@%s:%s/%s",
		mcp.dbUser, mcp.dbPass, mcp.dbHost, mcp.dbPort, mcp.dbName)

	var err error
	mcp.db, err = sql.Open("vertica", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Vertica: %w", err)
	}

	// Test connection
	if err := mcp.db.Ping(); err != nil {
		return fmt.Errorf("failed to ping Vertica: %w", err)
	}

	log.Printf("✅ Connected to Vertica at %s:%s", mcp.dbHost, mcp.dbPort)
	return nil
}

// Start starts the MCP server
func (mcp *MCPServer) Start() error {
	http.HandleFunc("/mcp", mcp.handleMCPRequest)
	http.HandleFunc("/health", mcp.handleHealth)
	http.HandleFunc("/", mcp.handleRoot)

	log.Printf("🚀 MCP Server starting on port %s", mcp.port)
	log.Printf("📊 Serving enriched market data from Vertica")
	log.Printf("🔗 Endpoints:")
	log.Printf("   - POST /mcp - MCP protocol endpoint")
	log.Printf("   - GET /health - Health check")
	log.Printf("   - GET / - Server info")

	return http.ListenAndServe(":"+mcp.port, nil)
}

// handleRoot provides server information
func (mcp *MCPServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"service":     "Polygon Microstructure MCP Server",
		"version":     "1.0.0",
		"description": "MCP server providing enriched market data with microstructure signals from Vertica",
		"endpoints": map[string]string{
			"mcp":    "POST /mcp - MCP protocol requests",
			"health": "GET /health - Health check",
		},
		"capabilities": []string{
			"get_enriched_data",
			"get_trading_signals",
			"get_jump_events",
			"get_symbol_metrics",
			"get_real_time_data",
		},
		"data_sources": []string{
			"Polygon.io WebSocket",
			"Vertica Analytics Database",
			"C++ Microstructure Library",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// handleHealth provides health check
func (mcp *MCPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	// Check database connection
	if mcp.db != nil {
		if err := mcp.db.Ping(); err != nil {
			health["status"] = "unhealthy"
			health["database_error"] = err.Error()
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			health["database"] = "connected"
		}
	} else {
		health["status"] = "unhealthy"
		health["database"] = "not_connected"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleMCPRequest handles MCP protocol requests
func (mcp *MCPServer) handleMCPRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req MCPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mcp.sendError(w, req.ID, -32700, "Parse error")
		return
	}

	log.Printf("📨 MCP Request: %s", req.Method)

	var result interface{}
	var err error

	switch req.Method {
	case "get_enriched_data":
		result, err = mcp.getEnrichedData(req.Params)
	case "get_trading_signals":
		result, err = mcp.getTradingSignals(req.Params)
	case "get_jump_events":
		result, err = mcp.getJumpEvents(req.Params)
	case "get_symbol_metrics":
		result, err = mcp.getSymbolMetrics(req.Params)
	case "get_real_time_data":
		result, err = mcp.getRealTimeData(req.Params)
	default:
		mcp.sendError(w, req.ID, -32601, "Method not found")
		return
	}

	if err != nil {
		log.Printf("❌ Error processing %s: %v", req.Method, err)
		mcp.sendError(w, req.ID, -32603, err.Error())
		return
	}

	response := MCPResponse{
		Result: result,
		ID:     req.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// getEnrichedData retrieves enriched market data with microstructure signals
func (mcp *MCPServer) getEnrichedData(params map[string]interface{}) (interface{}, error) {
	symbol := getStringParam(params, "symbol", "")
	limit := getIntParam(params, "limit", 100)
	hoursBack := getIntParam(params, "hours_back", 1)

	query := `
		SELECT 
			timestamp_ns, symbol, price, size,
			standardized_return, lee_mykland_stat, bns_stat,
			trade_intensity_zscore, acd_surprise, jump_detected,
			trading_signal, processed_at
		FROM enriched_market_data
		WHERE processed_at > NOW() - INTERVAL '%d hours'
	`

	args := []interface{}{hoursBack}
	if symbol != "" {
		query += " AND symbol = ?"
		args = append(args, symbol)
	}

	query += " ORDER BY timestamp_ns DESC LIMIT ?"
	args = append(args, limit)

	rows, err := mcp.db.Query(fmt.Sprintf(query, hoursBack), args[1:]...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var data []EnrichedMarketData
	for rows.Next() {
		var item EnrichedMarketData
		err := rows.Scan(
			&item.TimestampNs, &item.Symbol, &item.Price, &item.Size,
			&item.StandardizedReturn, &item.LeeMyklandStat, &item.BnsStat,
			&item.TradeIntensityZscore, &item.AcdSurprise, &item.JumpDetected,
			&item.TradingSignal, &item.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		data = append(data, item)
	}

	return map[string]interface{}{
		"data":  data,
		"count": len(data),
		"query_params": map[string]interface{}{
			"symbol":     symbol,
			"limit":      limit,
			"hours_back": hoursBack,
		},
	}, nil
}

// getTradingSignals retrieves recent trading signals
func (mcp *MCPServer) getTradingSignals(params map[string]interface{}) (interface{}, error) {
	symbol := getStringParam(params, "symbol", "")
	signalType := getStringParam(params, "signal_type", "") // BUY, SELL, HOLD
	limit := getIntParam(params, "limit", 50)

	query := `
		SELECT 
			timestamp_ns, symbol, trading_signal, standardized_return,
			lee_mykland_stat, jump_detected, processed_at
		FROM enriched_market_data
		WHERE processed_at > NOW() - INTERVAL '4 hours'
		  AND trading_signal != 'HOLD'
	`

	var args []interface{}
	if symbol != "" {
		query += " AND symbol = ?"
		args = append(args, symbol)
	}
	if signalType != "" {
		query += " AND trading_signal = ?"
		args = append(args, signalType)
	}

	query += " ORDER BY timestamp_ns DESC LIMIT ?"
	args = append(args, limit)

	rows, err := mcp.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var signals []map[string]interface{}
	for rows.Next() {
		var timestampNs int64
		var symbol, tradingSignal, processedAt string
		var standardizedReturn, leeMyklandStat float64
		var jumpDetected bool

		err := rows.Scan(&timestampNs, &symbol, &tradingSignal,
			&standardizedReturn, &leeMyklandStat, &jumpDetected, &processedAt)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		signals = append(signals, map[string]interface{}{
			"timestamp_ns":        timestampNs,
			"symbol":              symbol,
			"trading_signal":      tradingSignal,
			"standardized_return": standardizedReturn,
			"lee_mykland_stat":    leeMyklandStat,
			"jump_detected":       jumpDetected,
			"processed_at":        processedAt,
		})
	}

	return map[string]interface{}{
		"signals": signals,
		"count":   len(signals),
	}, nil
}

// getJumpEvents retrieves detected jump events
func (mcp *MCPServer) getJumpEvents(params map[string]interface{}) (interface{}, error) {
	symbol := getStringParam(params, "symbol", "")
	hoursBack := getIntParam(params, "hours_back", 24)
	limit := getIntParam(params, "limit", 100)

	query := `
		SELECT 
			timestamp_ns, symbol, standardized_return, lee_mykland_stat,
			bns_stat, trading_signal, processed_at
		FROM enriched_market_data
		WHERE jump_detected = true
		  AND processed_at > NOW() - INTERVAL '%d hours'
	`

	var args []interface{}
	if symbol != "" {
		query += " AND symbol = ?"
		args = append(args, symbol)
	}

	query += " ORDER BY ABS(standardized_return) DESC LIMIT ?"
	args = append(args, limit)

	rows, err := mcp.db.Query(fmt.Sprintf(query, hoursBack), args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var jumps []map[string]interface{}
	for rows.Next() {
		var timestampNs int64
		var symbol, tradingSignal, processedAt string
		var standardizedReturn, leeMyklandStat, bnsStat float64

		err := rows.Scan(&timestampNs, &symbol, &standardizedReturn,
			&leeMyklandStat, &bnsStat, &tradingSignal, &processedAt)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		jumps = append(jumps, map[string]interface{}{
			"timestamp_ns":        timestampNs,
			"symbol":              symbol,
			"standardized_return": standardizedReturn,
			"lee_mykland_stat":    leeMyklandStat,
			"bns_stat":            bnsStat,
			"trading_signal":      tradingSignal,
			"processed_at":        processedAt,
		})
	}

	return map[string]interface{}{
		"jump_events": jumps,
		"count":       len(jumps),
	}, nil
}

// getSymbolMetrics retrieves aggregated metrics by symbol
func (mcp *MCPServer) getSymbolMetrics(params map[string]interface{}) (interface{}, error) {
	hoursBack := getIntParam(params, "hours_back", 1)

	query := `
		SELECT 
			symbol,
			COUNT(*) as total_trades,
			SUM(CASE WHEN jump_detected THEN 1 ELSE 0 END) as jumps_detected,
			AVG(standardized_return) as avg_return,
			STDDEV(standardized_return) as return_volatility,
			MAX(ABS(lee_mykland_stat)) as max_lm_stat,
			COUNT(DISTINCT trading_signal) as signal_types,
			MAX(processed_at) as last_update
		FROM enriched_market_data
		WHERE processed_at > NOW() - INTERVAL '%d hours'
		GROUP BY symbol
		ORDER BY jumps_detected DESC, total_trades DESC
	`

	rows, err := mcp.db.Query(fmt.Sprintf(query, hoursBack))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var metrics []map[string]interface{}
	for rows.Next() {
		var symbol, lastUpdate string
		var totalTrades, jumpsDetected, signalTypes int
		var avgReturn, returnVolatility, maxLmStat float64

		err := rows.Scan(&symbol, &totalTrades, &jumpsDetected,
			&avgReturn, &returnVolatility, &maxLmStat, &signalTypes, &lastUpdate)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		metrics = append(metrics, map[string]interface{}{
			"symbol":            symbol,
			"total_trades":      totalTrades,
			"jumps_detected":    jumpsDetected,
			"avg_return":        avgReturn,
			"return_volatility": returnVolatility,
			"max_lm_stat":       maxLmStat,
			"signal_types":      signalTypes,
			"last_update":       lastUpdate,
		})
	}

	return map[string]interface{}{
		"symbol_metrics": metrics,
		"count":          len(metrics),
	}, nil
}

// getRealTimeData retrieves the most recent data
func (mcp *MCPServer) getRealTimeData(params map[string]interface{}) (interface{}, error) {
	symbols := getStringArrayParam(params, "symbols", []string{})
	limit := getIntParam(params, "limit", 10)

	query := `
		SELECT 
			timestamp_ns, symbol, price, standardized_return,
			jump_detected, trading_signal, processed_at
		FROM enriched_market_data
		WHERE processed_at > NOW() - INTERVAL '5 minutes'
	`

	var args []interface{}
	if len(symbols) > 0 {
		placeholders := strings.Repeat("?,", len(symbols)-1) + "?"
		query += " AND symbol IN (" + placeholders + ")"
		for _, symbol := range symbols {
			args = append(args, symbol)
		}
	}

	query += " ORDER BY timestamp_ns DESC LIMIT ?"
	args = append(args, limit)

	rows, err := mcp.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var data []map[string]interface{}
	for rows.Next() {
		var timestampNs int64
		var symbol, tradingSignal, processedAt string
		var price, standardizedReturn float64
		var jumpDetected bool

		err := rows.Scan(&timestampNs, &symbol, &price,
			&standardizedReturn, &jumpDetected, &tradingSignal, &processedAt)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		data = append(data, map[string]interface{}{
			"timestamp_ns":        timestampNs,
			"symbol":              symbol,
			"price":               price,
			"standardized_return": standardizedReturn,
			"jump_detected":       jumpDetected,
			"trading_signal":      tradingSignal,
			"processed_at":        processedAt,
		})
	}

	return map[string]interface{}{
		"real_time_data": data,
		"count":          len(data),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// Helper functions
func (mcp *MCPServer) sendError(w http.ResponseWriter, id string, code int, message string) {
	response := MCPResponse{
		Error: &MCPError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(response)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getStringParam(params map[string]interface{}, key, defaultValue string) string {
	if value, ok := params[key].(string); ok {
		return value
	}
	return defaultValue
}

func getIntParam(params map[string]interface{}, key string, defaultValue int) int {
	if value, ok := params[key].(float64); ok {
		return int(value)
	}
	if value, ok := params[key].(string); ok {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getStringArrayParam(params map[string]interface{}, key string, defaultValue []string) []string {
	if value, ok := params[key].([]interface{}); ok {
		var result []string
		for _, v := range value {
			if str, ok := v.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	return defaultValue
}

func main() {
	log.Println("🚀 Starting Polygon Microstructure MCP Server")

	server := NewMCPServer()

	// Connect to Vertica
	if err := server.Connect(); err != nil {
		log.Fatalf("❌ Failed to connect to Vertica: %v", err)
	}
	defer server.db.Close()

	// Start MCP server
	log.Fatal(server.Start())
}

