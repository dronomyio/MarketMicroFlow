package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	_ "github.com/vertica/vertica-sql-go"
)

// Configuration
type Config struct {
	PolygonAPIKey string
	KafkaBrokers  []string
	KafkaTopic    string
	VerticaHost   string
	VerticaPort   string
	VerticaDB     string
	VerticaUser   string
	VerticaPass   string
	Symbols       []string
	BatchSize     int
	FlushInterval time.Duration
}

// Market data structures matching Polygon documentation
type TradeData struct {
	Ticker               string  `json:"ticker"`
	ID                   string  `json:"id"`
	ParticipantTimestamp int64   `json:"participant_timestamp"`
	SipTimestamp         int64   `json:"sip_timestamp"`
	TrfTimestamp         int64   `json:"trf_timestamp,omitempty"`
	Price                float64 `json:"price"`
	Size                 int64   `json:"size"`
	Exchange             int     `json:"exchange"`
	Tape                 int     `json:"tape"`
	TrfID                int     `json:"trf_id,omitempty"`
	Conditions           string  `json:"conditions,omitempty"`
	Correction           int     `json:"correction,omitempty"`
	SequenceNumber       int64   `json:"sequence_number"`
	EventType            string  `json:"event_type"`
	KafkaOffset          int64   `json:"kafka_offset,omitempty"`
	KafkaPartition       int32   `json:"kafka_partition,omitempty"`
}

type QuoteData struct {
	Ticker               string  `json:"ticker"`
	ParticipantTimestamp int64   `json:"participant_timestamp"`
	SipTimestamp         int64   `json:"sip_timestamp"`
	TrfTimestamp         int64   `json:"trf_timestamp,omitempty"`
	BidPrice             float64 `json:"bid_price"`
	BidSize              int64   `json:"bid_size"`
	BidExchange          int     `json:"bid_exchange"`
	AskPrice             float64 `json:"ask_price"`
	AskSize              int64   `json:"ask_size"`
	AskExchange          int     `json:"ask_exchange"`
	Tape                 int     `json:"tape"`
	Conditions           string  `json:"conditions,omitempty"`
	Indicators           string  `json:"indicators,omitempty"`
	SequenceNumber       int64   `json:"sequence_number"`
	EventType            string  `json:"event_type"`
	KafkaOffset          int64   `json:"kafka_offset,omitempty"`
	KafkaPartition       int32   `json:"kafka_partition,omitempty"`
}

// Performance metrics
type PerformanceMetrics struct {
	mu                    sync.RWMutex
	messagesReceived      int64
	messagesKafka         int64
	messagesVertica       int64
	lastMessageTime       time.Time
	startTime             time.Time
	latencySum            time.Duration
	latencyCount          int64
	kafkaErrors           int64
	verticaErrors         int64
	batchesProcessed      int64
	lastVerticaBatchTime  time.Time
	verticaBatchSize      int
}

// Application struct
type PolygonStreamer struct {
	config          Config
	wsClient        *polygonws.Client
	kafkaProducer   sarama.AsyncProducer
	verticaDB       *sql.DB
	metrics         *PerformanceMetrics
	tradeBatch      []TradeData
	quoteBatch      []QuoteData
	batchMutex      sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

func NewPolygonStreamer(config Config) *PolygonStreamer {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &PolygonStreamer{
		config:     config,
		tradeBatch: make([]TradeData, 0, config.BatchSize),
		quoteBatch: make([]QuoteData, 0, config.BatchSize),
		metrics: &PerformanceMetrics{
			startTime: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ps *PolygonStreamer) initializeWebSocket() error {
	log.Println("🔌 Initializing Polygon WebSocket client...")
	
	wsClient, err := polygonws.New(polygonws.Config{
		APIKey: ps.config.PolygonAPIKey,
		Feed:   polygonws.RealTime,
		Market: polygonws.Stocks,
	})
	if err != nil {
		return fmt.Errorf("failed to create WebSocket client: %w", err)
	}
	
	ps.wsClient = wsClient
	log.Println("✅ WebSocket client initialized")
	return nil
}

func (ps *PolygonStreamer) initializeKafka() error {
	log.Println("📨 Initializing Kafka producer...")
	
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal // Fast acknowledgment
	config.Producer.Compression = sarama.CompressionNone // No compression for speed
	config.Producer.Flush.Frequency = 1 * time.Millisecond // Fast flush
	config.Producer.Flush.Messages = 1 // Send immediately
	config.Producer.MaxMessageBytes = 1000000
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	
	producer, err := sarama.NewAsyncProducer(ps.config.KafkaBrokers, config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	
	ps.kafkaProducer = producer
	log.Printf("✅ Kafka producer initialized (brokers: %v)", ps.config.KafkaBrokers)
	return nil
}

func (ps *PolygonStreamer) initializeVertica() error {
	log.Println("🗄️ Initializing Vertica database connection...")
	
	connStr := fmt.Sprintf("vertica://%s:%s@%s:%s/%s",
		ps.config.VerticaUser, ps.config.VerticaPass,
		ps.config.VerticaHost, ps.config.VerticaPort, ps.config.VerticaDB)
	
	db, err := sql.Open("vertica", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Vertica: %w", err)
	}
	
	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping Vertica: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	
	ps.verticaDB = db
	log.Printf("✅ Vertica connection established (%s:%s)", ps.config.VerticaHost, ps.config.VerticaPort)
	return nil
}

func (ps *PolygonStreamer) handleWebSocketMessages() {
	log.Println("🎧 Starting WebSocket message handler...")
	
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		
		for {
			select {
			case <-ps.ctx.Done():
				return
			case message := <-ps.wsClient.Output():
				ps.processMessage(message)
			}
		}
	}()
}

func (ps *PolygonStreamer) processMessage(message interface{}) {
	startTime := time.Now()
	
	ps.metrics.mu.Lock()
	ps.metrics.messagesReceived++
	ps.metrics.lastMessageTime = startTime
	ps.metrics.mu.Unlock()
	
	switch msg := message.(type) {
	case models.EquityTrade:
		ps.processTrade(msg, startTime)
	case models.EquityQuote:
		ps.processQuote(msg, startTime)
	default:
		// Skip other message types
		return
	}
	
	// Update latency metrics
	processingTime := time.Since(startTime)
	ps.metrics.mu.Lock()
	ps.metrics.latencySum += processingTime
	ps.metrics.latencyCount++
	ps.metrics.mu.Unlock()
}

func (ps *PolygonStreamer) processTrade(trade models.EquityTrade, startTime time.Time) {
	// Convert to our trade structure
	tradeData := TradeData{
		Ticker:               trade.Symbol,
		ID:                   fmt.Sprintf("%d", trade.ID),
		ParticipantTimestamp: int64(trade.ParticipantTimestamp),
		SipTimestamp:         int64(trade.SipTimestamp),
		TrfTimestamp:         int64(trade.TrfTimestamp),
		Price:                trade.Price,
		Size:                 int64(trade.Size),
		Exchange:             int(trade.Exchange),
		Tape:                 int(trade.Tape),
		TrfID:                int(trade.TrfID),
		Conditions:           ps.formatConditions(trade.Conditions),
		SequenceNumber:       int64(trade.SequenceNumber),
		EventType:            "T",
	}
	
	// Send to Kafka
	ps.sendToKafka(tradeData, "trade")
	
	// Add to Vertica batch
	ps.addTradeToBatch(tradeData)
	
	// Calculate and log latency
	marketLatency := startTime.Sub(time.Unix(0, int64(trade.ParticipantTimestamp)))
	processingTime := time.Since(startTime)
	
	log.Printf("[%s] T %s (Market Latency: %.3fms, Processing: %.1fμs)",
		startTime.Format("2006-01-02T15:04:05.000000"),
		trade.Symbol,
		float64(marketLatency.Nanoseconds())/1e6,
		float64(processingTime.Nanoseconds())/1e3)
}

func (ps *PolygonStreamer) processQuote(quote models.EquityQuote, startTime time.Time) {
	// Convert to our quote structure
	quoteData := QuoteData{
		Ticker:               quote.Symbol,
		ParticipantTimestamp: int64(quote.ParticipantTimestamp),
		SipTimestamp:         int64(quote.SipTimestamp),
		TrfTimestamp:         int64(quote.TrfTimestamp),
		BidPrice:             quote.BidPrice,
		BidSize:              int64(quote.BidSize),
		BidExchange:          int(quote.BidExchange),
		AskPrice:             quote.AskPrice,
		AskSize:              int64(quote.AskSize),
		AskExchange:          int(quote.AskExchange),
		Tape:                 int(quote.Tape),
		Conditions:           ps.formatConditions(quote.Conditions),
		Indicators:           ps.formatIndicators(quote.Indicators),
		SequenceNumber:       int64(quote.SequenceNumber),
		EventType:            "Q",
	}
	
	// Send to Kafka
	ps.sendToKafka(quoteData, "quote")
	
	// Add to Vertica batch
	ps.addQuoteToBatch(quoteData)
	
	// Calculate and log latency
	marketLatency := startTime.Sub(time.Unix(0, int64(quote.ParticipantTimestamp)))
	processingTime := time.Since(startTime)
	
	log.Printf("[%s] Q %s (Market Latency: %.3fms, Processing: %.1fμs)",
		startTime.Format("2006-01-02T15:04:05.000000"),
		quote.Symbol,
		float64(marketLatency.Nanoseconds())/1e6,
		float64(processingTime.Nanoseconds())/1e3)
}

func (ps *PolygonStreamer) sendToKafka(data interface{}, messageType string) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("❌ Failed to marshal %s data: %v", messageType, err)
		ps.metrics.mu.Lock()
		ps.metrics.kafkaErrors++
		ps.metrics.mu.Unlock()
		return
	}
	
	message := &sarama.ProducerMessage{
		Topic: ps.config.KafkaTopic,
		Value: sarama.StringEncoder(jsonData),
	}
	
	// Add symbol as key for partitioning
	if messageType == "trade" {
		if trade, ok := data.(TradeData); ok {
			message.Key = sarama.StringEncoder(trade.Ticker)
		}
	} else if messageType == "quote" {
		if quote, ok := data.(QuoteData); ok {
			message.Key = sarama.StringEncoder(quote.Ticker)
		}
	}
	
	select {
	case ps.kafkaProducer.Input() <- message:
		ps.metrics.mu.Lock()
		ps.metrics.messagesKafka++
		ps.metrics.mu.Unlock()
	case <-ps.ctx.Done():
		return
	default:
		log.Printf("⚠️ Kafka producer input channel full, dropping %s message", messageType)
		ps.metrics.mu.Lock()
		ps.metrics.kafkaErrors++
		ps.metrics.mu.Unlock()
	}
}

func (ps *PolygonStreamer) addTradeToBatch(trade TradeData) {
	ps.batchMutex.Lock()
	defer ps.batchMutex.Unlock()
	
	ps.tradeBatch = append(ps.tradeBatch, trade)
	
	if len(ps.tradeBatch) >= ps.config.BatchSize {
		ps.flushTradeBatch()
	}
}

func (ps *PolygonStreamer) addQuoteToBatch(quote QuoteData) {
	ps.batchMutex.Lock()
	defer ps.batchMutex.Unlock()
	
	ps.quoteBatch = append(ps.quoteBatch, quote)
	
	if len(ps.quoteBatch) >= ps.config.BatchSize {
		ps.flushQuoteBatch()
	}
}

func (ps *PolygonStreamer) flushTradeBatch() {
	if len(ps.tradeBatch) == 0 {
		return
	}
	
	trades := make([]TradeData, len(ps.tradeBatch))
	copy(trades, ps.tradeBatch)
	ps.tradeBatch = ps.tradeBatch[:0] // Reset slice
	
	go ps.insertTradesIntoVertica(trades)
}

func (ps *PolygonStreamer) flushQuoteBatch() {
	if len(ps.quoteBatch) == 0 {
		return
	}
	
	quotes := make([]QuoteData, len(ps.quoteBatch))
	copy(quotes, ps.quoteBatch)
	ps.quoteBatch = ps.quoteBatch[:0] // Reset slice
	
	go ps.insertQuotesIntoVertica(quotes)
}

func (ps *PolygonStreamer) insertTradesIntoVertica(trades []TradeData) {
	if len(trades) == 0 {
		return
	}
	
	startTime := time.Now()
	
	// Prepare batch insert statement
	query := `
		INSERT INTO polygon_trades (
			ticker, id, participant_timestamp, sip_timestamp, trf_timestamp,
			price, size, exchange, tape, trf_id, conditions, correction,
			sequence_number, kafka_offset, kafka_partition
		) VALUES `
	
	values := make([]string, len(trades))
	args := make([]interface{}, 0, len(trades)*14)
	
	for i, trade := range trades {
		values[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			trade.Ticker, trade.ID, trade.ParticipantTimestamp, trade.SipTimestamp,
			ps.nullableInt64(trade.TrfTimestamp), trade.Price, trade.Size,
			trade.Exchange, trade.Tape, ps.nullableInt(trade.TrfID),
			ps.nullableString(trade.Conditions), ps.nullableInt(trade.Correction),
			trade.SequenceNumber, ps.nullableInt64(trade.KafkaOffset),
			ps.nullableInt32(trade.KafkaPartition))
	}
	
	query += strings.Join(values, ", ")
	
	_, err := ps.verticaDB.Exec(query, args...)
	if err != nil {
		log.Printf("❌ Failed to insert %d trades into Vertica: %v", len(trades), err)
		ps.metrics.mu.Lock()
		ps.metrics.verticaErrors++
		ps.metrics.mu.Unlock()
		return
	}
	
	duration := time.Since(startTime)
	ps.metrics.mu.Lock()
	ps.metrics.messagesVertica += int64(len(trades))
	ps.metrics.batchesProcessed++
	ps.metrics.lastVerticaBatchTime = time.Now()
	ps.metrics.verticaBatchSize = len(trades)
	ps.metrics.mu.Unlock()
	
	log.Printf("✅ Inserted %d trades into Vertica (%.2fms)", len(trades), float64(duration.Nanoseconds())/1e6)
}

func (ps *PolygonStreamer) insertQuotesIntoVertica(quotes []QuoteData) {
	if len(quotes) == 0 {
		return
	}
	
	startTime := time.Now()
	
	// Prepare batch insert statement
	query := `
		INSERT INTO polygon_quotes (
			ticker, participant_timestamp, sip_timestamp, trf_timestamp,
			bid_price, bid_size, bid_exchange, ask_price, ask_size, ask_exchange,
			tape, conditions, indicators, sequence_number, kafka_offset, kafka_partition
		) VALUES `
	
	values := make([]string, len(quotes))
	args := make([]interface{}, 0, len(quotes)*16)
	
	for i, quote := range quotes {
		values[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			quote.Ticker, quote.ParticipantTimestamp, quote.SipTimestamp,
			ps.nullableInt64(quote.TrfTimestamp), ps.nullableFloat64(quote.BidPrice),
			ps.nullableInt64(quote.BidSize), ps.nullableInt(quote.BidExchange),
			ps.nullableFloat64(quote.AskPrice), ps.nullableInt64(quote.AskSize),
			ps.nullableInt(quote.AskExchange), quote.Tape,
			ps.nullableString(quote.Conditions), ps.nullableString(quote.Indicators),
			quote.SequenceNumber, ps.nullableInt64(quote.KafkaOffset),
			ps.nullableInt32(quote.KafkaPartition))
	}
	
	query += strings.Join(values, ", ")
	
	_, err := ps.verticaDB.Exec(query, args...)
	if err != nil {
		log.Printf("❌ Failed to insert %d quotes into Vertica: %v", len(quotes), err)
		ps.metrics.mu.Lock()
		ps.metrics.verticaErrors++
		ps.metrics.mu.Unlock()
		return
	}
	
	duration := time.Since(startTime)
	ps.metrics.mu.Lock()
	ps.metrics.messagesVertica += int64(len(quotes))
	ps.metrics.batchesProcessed++
	ps.metrics.lastVerticaBatchTime = time.Now()
	ps.metrics.verticaBatchSize = len(quotes)
	ps.metrics.mu.Unlock()
	
	log.Printf("✅ Inserted %d quotes into Vertica (%.2fms)", len(quotes), float64(duration.Nanoseconds())/1e6)
}

func (ps *PolygonStreamer) startBatchFlusher() {
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		
		ticker := time.NewTicker(ps.config.FlushInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ps.ctx.Done():
				// Final flush before shutdown
				ps.batchMutex.Lock()
				ps.flushTradeBatch()
				ps.flushQuoteBatch()
				ps.batchMutex.Unlock()
				return
			case <-ticker.C:
				ps.batchMutex.Lock()
				ps.flushTradeBatch()
				ps.flushQuoteBatch()
				ps.batchMutex.Unlock()
			}
		}
	}()
}

func (ps *PolygonStreamer) startKafkaHandlers() {
	// Handle Kafka successes
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		for {
			select {
			case <-ps.ctx.Done():
				return
			case success := <-ps.kafkaProducer.Successes():
				_ = success // Just consume the success
			}
		}
	}()
	
	// Handle Kafka errors
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		for {
			select {
			case <-ps.ctx.Done():
				return
			case err := <-ps.kafkaProducer.Errors():
				log.Printf("❌ Kafka producer error: %v", err)
				ps.metrics.mu.Lock()
				ps.metrics.kafkaErrors++
				ps.metrics.mu.Unlock()
			}
		}
	}()
}

func (ps *PolygonStreamer) startPerformanceReporter() {
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ps.ctx.Done():
				return
			case <-ticker.C:
				ps.reportPerformance()
			}
		}
	}()
}

func (ps *PolygonStreamer) reportPerformance() {
	ps.metrics.mu.RLock()
	defer ps.metrics.mu.RUnlock()
	
	runtime := time.Since(ps.metrics.startTime)
	avgLatency := time.Duration(0)
	if ps.metrics.latencyCount > 0 {
		avgLatency = ps.metrics.latencySum / time.Duration(ps.metrics.latencyCount)
	}
	
	msgRate := float64(ps.metrics.messagesReceived) / runtime.Seconds()
	kafkaRate := float64(ps.metrics.messagesKafka) / runtime.Seconds()
	verticaRate := float64(ps.metrics.messagesVertica) / runtime.Seconds()
	
	log.Printf(`
================================================================================
📊 REAL-TIME PERFORMANCE ANALYSIS
================================================================================
⏱️  Runtime: %.1fs
📨 Total Messages: %d (%.1f msg/s)
📤 Kafka Messages: %d (%.1f msg/s)
🗄️  Vertica Messages: %d (%.1f msg/s)
📋 Vertica Batches: %d (Last: %d msgs, %.1fs ago)
⚡ Avg Processing: %.1fμs
❌ Errors: Kafka=%d, Vertica=%d
================================================================================`,
		runtime.Seconds(),
		ps.metrics.messagesReceived, msgRate,
		ps.metrics.messagesKafka, kafkaRate,
		ps.metrics.messagesVertica, verticaRate,
		ps.metrics.batchesProcessed, ps.metrics.verticaBatchSize,
		time.Since(ps.metrics.lastVerticaBatchTime).Seconds(),
		float64(avgLatency.Nanoseconds())/1e3,
		ps.metrics.kafkaErrors, ps.metrics.verticaErrors)
}

func (ps *PolygonStreamer) Start() error {
	log.Println("🚀 Starting Polygon to Kafka to Vertica Streamer...")
	
	// Initialize components
	if err := ps.initializeWebSocket(); err != nil {
		return err
	}
	
	if err := ps.initializeKafka(); err != nil {
		return err
	}
	
	if err := ps.initializeVertica(); err != nil {
		return err
	}
	
	// Start background handlers
	ps.startKafkaHandlers()
	ps.startBatchFlusher()
	ps.startPerformanceReporter()
	
	// Connect WebSocket
	if err := ps.wsClient.Connect(); err != nil {
		return fmt.Errorf("failed to connect WebSocket: %w", err)
	}
	
	// Subscribe to symbols
	log.Printf("📡 Subscribing to symbols: %v", ps.config.Symbols)
	if err := ps.wsClient.Subscribe(polygonws.StocksTrades, ps.config.Symbols...); err != nil {
		return fmt.Errorf("failed to subscribe to trades: %w", err)
	}
	
	if err := ps.wsClient.Subscribe(polygonws.StocksQuotes, ps.config.Symbols...); err != nil {
		return fmt.Errorf("failed to subscribe to quotes: %w", err)
	}
	
	// Start message handling
	ps.handleWebSocketMessages()
	
	log.Println("✅ Streamer started successfully!")
	log.Println("📊 Data flow: Polygon WebSocket → Kafka → Vertica → Microstructure Processing")
	
	return nil
}

func (ps *PolygonStreamer) Stop() {
	log.Println("🛑 Stopping Polygon Streamer...")
	
	ps.cancel()
	
	if ps.wsClient != nil {
		ps.wsClient.Close()
	}
	
	if ps.kafkaProducer != nil {
		ps.kafkaProducer.Close()
	}
	
	if ps.verticaDB != nil {
		ps.verticaDB.Close()
	}
	
	ps.wg.Wait()
	log.Println("✅ Streamer stopped")
}

// Helper functions
func (ps *PolygonStreamer) formatConditions(conditions []int) string {
	if len(conditions) == 0 {
		return ""
	}
	strs := make([]string, len(conditions))
	for i, c := range conditions {
		strs[i] = strconv.Itoa(c)
	}
	return strings.Join(strs, ",")
}

func (ps *PolygonStreamer) formatIndicators(indicators []int) string {
	if len(indicators) == 0 {
		return ""
	}
	strs := make([]string, len(indicators))
	for i, ind := range indicators {
		strs[i] = strconv.Itoa(ind)
	}
	return strings.Join(strs, ",")
}

func (ps *PolygonStreamer) nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func (ps *PolygonStreamer) nullableInt(i int) interface{} {
	if i == 0 {
		return nil
	}
	return i
}

func (ps *PolygonStreamer) nullableInt32(i int32) interface{} {
	if i == 0 {
		return nil
	}
	return i
}

func (ps *PolygonStreamer) nullableInt64(i int64) interface{} {
	if i == 0 {
		return nil
	}
	return i
}

func (ps *PolygonStreamer) nullableFloat64(f float64) interface{} {
	if f == 0.0 {
		return nil
	}
	return f
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	log.Println("🚀 Polygon WebSocket to Kafka to Vertica Streamer v2.0")
	log.Println("========================================================")
	
	// Load configuration
	config := Config{
		PolygonAPIKey: getEnv("POLYGON_API_KEY", ""),
		KafkaBrokers:  strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "polygon-market-data"),
		VerticaHost:   getEnv("VERTICA_HOST", "localhost"),
		VerticaPort:   getEnv("VERTICA_PORT", "5433"),
		VerticaDB:     getEnv("VERTICA_DB", "polygon_data"),
		VerticaUser:   getEnv("VERTICA_USER", "dbadmin"),
		VerticaPass:   getEnv("VERTICA_PASS", ""),
		Symbols:       strings.Split(getEnv("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA"), ","),
		BatchSize:     100, // Batch size for Vertica inserts
		FlushInterval: 5 * time.Second, // Force flush every 5 seconds
	}
	
	if config.PolygonAPIKey == "" {
		log.Fatal("❌ POLYGON_API_KEY environment variable is required")
	}
	
	log.Printf("📊 Configuration:")
	log.Printf("   Symbols: %v", config.Symbols)
	log.Printf("   Kafka: %v → %s", config.KafkaBrokers, config.KafkaTopic)
	log.Printf("   Vertica: %s:%s/%s", config.VerticaHost, config.VerticaPort, config.VerticaDB)
	log.Printf("   Batch Size: %d", config.BatchSize)
	log.Printf("   Flush Interval: %v", config.FlushInterval)
	
	// Create and start streamer
	streamer := NewPolygonStreamer(config)
	
	if err := streamer.Start(); err != nil {
		log.Fatalf("❌ Failed to start streamer: %v", err)
	}
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	streamer.Stop()
}

