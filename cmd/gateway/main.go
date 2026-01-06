package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/auth"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/messages"
	_ "github.com/lib/pq" // PostgreSQL driver registration
	"github.com/segmentio/kafka-go"
)

const (
	ServiceName = "frkr-ingest-gateway"
	Version     = "0.1.0"
)

var (
	httpPort  = flag.Int("http-port", 8080, "HTTP server port")
	dbURL     = flag.String("db-url", "", "Postgres-compatible database connection URL (required)")
	brokerURL = flag.String("broker-url", "", "Broker URL (Kafka Protocol compliant, required)")
)

func main() {
	flag.Parse()

	// Load and validate configuration (12-factor app pattern)
	cfg := &gateway.Config{
		HTTPPort:  *httpPort,
		DBURL:     *dbURL,
		BrokerURL: *brokerURL,
	}
	gateway.MustLoadConfig(cfg)

	// Initialize database connection
	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Create Kafka writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BrokerURL),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	// Create health checker and start background health checks
	healthChecker := gateway.NewHealthChecker(ServiceName, Version)
	healthChecker.StartHealthCheckLoop(db, cfg.BrokerURL)

	// Set up HTTP handlers
	mux := http.NewServeMux()

	// Register standard health endpoints from shared package
	healthChecker.RegisterHealthEndpoints(mux, cfg.HTTPPort, cfg.DBURL, cfg.BrokerURL)

	// Business endpoint
	mux.HandleFunc("/ingest", makeIngestHandler(db, writer, cfg.BrokerURL, healthChecker))

	// Start server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: mux,
	}

	go func() {
		log.Printf("Starting %s v%s on port %d", ServiceName, Version, cfg.HTTPPort)
		log.Printf("  Database: %s", gateway.SanitizeURL(cfg.DBURL))
		log.Printf("  Broker:   %s", cfg.BrokerURL)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func makeIngestHandler(db *sql.DB, writer *kafka.Writer, brokerURL string, hc *gateway.HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if we're ready
		if !hc.IsReady() {
			http.Error(w, "Service unavailable - dependencies not ready", http.StatusServiceUnavailable)
			return
		}

		// Parse request
		var req messages.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
			return
		}

		// Authenticate
		authHeader := r.Header.Get("Authorization")
		if !auth.ValidateBasicAuthForStream(authHeader, req.StreamID, db) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Get stream topic from database
		topic, err := dbcommon.GetStreamTopic(db, req.StreamID)
		if err != nil {
			log.Printf("Failed to get stream topic: %v", err)
			http.Error(w, "Stream not found", http.StatusNotFound)
			return
		}

		// Serialize request
		messageData, err := json.Marshal(req.Request)
		if err != nil {
			http.Error(w, "Failed to serialize request", http.StatusInternalServerError)
			return
		}

		// Write to broker
		err = writer.WriteMessages(r.Context(), kafka.Message{
			Topic: topic,
			Key:   []byte(req.Request.RequestID),
			Value: messageData,
		})
		if err != nil {
			log.Printf("Failed to write to broker: %v", err)
			errStr := err.Error()
			if strings.Contains(errStr, "Unknown Topic") || strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "UnknownTopic") || strings.Contains(errStr, "topic or partition") {
				// Try to create the topic
				log.Printf("Topic %s not found for stream %s, attempting to create it...", topic, req.StreamID)
				if createErr := gateway.CreateTopicIfNotExists(brokerURL, topic); createErr != nil {
					log.Printf("Failed to create topic %s: %v", topic, createErr)
					http.Error(w, fmt.Sprintf("Topic not found and creation failed: %v", createErr), http.StatusInternalServerError)
					return
				}
				// Retry the write
				err = writer.WriteMessages(r.Context(), kafka.Message{
					Topic: topic,
					Key:   []byte(req.Request.RequestID),
					Value: messageData,
				})
				if err != nil {
					log.Printf("Failed to write to broker after topic creation: %v", err)
					http.Error(w, fmt.Sprintf("Failed to ingest request: %v", err), http.StatusInternalServerError)
					return
				}
			} else {
				http.Error(w, fmt.Sprintf("Failed to ingest request: %v", err), http.StatusInternalServerError)
				return
			}
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("OK"))
	}
}
