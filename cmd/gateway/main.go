package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/auth"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/messages"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

var (
	httpPort    = flag.Int("http-port", 8080, "HTTP server port")
	dbURL       = flag.String("db-url", "", "Postgres-compatible database connection URL")
	brokerURL = flag.String("broker-url", "localhost:19092", "Broker URL (Kafka Protocol compliant)")
)

func main() {
	flag.Parse()

	// Check environment variables if flags not set
	if *dbURL == "" {
		*dbURL = os.Getenv("DB_URL")
		if *dbURL == "" {
			*dbURL = "postgres://root@localhost:26257/frkrdb?sslmode=disable"
		}
	}

	if *brokerURL == "localhost:19092" {
		if envURL := os.Getenv("BROKER_URL"); envURL != "" {
			*brokerURL = envURL
		}
	}

	if *httpPort == 8080 {
		if envPort := os.Getenv("HTTP_PORT"); envPort != "" {
			if port, err := strconv.Atoi(envPort); err == nil {
				*httpPort = port
			}
		}
	}

	// Connect to database
	if *dbURL == "" {
		log.Fatal("DB_URL environment variable or flag is required")
	}
	db, err := sql.Open("postgres", *dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Validate broker URL
	if *brokerURL == "" || *brokerURL == "localhost:19092" {
		if envURL := os.Getenv("BROKER_URL"); envURL == "" {
			log.Fatal("BROKER_URL environment variable or flag is required")
		}
	}

	// Create writer for broker (Kafka Protocol compliant)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(*brokerURL),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	// HTTP handlers
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
		// This validates that the stream exists and returns the authorized topic name
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
			// Check if it's a topic not found error
			// Note: Topic auto-creation is only safe here because:
			// 1. User is already authenticated (checked above)
			// 2. Stream exists and topic name comes from database (not user input)
			// 3. Topic name matches the authorized stream's topic
			errStr := err.Error()
			log.Printf("Error string: %s", errStr)
			if strings.Contains(errStr, "Unknown Topic") || strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "UnknownTopic") || strings.Contains(errStr, "topic or partition") {
				// Try to create the topic
				// Security: Topic name comes from database (GetStreamTopic above), not user input
				// Only create if stream exists and user is authorized (both checked above)
				log.Printf("Topic %s not found for authorized stream %s, attempting to create it...", topic, req.StreamID)
				if createErr := createTopicIfNotExists(*brokerURL, topic); createErr != nil {
					log.Printf("Failed to create topic %s: %v", topic, createErr)
					http.Error(w, fmt.Sprintf("Topic not found and creation failed: %v", createErr), http.StatusInternalServerError)
					return
				}
				// Retry the write after creating the topic
				log.Printf("Topic %s created successfully, retrying write...", topic)
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
		w.Write([]byte("OK"))
	})

	// Start server
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", *httpPort),
	}

	go func() {
		log.Printf("Starting Ingest Gateway on port %d", *httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	server.Close()
}

// createTopicIfNotExists creates a topic if it doesn't exist (Kafka Protocol compliant).
// Security: This function should only be called after:
// 1. User authentication has been verified (ValidateBasicAuthForStream)
// 2. Stream existence has been validated (GetStreamTopic)
// 3. Topic name comes from the database (not user input)
func createTopicIfNotExists(brokerURL, topicName string) error {
	conn, err := kafka.Dial("tcp", brokerURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Topic might already exist, which is fine
		errStr := err.Error()
		if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "TOPIC_ALREADY_EXISTS") {
			return nil
		}
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

