package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/metrics"
	ingestv1 "github.com/frkr-io/frkr-proto/go/ingest/v1"
	"github.com/segmentio/kafka-go"
)

// IngestHandler handles POST /ingest requests
func (s *IngestGatewayServer) IngestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		statusCode := http.StatusAccepted
		streamID := ""

		defer func() {
			duration := time.Since(start).Seconds()
			metrics.RecordIngestRequest(r.Method, "/ingest", strconv.Itoa(statusCode), duration)
		}()

		// TODO: Is there a better way to do this? Maybe HTTP method annotations that are enforced by middleware?
		if r.Method != http.MethodPost {
			statusCode = http.StatusMethodNotAllowed
			http.Error(w, "Method not allowed", statusCode)
			return
		}

		// Check if we're ready
		if !s.HealthChecker.IsReady() {
			statusCode = http.StatusServiceUnavailable
			http.Error(w, "Service unavailable - dependencies not ready", statusCode)
			return
		}

		// Parse request
		var req ingestv1.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			statusCode = http.StatusBadRequest
			http.Error(w, fmt.Sprintf("Invalid request: %v", err), statusCode)
			return
		}
		streamID = req.StreamId

		// Authenticate and authorize
		ctx := r.Context()
		_, err := gateway.AuthenticateHTTPRequest(ctx, r, s.AuthPlugin, s.SecretPlugin, req.StreamId, "write")
		if err != nil {
			log.Printf("Authentication failed: %v", err)
			statusCode = http.StatusUnauthorized
			metrics.RecordAuthFailure("frkr-ingest-gateway", "auth_failed")
			http.Error(w, "Unauthorized", statusCode)
			return
		}

		// Get stream topic from database
		topic, err := dbcommon.GetStreamTopic(s.DB, req.StreamId)
		if err != nil {
			log.Printf("Failed to get stream topic: %v", err)
			statusCode = http.StatusNotFound
			http.Error(w, "Stream not found", statusCode)
			return
		}

		// Serialize request
		messageData, err := json.Marshal(req.Request)
		if err != nil {
			statusCode = http.StatusInternalServerError
			http.Error(w, "Failed to serialize request", statusCode)
			return
		}

		// Write to broker
		err = s.Writer.WriteMessages(r.Context(), kafka.Message{
			Topic: topic,
			Key:   []byte(req.Request.RequestId),
			Value: messageData,
		})
		if err != nil {
			log.Printf("Failed to write to broker: %v", err)
			errStr := err.Error()
			if strings.Contains(errStr, "Unknown Topic") || strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "UnknownTopic") || strings.Contains(errStr, "topic or partition") {
				// Try to create the topic
				log.Printf("Topic %s not found for stream %s, attempting to create it...", topic, req.StreamId)
				if createErr := gateway.CreateTopicIfNotExists(s.BrokerURL, topic); createErr != nil {
					log.Printf("Failed to create topic %s: %v", topic, createErr)
					statusCode = http.StatusInternalServerError
					metrics.RecordPublishError(streamID, "topic_creation_failed")
					http.Error(w, fmt.Sprintf("Topic not found and creation failed: %v", createErr), statusCode)
					return
				}
				// Retry the write
				err = s.Writer.WriteMessages(r.Context(), kafka.Message{
					Topic: topic,
					Key:   []byte(req.Request.RequestId),
					Value: messageData,
				})
				if err != nil {
					log.Printf("Failed to write to broker after topic creation: %v", err)
					statusCode = http.StatusInternalServerError
					metrics.RecordPublishError(streamID, "write_retry_failed")
					http.Error(w, fmt.Sprintf("Failed to ingest request: %v", err), statusCode)
					return
				}
			} else {
				statusCode = http.StatusInternalServerError
				metrics.RecordPublishError(streamID, "write_failed")
				http.Error(w, fmt.Sprintf("Failed to ingest request: %v", err), statusCode)
				return
			}
		}

		// Success
		metrics.RecordMessagePublished(streamID)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("OK"))
	}
}

