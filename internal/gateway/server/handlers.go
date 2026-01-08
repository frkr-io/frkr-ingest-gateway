package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/messages"
	"github.com/segmentio/kafka-go"
)

// IngestHandler handles POST /ingest requests
func (s *Server) IngestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: Is there a better way to do this? Maybe HTTP method annotations that are enforced by middleware?
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if we're ready
		if !s.HealthChecker.IsReady() {
			http.Error(w, "Service unavailable - dependencies not ready", http.StatusServiceUnavailable)
			return
		}

		// Parse request
		var req messages.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
			return
		}

		// Authenticate and authorize
		ctx := r.Context()
		_, err := gateway.AuthenticateHTTPRequest(ctx, r, s.AuthPlugin, s.SecretPlugin, req.StreamID, "write")
		if err != nil {
			log.Printf("Authentication failed: %v", err)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Get stream topic from database
		topic, err := dbcommon.GetStreamTopic(s.DB, req.StreamID)
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
		err = s.Writer.WriteMessages(r.Context(), kafka.Message{
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
				if createErr := gateway.CreateTopicIfNotExists(s.BrokerURL, topic); createErr != nil {
					log.Printf("Failed to create topic %s: %v", topic, createErr)
					http.Error(w, fmt.Sprintf("Topic not found and creation failed: %v", createErr), http.StatusInternalServerError)
					return
				}
				// Retry the write
				err = s.Writer.WriteMessages(r.Context(), kafka.Message{
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
