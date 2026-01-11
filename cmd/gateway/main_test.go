package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frkr-io/frkr-common/db"
	dbcommon "github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/frkr-io/frkr-ingest-gateway/internal/gateway/server"
	ingestv1 "github.com/frkr-io/frkr-proto/go/ingest/v1"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// setupTestUser creates a test user in the database
func setupTestUserForGateway(t *testing.T, dbConn *sql.DB, tenantID, username, password string) {
	_, err := dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
			username STRING(255) NOT NULL,
			password_hash STRING(255) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ,
			UNIQUE (tenant_id, username)
		)
	`)
	require.NoError(t, err)

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	require.NoError(t, err)

	_, err = dbConn.Exec(`
		INSERT INTO users (tenant_id, username, password_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, username) DO UPDATE SET password_hash = EXCLUDED.password_hash
	`, tenantID, username, string(passwordHash))
	require.NoError(t, err)
}

func TestIngestGateway_AuthenticatedRequest(t *testing.T) {
	testDB, _ := db.SetupTestDB(t, "../../../frkr-common/migrations")

	// Create tenant and user
	tenant, err := dbcommon.CreateOrGetTenant(testDB, "test-tenant-ingest")
	require.NoError(t, err)

	setupTestUserForGateway(t, testDB, tenant.ID, "ingestuser", "ingestpass123")

	// Create stream
	stream, err := dbcommon.CreateStream(testDB, tenant.ID, "test-stream", "Test stream", 7)
	require.NoError(t, err)

	// Initialize plugins
	secretPlugin, err := plugins.NewDatabaseSecretPlugin(testDB)
	require.NoError(t, err)

	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	// Create mock Kafka writer (we'll write to a test topic)
	// For now, we'll just verify the request is accepted
	mockWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"), // Won't actually connect in test
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 1 * time.Second,
	}

	healthChecker := gateway.NewGatewayHealthChecker("frkr-ingest-gateway", "0.1.0")
	// Manually check dependencies to set ready state
	healthChecker.CheckDependencies(testDB, "localhost:9092")

	// Create server and get handler
	srv := server.NewIngestGatewayServer(testDB, mockWriter, "localhost:9092", healthChecker, authPlugin, secretPlugin)
	cfg := &gateway.GatewayBaseConfig{
		HTTPPort: 8080,
		DBURL:    "test",
		BrokerURL: "localhost:9092",
	}
	mux := http.NewServeMux()
	srv.SetupHandlers(mux, cfg)
	handler := mux.ServeHTTP

	t.Run("successful authenticated request", func(t *testing.T) {
		reqBody := ingestv1.IngestRequest{
			StreamId: stream.Name, // GetStreamTopic expects stream name
			Request: &ingestv1.MirroredRequest{
				RequestId: "test-request-123",
				Method:    "GET",
				Path:      "/api/test",
				Headers:   map[string]string{"Content-Type": "application/json"},
				Body:      `{"test": "data"}`,
			},
		}

		bodyBytes, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(bodyBytes))
		credentials := base64.StdEncoding.EncodeToString([]byte("ingestuser:ingestpass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		// Request should be accepted (202) or error on Kafka write (which is expected without real Kafka)
		// The important part is that auth succeeded
		assert.NotEqual(t, http.StatusUnauthorized, w.Code, "Should not be unauthorized")
		assert.NotEqual(t, http.StatusNotFound, w.Code, "Stream should be found")
	})

	t.Run("unauthorized - missing auth header", func(t *testing.T) {
		reqBody := ingestv1.IngestRequest{
			StreamId: stream.Name,
			Request: &ingestv1.MirroredRequest{
				RequestId: "test-request-456",
				Method:    "GET",
				Path:      "/api/test",
			},
		}

		bodyBytes, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("unauthorized - invalid credentials", func(t *testing.T) {
		reqBody := ingestv1.IngestRequest{
			StreamId: stream.Name,
			Request: &ingestv1.MirroredRequest{
				RequestId: "test-request-789",
				Method:    "GET",
				Path:      "/api/test",
			},
		}

		bodyBytes, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(bodyBytes))
		credentials := base64.StdEncoding.EncodeToString([]byte("ingestuser:wrongpass"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("unauthorized - wrong tenant", func(t *testing.T) {
		// Create another tenant and stream
		otherTenant, err := dbcommon.CreateOrGetTenant(testDB, "other-tenant-ingest")
		require.NoError(t, err)

		otherStream, err := dbcommon.CreateStream(testDB, otherTenant.ID, "other-stream", "Other", 7)
		require.NoError(t, err)

		reqBody := ingestv1.IngestRequest{
			StreamId: otherStream.Name,
			Request: &ingestv1.MirroredRequest{
				RequestId: "test-request-cross-tenant",
				Method:    "GET",
				Path:      "/api/test",
			},
		}

		bodyBytes, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(bodyBytes))
		credentials := base64.StdEncoding.EncodeToString([]byte("ingestuser:ingestpass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("method not allowed", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ingest", nil)
		credentials := base64.StdEncoding.EncodeToString([]byte("ingestuser:ingestpass123"))
		req.Header.Set("Authorization", "Basic "+credentials)

		w := httptest.NewRecorder()
		handler(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}
