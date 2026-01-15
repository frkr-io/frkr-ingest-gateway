package server

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/metrics"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/segmentio/kafka-go"
)

// IngestGatewayServer holds the gateway server dependencies
type IngestGatewayServer struct {
	DB            *sql.DB
	Writer        *kafka.Writer
	BrokerURL     string
	HealthChecker *gateway.GatewayHealthChecker
	AuthPlugin    plugins.AuthPlugin
	SecretPlugin  plugins.SecretPlugin
}

// NewIngestGatewayServer creates a new ingest gateway server
func NewIngestGatewayServer(
	db *sql.DB,
	writer *kafka.Writer,
	brokerURL string,
	healthChecker *gateway.GatewayHealthChecker,
	authPlugin plugins.AuthPlugin,
	secretPlugin plugins.SecretPlugin,
) *IngestGatewayServer {
	// Register ingest-specific metrics
	metrics.RegisterIngestMetrics()
	metrics.SetServiceInfo("frkr-ingest-gateway", "0.1.0")

	return &IngestGatewayServer{
		DB:            db,
		Writer:        writer,
		BrokerURL:     brokerURL,
		HealthChecker: healthChecker,
		AuthPlugin:    authPlugin,
		SecretPlugin:  secretPlugin,
	}
}

// SetupHandlers registers all HTTP handlers on the provided mux
func (s *IngestGatewayServer) SetupHandlers(mux *http.ServeMux, cfg *gateway.GatewayBaseConfig) {
	// Build URLs for health endpoints
	var dbURL string
	if cfg.DBURL != "" {
		dbURL = cfg.DBURL
	} else {
		port := cfg.DBPort
		if port == "" {
			port = "26257"
		}
		if cfg.DBUser != "" {
			if cfg.DBPassword != "" {
				dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.DBUser, cfg.DBPassword, cfg.DBHost, port, cfg.DBName)
			} else {
				dbURL = fmt.Sprintf("postgres://%s@%s:%s/%s", cfg.DBUser, cfg.DBHost, port, cfg.DBName)
			}
		} else {
			dbURL = fmt.Sprintf("postgres://%s:%s/%s", cfg.DBHost, port, cfg.DBName)
		}
	}

	// Register standard health endpoints
	s.HealthChecker.RegisterHealthEndpoints(mux, cfg.HTTPPort, dbURL, s.BrokerURL)

	// Register Prometheus metrics endpoint
	mux.Handle("/metrics", metrics.Handler())

	// Business endpoint
	mux.HandleFunc("/ingest", s.IngestHandler())
}



