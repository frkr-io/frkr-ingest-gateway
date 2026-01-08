package server

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/segmentio/kafka-go"
)

// Server holds the gateway server dependencies
type Server struct {
	DB            *sql.DB
	Writer        *kafka.Writer
	BrokerURL     string
	HealthChecker *gateway.HealthChecker
	AuthPlugin    plugins.AuthPlugin
	SecretPlugin  plugins.SecretPlugin
}

// NewServer creates a new ingest gateway server
func NewServer(
	db *sql.DB,
	writer *kafka.Writer,
	brokerURL string,
	healthChecker *gateway.HealthChecker,
	authPlugin plugins.AuthPlugin,
	secretPlugin plugins.SecretPlugin,
) *Server {
	return &Server{
		DB:            db,
		Writer:        writer,
		BrokerURL:     brokerURL,
		HealthChecker: healthChecker,
		AuthPlugin:    authPlugin,
		SecretPlugin:  secretPlugin,
	}
}

// SetupHandlers registers all HTTP handlers on the provided mux
func (s *Server) SetupHandlers(mux *http.ServeMux, cfg *gateway.Config) {
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

	// Business endpoint
	mux.HandleFunc("/ingest", s.IngestHandler())
}
