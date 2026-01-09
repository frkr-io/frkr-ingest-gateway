package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/frkr-io/frkr-ingest-gateway/internal/gateway/server"
	"github.com/segmentio/kafka-go"
)

const (
	ServiceName = "frkr-ingest-gateway"
	Version     = "0.1.0"
)

// IngestGateway is the main gateway server
type IngestGateway struct {
	authPlugin   plugins.AuthPlugin
	secretPlugin plugins.SecretPlugin
}

// NewIngestGateway creates a new ingest gateway with injected plugins
func NewIngestGateway(authPlugin plugins.AuthPlugin, secretPlugin plugins.SecretPlugin) (*IngestGateway, error) {
	if authPlugin == nil {
		return nil, fmt.Errorf("authPlugin cannot be nil")
	}
	if secretPlugin == nil {
		return nil, fmt.Errorf("secretPlugin cannot be nil")
	}
	return &IngestGateway{
		authPlugin:   authPlugin,
		secretPlugin: secretPlugin,
	}, nil
}

// Start starts the gateway server
func (g *IngestGateway) Start(cfg *gateway.GatewayBaseConfig, db *sql.DB, writer *kafka.Writer) error {
	// Build broker URL for health checks
	var brokerURL string
	if cfg.BrokerURL != "" {
		brokerURL = cfg.BrokerURL
	} else if cfg.BrokerHost != "" {
		port := cfg.BrokerPort
		if port == "" {
			port = "9092"
		}
		brokerURL = fmt.Sprintf("%s:%s", cfg.BrokerHost, port)
	}

	// Build DB URL for health checks
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

	// Create health checker and start background health checks
	healthChecker := gateway.NewGatewayHealthChecker(ServiceName, Version)
	healthChecker.StartHealthCheckLoop(db, brokerURL)

	// Create and configure server with injected plugins
	srv := server.NewIngestGatewayServer(db, writer, brokerURL, healthChecker, g.authPlugin, g.secretPlugin)

	// Set up HTTP handlers
	mux := http.NewServeMux()
	srv.SetupHandlers(mux, cfg)

	// Start server
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: mux,
	}

	go func() {
		log.Printf("Starting %s v%s on port %d", ServiceName, Version, cfg.HTTPPort)
		log.Printf("  Database: %s", gateway.SanitizeURL(dbURL))
		log.Printf("  Broker:   %s", brokerURL)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	
	if err := httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	return nil
}
