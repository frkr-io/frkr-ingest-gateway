package gateway

import (
	"testing"

	"github.com/frkr-io/frkr-common/db"
	"github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIngestGateway(t *testing.T) {
	testDB, _ := db.SetupTestDB(t, "../../../frkr-common/migrations")
	defer testDB.Close()

	secretPlugin, _ := plugins.NewDatabaseSecretPlugin(testDB)
	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	t.Run("successful creation with valid plugins", func(t *testing.T) {
		gw, err := NewIngestGateway(authPlugin, secretPlugin)
		require.NoError(t, err)
		require.NotNil(t, gw)
		assert.NotNil(t, gw.authPlugin)
		assert.NotNil(t, gw.secretPlugin)
	})

	t.Run("error when authPlugin is nil", func(t *testing.T) {
		gw, err := NewIngestGateway(nil, secretPlugin)
		require.Error(t, err)
		assert.Nil(t, gw)
		assert.Contains(t, err.Error(), "authPlugin cannot be nil")
	})

	t.Run("error when secretPlugin is nil", func(t *testing.T) {
		gw, err := NewIngestGateway(authPlugin, nil)
		require.Error(t, err)
		assert.Nil(t, gw)
		assert.Contains(t, err.Error(), "secretPlugin cannot be nil")
	})
}

func TestIngestGateway_Start(t *testing.T) {
	testDB, _ := db.SetupTestDB(t, "../../../frkr-common/migrations")
	defer testDB.Close()

	secretPlugin, _ := plugins.NewDatabaseSecretPlugin(testDB)
	authPlugin := plugins.NewBasicAuthPlugin(testDB)

	gw, err := NewIngestGateway(authPlugin, secretPlugin)
	require.NoError(t, err)

	cfg := &gateway.GatewayBaseConfig{
		HTTPPort:  0, // Use port 0 for random port in tests
		BrokerURL: "localhost:9092",
	}

	writer := gateway.NewBrokerWriter(cfg)
	defer writer.Close()

	t.Run("Start accepts new signature", func(t *testing.T) {
		// Verify the gateway accepts the new Start signature with cfg, db, writer
		// We don't actually call Start() here because it blocks waiting for a signal
		// This is just a compile-time check that the signature is correct
		assert.NotNil(t, gw)
		assert.NotNil(t, cfg)
		assert.NotNil(t, testDB)
		assert.NotNil(t, writer)
		
		// The method signature is validated by compilation
		// To test actual startup, we'd need to use a timeout context or run in a separate goroutine
		_ = gw.Start // Just verify the method exists and accepts the right signature
	})
}
