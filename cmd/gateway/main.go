package main

import (
	"log"
	"os"

	gwcommon "github.com/frkr-io/frkr-common/gateway"
	"github.com/frkr-io/frkr-common/plugins"
	"github.com/frkr-io/frkr-ingest-gateway/internal/gateway"
)

func main() {
	cfg, err := gwcommon.LoadConfigFromFlags()
	if err != nil {
		log.Fatal(err)
	}

	db, err := gwcommon.ConnectGatewayDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writer := gwcommon.NewBrokerWriter(cfg)
	defer writer.Close()

	secretPlugin, err := plugins.NewDatabaseSecretPlugin(db)
	if err != nil {
		log.Fatal(err)
	}

	var authPlugin plugins.AuthPlugin
	authType := os.Getenv("AUTH_TYPE")
	if authType == "oidc" {
		log.Println("Using TrustedHeaderAuthPlugin (OIDC mode)")
		authPlugin = gateway.NewTrustedHeaderAuthPlugin(db)
	} else {
		log.Println("Using BasicAuthPlugin")
		authPlugin = plugins.NewBasicAuthPlugin(db)
	}

	gw, err := gateway.NewIngestGateway(authPlugin, secretPlugin)
	if err != nil {
		log.Fatal(err)
	}

	if err := gw.Start(cfg, db, writer); err != nil {
		log.Fatalf("Gateway failed: %v", err)
	}
}
