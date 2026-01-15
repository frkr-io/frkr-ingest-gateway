package gateway

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/frkr-io/frkr-common/plugins"
)

// TrustedHeaderAuthPlugin trusts that the request was authenticated by an upstream gateway (Envoy)
// and extracts user information from the Authorization header (JWT) without verifying signature again.
type TrustedHeaderAuthPlugin struct {
	db *sql.DB
}

// NewTrustedHeaderAuthPlugin creates a new TrustedHeaderAuthPlugin
func NewTrustedHeaderAuthPlugin(db *sql.DB) *TrustedHeaderAuthPlugin {
	return &TrustedHeaderAuthPlugin{db: db}
}

// ValidateRequest validates the request by decoding the JWT from Authorization header
func (p *TrustedHeaderAuthPlugin) ValidateRequest(ctx context.Context, r *http.Request, secretPlugin plugins.SecretPlugin) (*plugins.AuthResult, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, fmt.Errorf("TrustedHeaderAuthPlugin requires bearer token")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")

	// Token is "Bearer <jwt>"
	// We assume Envoy has already validated the signature.
	// We just need to parse claims to get user/client identity.

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %v", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JWT claims: %v", err)
	}

	// Extract identity
	var userID string
	if sub, ok := claims["sub"].(string); ok {
		userID = sub
	}
	
	// Create AuthResult
	// In OIDC mode, we map the external user to a transient session or lookup if needed.
	return &plugins.AuthResult{
		UserID:     userID,
		ClientType: "oidc_user",
		TenantID:   "default", 
		Roles:      []string{"user"},
		AuthSource: "oidc",
	}, nil
}

// CanAccessStream checks if the user/client can access a specific stream
func (p *TrustedHeaderAuthPlugin) CanAccessStream(ctx context.Context, authResult *plugins.AuthResult, streamID string, permission string) (bool, error) {
	if authResult.AuthSource != "oidc" {
		return false, fmt.Errorf("TrustedHeaderAuthPlugin cannot authorize user from source: %s", authResult.AuthSource)
	}
	// For trusted header/OIDC, we currently allow all access if authenticated (demo mode)
	return true, nil
}

// ValidateAuthHeader validates an Authorization header value directly (protocol-agnostic)
func (p *TrustedHeaderAuthPlugin) ValidateAuthHeader(ctx context.Context, authHeader string, secretPlugin plugins.SecretPlugin) (*plugins.AuthResult, error) {
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, fmt.Errorf("TrustedHeaderAuthPlugin requires bearer token")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")

	// Token is "<jwt>"
	// We assume Envoy has already validated the signature.
	// We just need to parse claims to get user/client identity.

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %v", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JWT claims: %v", err)
	}

	// Extract identity
	var userID string
	if sub, ok := claims["sub"].(string); ok {
		userID = sub
	}

	// Create AuthResult
	// In OIDC mode, we map the external user to a transient session or lookup if needed.
	return &plugins.AuthResult{
		UserID:     userID,
		ClientType: "oidc_user",
		TenantID:   "default",
		Roles:      []string{"user"},
		AuthSource: "oidc",
	}, nil
}

