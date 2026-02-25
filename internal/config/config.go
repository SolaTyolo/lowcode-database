package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all service configuration that used to come from env vars.
type Config struct {
	// TENANT_MODE: "single" (default) or "multi"
	TenantMode string

	// Single-tenant DSNs.
	SingleDatabaseURL string
	DatabaseURL       string

	// Multi-tenant.
	TenantDSNTemplate string
	TenantAdminDB     string

	// Server addresses.
	GRPCAddr string
	HTTPAddr string

	// MAX_ROW: default and maximum row count per ListRows call.
	// If <= 0, falls back to internal defaults.
	MaxRow int
}

// Load reads configuration from environment variables, optionally populating
// them from a local ".env" file if present.
func Load() (*Config, error) {
	loadDotEnvIfPresent()

	cfg := &Config{
		TenantMode:        getenvDefault("TENANT_MODE", "single"),
		SingleDatabaseURL: os.Getenv("SINGLE_DATABASE_URL"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		TenantDSNTemplate: os.Getenv("TENANT_DSN_TEMPLATE"),
		TenantAdminDB:     os.Getenv("TENANT_ADMIN_DB"),
		GRPCAddr:          getenvDefault("GRPC_ADDR", ":9090"),
		HTTPAddr:          getenvDefault("HTTP_ADDR", ":8080"),
		MaxRow:            getenvInt("MAX_ROW", 100),
	}

	// Fallback: if SINGLE_DATABASE_URL is empty, use DATABASE_URL.
	if cfg.SingleDatabaseURL == "" && cfg.DatabaseURL != "" {
		cfg.SingleDatabaseURL = cfg.DatabaseURL
	}

	// Default for multi-tenant admin DB.
	if cfg.TenantAdminDB == "" {
		cfg.TenantAdminDB = "postgres"
	}

	// Basic validation so we fail fast on misconfiguration.
	switch cfg.TenantMode {
	case "single":
		if cfg.SingleDatabaseURL == "" {
			return nil, fmt.Errorf("TENANT_MODE=single requires SINGLE_DATABASE_URL or DATABASE_URL")
		}
	case "multi":
		if cfg.TenantDSNTemplate == "" {
			return nil, fmt.Errorf("TENANT_MODE=multi requires TENANT_DSN_TEMPLATE")
		}
	default:
		return nil, fmt.Errorf("invalid TENANT_MODE %q (expected \"single\" or \"multi\")", cfg.TenantMode)
	}

	return cfg, nil
}

func getenvDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// loadDotEnvIfPresent loads key=value pairs from a local ".env" file into the
// process environment. It is intentionally minimal: blank lines and lines
// starting with "#" are ignored, and the whole text after the first "=" is
// treated as the value.
func loadDotEnvIfPresent() {
	f, err := os.Open(".env")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}
		_ = os.Setenv(key, val)
	}
}

