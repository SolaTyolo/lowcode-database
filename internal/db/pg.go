package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/solat/lowcode-database/internal/config"
	"github.com/solat/lowcode-database/internal/migrate"
)

// NewPool creates a pgx connection pool using config.DatabaseURL (single-tenant mode).
func NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("config.database_url is not set")
	}
	return NewPoolFromDSN(ctx, cfg.DatabaseURL)
}

// NewPoolFromDSN creates a pgx connection pool from a full DSN.
func NewPoolFromDSN(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	return pool, nil
}

// EnsureMetaSchema runs all pending schema migrations for the lowcode system.
func EnsureMetaSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return migrate.Migrate(ctx, pool)
}

