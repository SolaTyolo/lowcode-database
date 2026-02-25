package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a pgx connection pool using DATABASE_URL (single-tenant mode).
func NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}
	return NewPoolFromDSN(ctx, dsn)
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

// EnsureMetaSchema creates metadata tables if they do not exist.
func EnsureMetaSchema(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`,
		`CREATE TABLE IF NOT EXISTS lc_types (
			id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name       TEXT UNIQUE NOT NULL,
			pg_type    TEXT NOT NULL,
			config     JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);`,
		`CREATE TABLE IF NOT EXISTS lc_tables (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name        TEXT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name  TEXT NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (schema_name, table_name)
		);`,
		`CREATE TABLE IF NOT EXISTS lc_columns (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			table_id    UUID NOT NULL REFERENCES lc_tables(id) ON DELETE CASCADE,
			name        TEXT NOT NULL,
			type_id     UUID NOT NULL REFERENCES lc_types(id),
			pg_column   TEXT NOT NULL,
			is_nullable BOOLEAN NOT NULL DEFAULT TRUE,
			position    INT NOT NULL,
			config      JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (table_id, name),
			UNIQUE (table_id, pg_column)
		);`,
		`CREATE TABLE IF NOT EXISTS lc_indexes (
			id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			table_id   UUID NOT NULL REFERENCES lc_tables(id) ON DELETE CASCADE,
			name       TEXT NOT NULL,
			pg_index   TEXT NOT NULL,
			column_ids UUID[] NOT NULL,
			is_unique  BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (table_id, name),
			UNIQUE (table_id, pg_index)
		);`,
	}

	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ensure meta schema: %w", err)
		}
	}
	return nil
}

