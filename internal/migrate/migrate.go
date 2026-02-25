package migrate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type step struct {
	Version int
	Name    string
	Up      func(ctx context.Context, pool *pgxpool.Pool) error
}

var steps = []step{
	{
		Version: 1,
		Name:    "init core lc_* schema and seed types",
		Up:      stepInitCore,
	},
	{
		Version: 2,
		Name:    "seed virtual column types",
		Up:      stepSeedVirtualTypes,
	},
}

// Migrate applies all pending migrations against the given tenant database.
// It is idempotent and safe to call multiple times.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS lc_schema_migrations (
			version    INT PRIMARY KEY,
			name       TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create lc_schema_migrations: %w", err)
	}

	var current int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM lc_schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read current migration version: %w", err)
	}

	for _, s := range steps {
		if s.Version <= current {
			continue
		}
		if err := s.Up(ctx, pool); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", s.Version, s.Name, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO lc_schema_migrations (version, name) VALUES ($1, $2)`,
			s.Version, s.Name,
		); err != nil {
			return fmt.Errorf("record migration %d: %w", s.Version, err)
		}
	}
	return nil
}

// stepInitCore creates the core lc_* tables and seeds a few builtin types.
func stepInitCore(ctx context.Context, pool *pgxpool.Pool) error {
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
		// Seed a few basic logical types (idempotent)
		`INSERT INTO lc_types (name, pg_type, config)
		 VALUES
		   ('text', 'text', '{}'::jsonb),
		   ('number', 'numeric', '{}'::jsonb),
		   ('bool', 'boolean', '{}'::jsonb),
		   ('timestamp', 'timestamptz', '{}'::jsonb),
		   ('json', 'jsonb', '{}'::jsonb)
		 ON CONFLICT (name) DO NOTHING;`,
	}

	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("stepInitCore: %w", err)
		}
	}
	return nil
}

// stepSeedVirtualTypes 确保老库里也有 formula / relationship 这两种虚拟列类型。
func stepSeedVirtualTypes(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO lc_types (name, pg_type, config)
		VALUES
		  ('formula', 'jsonb', '{"kind":"formula"}'::jsonb),
		  ('relationship', 'jsonb', '{"kind":"relationship"}'::jsonb)
		ON CONFLICT (name) DO NOTHING;
	`)
	if err != nil {
		return fmt.Errorf("stepSeedVirtualTypes: %w", err)
	}
	return nil
}
