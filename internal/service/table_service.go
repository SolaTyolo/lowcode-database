package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Table --------

func (s *LowcodeService) CreateTable(ctx context.Context, req *lowcodev1.CreateTableRequest) (*lowcodev1.CreateTableResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	schemaName := req.GetSchemaName()
	if schemaName == "" {
		schemaName = "public"
	}
	physTable := "lc_t_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Ensure schema exists
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, pgx.Identifier{schemaName}.Sanitize())); err != nil {
		return nil, err
	}

	// Create physical table with id column
	createSQL := fmt.Sprintf(`CREATE TABLE %s.%s (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`,
		pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{physTable}.Sanitize())
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return nil, err
	}

	const ins = `
		INSERT INTO lc_tables (name, schema_name, table_name)
		VALUES ($1, $2, $3)
		RETURNING id, name, schema_name, table_name, created_at, updated_at
	`
	row := tx.QueryRow(ctx, ins, req.GetName(), schemaName, physTable)

	var t lowcodev1.Table
	if err := row.Scan(&t.Id, &t.Name, &t.SchemaName, &t.TableName, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.CreateTableResponse{Table: &t}, nil
}

func (s *LowcodeService) DeleteTable(ctx context.Context, req *lowcodev1.DeleteTableRequest) (*lowcodev1.DeleteTableResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var schemaName, tableName string
	if err := tx.QueryRow(ctx, `SELECT schema_name, table_name FROM lc_tables WHERE id = $1`, req.GetId()).
		Scan(&schemaName, &tableName); err != nil {
		if err == pgx.ErrNoRows {
			return &lowcodev1.DeleteTableResponse{}, nil
		}
		return nil, err
	}

	dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
		pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{tableName}.Sanitize())
	if _, err := tx.Exec(ctx, dropSQL); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM lc_tables WHERE id = $1`, req.GetId()); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.DeleteTableResponse{}, nil
}

func (s *LowcodeService) ListTables(ctx context.Context, _ *lowcodev1.ListTablesRequest) (*lowcodev1.ListTablesResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `SELECT id, name, schema_name, table_name, created_at, updated_at FROM lc_tables ORDER BY created_at`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res lowcodev1.ListTablesResponse
	for rows.Next() {
		var t lowcodev1.Table
		if err := rows.Scan(&t.Id, &t.Name, &t.SchemaName, &t.TableName, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		res.Tables = append(res.Tables, &t)
	}
	return &res, rows.Err()
}

// GetTableSchema 返回指定 table 以及其所有 columns 和 indexes。
func (s *LowcodeService) GetTableSchema(ctx context.Context, req *lowcodev1.GetTableSchemaRequest) (*lowcodev1.GetTableSchemaResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetTableId() == "" {
		return nil, fmt.Errorf("table_id is required")
	}

	// table
	var tbl lowcodev1.Table
	row := pool.QueryRow(ctx, `
		SELECT id, name, schema_name, table_name, created_at, updated_at
		FROM lc_tables
		WHERE id = $1
	`, req.GetTableId())
	if err := row.Scan(&tbl.Id, &tbl.Name, &tbl.SchemaName, &tbl.TableName, &tbl.CreatedAt, &tbl.UpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("table not found")
		}
		return nil, err
	}

	// columns
	colRows, err := pool.Query(ctx, `
		SELECT id, table_id, name, type_id, pg_column, is_nullable, position, config, created_at, updated_at
		FROM lc_columns
		WHERE table_id = $1
		ORDER BY position
	`, req.GetTableId())
	if err != nil {
		return nil, err
	}
	defer colRows.Close()

	var columns []*lowcodev1.Column
	for colRows.Next() {
		var c lowcodev1.Column
		var cfg map[string]any
		if err := colRows.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &cfg, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		if cfg != nil {
			c.Config = toStruct(cfg)
		}
		columns = append(columns, &c)
	}
	if err := colRows.Err(); err != nil {
		return nil, err
	}

	// indexes
	idxRows, err := pool.Query(ctx, `
		SELECT id, table_id, name, pg_index, column_ids, is_unique, created_at, updated_at
		FROM lc_indexes
		WHERE table_id = $1
		ORDER BY name
	`, req.GetTableId())
	if err != nil {
		return nil, err
	}
	defer idxRows.Close()

	var indexes []*lowcodev1.Index
	for idxRows.Next() {
		var idx lowcodev1.Index
		if err := idxRows.Scan(&idx.Id, &idx.TableId, &idx.Name, &idx.PgIndex, &idx.ColumnIds, &idx.IsUnique, &idx.CreatedAt, &idx.UpdatedAt); err != nil {
			return nil, err
		}
		indexes = append(indexes, &idx)
	}
	if err := idxRows.Err(); err != nil {
		return nil, err
	}

	return &lowcodev1.GetTableSchemaResponse{
		Table:   &tbl,
		Columns: columns,
		Indexes: indexes,
	}, nil
}

