package service

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	// 物理表名直接基于逻辑表名生成，形如 lc_t_<table_name>。
	// pgx.Identifier 会负责正确转义，避免 SQL 注入。
	physTable := "lc_t_" + req.GetName()

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
		RETURNING name, schema_name, table_name, created_at, updated_at
	`
	row := tx.QueryRow(ctx, ins, req.GetName(), schemaName, physTable)

	var t lowcodev1.Table
	var createdAt, updatedAt time.Time
	if err := row.Scan(&t.Name, &t.SchemaName, &t.TableName, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	// 对外约定：Table.Id 使用逻辑 name。
	t.Id = t.Name
	t.CreatedAt = timestamppb.New(createdAt)
	t.UpdatedAt = timestamppb.New(updatedAt)

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
	if err := tx.QueryRow(ctx, `SELECT schema_name, table_name FROM lc_tables WHERE name = $1`, req.GetId()).
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

	if _, err := tx.Exec(ctx, `DELETE FROM lc_tables WHERE name = $1`, req.GetId()); err != nil {
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
	const q = `SELECT name, schema_name, table_name, created_at, updated_at FROM lc_tables ORDER BY created_at`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res lowcodev1.ListTablesResponse
	for rows.Next() {
		var t lowcodev1.Table
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&t.Name, &t.SchemaName, &t.TableName, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		// 对外：Table.Id 使用逻辑 name。
		t.Id = t.Name
		t.CreatedAt = timestamppb.New(createdAt)
		t.UpdatedAt = timestamppb.New(updatedAt)
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
		SELECT name, schema_name, table_name, created_at, updated_at
		FROM lc_tables
		WHERE name = $1
	`, req.GetTableId())
	var tblCreatedAt, tblUpdatedAt time.Time
	if err := row.Scan(&tbl.Name, &tbl.SchemaName, &tbl.TableName, &tblCreatedAt, &tblUpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("table not found")
		}
		return nil, err
	}
	// 对外：Table.Id 使用逻辑 name。
	tbl.Id = tbl.Name
	tbl.CreatedAt = timestamppb.New(tblCreatedAt)
	tbl.UpdatedAt = timestamppb.New(tblUpdatedAt)

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
		var createdAt, updatedAt time.Time
		if err := colRows.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &cfg, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		c.CreatedAt = timestamppb.New(createdAt)
		c.UpdatedAt = timestamppb.New(updatedAt)
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
		var createdAt, updatedAt time.Time
		if err := idxRows.Scan(&idx.Id, &idx.TableId, &idx.Name, &idx.PgIndex, &idx.ColumnIds, &idx.IsUnique, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		idx.CreatedAt = timestamppb.New(createdAt)
		idx.UpdatedAt = timestamppb.New(updatedAt)
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

