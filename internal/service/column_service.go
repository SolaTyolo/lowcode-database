package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Column --------

func (s *LowcodeService) AddColumn(ctx context.Context, req *lowcodev1.AddColumnRequest) (*lowcodev1.AddColumnResponse, error) {
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
	if err := tx.QueryRow(ctx, `SELECT schema_name, table_name FROM lc_tables WHERE id = $1`, req.GetTableId()).
		Scan(&schemaName, &tableName); err != nil {
		return nil, err
	}

	var pgType, kind string
	if err := tx.QueryRow(ctx, `
		SELECT pg_type, COALESCE(config->>'kind', '')
		FROM lc_types
		WHERE id = $1`,
		req.GetTypeId(),
	).Scan(&pgType, &kind); err != nil {
		return nil, err
	}

	isVirtual := kind == "formula" || kind == "relationship"

	// 为物理列生成真实 PG 列名；虚拟列则使用一个不会在 SQL 中引用的占位名。
	pgColumn := "c_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
	if isVirtual {
		pgColumn = "v_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
	} else {
		nullSQL := "NULL"
		if !req.GetIsNullable() {
			nullSQL = "NOT NULL"
		}
		alter := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN %s %s %s`,
			pgx.Identifier{schemaName}.Sanitize(),
			pgx.Identifier{tableName}.Sanitize(),
			pgx.Identifier{pgColumn}.Sanitize(),
			pgType,
			nullSQL,
		)
		if _, err := tx.Exec(ctx, alter); err != nil {
			return nil, err
		}
	}

	const ins = `
		INSERT INTO lc_columns (table_id, name, type_id, pg_column, is_nullable, position, config)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, table_id, name, type_id, pg_column, is_nullable, position, config, created_at, updated_at
	`
	row := tx.QueryRow(ctx, ins,
		req.GetTableId(),
		req.GetName(),
		req.GetTypeId(),
		pgColumn,
		req.GetIsNullable(),
		req.GetPosition(),
		req.GetConfig().AsMap(),
	)

	var c lowcodev1.Column
	var cfg map[string]any
	if err := row.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &cfg, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	if cfg != nil {
		c.Config = toStruct(cfg)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.AddColumnResponse{Column: &c}, nil
}

func (s *LowcodeService) ListColumns(ctx context.Context, req *lowcodev1.ListColumnsRequest) (*lowcodev1.ListColumnsResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `
		SELECT id, table_id, name, type_id, pg_column, is_nullable, position, config, created_at, updated_at
		FROM lc_columns
		WHERE table_id = $1
		ORDER BY position
	`
	rows, err := pool.Query(ctx, q, req.GetTableId())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res lowcodev1.ListColumnsResponse
	for rows.Next() {
		var c lowcodev1.Column
		var cfg map[string]any
		if err := rows.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &cfg, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		if cfg != nil {
			c.Config = toStruct(cfg)
		}
		res.Columns = append(res.Columns, &c)
	}
	return &res, rows.Err()
}

func (s *LowcodeService) DeleteColumn(ctx context.Context, req *lowcodev1.DeleteColumnRequest) (*lowcodev1.DeleteColumnResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var tableID, schemaName, tableName, pgColumn, kind string
	if err := tx.QueryRow(ctx, `
		SELECT c.table_id, t.schema_name, t.table_name, c.pg_column, COALESCE(ty.config->>'kind', '')
		FROM lc_columns c
		JOIN lc_tables t ON c.table_id = t.id
		JOIN lc_types ty ON c.type_id = ty.id
		WHERE c.id = $1`,
		req.GetId(),
	).Scan(&tableID, &schemaName, &tableName, &pgColumn, &kind); err != nil {
		if err == pgx.ErrNoRows {
			return &lowcodev1.DeleteColumnResponse{}, nil
		}
		return nil, err
	}

	isVirtual := kind == "formula" || kind == "relationship"
	if !isVirtual {
		drop := fmt.Sprintf(`ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s`,
			pgx.Identifier{schemaName}.Sanitize(),
			pgx.Identifier{tableName}.Sanitize(),
			pgx.Identifier{pgColumn}.Sanitize())
		if _, err := tx.Exec(ctx, drop); err != nil {
			return nil, err
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM lc_columns WHERE id = $1`, req.GetId()); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.DeleteColumnResponse{}, nil
}

// 简化：UpdateColumn 目前只更新元数据，不做 PG 表 rename/alter。
func (s *LowcodeService) UpdateColumn(ctx context.Context, req *lowcodev1.UpdateColumnRequest) (*lowcodev1.UpdateColumnResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `
		UPDATE lc_columns
		SET name = COALESCE(NULLIF($2, ''), name),
		    is_nullable = COALESCE($3, is_nullable),
		    position = COALESCE(NULLIF($4, 0), position),
		    config = COALESCE($5, config),
		    updated_at = now()
		WHERE id = $1
		RETURNING id, table_id, name, type_id, pg_column, is_nullable, position, config, created_at, updated_at
	`
	var c lowcodev1.Column
	var cfgMap map[string]any
	var isNullable *bool
	if req.IsNullable {
		v := req.GetIsNullable()
		isNullable = &v
	}
	row := pool.QueryRow(ctx, q, req.GetId(), req.GetName(), isNullable, req.GetPosition(), req.GetConfig().AsMap())
	if err := row.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &cfgMap, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	if cfgMap != nil {
		c.Config = toStruct(cfgMap)
	}
	return &lowcodev1.UpdateColumnResponse{Column: &c}, nil
}

