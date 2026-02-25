package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Index --------

func (s *LowcodeService) CreateIndex(ctx context.Context, req *lowcodev1.CreateIndexRequest) (*lowcodev1.CreateIndexResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tableID := req.GetTableId()
	if tableID == "" {
		return nil, fmt.Errorf("table_id is required")
	}
	cols, schemaName, tableName, err := s.loadColumns(ctx, pool, tableID)
	if err != nil {
		return nil, err
	}

	colIDSet := make(map[string]struct{}, len(req.GetColumnIds()))
	for _, id := range req.GetColumnIds() {
		colIDSet[id] = struct{}{}
	}

	var pgColumns []string
	for _, c := range cols {
		if _, ok := colIDSet[c.Id]; ok {
			pgColumns = append(pgColumns, c.PgColumn)
		}
	}
	if len(pgColumns) == 0 {
		return nil, fmt.Errorf("no valid columns for index")
	}

	pgIndex := "lc_idx_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	indexSQL := fmt.Sprintf(`CREATE %s INDEX %s ON %s.%s (%s)`,
		func() string {
			if req.GetIsUnique() {
				return "UNIQUE"
			}
			return ""
		}(),
		pgx.Identifier{pgIndex}.Sanitize(),
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize(),
		strings.Join(pgColumns, ", "),
	)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, indexSQL); err != nil {
		return nil, err
	}
	const ins = `
		INSERT INTO lc_indexes (table_id, name, pg_index, column_ids, is_unique)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, table_id, name, pg_index, column_ids, is_unique, created_at, updated_at
	`
	row := tx.QueryRow(ctx, ins,
		tableID,
		req.GetName(),
		pgIndex,
		req.GetColumnIds(),
		req.GetIsUnique(),
	)

	var idx lowcodev1.Index
	if err := row.Scan(&idx.Id, &idx.TableId, &idx.Name, &idx.PgIndex, &idx.ColumnIds, &idx.IsUnique, &idx.CreatedAt, &idx.UpdatedAt); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.CreateIndexResponse{Index: &idx}, nil
}

func (s *LowcodeService) DeleteIndex(ctx context.Context, req *lowcodev1.DeleteIndexRequest) (*lowcodev1.DeleteIndexResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var schemaName, tableName, pgIndex string
	if err := tx.QueryRow(ctx, `
		SELECT t.schema_name, t.table_name, i.pg_index
		FROM lc_indexes i
		JOIN lc_tables t ON i.table_id = t.id
		WHERE i.id = $1`,
		req.GetId(),
	).Scan(&schemaName, &tableName, &pgIndex); err != nil {
		if err == pgx.ErrNoRows {
			return &lowcodev1.DeleteIndexResponse{}, nil
		}
		return nil, err
	}

	drop := fmt.Sprintf(`DROP INDEX IF EXISTS %s.%s`,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{pgIndex}.Sanitize(),
	)
	if _, err := tx.Exec(ctx, drop); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM lc_indexes WHERE id = $1`, req.GetId()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.DeleteIndexResponse{}, nil
}

func (s *LowcodeService) ListIndexes(ctx context.Context, req *lowcodev1.ListIndexesRequest) (*lowcodev1.ListIndexesResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `
		SELECT id, table_id, name, pg_index, column_ids, is_unique, created_at, updated_at
		FROM lc_indexes
		WHERE table_id = $1
		ORDER BY name
	`
	rows, err := pool.Query(ctx, q, req.GetTableId())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp lowcodev1.ListIndexesResponse
	for rows.Next() {
		var idx lowcodev1.Index
		if err := rows.Scan(&idx.Id, &idx.TableId, &idx.Name, &idx.PgIndex, &idx.ColumnIds, &idx.IsUnique, &idx.CreatedAt, &idx.UpdatedAt); err != nil {
			return nil, err
		}
		resp.Indexes = append(resp.Indexes, &idx)
	}
	return &resp, rows.Err()
}

