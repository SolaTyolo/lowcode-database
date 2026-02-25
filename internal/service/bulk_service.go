package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Bulk --------

func (s *LowcodeService) BulkUpsertRows(ctx context.Context, req *lowcodev1.BulkUpsertRowsRequest) (*lowcodev1.BulkUpsertRowsResponse, error) {
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
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns for table")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var resp lowcodev1.BulkUpsertRowsResponse

	for _, item := range req.GetItems() {
		if item.GetRowId() == "" {
			// insert
			var pgCols []string
			var args []any
			for _, c := range cols {
				val, ok := item.Cells[c.Id]
				if !ok {
					continue
				}
				pgCols = append(pgCols, c.PgColumn)
				args = append(args, valueToAny(val))
			}
			if len(pgCols) == 0 {
				continue
			}
			colsSQL := strings.Join(pgCols, ", ")
			params := make([]string, len(pgCols))
			for i := range params {
				params[i] = fmt.Sprintf("$%d", i+1)
			}
			paramSQL := strings.Join(params, ", ")
			insert := fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES (%s) RETURNING id`,
				pgx.Identifier{schemaName}.Sanitize(),
				pgx.Identifier{tableName}.Sanitize(),
				colsSQL, paramSQL)
			var id string
			if err := tx.QueryRow(ctx, insert, args...).Scan(&id); err != nil {
				return nil, err
			}
			resp.Rows = append(resp.Rows, &lowcodev1.Row{Id: id, Cells: item.Cells})
		} else {
			// update
			var setParts []string
			var args []any
			argIdx := 1
			for _, c := range cols {
				val, ok := item.Cells[c.Id]
				if !ok {
					continue
				}
				setParts = append(setParts, fmt.Sprintf("%s = $%d", c.PgColumn, argIdx))
				args = append(args, valueToAny(val))
				argIdx++
			}
			if len(setParts) == 0 {
				continue
			}
			args = append(args, item.GetRowId())
			update := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE id = $%d`,
				pgx.Identifier{schemaName}.Sanitize(),
				pgx.Identifier{tableName}.Sanitize(),
				strings.Join(setParts, ", "),
				argIdx,
			)
			if _, err := tx.Exec(ctx, update, args...); err != nil {
				return nil, err
			}
			resp.Rows = append(resp.Rows, &lowcodev1.Row{Id: item.GetRowId(), Cells: item.Cells})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (s *LowcodeService) BulkDeleteRows(ctx context.Context, req *lowcodev1.BulkDeleteRowsRequest) (*lowcodev1.BulkDeleteRowsResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tableID := req.GetTableId()
	if tableID == "" {
		return nil, fmt.Errorf("table_id is required")
	}
	_, schemaName, tableName, err := s.loadColumns(ctx, pool, tableID)
	if err != nil {
		return nil, err
	}
	if len(req.GetRowIds()) == 0 {
		return &lowcodev1.BulkDeleteRowsResponse{}, nil
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	del := fmt.Sprintf(`DELETE FROM %s.%s WHERE id = ANY($1)`,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize(),
	)
	if _, err := tx.Exec(ctx, del, req.GetRowIds()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &lowcodev1.BulkDeleteRowsResponse{}, nil
}

