package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/structpb"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Row / Cell --------

func (s *LowcodeService) CreateRow(ctx context.Context, req *lowcodev1.CreateRowRequest) (*lowcodev1.CreateRowResponse, error) {
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

	if len(req.GetCells()) == 0 {
		return nil, fmt.Errorf("cells is empty")
	}

	var pgCols []string
	var args []any
	argPos := 1
	for _, c := range cols {
		val, ok := req.Cells[c.Id]
		if !ok {
			continue
		}
		pgCols = append(pgCols, c.PgColumn)
		args = append(args, valueToAny(val))
		_ = argPos
	}

	if len(pgCols) == 0 {
		return nil, fmt.Errorf("no valid cells for known columns")
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
		colsSQL,
		paramSQL,
	)

	var rowID string
	if err := pool.QueryRow(ctx, insert, args...).Scan(&rowID); err != nil {
		return nil, err
	}

	return &lowcodev1.CreateRowResponse{
		Row: &lowcodev1.Row{
			Id:    rowID,
			Cells: req.GetCells(),
		},
	}, nil
}

// 这里为了简单，只实现按 id 精确匹配的 UpdateRow，BulkUpsertRows 里会复用。
func (s *LowcodeService) UpdateRow(ctx context.Context, req *lowcodev1.UpdateRowRequest) (*lowcodev1.UpdateRowResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tableID := req.GetTableId()
	if tableID == "" || req.GetRowId() == "" {
		return nil, fmt.Errorf("table_id and row_id are required")
	}

	cols, schemaName, tableName, err := s.loadColumns(ctx, pool, tableID)
	if err != nil {
		return nil, err
	}
	if len(req.GetCells()) == 0 {
		return nil, fmt.Errorf("cells is empty")
	}

	var setParts []string
	var args []any
	argIdx := 1
	for _, c := range cols {
		val, ok := req.Cells[c.Id]
		if !ok {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s = $%d", c.PgColumn, argIdx))
		args = append(args, valueToAny(val))
		argIdx++
	}
	if len(setParts) == 0 {
		return nil, fmt.Errorf("no valid cells for known columns")
	}

	args = append(args, req.GetRowId())
	update := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE id = $%d`,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize(),
		strings.Join(setParts, ", "),
		argIdx,
	)
	if _, err := pool.Exec(ctx, update, args...); err != nil {
		return nil, err
	}

	return &lowcodev1.UpdateRowResponse{
		Row: &lowcodev1.Row{
			Id:    req.GetRowId(),
			Cells: req.GetCells(),
		},
	}, nil
}

func (s *LowcodeService) DeleteRow(ctx context.Context, req *lowcodev1.DeleteRowRequest) (*lowcodev1.DeleteRowResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	tableID := req.GetTableId()
	if tableID == "" || req.GetRowId() == "" {
		return nil, fmt.Errorf("table_id and row_id are required")
	}

	_, schemaName, tableName, err := s.loadColumns(ctx, pool, tableID)
	if err != nil {
		return nil, err
	}

	del := fmt.Sprintf(`DELETE FROM %s.%s WHERE id = $1`,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize())
	if _, err := pool.Exec(ctx, del, req.GetRowId()); err != nil {
		return nil, err
	}
	return &lowcodev1.DeleteRowResponse{}, nil
}

// 简化实现：ListRows 只做无条件分页。
func (s *LowcodeService) ListRows(ctx context.Context, req *lowcodev1.ListRowsRequest) (*lowcodev1.ListRowsResponse, error) {
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
		return &lowcodev1.ListRowsResponse{}, nil
	}

	pageSize := req.GetPageSize()
	// Apply MAX_ROW config as both default and upper bound when set.
	maxRow := s.maxRow
	if pageSize <= 0 {
		if maxRow > 0 {
			pageSize = maxRow
		} else {
			pageSize = 50
		}
	}
	if maxRow > 0 && pageSize > maxRow {
		pageSize = maxRow
	} else if maxRow <= 0 && pageSize > 100 {
		// Backward-compatible hard cap when MAX_ROW is not configured.
		pageSize = 100
	}

	// 目前忽略 page_token，简单 offset=0。
	columnSQL := "id"
	for _, c := range cols {
		columnSQL += ", " + c.PgColumn
	}

	query := fmt.Sprintf(`SELECT %s FROM %s.%s ORDER BY id LIMIT $1`,
		columnSQL,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize(),
	)
	rows, err := pool.Query(ctx, query, pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp lowcodev1.ListRowsResponse
	for rows.Next() {
		scanTargets := make([]any, 1+len(cols))
		var id string
		scanTargets[0] = &id
		values := make([]any, len(cols))
		for i := range values {
			values[i] = new(any)
			scanTargets[i+1] = values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}

		row := &lowcodev1.Row{
			Id:    id,
			Cells: make(map[string]*lowcodev1.Value, len(cols)),
		}
		for i, c := range cols {
			vPtr := values[i].(*any)
			if *vPtr == nil {
				continue
			}
			row.Cells[c.Id] = anyToValue(*vPtr)
		}

		// 展开 relationship 列：把子表/关联表数据放入 cells，值为 json_value { "rows": [ { "id", "cells" }, ... ] }
		if len(req.GetExpandColumnIds()) > 0 {
			relCols, err := s.loadRelationshipColumns(ctx, pool, tableID, req.GetExpandColumnIds())
			if err != nil {
				return nil, err
			}
			for _, rel := range relCols {
				related, err := s.fetchRelatedRows(ctx, pool, rel, id, row.Cells)
				if err != nil {
					return nil, err
				}
				if related == nil {
					related = []*structpb.Value{}
				}
				listVal := &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: related}}}
				expandStruct := &structpb.Struct{Fields: map[string]*structpb.Value{"rows": listVal}}
				if row.Cells == nil {
					row.Cells = make(map[string]*lowcodev1.Value)
				}
				row.Cells[rel.Id] = &lowcodev1.Value{Kind: &lowcodev1.Value_JsonValue{JsonValue: expandStruct}}
			}
		}

		resp.Rows = append(resp.Rows, row)
	}
	return &resp, rows.Err()
}

// fetchRelatedRows 根据 relationship 配置查询关联行，返回可序列化为 JSON 的 []*structpb.Value（每项为 { "id", "cells" }）。
func (s *LowcodeService) fetchRelatedRows(ctx context.Context, pool *pgxpool.Pool, rel relationshipColumn, currentRowID string, currentRowCells map[string]*lowcodev1.Value) ([]*structpb.Value, error) {
	targetCols, targetSchema, targetTable, err := s.loadColumns(ctx, pool, rel.TargetTableId)
	if err != nil {
		return nil, err
	}
	if len(targetCols) == 0 {
		return nil, nil
	}

	var query string
	var args []any

	if rel.LinkColumnId != "" {
		// 一对多：子表中外键列 = 当前行 id
		var linkPgCol string
		if err := pool.QueryRow(ctx, `SELECT pg_column FROM lc_columns WHERE id = $1`, rel.LinkColumnId).Scan(&linkPgCol); err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		columnSQL := "id"
		for _, c := range targetCols {
			columnSQL += ", " + c.PgColumn
		}
		query = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE %s = $1 ORDER BY id`,
			columnSQL,
			pgx.Identifier{targetSchema}.Sanitize(),
			pgx.Identifier{targetTable}.Sanitize(),
			pgx.Identifier{linkPgCol}.Sanitize(),
		)
		args = []any{currentRowID}
	} else {
		// 多对一/一对一：当前行某列存目标行 id，查目标表 by id
		var relatedID string
		if v, ok := currentRowCells[rel.TargetColumnId]; ok && v != nil {
			if sv, ok := v.Kind.(*lowcodev1.Value_StringValue); ok && sv != nil {
				relatedID = sv.StringValue
			}
		}
		if relatedID == "" {
			return nil, nil
		}
		columnSQL := "id"
		for _, c := range targetCols {
			columnSQL += ", " + c.PgColumn
		}
		query = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE id = $1`,
			columnSQL,
			pgx.Identifier{targetSchema}.Sanitize(),
			pgx.Identifier{targetTable}.Sanitize(),
		)
		args = []any{relatedID}
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*structpb.Value
	for rows.Next() {
		scanTargets := make([]any, 1+len(targetCols))
		var rowID string
		scanTargets[0] = &rowID
		values := make([]any, len(targetCols))
		for i := range values {
			values[i] = new(any)
			scanTargets[i+1] = values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}
		cellsMap := make(map[string]interface{}, len(targetCols))
		for i, c := range targetCols {
			vPtr := values[i].(*any)
			if *vPtr != nil {
				cellsMap[c.Id] = *vPtr
			}
		}
		rowMap := map[string]interface{}{"id": rowID, "cells": cellsMap}
		s, err := structpb.NewStruct(rowMap)
		if err != nil {
			return nil, err
		}
		list = append(list, structpb.NewStructValue(s))
	}
	return list, rows.Err()
}

