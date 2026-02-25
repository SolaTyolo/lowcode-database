package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/solat/lowcode-database/internal/db"
	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

type LowcodeService struct {
	lowcodev1.UnimplementedLowcodeServiceServer
	tenants *db.TenantManager
}

func NewLowcodeService(tenants *db.TenantManager) *LowcodeService {
	return &LowcodeService{tenants: tenants}
}

// -------- Type --------

func (s *LowcodeService) CreateType(ctx context.Context, req *lowcodev1.CreateTypeRequest) (*lowcodev1.CreateTypeResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `
		INSERT INTO lc_types (name, pg_type, config)
		VALUES ($1, $2, $3)
		RETURNING id, name, pg_type, config, created_at, updated_at
	`
	row := pool.QueryRow(ctx, q, req.GetName(), req.GetPgType(), req.GetConfig().AsMap())

	var t lowcodev1.Type
	var cfg map[string]any
	if err := row.Scan(&t.Id, &t.Name, &t.PgType, &cfg, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	if cfg != nil {
		t.Config = toStruct(cfg)
	}
	return &lowcodev1.CreateTypeResponse{Type: &t}, nil
}

func (s *LowcodeService) ListTypes(ctx context.Context, _ *lowcodev1.ListTypesRequest) (*lowcodev1.ListTypesResponse, error) {
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	const q = `SELECT id, name, pg_type, config, created_at, updated_at FROM lc_types ORDER BY name`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res lowcodev1.ListTypesResponse
	for rows.Next() {
		var t lowcodev1.Type
		var cfg map[string]any
		if err := rows.Scan(&t.Id, &t.Name, &t.PgType, &cfg, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		if cfg != nil {
			t.Config = toStruct(cfg)
		}
		res.Types = append(res.Types, &t)
	}
	return &res, rows.Err()
}

func (s *LowcodeService) DeleteType(ctx context.Context, req *lowcodev1.DeleteTypeRequest) (*lowcodev1.DeleteTypeResponse, error) {
	const q = `DELETE FROM lc_types WHERE id = $1`
	pool, err := s.tenants.PoolFor(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, q, req.GetId()); err != nil {
		return nil, err
	}
	return &lowcodev1.DeleteTypeResponse{}, nil
}

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

	var pgType string
	if err := tx.QueryRow(ctx, `SELECT pg_type FROM lc_types WHERE id = $1`, req.GetTypeId()).
		Scan(&pgType); err != nil {
		return nil, err
	}

	pgColumn := "c_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
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

	var tableID, schemaName, tableName, pgColumn string
	if err := tx.QueryRow(ctx, `
		SELECT c.table_id, t.schema_name, t.table_name, c.pg_column
		FROM lc_columns c
		JOIN lc_tables t ON c.table_id = t.id
		WHERE c.id = $1`,
		req.GetId(),
	).Scan(&tableID, &schemaName, &tableName, &pgColumn); err != nil {
		if err == pgx.ErrNoRows {
			return &lowcodev1.DeleteColumnResponse{}, nil
		}
		return nil, err
	}

	drop := fmt.Sprintf(`ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s`,
		pgx.Identifier{schemaName}.Sanitize(),
		pgx.Identifier{tableName}.Sanitize(),
		pgx.Identifier{pgColumn}.Sanitize())
	if _, err := tx.Exec(ctx, drop); err != nil {
		return nil, err
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
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
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
		resp.Rows = append(resp.Rows, row)
	}
	return &resp, rows.Err()
}

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

// -------- helpers --------

type columnMeta struct {
	Id         string
	TableId    string
	Name       string
	TypeId     string
	PgColumn   string
	IsNullable bool
	Position   int32
}

func (s *LowcodeService) loadColumns(ctx context.Context, pool *pgxpool.Pool, tableID string) ([]columnMeta, string, string, error) {
	const q = `
		SELECT c.id, c.table_id, c.name, c.type_id, c.pg_column, c.is_nullable, c.position,
		       t.schema_name, t.table_name
		FROM lc_columns c
		JOIN lc_tables t ON c.table_id = t.id
		WHERE c.table_id = $1
		ORDER BY c.position
	`
	rows, err := pool.Query(ctx, q, tableID)
	if err != nil {
		return nil, "", "", err
	}
	defer rows.Close()

	var cols []columnMeta
	var schemaName, tableName string
	for rows.Next() {
		var c columnMeta
		if err := rows.Scan(&c.Id, &c.TableId, &c.Name, &c.TypeId, &c.PgColumn, &c.IsNullable, &c.Position, &schemaName, &tableName); err != nil {
			return nil, "", "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", "", err
	}
	return cols, schemaName, tableName, nil
}

// valueToAny 把 protobuf Value 转成可以写入 PG 的 Go 值。
func valueToAny(v *lowcodev1.Value) any {
	switch x := v.Kind.(type) {
	case *lowcodev1.Value_StringValue:
		return x.StringValue
	case *lowcodev1.Value_NumberValue:
		return x.NumberValue
	case *lowcodev1.Value_BoolValue:
		return x.BoolValue
	case *lowcodev1.Value_TimestampValue:
		return x.TimestampValue.AsTime()
	case *lowcodev1.Value_BytesValue:
		return x.BytesValue
	case *lowcodev1.Value_JsonValue:
		return x.JsonValue.AsMap()
	default:
		return nil
	}
}

// anyToValue 把 PG 返回的值转成 protobuf Value，简单处理常见类型。
func anyToValue(v any) *lowcodev1.Value {
	switch t := v.(type) {
	case string:
		return &lowcodev1.Value{Kind: &lowcodev1.Value_StringValue{StringValue: t}}
	case []byte:
		return &lowcodev1.Value{Kind: &lowcodev1.Value_BytesValue{BytesValue: t}}
	case bool:
		return &lowcodev1.Value{Kind: &lowcodev1.Value_BoolValue{BoolValue: t}}
	case int32, int64, float32, float64:
		return &lowcodev1.Value{Kind: &lowcodev1.Value_NumberValue{NumberValue: toFloat64(t)}}
	default:
		return &lowcodev1.Value{Kind: &lowcodev1.Value_StringValue{StringValue: fmt.Sprint(v)}}
	}
}

func toFloat64(v any) float64 {
	switch t := v.(type) {
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	default:
		return 0
	}
}

// toStruct: map[string]any -> google.protobuf.Struct
func toStruct(m map[string]any) *structpb.Struct {
	if m == nil {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}

