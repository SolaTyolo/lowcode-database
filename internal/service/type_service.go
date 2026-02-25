package service

import (
	"context"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

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

