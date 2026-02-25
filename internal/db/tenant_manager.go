package db

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/solat/lowcode-database/internal/tenant"
)

type TenantMode string

const (
	TenantModeSingle TenantMode = "single"
	TenantModeMulti  TenantMode = "multi"
)

// TenantManager manages connection pools per tenant (or a single shared pool).
type TenantManager struct {
	mode       TenantMode
	singlePool *pgxpool.Pool

	tenantTemplate string // e.g. "postgresql://user:pass@host:5432/%s"

	mu    sync.RWMutex
	pools map[string]*pgxpool.Pool
}

// NewTenantManager configures single or multi-tenant mode from env:
//   TENANT_MODE=single|multi (default: single)
//   SINGLE_DATABASE_URL=...  (fallback to DATABASE_URL)
//   TENANT_DSN_TEMPLATE=postgresql://user:pass@host:5432/%s (for multi)
func NewTenantManager(ctx context.Context) (*TenantManager, error) {
	mode := TenantMode(os.Getenv("TENANT_MODE"))
	if mode != TenantModeMulti {
		mode = TenantModeSingle
	}

	m := &TenantManager{
		mode:  mode,
		pools: make(map[string]*pgxpool.Pool),
	}

	if mode == TenantModeSingle {
		dsn := os.Getenv("SINGLE_DATABASE_URL")
		if dsn == "" {
			dsn = os.Getenv("DATABASE_URL")
		}
		if dsn == "" {
			return nil, fmt.Errorf("SINGLE_DATABASE_URL or DATABASE_URL must be set in single mode")
		}
		pool, err := NewPoolFromDSN(ctx, dsn)
		if err != nil {
			return nil, err
		}
		if err := EnsureMetaSchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		m.singlePool = pool
	} else {
		tpl := os.Getenv("TENANT_DSN_TEMPLATE")
		if tpl == "" {
			return nil, fmt.Errorf("TENANT_DSN_TEMPLATE must be set in multi mode")
		}
		m.tenantTemplate = tpl
	}

	return m, nil
}

// PoolFor returns the appropriate pool for the current request context.
// In single mode it always returns the shared pool.
// In multi mode it uses tenant ID from context to pick/create a tenant-specific pool.
func (m *TenantManager) PoolFor(ctx context.Context) (*pgxpool.Pool, error) {
	if m.mode == TenantModeSingle {
		return m.singlePool, nil
	}

	tenantID := tenant.FromContext(ctx)
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required in multi-tenant mode")
	}
	return m.poolForTenant(ctx, tenantID)
}

func (m *TenantManager) poolForTenant(ctx context.Context, tenantID string) (*pgxpool.Pool, error) {
	m.mu.RLock()
	if pool, ok := m.pools[tenantID]; ok {
		m.mu.RUnlock()
		return pool, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// double-check
	if pool, ok := m.pools[tenantID]; ok {
		return pool, nil
	}

	dsn := fmt.Sprintf(m.tenantTemplate, tenantID)
	pool, err := NewPoolFromDSN(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := EnsureMetaSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	m.pools[tenantID] = pool
	return pool, nil
}

