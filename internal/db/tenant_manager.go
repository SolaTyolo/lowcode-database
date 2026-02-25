package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/solat/lowcode-database/internal/config"
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
	adminPool      *pgxpool.Pool

	mu    sync.RWMutex
	pools map[string]*pgxpool.Pool
}

// NewTenantManager configures single or multi-tenant mode from Config.
//
//	tenant_mode=single|multi (default: single)
//	single_database_url=...              (logical data DB for single-tenant mode)
//	database_url=...                     (admin DSN used to create 'tables' DB in single mode)
//	tenant_dsn_template=postgresql://user:pass@host:5432/%s (for multi)
func NewTenantManager(ctx context.Context, cfg *config.Config) (*TenantManager, error) {
	mode := TenantMode(cfg.TenantMode)
	if mode != TenantModeMulti {
		mode = TenantModeSingle
	}

	m := &TenantManager{
		mode: mode,
		// pools only used in multi-tenant mode, but we can init eagerly.
		pools: make(map[string]*pgxpool.Pool),
	}

	if mode == TenantModeSingle {
		// single 模式：
		// 1. 使用 admin DSN 连接到管理库（通常是 postgres），从 SINGLE_DATABASE_URL 解析出 database 名字并执行 CREATE DATABASE。
		// 2. 用 SINGLE_DATABASE_URL（若未设置则回退到 admin DSN）创建连接池，并在其上做迁移。

		// 用于真正业务数据的 DSN。
		targetDSN := cfg.SingleDatabaseURL
		if targetDSN == "" {
			targetDSN = cfg.DatabaseURL
		}
		if targetDSN == "" {
			return nil, fmt.Errorf("single_database_url or database_url must be set in single mode")
		}

		dbName, err := dbNameFromDSN(targetDSN)
		if err != nil {
			return nil, fmt.Errorf("parse database name from DSN %q: %w", targetDSN, err)
		}

		// adminDSN 用于建库：优先使用 cfg.DatabaseURL（通常指向 postgres），否则退回到 targetDSN。
		adminDSN := cfg.DatabaseURL
		if adminDSN == "" {
			adminDSN = targetDSN
		}

		adminPool, err := NewPoolFromDSN(ctx, adminDSN)
		if err != nil {
			return nil, fmt.Errorf("create single-mode admin pool: %w", err)
		}

		// 按 SINGLE_DATABASE_URL 中的库名执行 CREATE DATABASE，若已存在则忽略 duplicate_database 错误。
		createStmt := fmt.Sprintf(`CREATE DATABASE %s`, (pgx.Identifier{dbName}).Sanitize())
		if _, err := adminPool.Exec(ctx, createStmt); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P04" {
				// duplicate_database -> ok
			} else {
				adminPool.Close()
				return nil, fmt.Errorf("create single-tenant database %q: %w", dbName, err)
			}
		}
		adminPool.Close()

		pool, err := NewPoolFromDSN(ctx, targetDSN)
		if err != nil {
			return nil, err
		}
		if err := EnsureMetaSchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		m.singlePool = pool
	} else {
		tpl := cfg.TenantDSNTemplate
		if tpl == "" {
			return nil, fmt.Errorf("tenant_dsn_template must be set in multi mode")
		}
		m.tenantTemplate = tpl

		// 建立一个用于创建数据库的管理连接（通常连到 postgres 库）。
		adminDB := cfg.TenantAdminDB
		if adminDB == "" {
			adminDB = "postgres"
		}
		adminDSN := fmt.Sprintf(tpl, adminDB)
		adminPool, err := NewPoolFromDSN(ctx, adminDSN)
		if err != nil {
			return nil, fmt.Errorf("create admin pool: %w", err)
		}
		m.adminPool = adminPool
	}

	return m, nil
}

// dbNameFromDSN 解析 PostgreSQL DSN 中的数据库名（path 最后一段）。
func dbNameFromDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return "", fmt.Errorf("dsn %q does not contain database name", dsn)
	}
	parts := strings.Split(path, "/")
	return parts[len(parts)-1], nil
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
	m.pools[tenantID] = pool
	return pool, nil
}

// CreateTenant 创建（如果不存在的话）并初始化一个 tenant 对应的数据库。
// 仅在多租户模式下有效。
func (m *TenantManager) CreateTenant(ctx context.Context, tenantID string) error {
	if m.mode != TenantModeMulti {
		return fmt.Errorf("CreateTenant is only supported in multi-tenant mode")
	}
	if tenantID == "" {
		return fmt.Errorf("tenantID is required")
	}
	if m.adminPool == nil {
		return fmt.Errorf("admin pool is not configured")
	}

	// 创建数据库（若已存在则忽略 42P04 错误）。
	createStmt := fmt.Sprintf(`CREATE DATABASE %s`, (pgx.Identifier{tenantID}).Sanitize())
	if _, err := m.adminPool.Exec(ctx, createStmt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P04" {
			// duplicate_database, 忽略
		} else {
			return fmt.Errorf("create database for tenant %s: %w", tenantID, err)
		}
	}

	// 为该 tenant 建立连接池并执行迁移（只在创建时执行一次）。
	dsn := fmt.Sprintf(m.tenantTemplate, tenantID)
	pool, err := NewPoolFromDSN(ctx, dsn)
	if err != nil {
		return fmt.Errorf("create pool for tenant %s: %w", tenantID, err)
	}
	if err := EnsureMetaSchema(ctx, pool); err != nil {
		pool.Close()
		return fmt.Errorf("migrate tenant %s: %w", tenantID, err)
	}

	m.mu.Lock()
	if old, ok := m.pools[tenantID]; ok {
		old.Close()
	}
	m.pools[tenantID] = pool
	m.mu.Unlock()

	return nil
}
