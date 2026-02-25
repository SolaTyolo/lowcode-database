package service

import (
	"github.com/solat/lowcode-database/internal/db"
	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

type LowcodeService struct {
	lowcodev1.UnimplementedLowcodeServiceServer
	tenants *db.TenantManager

	// maxRow controls the default and maximum rows returned by ListRows.
	// If <= 0, ListRows falls back to its internal defaults.
	maxRow int32
}

func NewLowcodeService(tenants *db.TenantManager, maxRow int) *LowcodeService {
	s := &LowcodeService{
		tenants: tenants,
	}
	if maxRow > 0 {
		s.maxRow = int32(maxRow)
	}
	return s
}

