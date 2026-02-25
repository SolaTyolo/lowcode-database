package service

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- Tenant --------

func (s *LowcodeService) CreateTenant(ctx context.Context, req *lowcodev1.CreateTenantRequest) (*lowcodev1.CreateTenantResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.tenants.CreateTenant(ctx, id); err != nil {
		return nil, status.Errorf(codes.Internal, "create tenant %s: %v", id, err)
	}
	return &lowcodev1.CreateTenantResponse{Id: id}, nil
}

