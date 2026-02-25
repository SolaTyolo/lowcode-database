package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/solat/lowcode-database/internal/db"
	"github.com/solat/lowcode-database/internal/service"
	"github.com/solat/lowcode-database/internal/tenant"
	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

func main() {
	var (
		grpcAddr = flag.String("grpc-addr", ":9090", "gRPC listen address")
		httpAddr = flag.String("http-addr", ":8080", "HTTP (grpc-gateway + static) listen address")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	tenantMgr, err := db.NewTenantManager(ctx)
	if err != nil {
		log.Fatalf("init tenant manager: %v", err)
	}

	// gRPC server
	unary := grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("x-tenant-id"); len(vals) > 0 {
				ctx = tenant.WithTenantID(ctx, vals[0])
			}
		}
		return handler(ctx, req)
	})

	grpcServer := grpc.NewServer(unary)
	lcSvc := service.NewLowcodeService(tenantMgr)
	lowcodev1.RegisterLowcodeServiceServer(grpcServer, lcSvc)

	go func() {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("listen gRPC: %v", err)
		}
		log.Printf("gRPC listening on %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// HTTP gateway + static
	gwMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if strings.ToLower(key) == "x-tenant-id" {
				return "x-tenant-id", true
			}
			return runtime.DefaultHeaderMatcher(key)
		}),
	)
	opts := []grpc.DialOption{grpc.WithInsecure()}
	if err := lowcodev1.RegisterLowcodeServiceHandlerFromEndpoint(
		ctx, gwMux, *grpcAddr, opts,
	); err != nil {
		log.Fatalf("register gateway: %v", err)
	}

	mux := http.NewServeMux()
	// API
	mux.Handle("/v1/", gwMux)

	// Static files (index.html)
	cwd, _ := os.Getwd()
	staticDir := filepath.Join(cwd, "static")
	fs := http.FileServer(http.Dir(staticDir))
	mux.Handle("/", fs)

	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("HTTP listening on %s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	grpcServer.GracefulStop()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)
}

