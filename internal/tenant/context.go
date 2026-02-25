package tenant

import "context"

type ctxKey struct{}

var key ctxKey

// WithTenantID stores tenant id in context.
func WithTenantID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key, id)
}

// FromContext extracts tenant id from context if present.
func FromContext(ctx context.Context) string {
	if v, ok := ctx.Value(key).(string); ok {
		return v
	}
	return ""
}

