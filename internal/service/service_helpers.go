package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/structpb"

	lowcodev1 "github.com/solat/lowcode-database/gen/lowcode/v1"
)

// -------- shared helpers --------

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
		JOIN lc_types ty ON c.type_id = ty.id
		WHERE c.table_id = $1
		  AND COALESCE(ty.config->>'kind', '') NOT IN ('formula', 'relationship')
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

// relationshipColumn 表示一个 relationship 类型列的元数据，用于 expand 查询。
// Config 约定：target_table_id=关联表 id；link_column_id=子表中外键列 id（一对多）；target_column_id=本表中外键列 id（多对一/一对一）。
type relationshipColumn struct {
	Id               string
	TargetTableId    string
	LinkColumnId     string   // 子表指向当前表行 id 的列，有则为一对多
	TargetColumnId   string   // 本表存目标行 id 的列，有则为多对一/一对一
}

// loadRelationshipColumns 加载表中指定 id 的 relationship 列及其 config。
func (s *LowcodeService) loadRelationshipColumns(ctx context.Context, pool *pgxpool.Pool, tableID string, columnIDs []string) ([]relationshipColumn, error) {
	if len(columnIDs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(columnIDs))
	args := make([]any, 0, 1+len(columnIDs))
	args = append(args, tableID)
	for i := range columnIDs {
		placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
		args = append(args, columnIDs[i])
	}
	q := fmt.Sprintf(`
		SELECT c.id, c.config
		FROM lc_columns c
		JOIN lc_types ty ON c.type_id = ty.id
		WHERE c.table_id = $1 AND c.id IN (%s)
		  AND COALESCE(ty.config->>'kind', '') = 'relationship'
	`, strings.Join(placeholders, ", "))
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []relationshipColumn
	for rows.Next() {
		var id string
		var cfg map[string]any
		if err := rows.Scan(&id, &cfg); err != nil {
			return nil, err
		}
		rc := relationshipColumn{Id: id}
		if cfg != nil {
			if v, _ := cfg["target_table_id"].(string); v != "" {
				rc.TargetTableId = v
			}
			if v, _ := cfg["link_column_id"].(string); v != "" {
				rc.LinkColumnId = v
			}
			if v, _ := cfg["target_column_id"].(string); v != "" {
				rc.TargetColumnId = v
			}
		}
		if rc.TargetTableId == "" {
			continue
		}
		if rc.LinkColumnId == "" && rc.TargetColumnId == "" {
			continue
		}
		out = append(out, rc)
	}
	return out, rows.Err()
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

