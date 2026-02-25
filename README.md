# Lowcode Database (Postgres + gRPC + HTTP)

一个基于 Postgres 的简易 low-code / nocode 表格服务，支持：

- **Type**：列类型定义
- **Table**：动态创建/删除逻辑表
- **Column**：按列增删改（底层 ALTER TABLE）
- **Row/Cell**：创建/更新/删除单行和批量行
- **Index**：按列创建/删除索引
- 同时支持 **gRPC** 与 **HTTP(JSON)**（通过 grpc-gateway）
- 支持 **单库模式** 与 **多租户（数据库级隔离）模式**

## 环境要求

- Go 1.22+
- protoc（Protocol Buffers 编译器）
- protoc 插件：
  - `protoc-gen-go`
  - `protoc-gen-go-grpc`
  - `protoc-gen-grpc-gateway`
- 一个可访问的 Postgres 实例

## 安装 proto 相关插件（示例）

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
```

确保 `$GOBIN`（或者 `$GOPATH/bin`）在 `PATH` 中。

## 生成 protobuf 对应的 Go 代码

在项目根目录执行：

```bash
make proto
```

生成的代码会放到 `gen/` 目录下（与 `proto/` 中的包路径一致）。

## 运行服务

### 单例模式（默认，不开放多租户）

使用固定数据库（例如 `tables`）：

```bash
export TENANT_MODE=single
export SINGLE_DATABASE_URL='postgresql://treelab:treelab@0.0.0.0:5432/tables'
# 或者使用 DATABASE_URL（当 SINGLE_DATABASE_URL 未设置时）
# export DATABASE_URL='postgresql://treelab:treelab@0.0.0.0:5432/tables'

make run
```

服务启动后：

- gRPC 监听：`localhost:9090`
- HTTP（grpc-gateway + 静态页面）：`http://localhost:8080/`

### 多租户模式（数据库级隔离）

每个租户使用一个独立的 Postgres 数据库，例如连接串为：

`postgresql://treelab:treelab@0.0.0.0:5432/<tenant_id>`

配置环境变量：

```bash
export TENANT_MODE=multi
export TENANT_DSN_TEMPLATE='postgresql://treelab:treelab@0.0.0.0:5432/%s'

make run
```

访问时在 HTTP 头或 gRPC metadata 中带上租户 ID：

- HTTP 头：`X-Tenant-Id: tenant_a`
- 将自动连接到：`postgresql://treelab:treelab@0.0.0.0:5432/tenant_a`

每个租户数据库都会独立创建自身的：

- `lc_types`
- `lc_tables`
- `lc_columns`
- `lc_indexes`

## 测试页面

项目内置了一个简单的 HTML 测试页：

- 路径：`static/index.html`
- 访问地址：`http://localhost:8080/`

功能：

- 创建 Table
- 为 Table 添加 Column
- 根据列信息生成表单并创建 Record（Row）

如果在多租户模式下测试，可以使用浏览器开发者工具或自行扩展该页面，在每次 `fetch` 请求中添加 `X-Tenant-Id` 头。

## 常用命令汇总

- **生成 proto 对应 Go 代码**

  ```bash
  make proto
  ```

- **启动服务**

  ```bash
  make run
  ```

- **清理生成代码**

  ```bash
  make clean
  ```

