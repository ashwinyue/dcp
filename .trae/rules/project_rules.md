# 项目构建规则

## 构建输出目录
- 所有构建的二进制文件必须放在 `bin/` 目录下
- 使用 `go build -o bin/<binary_name>` 命令进行构建
- 确保 `bin/` 目录在 `.gitignore` 中被正确配置

## 构建命令示例
```bash
# 构建 nightwatch 服务
go build -o bin/nightwatch ./cmd/nightwatch

# 构建 mb-apiserver 服务
go build -o bin/mb-apiserver ./cmd/mb-apiserver
```

## 项目目录结构
```
dcp/
├── .air.toml                    # Air 热重载配置
├── .gitignore                   # Git 忽略文件
├── .golangci.yaml              # Go 代码检查配置
├── .idea/                      # IDE 配置目录
├── .protolint.yaml             # Protocol Buffer 检查配置
├── .trae/                      # Trae IDE 配置
│   └── rules/
│       └── project_rules.md    # 项目规则文档
├── LICENSE                     # 许可证文件
├── Makefile                    # 构建脚本
├── README.md                   # 项目说明文档
├── a-demo/                     # 演示代码目录
├── api/                        # API 定义目录
│   └── openapi/
│       ├── apiserver/          # API 服务器 OpenAPI 定义
│       └── nightwatch/         # Nightwatch 服务 OpenAPI 定义
├── bin/                        # 构建输出目录
│   ├── nightwatch              # Nightwatch 服务二进制文件
│   └── mb-apiserver            # API 服务器二进制文件
├── build/                      # 构建相关文件
│   └── docker/
│       ├── mb-apiserver/       # API 服务器 Docker 文件
│       └── nightwatch/         # Nightwatch 服务 Docker 文件
├── cmd/                        # 命令行程序入口
│   ├── gen-gorm-model/         # GORM 模型生成工具
│   ├── mb-apiserver/           # API 服务器主程序
│   └── nightwatch/             # Nightwatch 服务主程序
├── configs/                    # 配置文件目录
│   ├── README.md
│   ├── job.sql                 # 任务相关 SQL
│   ├── mb-apiserver.yaml       # API 服务器配置
│   ├── miniblog.sql            # 数据库初始化 SQL
│   ├── nginx.loadbalance.conf  # Nginx 负载均衡配置
│   ├── nginx.reverse.conf      # Nginx 反向代理配置
│   └── nightwatch.yaml         # Nightwatch 服务配置
├── deployments/                # Kubernetes 部署文件
│   ├── mb-apiserver-configmap.yaml
│   ├── mb-apiserver-deployment.yaml
│   └── mb-apiserver-service.yaml
├── docs/                       # 文档目录
│   ├── book/                   # 技术书籍相关文档
│   ├── devel/                  # 开发文档
│   ├── guide/                  # 使用指南
│   └── images/                 # 图片资源
├── examples/                   # 示例代码
│   ├── client/                 # 客户端示例
│   ├── errorsx/                # 错误处理示例
│   ├── gin/                    # Gin 框架示例
│   ├── helper/                 # 辅助工具示例
│   ├── logpattern/             # 日志模式示例
│   ├── performance/            # 性能测试示例
│   ├── simple.mk               # 简单 Makefile 示例
│   └── validation/             # 验证示例
├── go.mod                      # Go 模块定义
├── go.sum                      # Go 模块校验和
├── init/                       # 初始化脚本
├── internal/                   # 内部包目录
│   ├── apiserver/              # API 服务器实现
│   │   ├── biz/                # 业务逻辑层
│   │   ├── grpcserver.go       # gRPC 服务器
│   │   ├── handler/            # 处理器层
│   │   ├── httpserver.go       # HTTP 服务器
│   │   ├── model/              # 数据模型
│   │   ├── pkg/                # 内部包
│   │   ├── server.go           # 服务器主逻辑
│   │   ├── store/              # 数据存储层
│   │   ├── wire.go             # 依赖注入配置
│   │   └── wire_gen.go         # 依赖注入生成代码
│   ├── nightwatch/             # Nightwatch 服务实现
│   │   ├── biz/                # 业务逻辑层
│   │   ├── grpcserver.go       # gRPC 服务器
│   │   ├── handler/            # 处理器层
│   │   ├── httpserver.go       # HTTP 服务器
│   │   ├── model/              # 数据模型
│   │   ├── pkg/                # 内部包
│   │   ├── server.go           # 服务器主逻辑
│   │   ├── store/              # 数据存储层
│   │   ├── watcher/            # 监控器实现
│   │   ├── wire.go             # 依赖注入配置
│   │   └── wire_gen.go         # 依赖注入生成代码
│   └── pkg/                    # 共享内部包
│       ├── contextx/           # 上下文扩展
│       ├── errno/              # 错误码定义
│       ├── known/              # 常量定义
│       ├── log/                # 日志包
│       ├── middleware/         # 中间件
│       ├── rid/                # 请求 ID
│       └── server/             # 服务器基础包
├── pkg/                        # 公共包目录
│   └── api/
│       ├── apiserver/          # API 服务器公共接口
│       └── nightwatch/         # Nightwatch 服务公共接口
├── scripts/                    # 脚本目录
│   ├── boilerplate.txt         # 代码模板
│   ├── coverage.awk            # 覆盖率统计脚本
│   ├── gen_token.sh            # Token 生成脚本
│   ├── make-rules/             # Makefile 规则
│   ├── test_nginx.sh           # Nginx 测试脚本
│   ├── test_smoke.sh           # 冒烟测试脚本
│   ├── test_tls.sh             # TLS 测试脚本
│   └── wrktest.sh              # 压力测试脚本
├── staging/                    # 暂存目录
│   └── src/
│       └── github.com/
└── third_party/                # 第三方依赖
    └── protobuf/
        ├── github.com/
        ├── google/
        └── protoc-gen-openapiv2/
```

## 数据库配置
- 默认使用 MySQL 数据库
- 配置文件位于 `configs/` 目录
- 支持自动迁移数据库表结构
- 测试环境可选择使用 SQLite 内存数据库