// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package nightwatch

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	genericoptions "github.com/onexstack/onexstack/pkg/options"
	"github.com/onexstack/onexstack/pkg/watch"
	"github.com/onexstack/onexstack/pkg/watch/logger/onex"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/biz"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/validation"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	_ "github.com/ashwinyue/dcp/internal/nightwatch/watcher/all"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/ashwinyue/dcp/internal/pkg/server"
)

const (
	// GRPCServerMode 定义 gRPC 服务模式.
	// 使用 gRPC 框架启动一个 gRPC 服务器.
	GRPCServerMode = "grpc"
	// GRPCGatewayServerMode 定义 gRPC + HTTP 服务模式.
	// 使用 gRPC 框架启动一个 gRPC 服务器 + HTTP 反向代理服务器.
	GRPCGatewayServerMode = "grpc-gateway"
	// GinServerMode 定义 Gin 服务模式.
	// 使用 Gin Web 框架启动一个 HTTP 服务器.
	GinServerMode = "gin"
)



// Config 配置结构体，用于存储应用相关的配置.
// 不用 viper.Get，是因为这种方式能更加清晰的知道应用提供了哪些配置项.
type Config struct {
	ServerMode        string
	EnableMemoryStore bool
	TLSOptions        *genericoptions.TLSOptions
	HTTPOptions       *genericoptions.HTTPOptions
	GRPCOptions       *genericoptions.GRPCOptions
	MySQLOptions      *genericoptions.MySQLOptions
	// Watcher related configurations
	WatchOptions      *watch.Options
	EnableWatcher     bool
	UserWatcherMaxWorkers int64
}

// UnionServer 定义一个联合服务器. 根据 ServerMode 决定要启动的服务器类型.
//
// 联合服务器分为以下 2 大类：
//  1. Gin 服务器：由 Gin 框架创建的标准的 REST 服务器。根据是否开启 TLS，
//     来判断启动 HTTP 或者 HTTPS；
//  2. GRPC 服务器：由 gRPC 框架创建的标准 RPC 服务器
//  3. HTTP 反向代理服务器：由 grpc-gateway 框架创建的 HTTP 反向代理服务器。
//     根据是否开启 TLS，来判断启动 HTTP 或者 HTTPS；
//
// HTTP 反向代理服务器依赖 gRPC 服务器，所以在开启 HTTP 反向代理服务器时，会先启动 gRPC 服务器.
type UnionServer struct {
	srv     server.Server
	watch   *watch.Watch
	db      *gorm.DB
	config  *Config
}

// ServerConfig 包含服务器的核心依赖和配置.
type ServerConfig struct {
	cfg       *Config
	biz       biz.IBiz
	val       *validation.Validator
}

// NewUnionServer 根据配置创建联合服务器.
func (cfg *Config) NewUnionServer() (*UnionServer, error) {

	log.Infow("Initializing federation server", "server-mode", cfg.ServerMode, "enable-memory-store", cfg.EnableMemoryStore, "enable-watcher", cfg.EnableWatcher)

	// 创建数据库连接
	db, err := cfg.NewDB()
	if err != nil {
		return nil, err
	}

	// 创建服务配置，这些配置可用来创建服务器
	srv, err := InitializeWebServer(cfg)
	if err != nil {
		return nil, err
	}

	var watchIns *watch.Watch
	if cfg.EnableWatcher {
		// 创建watcher配置
		watcherConfig, err := cfg.CreateWatcherConfig(db)
		if err != nil {
			return nil, err
		}

		// 初始化watcher
		initialize := watcher.NewInitializer(watcherConfig)
		opts := []watch.Option{
			watch.WithInitialize(initialize),
			watch.WithLogger(onex.NewLogger()),
		}

		watchIns, err = watch.NewWatch(cfg.WatchOptions, db, opts...)
		if err != nil {
			return nil, err
		}
	}

	return &UnionServer{
		srv:    srv,
		watch:  watchIns,
		db:     db,
		config: cfg,
	}, nil
}

// CreateWatcherConfig 创建watcher配置.
func (cfg *Config) CreateWatcherConfig(db *gorm.DB) (*watcher.AggregateConfig, error) {
	storeClient := store.NewStore(db)

	return &watcher.AggregateConfig{
		Store:                 storeClient,
		UserWatcherMaxWorkers: cfg.UserWatcherMaxWorkers,
	}, nil
}

// Run 运行应用.
func (s *UnionServer) Run() error {
	go s.srv.RunOrDie()

	// 启动watcher服务
	if s.watch != nil {
		log.Infow("Starting watcher service")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go s.watch.Start(ctx.Done())
	}

	// 创建一个 os.Signal 类型的 channel，用于接收系统信号
	quit := make(chan os.Signal, 1)
	// 当执行 kill 命令时（不带参数），默认会发送 syscall.SIGTERM 信号
	// 使用 kill -2 命令会发送 syscall.SIGINT 信号（例如按 CTRL+C 触发）
	// 使用 kill -9 命令会发送 syscall.SIGKILL 信号，但 SIGKILL 信号无法被捕获，因此无需监听和处理
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// 阻塞程序，等待从 quit channel 中接收到信号
	<-quit

	log.Infow("Shutting down server ...")

	// 优雅关闭服务
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 先关闭watcher服务
	if s.watch != nil {
		log.Infow("Stopping watcher service")
		s.watch.Stop()
	}

	// 再关闭依赖的服务，最后关闭被依赖的服务
	s.srv.GracefulStop(ctx)

	log.Infow("Server exited")
	return nil
}

// NewDB 创建一个 *gorm.DB 实例.
func (cfg *Config) NewDB() (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	if !cfg.EnableMemoryStore {
		log.Infow("Initializing database connection", "type", "mysql", "addr", cfg.MySQLOptions.Addr)
		db, err = cfg.MySQLOptions.NewDB()
	} else {
		log.Infow("Initializing database connection", "type", "memory", "engine", "SQLite")
		// 使用SQLite内存模式配置数据库
		// ?cache=shared 用于设置 SQLite 的缓存模式为 共享缓存模式 (shared)。
		// 默认情况下，SQLite 的每个数据库连接拥有自己的独立缓存，这种模式称为 专用缓存 (private)。
		// 使用 共享缓存模式 (shared) 后，不同连接可以共享同一个内存中的数据库和缓存。
		db, err = gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	}

	if err != nil {
		log.Errorw("Failed to create database connection", "err", err)
		return nil, err
	}

	// 自动迁移数据库表结构
	log.Infow("Auto-migrating database tables")
	if cfg.EnableMemoryStore {
		// 使用SQLite兼容的模型
		err = db.AutoMigrate(
			&model.CronJobSQLite{},
			&model.JobSQLite{},
		)
	} else {
		// 使用MySQL模型，只迁移nightwatch自己的表
		err = db.AutoMigrate(
			&model.CronJobM{},
			&model.JobM{},
			// 暂时移除PostM，避免与现有表结构冲突
			// &model.PostM{},
		)
	}
	if err != nil {
		log.Errorw("Failed to auto-migrate database tables", "err", err)
		return nil, err
	}
	log.Infow("Database tables auto-migration completed successfully")

	return db, nil
}



// ProvideDB 根据配置提供一个数据库实例。
func ProvideDB(cfg *Config) (*gorm.DB, error) {
	return cfg.NewDB()
}

func NewWebServer(serverMode string, serverConfig *ServerConfig) (server.Server, error) {
	// 根据服务模式创建对应的服务实例
	// 实际企业开发中，可以根据需要只选择一种服务器模式.
	// 这里为了方便给你展示，通过 cfg.ServerMode 同时支持了 Gin 和 GRPC 2 种服务器模式.
	// 默认为 gRPC 服务器模式.
	switch serverMode {
	case GinServerMode:
		return serverConfig.NewGinServer(), nil
	default:
		return serverConfig.NewGRPCServerOr()
	}
}
