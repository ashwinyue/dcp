// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

// nolint: err113
package options

import (
	"errors"
	"fmt"

	genericoptions "github.com/onexstack/onexstack/pkg/options"
	stringsutil "github.com/onexstack/onexstack/pkg/util/strings"
	"github.com/onexstack/onexstack/pkg/watch"
	"github.com/spf13/pflag"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/ashwinyue/dcp/internal/nightwatch"
)

// 定义支持的服务器模式集合.
var availableServerModes = sets.New(
	nightwatch.GinServerMode,
	nightwatch.GRPCServerMode,
	nightwatch.GRPCGatewayServerMode,
)

// ServerOptions 包含服务器配置选项.
type ServerOptions struct {
	// ServerMode 定义服务器模式：gRPC、Gin HTTP、HTTP Reverse Proxy.
	ServerMode string `json:"server-mode" mapstructure:"server-mode"`
	// EnableMemoryStore 指示是否启用内存数据库（用于测试或开发环境）.
	EnableMemoryStore bool `json:"enable-memory-store" mapstructure:"enable-memory-store"`
	// EnableWatcher 指示是否启用watcher服务.
	EnableWatcher bool `json:"enable-watcher" mapstructure:"enable-watcher"`
	// UserWatcherMaxWorkers 定义用户watcher的最大工作线程数.
	UserWatcherMaxWorkers int64 `json:"user-watcher-max-workers" mapstructure:"user-watcher-max-workers"`
	// TLSOptions 包含 TLS 配置选项.
	TLSOptions *genericoptions.TLSOptions `json:"tls" mapstructure:"tls"`
	// HTTPOptions 包含 HTTP 配置选项.
	HTTPOptions *genericoptions.HTTPOptions `json:"http" mapstructure:"http"`
	// GRPCOptions 包含 gRPC 配置选项.
	GRPCOptions *genericoptions.GRPCOptions `json:"grpc" mapstructure:"grpc"`
	// MySQLOptions 包含 MySQL 配置选项.
	MySQLOptions *genericoptions.MySQLOptions `json:"mysql" mapstructure:"mysql"`
	// MongoOptions 包含 MongoDB 配置选项.
	MongoOptions *genericoptions.MongoOptions `json:"mongo" mapstructure:"mongo"`
	// WatchOptions 包含 Watch 配置选项.
	WatchOptions *watch.Options `json:"watch" mapstructure:"watch"`
}

// NewServerOptions 创建带有默认值的 ServerOptions 实例.
func NewServerOptions() *ServerOptions {
	opts := &ServerOptions{
		ServerMode:            nightwatch.GRPCGatewayServerMode,
		EnableMemoryStore:     true,
		EnableWatcher:         true,
		UserWatcherMaxWorkers: 10,
		TLSOptions:            genericoptions.NewTLSOptions(),
		HTTPOptions:           genericoptions.NewHTTPOptions(),
		GRPCOptions:           genericoptions.NewGRPCOptions(),
		MySQLOptions:          genericoptions.NewMySQLOptions(),
		MongoOptions:          genericoptions.NewMongoOptions(),
		WatchOptions:          watch.NewOptions(),
	}
	opts.HTTPOptions.Addr = ":5555"
	opts.GRPCOptions.Addr = ":6666"
	return opts
}

// AddFlags 将 ServerOptions 的选项绑定到命令行标志.
// 通过使用 pflag 包，可以实现从命令行中解析这些选项的功能.
func (o *ServerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.ServerMode, "server-mode", o.ServerMode, fmt.Sprintf("Server mode, available options: %v", availableServerModes.UnsortedList()))
	fs.BoolVar(&o.EnableMemoryStore, "enable-memory-store", o.EnableMemoryStore, "Enable in-memory database (useful for testing or development).")
	fs.BoolVar(&o.EnableWatcher, "enable-watcher", o.EnableWatcher, "Enable watcher service for job monitoring and processing.")
	fs.Int64Var(&o.UserWatcherMaxWorkers, "user-watcher-max-workers", o.UserWatcherMaxWorkers, "Maximum number of workers for user watcher.")

	// 添加子选项的命令行标志
	o.TLSOptions.AddFlags(fs)
	o.HTTPOptions.AddFlags(fs)
	o.GRPCOptions.AddFlags(fs)
	o.MySQLOptions.AddFlags(fs)
	o.MongoOptions.AddFlags(fs)
	o.WatchOptions.AddFlags(fs)
}

// Validate 校验 ServerOptions 中的选项是否合法.
func (o *ServerOptions) Validate() error {
	errs := []error{}

	// 校验 ServerMode 是否有效
	if !availableServerModes.Has(o.ServerMode) {
		errs = append(errs, fmt.Errorf("invalid server mode: must be one of %v", availableServerModes.UnsortedList()))
	}

	// 校验 watcher 相关配置
	if o.EnableWatcher {
		if o.UserWatcherMaxWorkers <= 0 {
			errs = append(errs, errors.New("user-watcher-max-workers must be greater than 0 when watcher is enabled"))
		}
	}

	// 校验子选项
	errs = append(errs, o.TLSOptions.Validate()...)
	errs = append(errs, o.HTTPOptions.Validate()...)
	errs = append(errs, o.MySQLOptions.Validate()...)
	errs = append(errs, o.MongoOptions.Validate()...)
	if o.WatchOptions != nil {
		errs = append(errs, o.WatchOptions.Validate()...)
	}

	// 如果是 gRPC 或 gRPC-Gateway 模式，校验 gRPC 配置
	if stringsutil.StringIn(o.ServerMode, []string{nightwatch.GRPCServerMode, nightwatch.GRPCGatewayServerMode}) {
		errs = append(errs, o.GRPCOptions.Validate()...)
	}

	// 合并所有错误并返回
	return utilerrors.NewAggregate(errs)
}

// Config 基于 ServerOptions 构建 nightwatch.Config.
func (o *ServerOptions) Config() (*nightwatch.Config, error) {
	return &nightwatch.Config{
		ServerMode:            o.ServerMode,
		EnableMemoryStore:     o.EnableMemoryStore,
		EnableWatcher:         o.EnableWatcher,
		UserWatcherMaxWorkers: o.UserWatcherMaxWorkers,
		TLSOptions:            o.TLSOptions,
		HTTPOptions:           o.HTTPOptions,
		GRPCOptions:           o.GRPCOptions,
		MySQLOptions:          o.MySQLOptions,
		MongoOptions:          o.MongoOptions,
		WatchOptions:          o.WatchOptions,
	}, nil
}
