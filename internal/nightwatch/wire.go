// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

//go:build wireinject
// +build wireinject

package nightwatch

import (
	"github.com/google/wire"

	"github.com/ashwinyue/dcp/internal/nightwatch/biz"
	"github.com/ashwinyue/dcp/internal/nightwatch/cache"
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/validation"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/syncer"
	"github.com/ashwinyue/dcp/internal/pkg/log"
	"github.com/ashwinyue/dcp/internal/pkg/server"
)

func InitializeWebServer(*Config) (server.Server, error) {
	wire.Build(
		wire.NewSet(NewWebServer, wire.FieldsOf(new(*Config), "ServerMode")),
		wire.Struct(new(ServerConfig), "*"), // * 表示注入全部字段
		wire.NewSet(store.ProviderSet, biz.ProviderSet, cache.ProviderSet, syncer.ProviderSet),
		ProvideDB, // 提供数据库实例
		ProvideCacheOptions, // 提供缓存配置
		ProvideLogger, // 提供日志实例
		validation.ProviderSet,
	)
	return nil, nil
}

// ProvideCacheOptions 提供缓存配置给 Wire
func ProvideCacheOptions(cfg *Config) *cache.CacheOptions {
	if cfg.RedisOptions == nil {
		// 如果没有配置Redis，返回默认配置
		return &cache.CacheOptions{
			Addr:   "localhost:6379",
			DB:     0,
			Prefix: "nightwatch:",
		}
	}
	return &cache.CacheOptions{
		Addr:     cfg.RedisOptions.Addr,
		Password: cfg.RedisOptions.Password,
		DB:       cfg.RedisOptions.Database,
		Prefix:   "nightwatch:",
	}
}

// ProvideLogger 提供日志实例给 Wire
func ProvideLogger() log.Logger {
	return log.New(nil)
}
