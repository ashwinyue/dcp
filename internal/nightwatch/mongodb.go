// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package nightwatch

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"k8s.io/klog/v2"
)

// MongoManager 管理MongoDB连接和操作
type MongoManager struct {
	client   *mongo.Client
	database *mongo.Database
	config   *Config
}

// NewMongoManager 创建一个新的MongoDB管理器
func (cfg *Config) NewMongoManager() (*MongoManager, error) {
	if cfg.MongoOptions == nil {
		return nil, fmt.Errorf("MongoDB配置不能为空")
	}

	// 创建MongoDB客户端
	client, err := cfg.MongoOptions.NewClient()
	if err != nil {
		return nil, fmt.Errorf("创建MongoDB客户端失败: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("连接MongoDB失败: %w", err)
	}

	database := client.Database(cfg.MongoOptions.Database)

	klog.Infof("MongoDB连接成功, database: %s, url: %s", cfg.MongoOptions.Database, cfg.MongoOptions.URL)

	return &MongoManager{
		client:   client,
		database: database,
		config:   cfg,
	}, nil
}

// GetClient 返回MongoDB客户端
func (m *MongoManager) GetClient() *mongo.Client {
	return m.client
}

// GetDatabase 返回MongoDB数据库实例
func (m *MongoManager) GetDatabase() *mongo.Database {
	return m.database
}

// GetCollection 返回指定的集合
func (m *MongoManager) GetCollection(name string) *mongo.Collection {
	return m.database.Collection(name)
}

// Close 关闭MongoDB连接
func (m *MongoManager) Close() error {
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return m.client.Disconnect(ctx)
	}
	return nil
}

// CreateIndexes 创建必要的索引
func (m *MongoManager) CreateIndexes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 为documents集合创建索引
	documentsCollection := m.GetCollection("documents")

	// 创建ID索引
	_, err := documentsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    map[string]int{"document_id": 1},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("创建documents ID索引失败: %w", err)
	}

	// 创建时间索引
	_, err = documentsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: map[string]int{"created_at": -1},
	})
	if err != nil {
		return fmt.Errorf("创建documents时间索引失败: %w", err)
	}

	// 创建类型索引
	_, err = documentsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: map[string]int{"type": 1},
	})
	if err != nil {
		return fmt.Errorf("创建documents类型索引失败: %w", err)
	}

	klog.Info("MongoDB索引创建成功")
	return nil
}
