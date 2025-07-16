// Copyright 2024 孔令飞 <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file. The original repo for
// this file is https://github.com/onexstack/miniblog. The professional
// version of this repository is https://github.com/onexstack/onex.

package nightwatch

import (
	"context"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/onexstack/onexstack/pkg/core"

	handler "github.com/ashwinyue/dcp/internal/nightwatch/handler/http"
	"github.com/ashwinyue/dcp/internal/pkg/errno"
	"github.com/ashwinyue/dcp/internal/pkg/server"
)

// ginServer 定义一个使用 Gin 框架开发的 HTTP 服务器.
type ginServer struct {
	srv server.Server
}

// 确保 *ginServer 实现了 server.Server 接口.
var _ server.Server = (*ginServer)(nil)

// NewGinServer 初始化一个新的 Gin 服务器实例.
func (c *ServerConfig) NewGinServer() server.Server {
	// 创建 Gin 引擎
	engine := gin.New()

	// 注册全局中间件，用于恢复 panic、设置 HTTP 头、添加请求 ID 等
	engine.Use(gin.Recovery())

	// 注册 REST API 路由
	c.InstallRESTAPI(engine)

	httpsrv := server.NewHTTPServer(c.cfg.HTTPOptions, c.cfg.TLSOptions, engine)

	return &ginServer{srv: httpsrv}
}

// InstallRESTAPI 注册 API 路由。路由的路径和 HTTP 方法，严格遵循 REST 规范.
func (c *ServerConfig) InstallRESTAPI(engine *gin.Engine) {
	// 注册业务无关的 API 接口
	InstallGenericAPI(engine)

	// 创建核心业务处理器
	handler := handler.NewHandler(c.biz, c.val)

	// 注册健康检查接口
	engine.GET("/healthz", handler.Healthz)

	// 注册 v1 版本 API 路由分组
	v1 := engine.Group("/v1")
	{
		// 博客相关路由
		postv1 := v1.Group("/posts")
		{
			postv1.POST("", handler.CreatePost)       // 创建博客
			postv1.PUT(":postID", handler.UpdatePost) // 更新博客
			postv1.DELETE("", handler.DeletePost)     // 删除博客
			postv1.GET(":postID", handler.GetPost)    // 查询博客详情
			postv1.GET("", handler.ListPost)          // 查询博客列表
		}

		// CronJob相关路由
		cronjobv1 := v1.Group("/cronjobs")
		{
			cronjobv1.POST("", handler.CreateCronJob)          // 创建CronJob
			cronjobv1.PUT(":cronjobID", handler.UpdateCronJob) // 更新CronJob
			cronjobv1.DELETE("", handler.DeleteCronJob)        // 删除CronJob
			cronjobv1.GET(":cronjobID", handler.GetCronJob)    // 查询CronJob详情
			cronjobv1.GET("", handler.ListCronJob)             // 查询CronJob列表
		}

		// Job相关路由
		jobv1 := v1.Group("/jobs")
		{
			jobv1.POST("", handler.CreateJob)      // 创建Job
			jobv1.PUT(":jobID", handler.UpdateJob) // 更新Job
			jobv1.DELETE("", handler.DeleteJob)    // 删除Job
			jobv1.GET(":jobID", handler.GetJob)    // 查询Job详情
			jobv1.GET("", handler.ListJob)         // 查询Job列表
		}

		// Document相关路由 (MongoDB)
		documentv1 := v1.Group("/documents")
		{
			documentv1.POST("", handler.CreateDocument)                              // 创建文档
			documentv1.PUT(":id", handler.UpdateDocument)                            // 更新文档
			documentv1.DELETE(":id", handler.DeleteDocument)                         // 删除文档
			documentv1.GET(":id", handler.GetDocument)                               // 查询文档详情
			documentv1.GET("/by-object-id/:objectId", handler.GetDocumentByObjectID) // 根据ObjectID查询文档
			documentv1.GET("", handler.ListDocuments)                                // 查询文档列表
		}
	}
}

// InstallGenericAPI 注册业务无关的路由，例如 pprof、404 处理等.
func InstallGenericAPI(engine *gin.Engine) {
	// 注册 pprof 路由
	pprof.Register(engine)

	// 注册 404 路由处理
	engine.NoRoute(func(c *gin.Context) {
		core.WriteResponse(c, errno.ErrPageNotFound, nil)
	})
}

// RunOrDie 启动 Gin 服务器，出错则程序崩溃退出.
func (s *ginServer) RunOrDie() {
	s.srv.RunOrDie()
}

// GracefulStop 优雅停止服务器.
func (s *ginServer) GracefulStop(ctx context.Context) {
	s.srv.GracefulStop(ctx)
}
