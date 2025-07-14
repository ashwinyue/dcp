# Handler 内嵌 apiv1.UnimplementedMiniBlogServer 说明

在 Go 语言的 gRPC 实现中，`apiv1.UnimplementedMiniBlogServer` 是一种嵌套组合（embedding）的方式，用来简化和保证服务端实现的正确性。以下是其内嵌的关键原因及具体作用。


1. `UnimplementedMiniBlogServer` 由 `gRPC` 自动生成

- 在使用 protocol buffer 定义 gRPC 服务（在 .proto 文件中定义服务）时，gRPC 生成的 Go 代码会为每个服务生成一个名为 `Unimplemented<ServiceName>` 的结构体。在这个例子中，服务是 `MiniBlogServer`，因此生成了 `UnimplementedMiniBlogServer`。

- 这个 `UnimplementedMiniBlogServer` 是一个**默认实现的空结构体**，并且会包含接口中所有服务方法的默认实现。这些默认方法会返回 **"未实现"（`Unimplemented`）错误**。

例如，假设我们服务中定义了这样一个接口：

```protobuf
service MiniBlog {
  rpc Healthz(google.protobuf.Empty) returns (HealthzResponse) {}
  rpc CreateBlog(CreateBlogRequest) returns (Blog) {}
}
```

生成的 `UnimplementedMiniBlogServer` 结构体将类似：

```go
type UnimplementedMiniBlogServer struct{}

func (UnimplementedMiniBlogServer) Healthz(ctx context.Context, req *google.protobuf.Empty) (*HealthzResponse, error) {
    return nil, status.Errorf(codes.Unimplemented, "method Healthz not implemented")
}

func (UnimplementedMiniBlogServer) CreateBlog(ctx context.Context, req *CreateBlogRequest) (*Blog, error) {
    return nil, status.Errorf(codes.Unimplemented, "method CreateBlog not implemented")
}
```

2. 为什么要内嵌 `UnimplementedMiniBlogServer`？

(1) 确保向后兼容

gRPC 中的服务定义是接口（如 `apiv1.MiniBlogServer`），当服务端开发者实现这个接口时，需要实现接口中所有方法。如果接口发生变化，例如在 proto 文件中新增了一个 RPC 方法，那么服务端的实现就需要立即新增一个方法的实现，否则无法通过编译。

通过内嵌 `UnimplementedMiniBlogServer`，可以自动为新增的方法提供默认实现（返回 "未实现" 错误）。例如：

- 原来的接口只有 `Healthz` 方法，后来增加了 `CreateBlog` 方法。
- 由于 `UnimplementedMiniBlogServer` 默认提供了所有方法的默认实现，你的服务端实现不需要立即定义新方法，代码依然可以编译并运行。

(2) 简化服务实现过程

开发者只需要实现自己需要的方法，而不必为每个方法都提供一个默认的 "未实现" 错误。例如，在 `Handler` 中：

```go
type Handler struct {
    apiv1.UnimplementedMiniBlogServer
}
```

开发者只需要实现特定的方法，如：

```go
func (h *Handler) Healthz(ctx context.Context, req *google.protobuf.Empty) (*HealthzResponse, error) {
    return &HealthzResponse{Status: "OK"}, nil
}
```

而未实现的 `CreateBlog` 方法仍然可以通过 `UnimplementedMiniBlogServer` 提供默认行为，从而避免重复编码。

(3) 提高代码可维护性

- 在大型项目中，gRPC 接口可能会频繁变更，通过使用 `UnimplementedMiniBlogServer`，开发者可以将注意力集中在需要实现的具体服务方法上，而不是所有的 RPC 方法。
- 同时，如果未实现的接口方法被调用，gRPC 会自动返回 `Unimplemented` 错误 给客户端，提示该方法尚未实现，而不会造成运行时的崩溃或异常。

3. gRPC 规范要求

gRPC 框架的注册机制中，如果服务端实现未嵌套 `UnimplementedMiniBlogServer` 或未通过完全实现 RPC 接口的方法（即实现了 `apiv1.MiniBlogServer` 接口），会导致注册时出错。例如：

```go
apiv1.RegisterMiniBlogServer(grpcServer, handlerInstance)
```

如果 `handlerInstance` 未完全实现 `MiniBlogServer`，则会报类似于以下错误：

```
cannot use handlerInstance (type *Handler) as apiv1.MiniBlogServer in argument
to apiv1.RegisterMiniBlogServer
```

通过嵌套 `UnimplementedMiniBlogServer` 的方式，可以确保类型满足接口，同时为未实现的方法提供默认行为。
