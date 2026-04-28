# tcp-forward

[![GitHub license](https://img.shields.io/github/license/Coconut-Fish/tcp-forword?color=yellow)](https://github.com/Coconut-Fish/tcp-forword/blob/main/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/Coconut-Fish/tcp-forword?style=social)](https://github.com/Coconut-Fish/tcp-forword/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/Coconut-Fish/tcp-forword?style=social)](https://github.com/Coconut-Fish/tcp-forword/network/members)
[![Gitee stars](https://gitee.com/Coconut-Fish/tcp-forword/badge/star.svg?theme=gvp)](https://gitee.com/Coconut-Fish/tcp-forword/stargazers)
[![Gitee forks](https://gitee.com/Coconut-Fish/tcp-forword/badge/fork.svg?theme=gvp)](https://gitee.com/Coconut-Fish/tcp-forword/members)

`tcp-forward` 是一个使用 Rust 编写的 TCP 转发工具，面向“多个下游客户端连接到同一个上游服务”的场景。它支持配置文件与命令行参数、上游断线自动重连，以及将上游返回的数据广播给所有已连接客户端。

## 功能概览

- 使用 TCP 监听本地地址，接收多个客户端连接
- 将客户端发送的数据转发到单个上游 TCP 服务
- 将上游返回的数据广播给所有当前在线客户端
- 上游断开后自动按退避策略重连
- 支持 `TOML` 配置文件
- 支持命令行参数覆盖配置文件内容

## 使用方法

### 1. 直接运行

在仓库根目录执行：

```powershell
cargo run
```

默认情况下：

- 监听地址：`0.0.0.0:7000`
- 上游地址：`127.0.0.1:9000`

### 2. 使用配置文件运行

项目根目录提供了示例配置文件 `config.example.toml`。可以这样启动：

```powershell
cargo run -- --config .\config.example.toml
```

### 3. 使用命令行参数覆盖配置

例如：

```powershell
cargo run -- --config .\config.example.toml --listen 0.0.0.0:7001 --upstream 127.0.0.1:9100 --log-level debug
```

如果同时提供配置文件和命令行参数，则命令行参数优先。

## 配置说明

支持的主要配置项如下：

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `listen` | 本地监听地址 | `0.0.0.0:7000` |
| `upstream` | 上游 TCP 服务地址 | `127.0.0.1:9000` |
| `reconnect_initial_ms` | 首次重连等待时间，单位毫秒 | `500` |
| `reconnect_max_ms` | 最大重连等待时间，单位毫秒 | `10000` |
| `connect_timeout_ms` | 连接上游超时时间，单位毫秒 | `3000` |
| `queue_capacity` | 上游写入队列容量 | `256` |
| `log_level` | 日志级别，可选 `trace`、`debug`、`info`、`warn`、`error` | `info` |

示例配置：

```toml
listen = "0.0.0.0:7001"
upstream = "127.0.0.1:9001"
reconnect_initial_ms = 500
reconnect_max_ms = 10000
connect_timeout_ms = 3000
queue_capacity = 256
log_level = "info"
```

## 构建方式

### 开发构建

```powershell
cargo build
```

### 发布构建

```powershell
cargo build --release
```

生成的可执行文件位于：

- 开发版：`target\debug\`
- 发布版：`target\release\`

## 使用的语言与技术

本项目主要使用以下语言与技术栈：

- 语言：Rust
- 异步运行时：Tokio
- 命令行解析：Clap
- 配置解析：Serde + TOML
- 日志：Tracing + tracing-subscriber
- 错误处理：Anyhow

如果你希望了解代码入口，可以从以下文件开始：

- `src/main.rs`：程序入口与日志初始化
- `src/config.rs`：配置加载、参数解析与校验
- `src/proxy.rs`：客户端连接管理与转发逻辑
- `src/upstream.rs`：上游连接管理、断线重连与广播

## 开发说明

### 环境要求

- 安装稳定版 Rust
- 建议安装 Cargo 配套工具链

查看版本：

```powershell
rustc --version
cargo --version
```

### 常用开发命令

```powershell
cargo build
cargo test
cargo fmt -- --check
```

如果需要自动格式化代码：

```powershell
cargo fmt
```

### 测试

项目当前包含集成测试，覆盖了以下行为：

- 多客户端数据汇聚到同一个上游连接
- 上游数据广播给多个客户端
- 上游断线后自动重连
- 单个客户端断开不会影响其他客户端

运行测试：

```powershell
cargo test
```

## 如何贡献

欢迎提交 Issue 或 Pull Request。

建议的贡献流程如下：

1. Fork 仓库并创建功能分支
2. 完成代码修改，并尽量补充对应测试
3. 运行格式检查和测试
4. 提交 Pull Request，说明修改内容、原因和验证方式

提交前建议至少执行：

```powershell
cargo fmt -- --check
cargo test
```

## 开发建议

- 保持 `main.rs` 尽量精简
- 新功能优先拆分到 `src/` 下独立模块
- 单元测试尽量靠近被测试代码
- 端到端行为测试放在 `tests/` 目录下

## 许可证

本项目使用仓库中的 `LICENSE` 文件作为许可证说明。
