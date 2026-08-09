# Epoll Reactor 渐进式演进项目 (Epoll Reactor Evolution)

本仓库完整展示了从 Linux 原生面向过程的 `epoll` 服务端，一步步演进到现代高并发 **Main-Sub Multi-Reactor + ThreadPool + HTTP 解析器** 的全过程实现。每个阶段均为独立的 CMake C 语言工程。

---

## 🚀 6 大演进阶段目录

| 阶段目录 | 架构名称 | 核心特征与原理 |
| :--- | :--- | :--- |
| **`stage1_epoll_base`** | **过程导向 epoll** | 单线程、原生 `epoll_create1`/`epoll_ctl`/`epoll_wait` 巨型 `while(1)` 循环、非阻塞 Echo |
| **`stage2_single_reactor`** | **Single-Threaded Reactor** | 面向对象/事件驱动，使用 `struct Event` 绑定 Context，`epoll_data.ptr` 穿透传递指针，实现 `accept_cb`/`read_cb`/`write_cb` 状态机切换 |
| **`stage3_reactor_threadpool`** | **Reactor + 线程池** | 职责分离：主线程处理 Network I/O，Worker 线程池处理计算/业务。使用 `EPOLLONESHOT` 防范多线程竞态条件 |
| **`stage4_main_sub_reactor`** | **Main-Sub Multi-Reactor** | One Loop Per Thread：Main-Reactor 只负责 `accept`，Round-Robin 分发给 Sub-Reactors，使用 `eventfd` 跨线程唤醒 |
| **`stage5_ultimate_reactor_threadpool`** | **Main-Sub Multi-Reactor + ThreadPool (终极形态)** | 三层解耦：Main-Reactor (连接监听) + Sub-Reactors (多 I/O 线程) + Worker ThreadPool (业务计算线程池) |
| **`stage6_http_reactor`** | **Main-Sub Multi-Reactor + ThreadPool + HTTP Server** | **Web 服务形态**：集成 HTTP 零拷贝解析器与路由响应分发（支持 HTML 页面、JSON API、404 处理等） |

---

## 🛠️ 编译与运行方式

项目依赖 GCC / Clang 与 CMake (>= 3.10)。

### 运行阶段六（HTTP 服务器形态）：

```bash
cd stage6_http_reactor

# 使用 CMake 编译
cmake -B build
cmake --build build

# 运行 HTTP 服务器 (默认监听 8080 端口)
./build/stage6_http_reactor
```

### 访问与测试方式：

1. **浏览器访问**：打开 `http://127.0.0.1:8080/` 即可看到 HTML 页面。
2. **JSON API 测试**：访问 `http://127.0.0.1:8080/api/hello` 或 `http://127.0.0.1:8080/json`。
3. **命令行 curl 测试**：
   ```bash
   curl -i http://127.0.0.1:8080/
   curl -i http://127.0.0.1:8080/api/hello
   curl -i http://127.0.0.1:8080/ping
   ```

---

## 🏛️ 阶段六 HTTP 架构分层设计

```text
                            +-----------------------+
                            | Main-Reactor (主线程)  |  <--- listen & accept HTTP 连接
                            +-----------+-----------+
                                        | (Round-Robin + eventfd 唤醒)
                        +---------------+---------------+
                        |                               |
             +----------v----------+         +----------v----------+
             | Sub-Reactor 0 (线程) |         | Sub-Reactor 1 (线程) |  <--- 专门做 HTTP Socket I/O (Read/Write)
             +----------+----------+         +----------+----------+
                        |                               |
                        +---------------+---------------+
                                        | (读完 HTTP 报文后投递 Task)
                            +-----------v-----------+
                            | Worker ThreadPool     |  <--- 专门做 HTTP 解析/路由/构建 Response
                            +-----------------------+
```
