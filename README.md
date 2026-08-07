# Epoll Reactor 渐进式演进项目 (Epoll Reactor Evolution)

本仓库展示了从 Linux 原生面向过程的 `epoll` 服务端，逐步重变演进到现代高并发 **Main-Sub Multi-Reactor + ThreadPool** 终极架构的全过程实现。每个阶段均为独立的 CMake C 语言工程。

---

## 🚀 5大演进阶段目录

| 阶段目录 | 架构名称 | 核心特征与原理 |
| :--- | :--- | :--- |
| **`stage1_epoll_base`** | **过程导向 epoll** | 单线程、原生 `epoll_create1`/`epoll_ctl`/`epoll_wait` 巨型 `while(1)` 循环、非阻塞 Echo |
| **`stage2_single_reactor`** | **Single-Threaded Reactor** | 面向对象/事件驱动，使用 `struct Event` 绑定 Context，`epoll_data.ptr` 穿透传递指针，实现 `accept_cb`/`read_cb`/`write_cb` 状态机切换 |
| **`stage3_reactor_threadpool`** | **Reactor + 线程池** | 职责分离：主线程处理 Network I/O，Worker 线程池处理计算/业务。使用 `EPOLLONESHOT` 防范多线程竞态条件 |
| **`stage4_main_sub_reactor`** | **Main-Sub Multi-Reactor** | One Loop Per Thread：Main-Reactor 只负责 `accept`，Round-Robin 分发给 Sub-Reactors，使用 `eventfd` 跨线程唤醒 |
| **`stage5_ultimate_reactor_threadpool`** | **Main-Sub Multi-Reactor + ThreadPool (终极形态)** | 三层解耦：Main-Reactor (连接监听) + Sub-Reactors (多 I/O 线程) + Worker ThreadPool (业务计算线程池) |

---

## 🛠️ 编译与运行方式

项目依赖 GCC / Clang 与 CMake (>= 3.10)。

```bash
# 以阶段五 (终极形态) 为例：
cd stage5_ultimate_reactor_threadpool

# 使用 CMake 编译
cmake -B build
cmake --build build

# 运行服务器 (默认监听 8080 端口)
./build/stage5_ultimate_reactor_threadpool
```

使用 `nc` (netcat) 进行测试：
```bash
nc 127.0.0.1 8080
```
