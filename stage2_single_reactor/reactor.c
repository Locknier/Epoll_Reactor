#include "reactor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

Event* event_create(int fd, int epoll_fd, event_handler_t handler) {
    Event *ev = (Event *)malloc(sizeof(Event));
    if (!ev) {
        perror("malloc Event error");
        return NULL;
    }
    memset(ev, 0, sizeof(Event));
    ev->fd = fd;
    ev->epoll_fd = epoll_fd;
    ev->handler = handler;
    return ev;
}

void event_free(Event *ev) {
    if (ev) {
        free(ev);
    }
}

int reactor_add_event(int epoll_fd, int events, Event *ev) {
    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = events;
    ep_ev.data.ptr = ev; // 核心：使用 ptr 而不是 fd
    ev->events = events;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, ev->fd, &ep_ev) < 0) {
        perror("epoll_ctl ADD error");
        return -1;
    }
    return 0;
}

int reactor_mod_event(int epoll_fd, int events, Event *ev) {
    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = events;
    ep_ev.data.ptr = ev; // 核心：使用 ptr 传递上下文
    ev->events = events;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev) < 0) {
        perror("epoll_ctl MOD error");
        return -1;
    }
    return 0;
}

// 资源清理三部曲：从 epoll 摘除 -> 关闭 socket -> 释放 Event 结构体
int reactor_del_event(Event *ev) {
    if (!ev) return 0;

    printf("[-] 执行清理三部曲: close fd=%d & free Event\n", ev->fd);
    epoll_ctl(ev->epoll_fd, EPOLL_CTL_DEL, ev->fd, NULL);
    close(ev->fd);
    event_free(ev);
    return 0;
}

int reactor_init(Reactor *reactor, int port) {
    memset(reactor, 0, sizeof(Reactor));

    // 1. 创建 socket 并允许端口重用
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket error");
        return -1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind error");
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen error");
        close(listen_fd);
        return -1;
    }

    set_nonblocking(listen_fd);
    reactor->listen_fd = listen_fd;

    // 2. 创建 epoll 实例
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 error");
        close(listen_fd);
        return -1;
    }
    reactor->epoll_fd = epoll_fd;

    // 3. 为 listen_fd 创建 Event 对象，指定 accept_cb 回调
    Event *listen_ev = event_create(listen_fd, epoll_fd, accept_cb);
    if (!listen_ev) {
        close(listen_fd);
        close(epoll_fd);
        return -1;
    }

    reactor_add_event(epoll_fd, EPOLLIN, listen_ev);
    printf("[阶段二] Single-Threaded Reactor 启动，监听端口: %d\n", port);
    return 0;
}

// 主循环分发员 (Dispatcher)
void reactor_run(Reactor *reactor) {
    while (1) {
        int nfds = epoll_wait(reactor->epoll_fd, reactor->events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            // 核心：盲目但极速的分发 (拿指针 -> 执行 handler())
            Event *ev = (Event *)reactor->events[i].data.ptr;
            if (ev && ev->handler) {
                ev->handler(ev);
            }
        }
    }
}

void reactor_destroy(Reactor *reactor) {
    if (reactor->listen_fd > 0) close(reactor->listen_fd);
    if (reactor->epoll_fd > 0) close(reactor->epoll_fd);
}

// ================= Handlers 实现 =================

// accept_cb：专门处理 Listening Socket
void accept_cb(Event *ev) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(ev->fd, (struct sockaddr *)&client_addr, &client_len);
    if (client_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("accept error");
        }
        return;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    printf("[+] [accept_cb] 接受新连接: IP=%s, Port=%d, client_fd=%d\n",
           client_ip, ntohs(client_addr.sin_port), client_fd);

    set_nonblocking(client_fd);

    // 为 client_fd 动态 malloc 一个全新的 Event 结构体，设 handler 为 read_cb
    Event *client_ev = event_create(client_fd, ev->epoll_fd, read_cb);
    if (!client_ev) {
        close(client_fd);
        return;
    }

    // 采用 ET (边缘触发) 模式注册 EPOLLIN
    reactor_add_event(ev->epoll_fd, EPOLLIN | EPOLLET, client_ev);
}

// read_cb：专门处理资料读取
void read_cb(Event *ev) {
    // ET 模式下必须循环读取直至 EAGAIN
    while (1) {
        ssize_t bytes = read(ev->fd, ev->buffer + ev->length, sizeof(ev->buffer) - ev->length - 1);
        
        if (bytes > 0) {
            ev->length += bytes;
            ev->buffer[ev->length] = '\0';
        } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // 代表暂时没有更多数据可读，挂起等待下一次事件
            break;
        } else {
            // bytes == 0 (断线) 或 bytes < 0 (真正错误)，触发清理三部曲
            if (bytes == 0) {
                printf("[-] [read_cb] 客户端主动断开 client_fd=%d\n", ev->fd);
            } else {
                perror("[read_cb] read error");
            }
            reactor_del_event(ev);
            return;
        }
    }

    if (ev->length > 0) {
        printf("[<-] [read_cb] 读入 %d 字节数据: %s", ev->length, ev->buffer);

        // 状态机切换 (State Machine Shift):
        // 1. 将 handler 切换为 write_cb
        // 2. 将 epoll 监听事件修改为 EPOLLOUT (可写)
        ev->handler = write_cb;
        reactor_mod_event(ev->epoll_fd, EPOLLOUT | EPOLLET, ev);
    }
}

// write_cb：专门处理资料发送
void write_cb(Event *ev) {
    ssize_t bytes = write(ev->fd, ev->buffer, ev->length);
    if (bytes > 0) {
        printf("[->] [write_cb] Echo 回传 %ld 字节数据给 client_fd=%d\n", bytes, ev->fd);

        // 写完后状态机再次切回：准备接收下一次 Request
        ev->length = 0;
        ev->handler = read_cb;
        reactor_mod_event(ev->epoll_fd, EPOLLIN | EPOLLET, ev);
    } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // TCP 发送缓冲区已满，保持 EPOLLOUT 挂起
        return;
    } else {
        perror("[write_cb] write error");
        reactor_del_event(ev);
    }
}
