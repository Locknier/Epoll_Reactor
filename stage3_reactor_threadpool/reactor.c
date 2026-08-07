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

Event* event_create(int fd, int epoll_fd, ThreadPool *pool, event_handler_t handler) {
    Event *ev = (Event *)malloc(sizeof(Event));
    if (!ev) {
        perror("malloc Event error");
        return NULL;
    }
    memset(ev, 0, sizeof(Event));
    ev->fd = fd;
    ev->epoll_fd = epoll_fd;
    ev->pool = pool;
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
    ep_ev.data.ptr = ev;
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
    ep_ev.data.ptr = ev;
    ev->events = events;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev) < 0) {
        perror("epoll_ctl MOD error");
        return -1;
    }
    return 0;
}

int reactor_del_event(Event *ev) {
    if (!ev) return 0;

    printf("[-] 执行清理三部曲: epoll_ctl(DEL) -> close(fd=%d) -> free(Event)\n", ev->fd);
    epoll_ctl(ev->epoll_fd, EPOLL_CTL_DEL, ev->fd, NULL);
    close(ev->fd);
    event_free(ev);
    return 0;
}

int reactor_init(Reactor *reactor, int port, ThreadPool *pool) {
    memset(reactor, 0, sizeof(Reactor));
    reactor->pool = pool;

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

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 error");
        close(listen_fd);
        return -1;
    }
    reactor->epoll_fd = epoll_fd;

    // listen_fd 不需要 EPOLLONESHOT，常规 EPOLLIN 即可
    Event *listen_ev = event_create(listen_fd, epoll_fd, pool, accept_cb);
    reactor_add_event(epoll_fd, EPOLLIN, listen_ev);

    printf("[阶段三] Reactor + 线程池 启动，监听端口: %d\n", port);
    return 0;
}

void reactor_run(Reactor *reactor) {
    while (1) {
        int nfds = epoll_wait(reactor->epoll_fd, reactor->events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
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

// ================= Handlers & Business Logic =================

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

    Event *client_ev = event_create(client_fd, ev->epoll_fd, ev->pool, read_cb);
    if (!client_ev) {
        close(client_fd);
        return;
    }

    // 核心防范：注册时加上 EPOLLONESHOT 标志，确保同一时间只有一个线程处理该 fd
    reactor_add_event(ev->epoll_fd, EPOLLIN | EPOLLET | EPOLLONESHOT, client_ev);
}

// 主线程 read_cb：仅负责快速将数据从内核 Socket 读到 Buffer，然后把任务甩给线程池
void read_cb(Event *ev) {
    while (1) {
        ssize_t bytes = read(ev->fd, ev->buffer + ev->length, sizeof(ev->buffer) - ev->length - 1);
        if (bytes > 0) {
            ev->length += bytes;
            ev->buffer[ev->length] = '\0';
        } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            if (bytes == 0) {
                printf("[-] [read_cb] 客户端断开 client_fd=%d\n", ev->fd);
            } else {
                perror("[read_cb] read error");
            }
            reactor_del_event(ev);
            return;
        }
    }

    if (ev->length > 0) {
        printf("[<-] [主线程 read_cb] I/O 读取完成 (%d 字节)，即将打包 Task 投递给 ThreadPool\n", ev->length);

        // 打包任务，投递给 Worker ThreadPool
        if (threadpool_add_task(ev->pool, worker_business_task, ev) < 0) {
            fprintf(stderr, "threadpool_add_task failed for fd=%d\n", ev->fd);
            reactor_del_event(ev);
        }
        // read_cb 立刻结束！主线程解锁回去继续执行 epoll_wait
    } else {
        // 如果触发了 EPOLLIN 但没读到有效数据且未出错，需重新使能 EPOLLONESHOT 重新监听
        reactor_mod_event(ev->epoll_fd, EPOLLIN | EPOLLET | EPOLLONESHOT, ev);
    }
}

// Worker 线程执行的具体耗时业务逻辑
void worker_business_task(void *arg) {
    Event *ev = (Event *)arg;
    pthread_t tid = pthread_self();

    printf("[*] [Worker 线程 %lu] 开始处理 client_fd=%d 的耗时业务逻辑...\n", (unsigned long)tid, ev->fd);

    // 模拟耗时业务计算 (比如 20ms 的 Hash 计算或 DB 查询)
    usleep(20000);

    printf("[*] [Worker 线程 %lu] 耗时业务计算完成，切换为 write_cb 准备回写\n", (unsigned long)tid);

    // 业务处理完成，切换为 write_cb
    ev->handler = write_cb;

    // 重新通过 epoll_ctl(MOD) 重新使能 EPOLLOUT (可写) 并保留 EPOLLONESHOT
    reactor_mod_event(ev->epoll_fd, EPOLLOUT | EPOLLET | EPOLLONESHOT, ev);
}

// write_cb：负责将结果写回客户端
void write_cb(Event *ev) {
    ssize_t bytes = write(ev->fd, ev->buffer, ev->length);
    if (bytes > 0) {
        printf("[->] [write_cb] Echo 成功回传 %ld 字节给 client_fd=%d\n", bytes, ev->fd);

        // 写完后切回 read_cb
        ev->length = 0;
        ev->handler = read_cb;

        // 重新 MOD 重新开启 EPOLLIN 监听 (重新开启 EPOLLONESHOT)
        reactor_mod_event(ev->epoll_fd, EPOLLIN | EPOLLET | EPOLLONESHOT, ev);
    } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // 缓冲区满，重置 EPOLLOUT | EPOLLONESHOT
        reactor_mod_event(ev->epoll_fd, EPOLLOUT | EPOLLET | EPOLLONESHOT, ev);
    } else {
        perror("[write_cb] write error");
        reactor_del_event(ev);
    }
}
