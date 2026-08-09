#include "main_reactor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void main_accept_cb(Event *ev) {
    MainReactor *main_r = (MainReactor *)ev->sub;

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int conn_fd = accept(ev->fd, (struct sockaddr *)&client_addr, &client_len);
    if (conn_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("accept error");
        }
        return;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    printf("[+] [Main-Reactor] 接收到 HTTP 客户端连接: IP=%s, Port=%d, conn_fd=%d\n",
           client_ip, ntohs(client_addr.sin_port), conn_fd);

    // Round-Robin 选择 Sub-Reactor
    int target_index = main_r->next_sub_index;
    SubReactor *target_sub = &main_r->sub_reactors[target_index];
    main_r->next_sub_index = (main_r->next_sub_index + 1) % SUB_REACTOR_NUM;

    printf("[->] [Main-Reactor] 将 HTTP conn_fd=%d 分发给 Sub-Reactor %d\n", conn_fd, target_sub->id);
    sub_reactor_dispatch_conn(target_sub, conn_fd);
}

int main_reactor_init(MainReactor *main_r, int port) {
    memset(main_r, 0, sizeof(MainReactor));
    main_r->next_sub_index = 0;

    // 1. 创建共享 Worker 线程池
    main_r->worker_pool = threadpool_create(WORKER_THREAD_NUM);
    if (!main_r->worker_pool) {
        fprintf(stderr, "Failed to create ThreadPool for MainReactor\n");
        return -1;
    }

    // 2. 初始化并启动所有 Sub-Reactors (子线程群)
    for (int i = 0; i < SUB_REACTOR_NUM; i++) {
        if (sub_reactor_init(&main_r->sub_reactors[i], i, main_r->worker_pool) < 0) {
            fprintf(stderr, "Failed to init SubReactor %d\n", i);
            return -1;
        }
        sub_reactor_start(&main_r->sub_reactors[i]);
    }

    // 3. 创建 Socket 并监听
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
    main_r->listen_fd = listen_fd;

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 failed for MainReactor");
        close(listen_fd);
        return -1;
    }
    main_r->epoll_fd = epoll_fd;

    Event *listen_ev = (Event *)malloc(sizeof(Event));
    memset(listen_ev, 0, sizeof(Event));
    listen_ev->fd = listen_fd;
    listen_ev->epoll_fd = epoll_fd;
    listen_ev->handler = main_accept_cb;
    listen_ev->sub = (SubReactor *)main_r;

    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = EPOLLIN;
    ep_ev.data.ptr = listen_ev;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ep_ev) < 0) {
        perror("epoll_ctl listen_fd ADD failed for MainReactor");
        close(listen_fd);
        close(epoll_fd);
        free(listen_ev);
        return -1;
    }

    printf("[阶段六] Main-Sub Multi-Reactor + ThreadPool + HTTP 服务器启动！\n");
    printf("        监听端口: %d | Sub-Reactors (I/O 线程): %d | Worker ThreadPool (解析/业务线程): %d\n",
           port, SUB_REACTOR_NUM, WORKER_THREAD_NUM);
    printf("        测试链接: http://127.0.0.1:%d/ 或 http://127.0.0.1:%d/api/hello\n", port, port);
    return 0;
}

void main_reactor_run(MainReactor *main_r) {
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int nfds = epoll_wait(main_r->epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("main_reactor epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            Event *ev = (Event *)events[i].data.ptr;
            if (ev && ev->handler) {
                ev->handler(ev);
            }
        }
    }
}

void main_reactor_destroy(MainReactor *main_r) {
    if (main_r->listen_fd > 0) close(main_r->listen_fd);
    if (main_r->epoll_fd > 0) close(main_r->epoll_fd);
    for (int i = 0; i < SUB_REACTOR_NUM; i++) {
        sub_reactor_destroy(&main_r->sub_reactors[i]);
    }
    if (main_r->worker_pool) {
        threadpool_destroy(main_r->worker_pool);
    }
}
