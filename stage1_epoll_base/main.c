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
#include <sys/epoll.h>

#define PORT 8080
#define MAX_EVENTS 1024
#define BUFFER_SIZE 1024

// 设置 fd 为非阻塞模式
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL O_NONBLOCK");
        return -1;
    }
    return 0;
}

int main() {
    // 1. 允许 Port 快速重用 (防止 Address already in use)
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket error");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    // 2. TCP Socket API 三部曲: socket() -> bind() -> listen()
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(PORT);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind error");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen error");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    set_nonblocking(listen_fd);
    printf("[阶段一] Epoll 基础服务器启动，监听端口: %d\n", PORT);

    // 3. 引入 epoll 机制；申请一个 epoll 实例 (epoll_create1(0))
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 error");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    // 向 epoll 树注册 listen_fd 的 EPOLLIN 事件
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("epoll_ctl listen_fd ADD");
        close(listen_fd);
        close(epoll_fd);
        exit(EXIT_FAILURE);
    }

    // 4. 设计主循环 (Event loop) 与事件分发逻辑
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            // 如果被信号中断（如 Ctrl+C），继续循环而非直接报错退出
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int current_fd = events[i].data.fd;

            // 分支 A：current_fd == listen_fd，代表有新连接
            if (current_fd == listen_fd) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
                if (conn_fd < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("accept error");
                    }
                    continue;
                }

                char client_ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
                printf("[+] 新客户端连接成功: IP=%s, Port=%d, conn_fd=%d\n",
                       client_ip, ntohs(client_addr.sin_port), conn_fd);

                // 设置非阻塞并注册到 epoll 树
                set_nonblocking(conn_fd);
                struct epoll_event conn_ev;
                conn_ev.events = EPOLLIN;
                conn_ev.data.fd = conn_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &conn_ev) < 0) {
                    perror("epoll_ctl conn_fd ADD");
                    close(conn_fd);
                }
            } 
            // 分支 B：current_fd != listen_fd，代表旧连接传送了资料
            else {
                char buffer[BUFFER_SIZE];
                ssize_t bytes_read = read(current_fd, buffer, sizeof(buffer) - 1);

                if (bytes_read > 0) {
                    buffer[bytes_read] = '\0';
                    printf("[<-] 从 conn_fd=%d 收到 %ld 字节: %s", current_fd, bytes_read, buffer);
                    
                    // Echo 回传给客户端
                    ssize_t bytes_written = write(current_fd, buffer, bytes_read);
                    if (bytes_written > 0) {
                        printf("[->] Echo 成功回传 %ld 字节到 conn_fd=%d\n", bytes_written, current_fd);
                    }
                } 
                // 边界条件与断线处理：bytes_read == 0 代表客户端断开连接，< 0 且非 EAGAIN 代表读取错误
                else if (bytes_read == 0 || (bytes_read < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                    if (bytes_read == 0) {
                        printf("[-] 客户端主动断开连接 conn_fd=%d\n", current_fd);
                    } else {
                        perror("read error");
                    }
                    // 手动把该 FD 从 epoll 注册表中删除并 close 释放资源
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, NULL);
                    close(current_fd);
                }
            }
        }
    }

    close(listen_fd);
    close(epoll_fd);
    return 0;
}
