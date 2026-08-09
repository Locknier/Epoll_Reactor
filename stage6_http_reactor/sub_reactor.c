#include "sub_reactor.h"
#include "http_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static Event* event_create(int fd, int epoll_fd, SubReactor *sub, event_handler_t handler) {
    Event *ev = (Event *)malloc(sizeof(Event));
    if (!ev) return NULL;
    memset(ev, 0, sizeof(Event));
    ev->fd = fd;
    ev->epoll_fd = epoll_fd;
    ev->sub = sub;
    ev->handler = handler;
    return ev;
}

static void event_free(Event *ev) {
    if (ev) free(ev);
}

static int sub_event_del(Event *ev) {
    if (!ev) return 0;
    printf("[-] [Sub-Reactor %d] 清理 HTTP 连接: epoll_ctl(DEL) -> close client_fd=%d -> free Event\n",
           ev->sub ? ev->sub->id : -1, ev->fd);
    epoll_ctl(ev->epoll_fd, EPOLL_CTL_DEL, ev->fd, NULL);
    close(ev->fd);
    event_free(ev);
    return 0;
}

static void sub_wakeup_cb(Event *ev) {
    uint64_t val;
    ssize_t s = read(ev->fd, &val, sizeof(val));
    (void)s;

    SubReactor *sub = ev->sub;
    printf("[*] [Sub-Reactor %d] 被 eventfd 唤醒！正在处理新 HTTP 连接...\n", sub->id);

    pthread_mutex_lock(&sub->mutex);
    while (sub->queue_head != sub->queue_tail) {
        int new_conn_fd = sub->conn_queue[sub->queue_head];
        sub->queue_head = (sub->queue_head + 1) % QUEUE_SIZE;

        set_nonblocking(new_conn_fd);
        Event *conn_ev = event_create(new_conn_fd, sub->epoll_fd, sub, sub_read_cb);
        if (conn_ev) {
            struct epoll_event ep_ev;
            memset(&ep_ev, 0, sizeof(ep_ev));
            ep_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            ep_ev.data.ptr = conn_ev;

            if (epoll_ctl(sub->epoll_fd, EPOLL_CTL_ADD, new_conn_fd, &ep_ev) == 0) {
                printf("[+] [Sub-Reactor %d] 成功将 HTTP client_fd=%d 挂载至 epoll 树！\n", sub->id, new_conn_fd);
            } else {
                perror("epoll_ctl ADD failed in sub_reactor");
                close(new_conn_fd);
                event_free(conn_ev);
            }
        } else {
            close(new_conn_fd);
        }
    }
    pthread_mutex_unlock(&sub->mutex);
}

static void* sub_reactor_thread_routine(void *arg) {
    SubReactor *sub = (SubReactor *)arg;
    printf("[Sub-Reactor %d] HTTP Event Loop I/O 线程已启动 (TID=%lu)\n", sub->id, (unsigned long)pthread_self());

    while (1) {
        int nfds = epoll_wait(sub->epoll_fd, sub->events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("sub_reactor epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            Event *ev = (Event *)sub->events[i].data.ptr;
            if (ev && ev->handler) {
                ev->handler(ev);
            }
        }
    }
    return NULL;
}

int sub_reactor_init(SubReactor *sub, int id, ThreadPool *pool) {
    memset(sub, 0, sizeof(SubReactor));
    sub->id = id;
    sub->pool = pool;

    sub->epoll_fd = epoll_create1(0);
    if (sub->epoll_fd < 0) {
        perror("epoll_create1 failed for SubReactor");
        return -1;
    }

    sub->wakeup_fd = eventfd(0, EFD_NONBLOCK);
    if (sub->wakeup_fd < 0) {
        perror("eventfd create failed");
        close(sub->epoll_fd);
        return -1;
    }

    pthread_mutex_init(&sub->mutex, NULL);
    sub->queue_head = 0;
    sub->queue_tail = 0;

    Event *wakeup_ev = event_create(sub->wakeup_fd, sub->epoll_fd, sub, sub_wakeup_cb);
    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = EPOLLIN | EPOLLET;
    ep_ev.data.ptr = wakeup_ev;
    epoll_ctl(sub->epoll_fd, EPOLL_CTL_ADD, sub->wakeup_fd, &ep_ev);

    return 0;
}

void sub_reactor_start(SubReactor *sub) {
    pthread_create(&sub->thread_id, NULL, sub_reactor_thread_routine, (void *)sub);
}

void sub_reactor_dispatch_conn(SubReactor *sub, int conn_fd) {
    pthread_mutex_lock(&sub->mutex);
    sub->conn_queue[sub->queue_tail] = conn_fd;
    sub->queue_tail = (sub->queue_tail + 1) % QUEUE_SIZE;
    pthread_mutex_unlock(&sub->mutex);

    uint64_t u = 1;
    ssize_t s = write(sub->wakeup_fd, &u, sizeof(u));
    (void)s;
}

void sub_reactor_destroy(SubReactor *sub) {
    if (sub->wakeup_fd > 0) close(sub->wakeup_fd);
    if (sub->epoll_fd > 0) close(sub->epoll_fd);
    pthread_mutex_destroy(&sub->mutex);
}

// ================= HTTP Handlers & Worker Task =================

void sub_read_cb(Event *ev) {
    while (1) {
        ssize_t bytes = read(ev->fd, ev->buffer + ev->length, sizeof(ev->buffer) - ev->length - 1);
        if (bytes > 0) {
            ev->length += bytes;
            ev->buffer[ev->length] = '\0';
        } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            if (bytes == 0) {
                printf("[-] [Sub-Reactor %d] 客户端关闭连接 client_fd=%d\n", ev->sub->id, ev->fd);
            } else {
                perror("[sub_read_cb] read error");
            }
            sub_event_del(ev);
            return;
        }
    }

    if (ev->length > 0) {
        printf("[<-] [Sub-Reactor %d] I/O 收到 HTTP 报文 (%d 字节)，投递给 Worker 线程池解析\n",
               ev->sub->id, ev->length);

        // 甩给共享 Worker 线程池进行 HTTP 解析与路由生成
        if (threadpool_add_task(ev->sub->pool, worker_http_task, ev) < 0) {
            fprintf(stderr, "threadpool_add_task failed in Sub-Reactor %d for fd=%d\n", ev->sub->id, ev->fd);
            sub_event_del(ev);
        }
    } else {
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    }
}

// Worker 线程池：进行 HTTP 请求解析、路由分发与 HTTP 响应报文构建
void worker_http_task(void *arg) {
    Event *ev = (Event *)arg;
    pthread_t tid = pthread_self();

    HttpRequest req;
    if (parse_http_request(ev->buffer, ev->length, &req) < 0) {
        printf("[!] [Worker 线程 %lu] HTTP 请求解析失败 (fd=%d)\n", (unsigned long)tid, ev->fd);

        HttpResponse res = {
            .status_code = 400,
            .status_phrase = "Bad Request",
            .content_type = "text/plain; charset=utf-8",
            .body = "400 Bad Request\n",
            .body_length = 16
        };
        ev->length = build_http_response(&res, ev->buffer, sizeof(ev->buffer));
    } else {
        printf("[🌐] [Worker 线程 %lu] 解析 HTTP 成功: Method=%s, Path=%s, Version=%s\n",
               (unsigned long)tid, req.method, req.path, req.version);

        HttpResponse res;
        char body_buf[1024];

        // 路由解析与响应分发
        if (strcmp(req.path, "/") == 0 || strcmp(req.path, "/index.html") == 0) {
            res.status_code = 200;
            res.status_phrase = "OK";
            res.content_type = "text/html; charset=utf-8";
            snprintf(body_buf, sizeof(body_buf),
                "<!DOCTYPE html><html><head><title>Epoll Multi-Reactor Stage 6</title></head>"
                "<body style='font-family:sans-serif; background:#f4f6f9; padding:40px;'>"
                "<h1 style='color:#0066cc;'>🚀 Stage 6: Epoll Multi-Reactor HTTP Server</h1>"
                "<p>Congratulations! You are visiting a high-performance C HTTP Server powered by Main-Sub Multi-Reactor + ThreadPool architecture.</p>"
                "<ul><li><strong>Request Method:</strong> %s</li><li><strong>URI Path:</strong> %s</li><li><strong>Protocol:</strong> %s</li></ul>"
                "</body></html>",
                req.method, req.path, req.version);
            res.body = body_buf;
            res.body_length = strlen(body_buf);
        } else if (strcmp(req.path, "/api/hello") == 0 || strcmp(req.path, "/json") == 0) {
            res.status_code = 200;
            res.status_phrase = "OK";
            res.content_type = "application/json; charset=utf-8";
            snprintf(body_buf, sizeof(body_buf),
                "{\"code\":200,\"status\":\"success\",\"message\":\"Hello from Stage 6 Epoll Multi-Reactor HTTP Server!\",\"method\":\"%s\",\"path\":\"%s\"}",
                req.method, req.path);
            res.body = body_buf;
            res.body_length = strlen(body_buf);
        } else if (strcmp(req.path, "/ping") == 0) {
            res.status_code = 200;
            res.status_phrase = "OK";
            res.content_type = "text/plain; charset=utf-8";
            res.body = "pong\n";
            res.body_length = 5;
        } else {
            res.status_code = 404;
            res.status_phrase = "Not Found";
            res.content_type = "text/html; charset=utf-8";
            snprintf(body_buf, sizeof(body_buf),
                "<!DOCTYPE html><html><body><h1>404 Not Found</h1><p>The path <code>%s</code> was not found on this Epoll server.</p></body></html>",
                req.path);
            res.body = body_buf;
            res.body_length = strlen(body_buf);
        }

        // 构建 HTTP Response 存入 ev->buffer
        ev->length = build_http_response(&res, ev->buffer, sizeof(ev->buffer));
    }

    // 切换 handler 为 sub_write_cb，通知 Sub-Reactor 准备发送 HTTP Response
    ev->handler = sub_write_cb;

    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
    ep_ev.data.ptr = ev;

    epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
}

void sub_write_cb(Event *ev) {
    ssize_t bytes = write(ev->fd, ev->buffer, ev->length);
    if (bytes > 0) {
        printf("[->] [Sub-Reactor %d] HTTP 响应发送成功 (%ld 字节) 给 client_fd=%d\n",
               ev->sub->id, bytes, ev->fd);

        // HTTP/1.1 短连接 (Connection: close)，发送完毕后释放连接
        sub_event_del(ev);
    } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    } else {
        perror("[sub_write_cb] write error");
        sub_event_del(ev);
    }
}
