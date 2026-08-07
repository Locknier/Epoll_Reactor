#include "sub_reactor.h"
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
    printf("[-] [Sub-Reactor %d] 清理三部曲: epoll_ctl(DEL) -> close client_fd=%d -> free Event\n", ev->sub->id, ev->fd);
    epoll_ctl(ev->epoll_fd, EPOLL_CTL_DEL, ev->fd, NULL);
    close(ev->fd);
    event_free(ev);
    return 0;
}

// Sub-Reactor 的 eventfd 唤醒回调处理
static void sub_wakeup_cb(Event *ev) {
    uint64_t val;
    ssize_t s = read(ev->fd, &val, sizeof(val));
    (void)s;

    SubReactor *sub = ev->sub;
    printf("[*] [Sub-Reactor %d] 被 eventfd 叮咚唤醒！正在从队列取出新连接...\n", sub->id);

    pthread_mutex_lock(&sub->mutex);
    while (sub->queue_head != sub->queue_tail) {
        int new_conn_fd = sub->conn_queue[sub->queue_head];
        sub->queue_head = (sub->queue_head + 1) % QUEUE_SIZE;

        set_nonblocking(new_conn_fd);
        Event *conn_ev = event_create(new_conn_fd, sub->epoll_fd, sub, sub_read_cb);
        if (conn_ev) {
            struct epoll_event ep_ev;
            memset(&ep_ev, 0, sizeof(ep_ev));
            // 核心防范：包含 EPOLLONESHOT，确保安全
            ep_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            ep_ev.data.ptr = conn_ev;
            
            if (epoll_ctl(sub->epoll_fd, EPOLL_CTL_ADD, new_conn_fd, &ep_ev) == 0) {
                printf("[+] [Sub-Reactor %d] 成功将 client_fd=%d 挂载到本线程 epoll 树！\n", sub->id, new_conn_fd);
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
    printf("[Sub-Reactor %d] Event Loop I/O 线程已启动 (TID=%lu)\n", sub->id, (unsigned long)pthread_self());

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

// ================= Handlers & Worker Task 实现 =================

// sub_read_cb：Sub-Reactor 线程快速将内核数据读入 Buffer，然后将业务打包丢给 ThreadPool
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
                printf("[-] [Sub-Reactor %d] 客户端断开 client_fd=%d\n", ev->sub->id, ev->fd);
            } else {
                perror("[sub_read_cb] read error");
            }
            sub_event_del(ev);
            return;
        }
    }

    if (ev->length > 0) {
        printf("[<-] [Sub-Reactor %d] I/O 快速读取完成 (%d 字节)，即将打包 Task 投递给 Worker 线程池\n",
               ev->sub->id, ev->length);

        // 投递给共享 Worker 线程池
        if (threadpool_add_task(ev->sub->pool, worker_business_task, ev) < 0) {
            fprintf(stderr, "threadpool_add_task failed in Sub-Reactor %d for fd=%d\n", ev->sub->id, ev->fd);
            sub_event_del(ev);
        }
        // Sub-Reactor I/O 线程立刻返回，回到 epoll_wait 继续服务其他客户端 Socket！
    } else {
        // 未读到有效数据且未出错，重新 MOD 使能 EPOLLONESHOT
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    }
}

// Worker 线程池执行的具体耗时业务逻辑
void worker_business_task(void *arg) {
    Event *ev = (Event *)arg;
    pthread_t tid = pthread_self();

    printf("[*] [Worker 业务线程 %lu] 接收到 Sub-Reactor %d 甩来的 Task (client_fd=%d)，开始耗时业务计算...\n",
           (unsigned long)tid, ev->sub->id, ev->fd);

    // 模拟耗时计算 (20ms 业务逻辑)
    usleep(20000);

    printf("[*] [Worker 业务线程 %lu] 耗时业务处理完成！通知 Sub-Reactor %d 重新开启监听以回写\n",
           (unsigned long)tid, ev->sub->id);

    // 切换 handler 为 sub_write_cb
    ev->handler = sub_write_cb;

    // 修改该 Sub-Reactor 的 epoll 树监听为 EPOLLOUT (包含 EPOLLONESHOT)
    struct epoll_event ep_ev;
    memset(&ep_ev, 0, sizeof(ep_ev));
    ep_ev.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
    ep_ev.data.ptr = ev;

    epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
}

// sub_write_cb：被 Sub-Reactor 触发，负责将 Buffer 回写给客户端
void sub_write_cb(Event *ev) {
    ssize_t bytes = write(ev->fd, ev->buffer, ev->length);
    if (bytes > 0) {
        printf("[->] [Sub-Reactor %d] Echo 成功回传 %ld 字节给 client_fd=%d\n", ev->sub->id, bytes, ev->fd);

        ev->length = 0;
        ev->handler = sub_read_cb;

        // 重新 MOD 重新开启 EPOLLIN 监听 (重新使能 EPOLLONESHOT)
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // 缓冲区满，保持 EPOLLOUT | EPOLLONESHOT
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
