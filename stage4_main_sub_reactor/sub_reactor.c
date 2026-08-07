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
    printf("[-] [Sub-Reactor %d] 清理三部曲: close client_fd=%d & free Event\n", ev->sub->id, ev->fd);
    epoll_ctl(ev->epoll_fd, EPOLL_CTL_DEL, ev->fd, NULL);
    close(ev->fd);
    event_free(ev);
    return 0;
}

// Sub-Reactor 的 eventfd 唤醒回调处理
static void sub_wakeup_cb(Event *ev) {
    uint64_t val;
    ssize_t s = read(ev->fd, &val, sizeof(val)); // 清空 eventfd 计数器
    (void)s;

    SubReactor *sub = ev->sub;
    printf("[*] [Sub-Reactor %d] 被 eventfd 叮咚唤醒！正在从队列取出新连接...\n", sub->id);

    pthread_mutex_lock(&sub->mutex);
    while (sub->queue_head != sub->queue_tail) {
        int new_conn_fd = sub->conn_queue[sub->queue_head];
        sub->queue_head = (sub->queue_head + 1) % QUEUE_SIZE;

        // 为新 conn_fd 创建 Event 并挂载到当前 Sub-Reactor 的 epoll 树上
        set_nonblocking(new_conn_fd);
        Event *conn_ev = event_create(new_conn_fd, sub->epoll_fd, sub, sub_read_cb);
        if (conn_ev) {
            struct epoll_event ep_ev;
            memset(&ep_ev, 0, sizeof(ep_ev));
            ep_ev.events = EPOLLIN | EPOLLET;
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
    printf("[Sub-Reactor %d] Event Loop 线程已启动 (TID=%lu)\n", sub->id, (unsigned long)pthread_self());

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

int sub_reactor_init(SubReactor *sub, int id) {
    memset(sub, 0, sizeof(SubReactor));
    sub->id = id;

    sub->epoll_fd = epoll_create1(0);
    if (sub->epoll_fd < 0) {
        perror("epoll_create1 failed for SubReactor");
        return -1;
    }

    // 核心步骤：建立跨线程通知工具 eventfd
    sub->wakeup_fd = eventfd(0, EFD_NONBLOCK);
    if (sub->wakeup_fd < 0) {
        perror("eventfd create failed");
        close(sub->epoll_fd);
        return -1;
    }

    pthread_mutex_init(&sub->mutex, NULL);
    sub->queue_head = 0;
    sub->queue_tail = 0;

    // 将 wakeup_fd 注册到自己的 epoll 树中
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

// Main-Reactor 呼叫：将 conn_fd 塞入队列并向 eventfd 写入 8 字节唤醒 Sub-Reactor
void sub_reactor_dispatch_conn(SubReactor *sub, int conn_fd) {
    pthread_mutex_lock(&sub->mutex);
    sub->conn_queue[sub->queue_tail] = conn_fd;
    sub->queue_tail = (sub->queue_tail + 1) % QUEUE_SIZE;
    pthread_mutex_unlock(&sub->mutex);

    // 唤醒动作：写 8 字节数值到 eventfd
    uint64_t u = 1;
    ssize_t s = write(sub->wakeup_fd, &u, sizeof(u));
    (void)s;
}

void sub_reactor_destroy(SubReactor *sub) {
    if (sub->wakeup_fd > 0) close(sub->wakeup_fd);
    if (sub->epoll_fd > 0) close(sub->epoll_fd);
    pthread_mutex_destroy(&sub->mutex);
}

// ================= Sub-Reactor 读写 Handlers =================

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
        printf("[<-] [Sub-Reactor %d] 读入 %d 字节数据: %s", ev->sub->id, ev->length, ev->buffer);

        // 状态机切换为 sub_write_cb，注册 EPOLLOUT
        ev->handler = sub_write_cb;
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLOUT | EPOLLET;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    }
}

void sub_write_cb(Event *ev) {
    ssize_t bytes = write(ev->fd, ev->buffer, ev->length);
    if (bytes > 0) {
        printf("[->] [Sub-Reactor %d] Echo 回传 %ld 字节给 client_fd=%d\n", ev->sub->id, bytes, ev->fd);

        ev->length = 0;
        ev->handler = sub_read_cb;
        struct epoll_event ep_ev;
        memset(&ep_ev, 0, sizeof(ep_ev));
        ep_ev.events = EPOLLIN | EPOLLET;
        ep_ev.data.ptr = ev;
        epoll_ctl(ev->epoll_fd, EPOLL_CTL_MOD, ev->fd, &ep_ev);
    } else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    } else {
        perror("[sub_write_cb] write error");
        sub_event_del(ev);
    }
}
