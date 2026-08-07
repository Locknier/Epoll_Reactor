#ifndef REACTOR_H
#define REACTOR_H

#include <stdint.h>
#include <sys/epoll.h>

#define MAX_EVENTS 1024
#define BUFFER_SIZE 1024

struct Event;

// 定义回调函数类型
typedef void (*event_handler_t)(struct Event *ev);

// 事件上下文结构体 (Event Context)
typedef struct Event {
    int fd;                             // 绑定的文件描述符
    int epoll_fd;                       // 归属的 epoll 实例
    uint32_t events;                    // 监控的事件 (EPOLLIN / EPOLLOUT)
    event_handler_t handler;            // 回调函数指针 (accept_cb / read_cb / write_cb)
    char buffer[BUFFER_SIZE];           // 接收/发送数据缓冲区
    int length;                         // 缓冲区数据有效长度
} Event;

// Reactor 结构体 (反应器主体)
typedef struct Reactor {
    int epoll_fd;
    int listen_fd;
    struct epoll_event events[MAX_EVENTS];
} Reactor;

// 工具与 Reactor API
int set_nonblocking(int fd);

Event* event_create(int fd, int epoll_fd, event_handler_t handler);
void event_free(Event *ev);

int reactor_init(Reactor *reactor, int port);
void reactor_run(Reactor *reactor);
void reactor_destroy(Reactor *reactor);

int reactor_add_event(int epoll_fd, int events, Event *ev);
int reactor_mod_event(int epoll_fd, int events, Event *ev);
int reactor_del_event(Event *ev);

// 具体 Handlers
void accept_cb(Event *ev);
void read_cb(Event *ev);
void write_cb(Event *ev);

#endif // REACTOR_H
