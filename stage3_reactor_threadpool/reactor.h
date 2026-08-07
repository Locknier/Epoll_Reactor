#ifndef REACTOR_H
#define REACTOR_H

#include "threadpool.h"
#include <stdint.h>
#include <sys/epoll.h>

#define MAX_EVENTS 1024
#define BUFFER_SIZE 1024

struct Event;
typedef void (*event_handler_t)(struct Event *ev);

typedef struct Event {
    int fd;
    int epoll_fd;
    uint32_t events;
    event_handler_t handler;
    char buffer[BUFFER_SIZE];
    int length;
    ThreadPool *pool; // 指向全域共享线程池
} Event;

typedef struct Reactor {
    int epoll_fd;
    int listen_fd;
    ThreadPool *pool;
    struct epoll_event events[MAX_EVENTS];
} Reactor;

int set_nonblocking(int fd);
Event* event_create(int fd, int epoll_fd, ThreadPool *pool, event_handler_t handler);
void event_free(Event *ev);

int reactor_init(Reactor *reactor, int port, ThreadPool *pool);
void reactor_run(Reactor *reactor);
void reactor_destroy(Reactor *reactor);

int reactor_add_event(int epoll_fd, int events, Event *ev);
int reactor_mod_event(int epoll_fd, int events, Event *ev);
int reactor_del_event(Event *ev);

void accept_cb(Event *ev);
void read_cb(Event *ev);
void write_cb(Event *ev);
void worker_business_task(void *arg);

#endif // REACTOR_H
