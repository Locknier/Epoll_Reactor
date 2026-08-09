#ifndef SUB_REACTOR_H
#define SUB_REACTOR_H

#include "threadpool.h"
#include <pthread.h>
#include <stdint.h>
#include <sys/epoll.h>

#define MAX_EVENTS 1024
#define BUFFER_SIZE 4096
#define QUEUE_SIZE 1024

struct SubReactor;
struct Event;

typedef void (*event_handler_t)(struct Event *ev);

typedef struct Event {
    int fd;
    int epoll_fd;
    uint32_t events;
    event_handler_t handler;
    char buffer[BUFFER_SIZE];
    int length;
    struct SubReactor *sub;
} Event;

typedef struct SubReactor {
    int id;
    pthread_t thread_id;
    int epoll_fd;
    int wakeup_fd;
    ThreadPool *pool;

    int conn_queue[QUEUE_SIZE];
    int queue_head;
    int queue_tail;
    pthread_mutex_t mutex;

    struct epoll_event events[MAX_EVENTS];
} SubReactor;

int sub_reactor_init(SubReactor *sub, int id, ThreadPool *pool);
void sub_reactor_start(SubReactor *sub);
void sub_reactor_dispatch_conn(SubReactor *sub, int conn_fd);
void sub_reactor_destroy(SubReactor *sub);

void sub_read_cb(Event *ev);
void sub_write_cb(Event *ev);
void worker_http_task(void *arg);

#endif // SUB_REACTOR_H
