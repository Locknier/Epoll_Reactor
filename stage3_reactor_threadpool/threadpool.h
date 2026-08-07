#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>

typedef void (*task_func_t)(void *arg);

typedef struct Task {
    task_func_t function;
    void *arg;
    struct Task *next;
} Task;

typedef struct ThreadPool {
    pthread_mutex_t lock;
    pthread_cond_t notify;
    pthread_t *threads;
    Task *queue_head;
    Task *queue_tail;
    int thread_count;
    int shutdown;
} ThreadPool;

ThreadPool* threadpool_create(int thread_count);
int threadpool_add_task(ThreadPool *pool, task_func_t function, void *arg);
int threadpool_destroy(ThreadPool *pool);

#endif // THREADPOOL_H
