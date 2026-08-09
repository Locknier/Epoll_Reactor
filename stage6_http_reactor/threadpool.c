#include "threadpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void* worker_thread_routine(void *arg) {
    ThreadPool *pool = (ThreadPool *)arg;

    while (1) {
        pthread_mutex_lock(&pool->lock);

        while (pool->queue_head == NULL && !pool->shutdown) {
            pthread_cond_wait(&pool->notify, &pool->lock);
        }

        if (pool->shutdown && pool->queue_head == NULL) {
            pthread_mutex_unlock(&pool->lock);
            pthread_exit(NULL);
        }

        Task *task = pool->queue_head;
        if (task) {
            pool->queue_head = task->next;
            if (pool->queue_head == NULL) {
                pool->queue_tail = NULL;
            }
        }

        pthread_mutex_unlock(&pool->lock);

        if (task) {
            if (task->function) {
                task->function(task->arg);
            }
            free(task);
        }
    }
    return NULL;
}

ThreadPool* threadpool_create(int thread_count) {
    if (thread_count <= 0) thread_count = 4;

    ThreadPool *pool = (ThreadPool *)malloc(sizeof(ThreadPool));
    if (!pool) return NULL;

    pool->thread_count = thread_count;
    pool->queue_head = NULL;
    pool->queue_tail = NULL;
    pool->shutdown = 0;

    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->notify, NULL);

    pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);
    if (!pool->threads) {
        free(pool);
        return NULL;
    }

    for (int i = 0; i < thread_count; i++) {
        if (pthread_create(&pool->threads[i], NULL, worker_thread_routine, (void *)pool) != 0) {
            threadpool_destroy(pool);
            return NULL;
        }
    }

    printf("[ThreadPool] Worker 业务线程池创建成功，工作线程数量: %d\n", thread_count);
    return pool;
}

int threadpool_add_task(ThreadPool *pool, task_func_t function, void *arg) {
    if (!pool || !function) return -1;

    Task *task = (Task *)malloc(sizeof(Task));
    if (!task) return -1;

    task->function = function;
    task->arg = arg;
    task->next = NULL;

    pthread_mutex_lock(&pool->lock);

    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->lock);
        free(task);
        return -1;
    }

    if (pool->queue_tail == NULL) {
        pool->queue_head = task;
        pool->queue_tail = task;
    } else {
        pool->queue_tail->next = task;
        pool->queue_tail = task;
    }

    pthread_cond_signal(&pool->notify);
    pthread_mutex_unlock(&pool->lock);

    return 0;
}

int threadpool_destroy(ThreadPool *pool) {
    if (!pool) return -1;

    pthread_mutex_lock(&pool->lock);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->notify);
    pthread_mutex_unlock(&pool->lock);

    for (int i = 0; i < pool->thread_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    free(pool->threads);

    Task *cur = pool->queue_head;
    while (cur) {
        Task *next = cur->next;
        free(cur);
        cur = next;
    }

    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->notify);
    free(pool);
    return 0;
}
