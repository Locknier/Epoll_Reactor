#include "reactor.h"
#include "threadpool.h"
#include <stdio.h>
#include <stdlib.h>

#define PORT 8080
#define THREAD_NUM 4

int main() {
    ThreadPool *pool = threadpool_create(THREAD_NUM);
    if (!pool) {
        fprintf(stderr, "Failed to create ThreadPool\n");
        return EXIT_FAILURE;
    }

    Reactor reactor;
    if (reactor_init(&reactor, PORT, pool) < 0) {
        fprintf(stderr, "Failed to initialize reactor\n");
        threadpool_destroy(pool);
        return EXIT_FAILURE;
    }

    reactor_run(&reactor);

    reactor_destroy(&reactor);
    threadpool_destroy(pool);
    return 0;
}
