#ifndef MAIN_REACTOR_H
#define MAIN_REACTOR_H

#include "sub_reactor.h"

#define SUB_REACTOR_NUM 4

typedef struct MainReactor {
    int listen_fd;
    int epoll_fd;
    SubReactor sub_reactors[SUB_REACTOR_NUM];
    int next_sub_index;
} MainReactor;

int main_reactor_init(MainReactor *main_r, int port);
void main_reactor_run(MainReactor *main_r);
void main_reactor_destroy(MainReactor *main_r);

void main_accept_cb(Event *ev);

#endif // MAIN_REACTOR_H
