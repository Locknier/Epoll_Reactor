#include "reactor.h"
#include <stdio.h>
#include <stdlib.h>

#define PORT 8080

int main() {
    Reactor reactor;
    if (reactor_init(&reactor, PORT) < 0) {
        fprintf(stderr, "Failed to initialize reactor\n");
        return EXIT_FAILURE;
    }

    reactor_run(&reactor);
    reactor_destroy(&reactor);

    return 0;
}
