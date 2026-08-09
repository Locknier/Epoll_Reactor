#include "main_reactor.h"
#include <stdio.h>
#include <stdlib.h>

#define PORT 8080

int main() {
    MainReactor main_reactor;
    if (main_reactor_init(&main_reactor, PORT) < 0) {
        fprintf(stderr, "Failed to initialize MainReactor (Stage 6)\n");
        return EXIT_FAILURE;
    }

    main_reactor_run(&main_reactor);
    main_reactor_destroy(&main_reactor);

    return 0;
}
