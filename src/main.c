#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    runtime_t runtime;
    runtime_init(&runtime);

    runtime_status_t status = runtime_bootstrap(&runtime, argc, argv);
    if (status != RUNTIME_OK) {
        runtime_shutdown(&runtime);
        return EXIT_FAILURE;
    }

    status = runtime_start(&runtime);
    runtime_shutdown(&runtime);

    return (status == RUNTIME_OK) ? EXIT_SUCCESS : EXIT_FAILURE;
}
