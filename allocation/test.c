#define _POSIX_C_SOURCE 199309L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <inttypes.h>
#include "allocation.h"

#define ROWS 10000000
#define MIN_SIZE 16
#define MAX_SIZE 64
#define RUNS 5
#define CHUNK_SIZE (4000000)

static size_t rand_size(void) {
    return MIN_SIZE + (size_t)(rand() % (MAX_SIZE - MIN_SIZE + 1));
}

static double now_sec(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static inline void touch(uint8_t *p, size_t size, volatile uint64_t *sink) {
    for (size_t i = 0; i < size; i++) {
        p[i] = (uint8_t)i;
    }
    *sink += p[0];
}

int main(void) {
    srand(42);

    size_t *sizes = (size_t *)malloc(sizeof(size_t) * ROWS);
    uint8_t **ptrs = (uint8_t **)malloc(sizeof(uint8_t *) * ROWS);

    if (!sizes || !ptrs) {
        fprintf(stderr, "failed to allocate helper arrays\n");
        free(sizes);
        free(ptrs);
        return 1;
    }

    for (size_t i = 0; i < ROWS; i++) {
        sizes[i] = rand_size();
    }

    size_t single_capacity = 0;
    for (size_t i = 0; i < ROWS; i++) {
        single_capacity += sizes[i] + sizeof(max_align_t);
    }

    alloc_area single_area;
    if (!malloc_area(&single_area, single_capacity)) {
        fprintf(stderr, "malloc_area failed\n");
        free(sizes);
        free(ptrs);
        return 1;
    }

    volatile uint64_t sink = 0;

    /* warmup: malloc */
    for (size_t i = 0; i < ROWS; i++) {
        ptrs[i] = (uint8_t *)malloc(sizes[i]);
        if (!ptrs[i]) {
            fprintf(stderr, "warmup malloc failed\n");
            free_area(&single_area);
            free(sizes);
            free(ptrs);
            return 1;
        }
        touch(ptrs[i], sizes[i], &sink);
    }
    for (size_t i = 0; i < ROWS; i++) {
        free(ptrs[i]);
    }

    /* warmup: single area */
    reset_area(&single_area);
    for (size_t i = 0; i < ROWS; i++) {
        uint8_t *p = (uint8_t *)alloc_on_area(&single_area, sizes[i]);
        if (!p) {
            fprintf(stderr, "warmup alloc_on_area failed\n");
            free_area(&single_area);
            free(sizes);
            free(ptrs);
            return 1;
        }
        touch(p, sizes[i], &sink);
    }

    /* warmup: chunked area */
    {
        chunked_alloc_area warm_chunked = {0};
        if (!malloc_chunked_alloc_area(&warm_chunked, CHUNK_SIZE)) {
            fprintf(stderr, "warmup malloc_chunked_alloc_area failed\n");
            free_area(&single_area);
            free(sizes);
            free(ptrs);
            return 1;
        }

        for (size_t i = 0; i < ROWS; i++) {
            uint8_t *p = (uint8_t *)alloc_on_chunked_alloc_area(&warm_chunked, sizes[i]);
            if (!p) {
                fprintf(stderr, "warmup alloc_on_chunked_alloc_area failed\n");
                free_chunked_alloc_area(&warm_chunked);
                free_area(&single_area);
                free(sizes);
                free(ptrs);
                return 1;
            }
            touch(p, sizes[i], &sink);
        }

        free_chunked_alloc_area(&warm_chunked);
    }

    double malloc_total = 0.0;
    double single_total = 0.0;
    double chunked_total = 0.0;

    for (int run = 0; run < RUNS; run++) {
        /* malloc/free */
        double t0 = now_sec();
        for (size_t i = 0; i < ROWS; i++) {
            ptrs[i] = (uint8_t *)malloc(sizes[i]);
            if (!ptrs[i]) {
                fprintf(stderr, "malloc failed\n");
                free_area(&single_area);
                free(sizes);
                free(ptrs);
                return 1;
            }
            touch(ptrs[i], sizes[i], &sink);
        }
        for (size_t i = 0; i < ROWS; i++) {
            free(ptrs[i]);
        }
        double t1 = now_sec();
        malloc_total += (t1 - t0);

        /* single area */
        reset_area(&single_area);
        double t2 = now_sec();
        for (size_t i = 0; i < ROWS; i++) {
            uint8_t *p = (uint8_t *)alloc_on_area(&single_area, sizes[i]);
            if (!p) {
                fprintf(stderr, "alloc_on_area failed\n");
                free_area(&single_area);
                free(sizes);
                free(ptrs);
                return 1;
            }
            touch(p, sizes[i], &sink);
        }
        double t3 = now_sec();
        single_total += (t3 - t2);

        /* chunked area: fresh instance per run */
        chunked_alloc_area chunked = {0};
        if (!malloc_chunked_alloc_area(&chunked, CHUNK_SIZE)) {
            fprintf(stderr, "malloc_chunked_alloc_area failed\n");
            free_area(&single_area);
            free(sizes);
            free(ptrs);
            return 1;
        }

        double t4 = now_sec();
        for (size_t i = 0; i < ROWS; i++) {
            uint8_t *p = (uint8_t *)alloc_on_chunked_alloc_area(&chunked, sizes[i]);
            if (!p) {
                fprintf(stderr, "alloc_on_chunked_alloc_area failed\n");
                free_chunked_alloc_area(&chunked);
                free_area(&single_area);
                free(sizes);
                free(ptrs);
                return 1;
            }
            touch(p, sizes[i], &sink);
        }
        double t5 = now_sec();
        chunked_total += (t5 - t4);

        free_chunked_alloc_area(&chunked);
    }

    printf("Rows: %d, Runs: %d\n", ROWS, RUNS);
    printf("malloc/free avg: %.6f s\n", malloc_total / RUNS);
    printf("single area avg: %.6f s\n", single_total / RUNS);
    printf("chunked area avg: %.6f s\n", chunked_total / RUNS);
    printf("malloc vs single: %.2fx\n", (malloc_total / RUNS) / (single_total / RUNS));
    printf("malloc vs chunked: %.2fx\n", (malloc_total / RUNS) / (chunked_total / RUNS));
    printf("sink: %" PRIu64 "\n", (uint64_t)sink);

    free_area(&single_area);
    free(ptrs);
    free(sizes);
    return 0;
}