#ifndef ALLOCATION_H
#define ALLOCATION_H

#include <stddef.h>
#include <stdint.h>

typedef struct alloc_area {
    uint8_t *data;
    size_t capacity;
    size_t offset;
} alloc_area;

typedef struct chunk {
    alloc_area alloc_area;
    struct chunk *next;
} chunk;

typedef struct chunked_alloc_area {
    chunk *head;
    chunk *tail;
    size_t chunk_size;
} chunked_alloc_area;

uint8_t malloc_area(alloc_area *a, size_t capacity);
void free_area(alloc_area *a);
void reset_area(alloc_area *a);

static inline alloc_area stackalloc_from_buffer(uint8_t *buffer, size_t size) {
    alloc_area a;
    a.data = buffer;
    a.capacity = size;
    a.offset = 0;
    return a;
}

void *alloc_on_area(alloc_area *a, size_t size);

uint8_t malloc_chunked_alloc_area(chunked_alloc_area *a, size_t chunk_size);
void free_chunked_alloc_area(chunked_alloc_area *a);
void *alloc_on_chunked_alloc_area(chunked_alloc_area *a, size_t size);

#endif