#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdalign.h>
#include <string.h>
#include "allocation.h"

/**
 * 1 - success
 * 0 - failure
 */
uint8_t malloc_area(alloc_area *a, size_t capacity) {
    a->data = (uint8_t*)malloc(capacity);
    if (!a->data) return 0;

    a->capacity = capacity;
    a->offset = 0;
    return 1;
}

void free_area(alloc_area *a) {
    free(a->data);
    a->data = NULL;
    a->capacity = 0;
    a->offset = 0;
}

void reset_area(alloc_area *a) {
    a->offset = 0;
}

/**
 * vaild pointer on success
 * NULL on failure
 */
void *alloc_on_area(alloc_area *a, size_t size) {
    size_t align = alignof(max_align_t);
    size_t offset = (a -> offset + align - 1) & ~(align - 1);
    if (offset + size > a->capacity) {
        return NULL;
    }
    void *ptr = a -> data + offset;
    a->offset = offset + size;
    return ptr;
}

/**
 * 0 - failure
 * 1 - success
 */
uint8_t malloc_chunked_alloc_area(chunked_alloc_area *a, size_t chunk_size){
    if(a == NULL) return 0;
    chunk *head = (chunk *)malloc(sizeof(chunk));
    if(!head) return 0;
    if(!malloc_area(&head->alloc_area, chunk_size)){
        free(head);
        return 0;
    }
    head->next = NULL;
    a->head = head;
    a->tail = head;
    a->chunk_size = chunk_size;
    return 1;
}

void* alloc_on_chunked_alloc_area(chunked_alloc_area *a, size_t size){
    void* ptr = alloc_on_area(&a->tail->alloc_area, size);
    if(ptr != NULL){
        return ptr;
    }
    size_t new_chunk_size = a->chunk_size > size ? a->chunk_size : size;
    chunk *new_chunk = (chunk *)malloc(sizeof(chunk));
    if(!new_chunk) return NULL;
    if(!malloc_area(&new_chunk->alloc_area, new_chunk_size)){
        free(new_chunk);
        return NULL;
    }
    new_chunk->next = NULL;
    a->tail->next = new_chunk;
    a->tail = new_chunk;

    return alloc_on_area(&a->tail->alloc_area, size);
}

void free_chunked_alloc_area(chunked_alloc_area *a) {
    if (!a) return;

    chunk *c = a->head;

    while (c) {
        chunk *next = c->next;
        free_area(&c->alloc_area);
        free(c);
        c = next;
    }

    a->head = NULL;
    a->tail = NULL;
    a->chunk_size = 0;
}
