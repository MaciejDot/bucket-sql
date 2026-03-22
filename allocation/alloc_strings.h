#ifndef ALLOC_STRINGS_H
#define ALLOC_STRINGS_H

#include <stddef.h>
#include <stdint.h>
#include "allocation.h"

typedef struct string_t {
    uint8_t *data;
    size_t len;
} string_t;

string_t* alloc_string_on_area(alloc_area* a, size_t length);

#endif