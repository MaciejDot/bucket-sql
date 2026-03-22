#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdalign.h>
#include <string.h>
#include "allocation.h"
#include "alloc_strings.h"

string_t* alloc_string_on_area(alloc_area* a, size_t length){
    string_t* ptr = alloc_on_area(a, sizeof(string_t));
    if(ptr == NULL) return NULL;

    ptr->len = length;
    ptr->data = alloc_on_area(a, (length + 1) * sizeof(uint8_t));
    if(ptr->data == NULL) return NULL;

    ptr->data[length] = '\0';
    return ptr;
}