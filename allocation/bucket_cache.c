#include <stddef.h>
#include <stdint.h>

typedef struct bucket_cache {

} bucket_cache_t;

typedef struct bucket_options {
    size_t max_size_of_bucket_bytes;
    size_t default_size_of_page_bytes;
    size_t max_number_of_active_buckets;
    size_t max_memory_footprint;
}
bucket_options_t;

void initialize(bucket_cache_t* bucket_cache, bucket_options_t bucket_options){

}

void get_bucket(){}

void put_bucket(){}

