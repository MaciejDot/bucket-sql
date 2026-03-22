#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>    /* <<<<< add this */
#include <string.h>
#include <time.h>

#include "sql_tokenizer.h"
#include "sql_parser.h"
#include "sql_executor.h"

static inline uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

table_t create_10000_row_bucket(void) {
    table_t bucket;
    bucket.column_count = 3;
    bucket.row_count = 100000;

    /* Allocate columns */
    bucket.columns = malloc(sizeof(table_column_t) * bucket.column_count);
    if (!bucket.columns) { bucket.row_count = 0; return bucket; }

    bucket.columns[0].name.ptr = (const uint8_t*)"id";
    bucket.columns[0].name.len = 2;
    bucket.columns[0].type = ROW_VAL_INT;

    bucket.columns[1].name.ptr = (const uint8_t*)"name";
    bucket.columns[1].name.len = 4;
    bucket.columns[1].type = ROW_VAL_TEXT;

    bucket.columns[2].name.ptr = (const uint8_t*)"active";
    bucket.columns[2].name.len = 6;
    bucket.columns[2].type = ROW_VAL_BOOL;

    /* Allocate rows array */
    bucket.rows = malloc(sizeof(row_value_t*) * bucket.row_count);
    if (!bucket.rows) { free(bucket.columns); bucket.row_count = 0; return bucket; }

    for (size_t r = 0; r < bucket.row_count; ++r) {
        bucket.rows[r] = malloc(sizeof(row_value_t) * bucket.column_count);
        if (!bucket.rows[r]) {
            // Free previous allocations
            for (size_t i = 0; i < r; ++i) free(bucket.rows[i]);
            free(bucket.rows);
            free(bucket.columns);
            bucket.row_count = 0;
            return bucket;
        }

        /* id column */
        bucket.rows[r][0].kind = ROW_VAL_INT;
        bucket.rows[r][0].as.i64 = (int64_t)(r + 1);

        /* name column: "NameX" */
        char tmp[32];
        int len = snprintf(tmp, sizeof(tmp), "Name%zu", r + 1);
        bucket.rows[r][1].kind = ROW_VAL_TEXT;
        uint8_t* name_copy = malloc(len); /* allocate actual string */
        memcpy(name_copy, tmp, len);
        bucket.rows[r][1].as.text.ptr = name_copy;
        bucket.rows[r][1].as.text.len = len;

        /* active column: alternating TRUE/FALSE */
        bucket.rows[r][2].kind = ROW_VAL_BOOL;
        bucket.rows[r][2].as.boolean = (r % 2 == 0) ? 1 : 0;
    }

    return bucket;
}



int main(void) {
    /* --- 1. Setup in-memory table (bucket) --- */
    table_t bucket = create_10000_row_bucket();

    uint64_t start = now_ns();
    /* --- 2. Setup SQL query --- */
    const uint8_t* query = (const uint8_t*)"SELECT id, name, active FROM bucket WHERE active = TRUE";

    /* no placeholders in this query */
    sql_arg_t args[0];

    /* --- 3. Tokenize + plan --- */
    sql_plan_result_t r = sql_pipeline_plan(query, args, 0);

    if (r.status != 0) {
        printf("Planner error: %.*s\n", (int)r.error_length, r.error);
        sql_plan_result_free(&r);
        return 1;
    }

    /* --- 4. Execute plan --- */
    exec_result_t exec = sql_execute(&r.plan, &bucket);

    if (exec.status != 0) {
        printf("Execution error: %.*s\n", (int)exec.error_length, exec.error);
        sql_plan_result_free(&r);
        exec_result_free(&exec);
        return 1;
    }

    uint64_t end = now_ns();
    printf("\n%f ms\n", (end - start) / ((float) 1000000));
    /*
  
    printf("Query result (%zu rows):\n", exec.output.row_count);
    for (size_t i = 0; i < exec.output.row_count; ++i) {
        row_value_t* row = exec.output.rows[i];
        for (size_t c = 0; c < exec.output.column_count; ++c) {
            row_value_t val = row[c];
            switch (val.kind) {
                case ROW_VAL_INT: printf("%lld", (long long)val.as.i64); break;
                case ROW_VAL_FLOAT: printf("%f", val.as.f64); break;
                case ROW_VAL_BOOL: printf("%s", val.as.boolean ? "TRUE" : "FALSE"); break;
                case ROW_VAL_TEXT: printf("%.*s", (int)val.as.text.len, val.as.text.ptr); break;
                case ROW_VAL_NULL: printf("NULL"); break;
            }
            if (c + 1 < exec.output.column_count) printf(", ");
        }
        printf("\n");
    }*/

    /* --- 6. Cleanup --- */
    sql_plan_result_free(&r);
    exec_result_free(&exec);

    /* free bucket memory */
    for (size_t r = 0; r < bucket.row_count; ++r) free(bucket.rows[r]);
    free(bucket.rows);
    free(bucket.columns);

    return 0;
}