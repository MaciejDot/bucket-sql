#ifndef SQL_EXECUTOR_H
#define SQL_EXECUTOR_H

#include <stddef.h>
#include <stdint.h>
#include "sql_parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Column values in a row */
typedef enum row_value_kind {
    ROW_VAL_NULL,
    ROW_VAL_INT,
    ROW_VAL_FLOAT,
    ROW_VAL_BOOL,
    ROW_VAL_TEXT
} row_value_kind_t;

typedef struct row_value {
    row_value_kind_t kind;
    union {
        int64_t i64;
        double f64;
        int boolean;
        struct {
            const uint8_t* ptr;
            size_t len;
        } text;
    } as;
} row_value_t;

/* Table schema: column names and types */
typedef struct table_column {
    sql_text_ref_t name;
    row_value_kind_t type;
} table_column_t;

/* Simple in-memory table */
typedef struct table {
    table_column_t* columns;
    size_t column_count;
    row_value_t** rows;   /* array of pointers to row arrays */
    size_t row_count;
} table_t;

/* Executor result */
typedef struct exec_result {
    int status;               /* 0 = ok, non-zero = error */
    const char* error;
    size_t error_length;

    table_t output;           /* selected rows */
} exec_result_t;

/* Main executor: runs a plan on a single in-memory table */
exec_result_t sql_execute(const sql_plan_t* plan, const table_t* input_table);

/* Frees memory in an exec_result */
void exec_result_free(exec_result_t* result);

#ifdef __cplusplus
}
#endif

#endif