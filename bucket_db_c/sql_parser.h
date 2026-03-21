#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <stddef.h>
#include <stdint.h>

#include "sql_tokenizer.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sql_text_ref {
    const uint8_t* ptr;   /* points into planner-owned source copy or arg copy */
    uint32_t len;
    uint8_t quoted;       /* 1 if came from quoted SQL syntax or quoted string */
} sql_text_ref_t;

typedef enum sql_arg_kind {
    SQL_ARG_NULL,
    SQL_ARG_BOOL,
    SQL_ARG_INT,
    SQL_ARG_FLOAT,
    SQL_ARG_TEXT
} sql_arg_kind_t;

typedef struct sql_arg {
    const uint8_t* name;
    size_t name_len;
    sql_arg_kind_t kind;
    union {
        int boolean;
        int64_t i64;
        double f64;
        struct {
            const uint8_t* bytes;
            size_t len;
        } text;
    } as;
} sql_arg_t;

typedef enum sql_expr_kind {
    SQL_EXPR_NAME,
    SQL_EXPR_STRING,
    SQL_EXPR_INT,
    SQL_EXPR_FLOAT,
    SQL_EXPR_BOOL,
    SQL_EXPR_NULL,
    SQL_EXPR_STAR,
    SQL_EXPR_UNARY,
    SQL_EXPR_BINARY,
    SQL_EXPR_CALL
} sql_expr_kind_t;

typedef enum sql_unary_op {
    SQL_UNARY_POSITIVE,
    SQL_UNARY_NEGATIVE,
    SQL_UNARY_NOT
} sql_unary_op_t;

typedef enum sql_binary_op {
    SQL_BINARY_OR,
    SQL_BINARY_AND,
    SQL_BINARY_EQ,
    SQL_BINARY_NEQ,
    SQL_BINARY_LT,
    SQL_BINARY_LTE,
    SQL_BINARY_GT,
    SQL_BINARY_GTE,
    SQL_BINARY_ADD,
    SQL_BINARY_SUB,
    SQL_BINARY_MUL,
    SQL_BINARY_DIV,
    SQL_BINARY_MOD,
    SQL_BINARY_LIKE
} sql_binary_op_t;

typedef struct sql_order_item sql_order_item_t;
typedef struct sql_expr sql_expr_t;

typedef struct sql_window_spec {
    sql_expr_t** partition_by;
    size_t partition_by_count;

    sql_order_item_t* order_by;
    size_t order_by_count;
} sql_window_spec_t;

struct sql_expr {
    sql_expr_kind_t kind;
    uint32_t start;
    uint32_t length;

    union {
        sql_text_ref_t text; /* SQL names and string literals */
        int64_t int_value;
        double float_value;
        int bool_value;

        struct {
            sql_unary_op_t op;
            sql_expr_t* child;
        } unary;

        struct {
            sql_binary_op_t op;
            sql_expr_t* left;
            sql_expr_t* right;
        } binary;

        struct {
            sql_text_ref_t name;
            sql_expr_t** args;
            size_t arg_count;
            int has_window;
            sql_window_spec_t window;
        } call;
    } as;
};

typedef struct sql_select_item {
    sql_expr_t* expr;
    int has_alias;
    sql_text_ref_t alias;
} sql_select_item_t;

struct sql_order_item {
    sql_expr_t* expr;
    int descending;
};

typedef struct sql_select_plan {
    const uint8_t* source;   /* planner-owned copy of the query */
    size_t source_length;

    int distinct;

    sql_select_item_t* select_items;
    size_t select_item_count;

    sql_text_ref_t from_table;

    sql_expr_t* where_expr;

    sql_expr_t** group_by;
    size_t group_by_count;

    sql_expr_t* having_expr;

    sql_order_item_t* order_by;
    size_t order_by_count;

    int has_limit;
    int64_t limit;
    int64_t offset;
} sql_select_plan_t;

typedef enum sql_statement_kind {
    SQL_STMT_SELECT
} sql_statement_kind_t;

typedef struct sql_plan {
    sql_statement_kind_t kind;
    union {
        sql_select_plan_t select;
    } as;
} sql_plan_t;

typedef struct sql_plan_result {
    int status;                /* 0 = ok */
    const char* error;
    size_t error_length;
    sql_plan_t plan;

    void* _owner;              /* internal arena pointer */
} sql_plan_result_t;

/* main parser/planner */
sql_plan_result_t sql_plan_query(const tokenization_view_t* view,
                                 const sql_arg_t* args,
                                 size_t arg_count);

/* convenience wrapper: tokenizes + plans + frees tokenizer result */
sql_plan_result_t sql_pipeline_plan(const uint8_t* query,
                                    const sql_arg_t* args,
                                    size_t arg_count);

void sql_plan_result_free(sql_plan_result_t* result);

#ifdef __cplusplus
}
#endif

#endif