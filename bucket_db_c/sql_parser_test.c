#include "sql_parser.h"

#include <time.h>
#include <stdio.h>
#include <stdint.h>

static void print_indent(int n) {
    for (int i = 0; i < n; i++) printf("  ");
}

static void print_text(sql_text_ref_t t) {
    printf("%.*s", t.len, t.ptr);
}

static void print_expr(sql_expr_t* e, int indent);

static void print_expr(sql_expr_t* e, int indent) {
    if (!e) {
        print_indent(indent);
        printf("NULL\n");
        return;
    }

    print_indent(indent);

    switch (e->kind) {
        case SQL_EXPR_NAME:
            printf("NAME: ");
            print_text(e->as.text);
            printf("\n");
            break;

        case SQL_EXPR_STRING:
            printf("STRING: ");
            print_text(e->as.text);
            printf("\n");
            break;

        case SQL_EXPR_INT:
            printf("INT: %lld\n", (long long)e->as.int_value);
            break;

        case SQL_EXPR_FLOAT:
            printf("FLOAT: %f\n", e->as.float_value);
            break;

        case SQL_EXPR_BOOL:
            printf("BOOL: %s\n", e->as.bool_value ? "true" : "false");
            break;

        case SQL_EXPR_NULL:
            printf("NULL\n");
            break;

        case SQL_EXPR_STAR:
            printf("STAR (*)\n");
            break;

        case SQL_EXPR_UNARY:
            printf("UNARY op=%d\n", e->as.unary.op);
            print_expr(e->as.unary.child, indent + 1);
            break;

        case SQL_EXPR_BINARY:
            printf("BINARY op=%d\n", e->as.binary.op);
            print_expr(e->as.binary.left, indent + 1);
            print_expr(e->as.binary.right, indent + 1);
            break;

        case SQL_EXPR_CALL:
            printf("CALL: ");
            print_text(e->as.call.name);
            printf("\n");

            print_indent(indent + 1);
            printf("ARGS (%zu):\n", e->as.call.arg_count);
            for (size_t i = 0; i < e->as.call.arg_count; i++) {
                print_expr(e->as.call.args[i], indent + 2);
            }

            if (e->as.call.has_window) {
                print_indent(indent + 1);
                printf("WINDOW:\n");

                print_indent(indent + 2);
                printf("PARTITION BY (%zu):\n", e->as.call.window.partition_by_count);
                for (size_t i = 0; i < e->as.call.window.partition_by_count; i++) {
                    print_expr(e->as.call.window.partition_by[i], indent + 3);
                }

                print_indent(indent + 2);
                printf("ORDER BY (%zu):\n", e->as.call.window.order_by_count);
                for (size_t i = 0; i < e->as.call.window.order_by_count; i++) {
                    print_indent(indent + 3);
                    printf("DESC=%d\n", e->as.call.window.order_by[i].descending);
                    print_expr(e->as.call.window.order_by[i].expr, indent + 4);
                }
            }
            break;
    }
}

void print_plan(sql_plan_t* plan) {
    if (!plan) {
        printf("PLAN NULL\n");
        return;
    }

    printf("=== PLAN ===\n");

    if (plan->kind == SQL_STMT_SELECT) {
        sql_select_plan_t* s = &plan->as.select;

        printf("TYPE: SELECT\n");
        printf("DISTINCT: %d\n", s->distinct);

        printf("SELECT ITEMS (%zu):\n", s->select_item_count);
        for (size_t i = 0; i < s->select_item_count; i++) {
            printf("  ITEM %zu:\n", i);
            print_expr(s->select_items[i].expr, 2);

            if (s->select_items[i].has_alias) {
                printf("    ALIAS: ");
                print_text(s->select_items[i].alias);
                printf("\n");
            }
        }

        printf("FROM: ");
        print_text(s->from_table);
        printf("\n");

        printf("WHERE:\n");
        print_expr(s->where_expr, 1);

        printf("GROUP BY (%zu):\n", s->group_by_count);
        for (size_t i = 0; i < s->group_by_count; i++) {
            print_expr(s->group_by[i], 1);
        }

        printf("HAVING:\n");
        print_expr(s->having_expr, 1);

        printf("ORDER BY (%zu):\n", s->order_by_count);
        for (size_t i = 0; i < s->order_by_count; i++) {
            printf("  ITEM %zu DESC=%d:\n", i, s->order_by[i].descending);
            print_expr(s->order_by[i].expr, 2);
        }

        printf("LIMIT: %s\n", s->has_limit ? "YES" : "NO");
        if (s->has_limit) {
            printf("  VALUE: %lld\n", (long long)s->limit);
        }

        printf("OFFSET: %lld\n", (long long)s->offset);
    }

    printf("=== END PLAN ===\n");
}

#define ITER 100000

static inline uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

int run_many(){

    uint64_t total = 0;
    const uint8_t* query = (const uint8_t*)
        "SELECT a, sum(b) OVER (PARTITION BY grp ORDER BY ts DESC) "
        "FROM tesco "
        "WHERE x >= @min_x "
        "GROUP BY a, grp "
        "HAVING sum(b) > @min_sum "
        "ORDER BY ts DESC "
        "LIMIT @limit OFFSET @offset;";
            sql_arg_t args[] = {
        {
            .name = (const uint8_t*)"min_x",
            .name_len = 5,
            .kind = SQL_ARG_INT,
            .as.i64 = 10
        },
        {
            .name = (const uint8_t*)"min_sum",
            .name_len = 7,
            .kind = SQL_ARG_INT,
            .as.i64 = 100
        },
        {
            .name = (const uint8_t*)"limit",
            .name_len = 5,
            .kind = SQL_ARG_INT,
            .as.i64 = 50
        },
        {
            .name = (const uint8_t*)"offset",
            .name_len = 6,
            .kind = SQL_ARG_INT,
            .as.i64 = 0
        }
    };

size_t arg_count = sizeof(args) / sizeof(args[0]);
// warmup (important for CPU cache + branch predictor)
for (int i = 0; i < 1000; i++) {
    sql_plan_result_t r = sql_pipeline_plan(query, args, arg_count);
    sql_plan_result_free(&r);
}

// benchmark
for (int i = 0; i < ITER; i++) {
    uint64_t start = now_ns();

    sql_plan_result_t r = sql_pipeline_plan(query, args, arg_count);

    uint64_t end = now_ns();
    total += (end - start);

    sql_plan_result_free(&r);
}

printf("avg time: %llu ns\n", (unsigned long long)(total / ITER));
}

int main(void) {
    const uint8_t* query = (const uint8_t*)
        "SELECT a, sum(b) OVER (PARTITION BY grp ORDER BY ts DESC) "
        "FROM tesco "
        "WHERE x >= @min_x "
        "GROUP BY a, grp "
        "HAVING sum(b) > @min_sum "
        "ORDER BY ts DESC "
        "LIMIT @limit OFFSET @offset;";

    sql_arg_t args[] = {
        {
            .name = (const uint8_t*)"min_x",
            .name_len = 5,
            .kind = SQL_ARG_INT,
            .as.i64 = 10
        },
        {
            .name = (const uint8_t*)"min_sum",
            .name_len = 7,
            .kind = SQL_ARG_INT,
            .as.i64 = 100
        },
        {
            .name = (const uint8_t*)"limit",
            .name_len = 5,
            .kind = SQL_ARG_INT,
            .as.i64 = 50
        },
        {
            .name = (const uint8_t*)"offset",
            .name_len = 6,
            .kind = SQL_ARG_INT,
            .as.i64 = 0
        }
    };

    sql_plan_result_t r = sql_pipeline_plan(query, args, sizeof(args) / sizeof(args[0]));
    if (r.status != 0) {
        fprintf(stderr, "plan error: %s\n", r.error);
        return 1;
    }


    print_plan(&r.plan);


    sql_plan_result_free(&r);
    run_many();
    return 0;
}