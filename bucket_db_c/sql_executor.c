#include "sql_executor.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Helper: copy a row_value_t */
static row_value_t row_value_copy(row_value_t v) {
    if (v.kind == ROW_VAL_TEXT) {
        uint8_t* dst = malloc(v.as.text.len);
        memcpy(dst, v.as.text.ptr, v.as.text.len);
        v.as.text.ptr = dst;
    }
    return v;
}

static void row_value_free(row_value_t* v) {
    if (!v) return;
    if (v->kind == ROW_VAL_TEXT && v->as.text.ptr) {
        free((void*)v->as.text.ptr);
        v->as.text.ptr = NULL;
    }
}

static void free_row(row_value_t* row, size_t col_count) {
    for (size_t i = 0; i < col_count; ++i) {
        row_value_free(&row[i]);
    }
    free(row);
}

void exec_result_free(exec_result_t* result) {
    if (!result) return;
    table_t* t = &result->output;
    if (t->rows) {
        for (size_t r = 0; r < t->row_count; ++r) {
            free_row(t->rows[r], t->column_count);
        }
        free(t->rows);
        t->rows = NULL;
    }
    if (t->columns) free(t->columns);
    memset(result, 0, sizeof(*result));
}

/* Lookup column by name (case-sensitive, exact match) */
static int table_column_index(const table_t* tbl, sql_text_ref_t name) {
    for (size_t i = 0; i < tbl->column_count; ++i) {
        table_column_t* col = &tbl->columns[i];
        if (col->name.len == name.len && memcmp(col->name.ptr, name.ptr, name.len) == 0)
            return (int)i;
    }
    return -1;
}

/* Evaluate simple column reference expressions */
static int eval_expr_column(const sql_expr_t* expr, const table_t* tbl, const row_value_t* row, row_value_t* out_val) {
    if (expr->kind != SQL_EXPR_NAME) return -1;
    int idx = table_column_index(tbl, expr->as.text);
    if (idx < 0) return -1;
    *out_val = row[idx];
    return 0;
}

/* Evaluate expressions recursively (very simple subset: names, int, float, bool, null) */
/* this function does not work for WHERE active= TRUE becauese it goes to SQL_EXPR BINARY */
static int eval_expr(const sql_expr_t* expr, const table_t* tbl, const row_value_t* row, row_value_t* out_val) {
    if (!expr || !out_val) return -1;

    row_value_t left_val, right_val;

    switch (expr->kind) {
        case SQL_EXPR_NAME:
            return eval_expr_column(expr, tbl, row, out_val);

        case SQL_EXPR_INT:
            out_val->kind = ROW_VAL_INT;
            out_val->as.i64 = expr->as.int_value;
            return 0;

        case SQL_EXPR_FLOAT:
            out_val->kind = ROW_VAL_FLOAT;
            out_val->as.f64 = expr->as.float_value;
            return 0;

        case SQL_EXPR_BOOL:
            out_val->kind = ROW_VAL_BOOL;
            out_val->as.boolean = expr->as.bool_value;
            return 0;

        case SQL_EXPR_NULL:
            out_val->kind = ROW_VAL_NULL;
            return 0;

        case SQL_EXPR_BINARY:
            if (eval_expr(expr->as.binary.left, tbl, row, &left_val) != 0) return -1;
            if (eval_expr(expr->as.binary.right, tbl, row, &right_val) != 0) return -1;

            out_val->kind = ROW_VAL_BOOL;

            switch (expr->as.binary.op) {
                case SQL_BINARY_EQ:
                    if (left_val.kind == right_val.kind) {
                        switch (left_val.kind) {
                            case ROW_VAL_INT: out_val->as.boolean = (left_val.as.i64 == right_val.as.i64); break;
                            case ROW_VAL_FLOAT: out_val->as.boolean = (left_val.as.f64 == right_val.as.f64); break;
                            case ROW_VAL_BOOL: out_val->as.boolean = (left_val.as.boolean == right_val.as.boolean); break;
                            case ROW_VAL_TEXT:
                                out_val->as.boolean = (left_val.as.text.len == right_val.as.text.len &&
                                                       memcmp(left_val.as.text.ptr, right_val.as.text.ptr, left_val.as.text.len) == 0);
                                break;
                            case ROW_VAL_NULL: out_val->as.boolean = 0; break;
                        }
                    } else {
                        out_val->as.boolean = 0; /* different types => false */
                    }
                    break;

                case SQL_BINARY_NEQ:
                    if (left_val.kind == right_val.kind) {
                        switch (left_val.kind) {
                            case ROW_VAL_INT: out_val->as.boolean = (left_val.as.i64 != right_val.as.i64); break;
                            case ROW_VAL_FLOAT: out_val->as.boolean = (left_val.as.f64 != right_val.as.f64); break;
                            case ROW_VAL_BOOL: out_val->as.boolean = (left_val.as.boolean != right_val.as.boolean); break;
                            case ROW_VAL_TEXT:
                                out_val->as.boolean = !(left_val.as.text.len == right_val.as.text.len &&
                                                       memcmp(left_val.as.text.ptr, right_val.as.text.ptr, left_val.as.text.len) == 0);
                                break;
                            case ROW_VAL_NULL: out_val->as.boolean = 1; break;
                        }
                    } else {
                        out_val->as.boolean = 1;
                    }
                    break;

                case SQL_BINARY_AND:
                    out_val->as.boolean = (left_val.as.boolean && right_val.as.boolean);
                    break;

                case SQL_BINARY_OR:
                    out_val->as.boolean = (left_val.as.boolean || right_val.as.boolean);
                    break;

                default:
                    return -1; /* unsupported binary op */
            }

            return 0;

        default:
            return -1; /* unsupported expression kind */
    }
}

/* Evaluate WHERE clause, returns 1 = true, 0 = false, -1 = error */
static int eval_where(const sql_expr_t* expr, const table_t* tbl, const row_value_t* row) {
    if (!expr) return 1; /* no WHERE = always true */
    row_value_t val;
    if (eval_expr(expr, tbl, row, &val) != 0) return -1;
    int result = 0;
    if (val.kind == ROW_VAL_BOOL) result = val.as.boolean ? 1 : 0;
    else if (val.kind == ROW_VAL_NULL) result = 0;
    else result = 1; /* treat other literals as true */
    return result;
}

/* Main executor for SELECT plans */
exec_result_t sql_execute(const sql_plan_t* plan, const table_t* input_table) {
    exec_result_t res;
    memset(&res, 0, sizeof(res));

    if (!plan || plan->kind != SQL_STMT_SELECT) {
        res.status = 1;
        res.error = "unsupported plan type";
        return res;
    }

    const sql_select_plan_t* sel = &plan->as.select;
    table_t* out_tbl = &res.output;

    /* copy column schema: one output column per select_item */
    out_tbl->column_count = sel->select_item_count;
    if (out_tbl->column_count > 0) {
        out_tbl->columns = malloc(sizeof(table_column_t) * out_tbl->column_count);
        if (!out_tbl->columns) { res.status = 1; res.error = "malloc failure"; return res; }
        for (size_t i = 0; i < out_tbl->column_count; ++i) {
            sql_select_item_t* item = &sel->select_items[i];
            sql_text_ref_t name = item->has_alias ? item->alias : item->expr->as.text;
            out_tbl->columns[i].name = name;
            out_tbl->columns[i].type = ROW_VAL_NULL; /* will detect from first row */
        }
    }

    /* Filter input rows by WHERE */
    size_t matched_rows = 0;
    row_value_t** temp_rows = malloc(sizeof(row_value_t*) * input_table->row_count);
    if (!temp_rows) { res.status = 1; res.error = "malloc failure"; return res; }

    for (size_t r = 0; r < input_table->row_count; ++r) {
        row_value_t* row = input_table->rows[r];
        int ok = eval_where(sel->where_expr, input_table, row);
        if (ok == 1) temp_rows[matched_rows++] = row;
        else if (ok < 0) { res.status = 1; res.error = "error evaluating WHERE"; free(temp_rows); return res; }
    }

    /* Produce output rows (evaluate select expressions) */
    out_tbl->row_count = matched_rows;
    if (matched_rows > 0) {
        out_tbl->rows = malloc(sizeof(row_value_t*) * matched_rows);
        if (!out_tbl->rows) { free(temp_rows); res.status = 1; res.error = "malloc failure"; return res; }

        for (size_t r = 0; r < matched_rows; ++r) {
            row_value_t* in_row = temp_rows[r];
            row_value_t* out_row = malloc(sizeof(row_value_t) * out_tbl->column_count);
            if (!out_row) { free(temp_rows); res.status = 1; res.error = "malloc failure"; return res; }

            for (size_t c = 0; c < out_tbl->column_count; ++c) {
                sql_select_item_t* item = &sel->select_items[c];
                row_value_t val;
                if (eval_expr(item->expr, input_table, in_row, &val) != 0) {
                    free_row(out_row, c);
                    free(out_row);
                    free(temp_rows);
                    res.status = 1;
                    res.error = "error evaluating SELECT expression";
                    return res;
                }
                out_row[c] = row_value_copy(val);
            }
            out_tbl->rows[r] = out_row;
        }
    }

    free(temp_rows);
    return res;
}