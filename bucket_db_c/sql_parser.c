#include "sql_parser.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdalign.h>
#include "sql_tokenizer.h"

typedef struct sql_plan_blob {
    size_t capacity;
    size_t used;
    unsigned char data[];
} sql_plan_blob_t;

typedef struct parser {
    const tokenization_view_t* tv;
    size_t pos;

    const uint8_t* source;   /* planner-owned copy */
    size_t source_length;

    const sql_arg_t* args;
    size_t arg_count;

    sql_plan_blob_t* blob;

    sql_expr_t* expr_pool;
    size_t expr_pool_cap;
    size_t expr_pool_used;

    sql_expr_t** ptr_pool;
    size_t ptr_pool_cap;
    size_t ptr_pool_used;

    sql_select_item_t* select_items_pool;
    size_t select_items_cap;
    size_t select_items_used;

    sql_order_item_t* order_pool;
    size_t order_pool_cap;
    size_t order_pool_used;

    sql_plan_result_t* out;
} parser_t;

static size_t align_up(size_t v, size_t a) {
    return (v + (a - 1)) & ~(a - 1);
}

static void* arena_alloc(sql_plan_blob_t* blob, size_t* used, size_t size, size_t align) {
    size_t off = align_up(*used, align);
    if (off > blob->capacity || size > blob->capacity - off) return NULL;
    void* p = blob->data + off;
    *used = off + size;
    return p;
}

static int add_error(sql_plan_result_t* out, int status, const char* msg) {
    out->status = status;
    out->error = msg;
    out->error_length = strlen(msg);
    return 0;
}

static const token_t* peek(parser_t* p) {
    if (p->pos >= p->tv->token_count) return NULL;
    return &p->tv->tokens[p->pos];
}

static const token_t* peek_n(parser_t* p, size_t n) {
    size_t idx = p->pos + n;
    if (idx >= p->tv->token_count) return NULL;
    return &p->tv->tokens[idx];
}

static int at_end(parser_t* p) {
    return p->pos >= p->tv->token_count;
}

static int match_symbol(parser_t* p, symbol_t sym) {
    const token_t* t = peek(p);
    if (!t || t->kind != TOKEN_SYMBOL || t->as.symbol != sym) return 0;
    ++p->pos;
    return 1;
}

static int match_keyword(parser_t* p, keyword_t kw) {
    const token_t* t = peek(p);
    if (!t || t->kind != TOKEN_KEYWORD || t->as.keyword != kw) return 0;
    ++p->pos;
    return 1;
}

static int expect_symbol(parser_t* p, symbol_t sym, sql_plan_result_t* out, const char* msg) {
    if (match_symbol(p, sym)) return 1;
    return add_error(out, 2, msg);
}

static int expect_keyword(parser_t* p, keyword_t kw, sql_plan_result_t* out, const char* msg) {
    if (match_keyword(p, kw)) return 1;
    return add_error(out, 2, msg);
}

static int is_ident_like(const token_t* t) {
    return t && (t->kind == TOKEN_IDENTIFIER || t->kind == TOKEN_QUOTED_IDENTIFIER);
}

static sql_text_ref_t make_source_ref(parser_t* p, const token_t* start_tok, const token_t* end_tok, int quoted) {
    sql_text_ref_t r;
    r.ptr = p->source + start_tok->start;
    r.len = (uint32_t)((end_tok->start + end_tok->length) - start_tok->start);
    r.quoted = (uint8_t)quoted;
    return r;
}

static sql_text_ref_t make_token_ref(parser_t* p, const token_t* t, int quoted) {
    sql_text_ref_t r;
    r.ptr = p->source + t->start;
    r.len = t->length;
    r.quoted = (uint8_t)quoted;
    return r;
}

static const sql_arg_t* find_arg(const parser_t* p, sql_text_ref_t name) {
    for (size_t i = 0; i < p->arg_count; ++i) {
        const sql_arg_t* a = &p->args[i];
        if (a->name_len == name.len && memcmp(a->name, name.ptr, name.len) == 0) {
            return a;
        }
    }
    return NULL;
}

static sql_expr_t* new_expr(parser_t* p, sql_expr_kind_t kind) {
    if (p->expr_pool_used >= p->expr_pool_cap) return NULL;
    sql_expr_t* e = &p->expr_pool[p->expr_pool_used++];
    memset(e, 0, sizeof(*e));
    e->kind = kind;
    return e;
}

static sql_expr_t** alloc_expr_ptr_slice(parser_t* p, size_t n) {
    if (p->ptr_pool_used + n > p->ptr_pool_cap) return NULL;
    sql_expr_t** s = &p->ptr_pool[p->ptr_pool_used];
    p->ptr_pool_used += n;
    return s;
}

static sql_select_item_t* new_select_item(parser_t* p) {
    if (p->select_items_used >= p->select_items_cap) return NULL;
    sql_select_item_t* s = &p->select_items_pool[p->select_items_used++];
    memset(s, 0, sizeof(*s));
    return s;
}

static sql_order_item_t* new_order_item(parser_t* p) {
    if (p->order_pool_used >= p->order_pool_cap) return NULL;
    sql_order_item_t* s = &p->order_pool[p->order_pool_used++];
    memset(s, 0, sizeof(*s));
    return s;
}

static int token_is_number(const token_t* t) {
    return t && (t->kind == TOKEN_INT || t->kind == TOKEN_FLOAT);
}

static sql_expr_t* parse_expression(parser_t* p, sql_plan_result_t* out);

static sql_text_ref_t parse_name_ref(parser_t* p, sql_plan_result_t* out) {
    const token_t* first = peek(p);
    if (!is_ident_like(first)) {
        add_error(out, 2, "expected identifier");
        sql_text_ref_t z = {0};
        return z;
    }

    const token_t* last = first;
    int quoted = (first->kind == TOKEN_QUOTED_IDENTIFIER);

    ++p->pos;

    while (match_symbol(p, SYMBOL_DOT)) {
        const token_t* part = peek(p);
        if (!is_ident_like(part)) {
            add_error(out, 2, "expected identifier after '.'");
            sql_text_ref_t z = {0};
            return z;
        }
        quoted |= (part->kind == TOKEN_QUOTED_IDENTIFIER);
        last = part;
        ++p->pos;
    }

    return make_source_ref(p, first, last, quoted);
}

static sql_expr_t* parse_primary(parser_t* p, sql_plan_result_t* out);

static sql_expr_t* parse_function_call(parser_t* p, sql_plan_result_t* out, sql_text_ref_t name, uint32_t start_pos, int quoted_name) {
    if (!expect_symbol(p, SYMBOL_LPAREN, out, "expected '(' after function name")) return NULL;

    sql_expr_t** args = NULL;
    size_t arg_count = 0;

    if (!match_symbol(p, SYMBOL_RPAREN)) {
        size_t slice_start = p->ptr_pool_used;

        do {
            sql_expr_t* a = parse_expression(p, out);
            if (!a) return NULL;
            sql_expr_t** slot = alloc_expr_ptr_slice(p, 1);
            if (!slot) return NULL;
            *slot = a;
            ++arg_count;
        } while (match_symbol(p, SYMBOL_COMMA));

        if (!expect_symbol(p, SYMBOL_RPAREN, out, "expected ')' after function arguments")) return NULL;

        args = &p->ptr_pool[slice_start];
    }

    sql_expr_t* e = new_expr(p, SQL_EXPR_CALL);
    if (!e) {
        add_error(out, 1, "expression arena exhausted");
        return NULL;
    }
    e->start = start_pos;
    e->length = (uint32_t)((peek_n(p, 0) ? peek_n(p, 0)->start : p->tv->source_length) - start_pos);
    e->as.call.name = name;
    e->as.call.args = args;
    e->as.call.arg_count = arg_count;
    e->as.call.has_window = 0;

    (void)quoted_name;

    if (match_keyword(p, KEYWORD_OVER)) {
        if (!expect_symbol(p, SYMBOL_LPAREN, out, "expected '(' after OVER")) return NULL;

        sql_window_spec_t w;
        memset(&w, 0, sizeof(w));

        if (match_keyword(p, KEYWORD_PARTITION)) {
            if (!expect_keyword(p, KEYWORD_BY, out, "expected BY after PARTITION")) return NULL;
            size_t start = p->ptr_pool_used;
            do {
                sql_expr_t* part = parse_expression(p, out);
                if (!part) return NULL;
                sql_expr_t** slot = alloc_expr_ptr_slice(p, 1);
                if (!slot) return NULL;
                *slot = part;
                ++w.partition_by_count;
            } while (match_symbol(p, SYMBOL_COMMA));
            w.partition_by = &p->ptr_pool[start];
        }

        if (match_keyword(p, KEYWORD_ORDER)) {
            if (!expect_keyword(p, KEYWORD_BY, out, "expected BY after ORDER")) return NULL;
            size_t start = p->order_pool_used;
            do {
                sql_expr_t* ord_expr = parse_expression(p, out);
                if (!ord_expr) return NULL;
                sql_order_item_t* oi = new_order_item(p);
                if (!oi) {
                    add_error(out, 1, "order-item arena exhausted");
                    return NULL;
                }
                oi->expr = ord_expr;
                oi->descending = 0;
                if (match_keyword(p, KEYWORD_DESC)) oi->descending = 1;
                else (void)match_keyword(p, KEYWORD_ASC);
                ++w.order_by_count;
            } while (match_symbol(p, SYMBOL_COMMA));
            w.order_by = &p->order_pool[start];
        }

        if (!expect_symbol(p, SYMBOL_RPAREN, out, "expected ')' after window specification")) return NULL;
        e->as.call.has_window = 1;
        e->as.call.window = w;
    }

    return e;
}

static sql_expr_t* parse_name_or_call(parser_t* p, sql_plan_result_t* out) {
    const token_t* first = peek(p);
    if (!is_ident_like(first)) {
        add_error(out, 2, "expected name");
        return NULL;
    }

    uint32_t start_pos = first->start;
    sql_text_ref_t name = parse_name_ref(p, out);
    if (out->status != 0) return NULL;

    if (match_symbol(p, SYMBOL_LPAREN)) {
        p->pos--; /* let parse_function_call consume '(' */
        return parse_function_call(p, out, name, start_pos, first->kind == TOKEN_QUOTED_IDENTIFIER);
    }

    sql_expr_t* e = new_expr(p, SQL_EXPR_NAME);
    if (!e) {
        add_error(out, 1, "expression arena exhausted");
        return NULL;
    }
    e->start = start_pos;
    e->length = name.len;
    e->as.text = name;
    return e;
}

static sql_expr_t* parse_primary(parser_t* p, sql_plan_result_t* out) {
    const token_t* t = peek(p);
    if (!t) {
        add_error(out, 2, "unexpected end of input");
        return NULL;
    }

    if (t->kind == TOKEN_SYMBOL && t->as.symbol == SYMBOL_STAR) {
        ++p->pos;
        sql_expr_t* e = new_expr(p, SQL_EXPR_STAR);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        return e;
    }

    if (t->kind == TOKEN_INT) {
        ++p->pos;
        sql_expr_t* e = new_expr(p, SQL_EXPR_INT);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.int_value = t->as.int_value;
        return e;
    }

    if (t->kind == TOKEN_FLOAT) {
        ++p->pos;
        sql_expr_t* e = new_expr(p, SQL_EXPR_FLOAT);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.float_value = t->as.float_value;
        return e;
    }

    if (t->kind == TOKEN_STRING) {
        ++p->pos;
        sql_expr_t* e = new_expr(p, SQL_EXPR_STRING);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.text = make_token_ref(p, t, 1);
        return e;
    }

    if (t->kind == TOKEN_KEYWORD) {
        if (t->as.keyword == KEYWORD_NULL) {
            ++p->pos;
            sql_expr_t* e = new_expr(p, SQL_EXPR_NULL);
            if (!e) {
                add_error(out, 1, "expression arena exhausted");
                return NULL;
            }
            e->start = t->start;
            e->length = t->length;
            return e;
        }
        if (t->as.keyword == KEYWORD_TRUE || t->as.keyword == KEYWORD_FALSE) {
            ++p->pos;
            sql_expr_t* e = new_expr(p, SQL_EXPR_BOOL);
            if (!e) {
                add_error(out, 1, "expression arena exhausted");
                return NULL;
            }
            e->start = t->start;
            e->length = t->length;
            e->as.bool_value = (t->as.keyword == KEYWORD_TRUE);
            return e;
        }
        add_error(out, 2, "unexpected keyword in expression");
        return NULL;
    }

    if (t->kind == TOKEN_PLACEHOLDER) {
        /* name is everything after '@' */
        sql_text_ref_t ph;
        ph.ptr = p->source + t->start + 1;
        ph.len = t->length - 1;
        ph.quoted = 0;

        const sql_arg_t* a = find_arg(p, ph);
        if (!a) {
            add_error(out, 2, "unknown placeholder");
            return NULL;
        }

        ++p->pos;

        switch (a->kind) {
            case SQL_ARG_NULL: {
                sql_expr_t* e = new_expr(p, SQL_EXPR_NULL);
                if (!e) { add_error(out, 1, "expression arena exhausted"); return NULL; }
                e->start = t->start;
                e->length = t->length;
                return e;
            }
            case SQL_ARG_BOOL: {
                sql_expr_t* e = new_expr(p, SQL_EXPR_BOOL);
                if (!e) { add_error(out, 1, "expression arena exhausted"); return NULL; }
                e->start = t->start;
                e->length = t->length;
                e->as.bool_value = a->as.boolean ? 1 : 0;
                return e;
            }
            case SQL_ARG_INT: {
                sql_expr_t* e = new_expr(p, SQL_EXPR_INT);
                if (!e) { add_error(out, 1, "expression arena exhausted"); return NULL; }
                e->start = t->start;
                e->length = t->length;
                e->as.int_value = a->as.i64;
                return e;
            }
            case SQL_ARG_FLOAT: {
                sql_expr_t* e = new_expr(p, SQL_EXPR_FLOAT);
                if (!e) { add_error(out, 1, "expression arena exhausted"); return NULL; }
                e->start = t->start;
                e->length = t->length;
                e->as.float_value = a->as.f64;
                return e;
            }
            case SQL_ARG_TEXT: {
                if (a->as.text.len > UINT32_MAX) {
                    add_error(out, 1, "argument text too large");
                    return NULL;
                }
                uint8_t* dst = arena_alloc(p->blob, &p->blob->used, a->as.text.len + 1, 1);
                if (!dst) {
                    add_error(out, 1, "arena exhausted");
                    return NULL;
                }
                memcpy(dst, a->as.text.bytes, a->as.text.len);
                dst[a->as.text.len] = '\0';

                sql_expr_t* e = new_expr(p, SQL_EXPR_STRING);
                if (!e) { add_error(out, 1, "expression arena exhausted"); return NULL; }
                e->start = t->start;
                e->length = t->length;
                e->as.text.ptr = dst;
                e->as.text.len = (uint32_t)a->as.text.len;
                e->as.text.quoted = 0;
                return e;
            }
        }
    }

    if (t->kind == TOKEN_IDENTIFIER || t->kind == TOKEN_QUOTED_IDENTIFIER) {
        return parse_name_or_call(p, out);
    }

    if (match_symbol(p, SYMBOL_LPAREN)) {
        sql_expr_t* e = parse_expression(p, out);
        if (!e) return NULL;
        if (!expect_symbol(p, SYMBOL_RPAREN, out, "expected ')'")) return NULL;
        return e;
    }

    add_error(out, 2, "expected expression");
    return NULL;
}

static sql_expr_t* parse_unary(parser_t* p, sql_plan_result_t* out) {
    const token_t* t = peek(p);
    if (t && t->kind == TOKEN_KEYWORD && t->as.keyword == KEYWORD_NOT) {
        ++p->pos;
        sql_expr_t* child = parse_unary(p, out);
        if (!child) return NULL;
        sql_expr_t* e = new_expr(p, SQL_EXPR_UNARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.unary.op = SQL_UNARY_NOT;
        e->as.unary.child = child;
        return e;
    }

    if (t && t->kind == TOKEN_SYMBOL && t->as.symbol == SYMBOL_PLUS) {
        ++p->pos;
        sql_expr_t* child = parse_unary(p, out);
        if (!child) return NULL;
        sql_expr_t* e = new_expr(p, SQL_EXPR_UNARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.unary.op = SQL_UNARY_POSITIVE;
        e->as.unary.child = child;
        return e;
    }

    if (t && t->kind == TOKEN_SYMBOL && t->as.symbol == SYMBOL_MINUS) {
        ++p->pos;
        sql_expr_t* child = parse_unary(p, out);
        if (!child) return NULL;
        sql_expr_t* e = new_expr(p, SQL_EXPR_UNARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = t->start;
        e->length = t->length;
        e->as.unary.op = SQL_UNARY_NEGATIVE;
        e->as.unary.child = child;
        return e;
    }

    return parse_primary(p, out);
}

static sql_expr_t* parse_mul(parser_t* p, sql_plan_result_t* out) {
    sql_expr_t* left = parse_unary(p, out);
    if (!left) return NULL;

    for (;;) {
        const token_t* t = peek(p);
        sql_binary_op_t op;
        if (t && t->kind == TOKEN_SYMBOL) {
            if (t->as.symbol == SYMBOL_STAR) op = SQL_BINARY_MUL;
            else if (t->as.symbol == SYMBOL_SLASH) op = SQL_BINARY_DIV;
            else if (t->as.symbol == SYMBOL_PERCENT) op = SQL_BINARY_MOD;
            else break;
        } else {
            break;
        }

        ++p->pos;
        sql_expr_t* right = parse_unary(p, out);
        if (!right) return NULL;

        sql_expr_t* e = new_expr(p, SQL_EXPR_BINARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = left->start;
        e->length = right->start + right->length - left->start;
        e->as.binary.op = op;
        e->as.binary.left = left;
        e->as.binary.right = right;
        left = e;
    }

    return left;
}

static sql_expr_t* parse_add(parser_t* p, sql_plan_result_t* out) {
    sql_expr_t* left = parse_mul(p, out);
    if (!left) return NULL;

    for (;;) {
        const token_t* t = peek(p);
        sql_binary_op_t op;
        if (t && t->kind == TOKEN_SYMBOL) {
            if (t->as.symbol == SYMBOL_PLUS) op = SQL_BINARY_ADD;
            else if (t->as.symbol == SYMBOL_MINUS) op = SQL_BINARY_SUB;
            else break;
        } else {
            break;
        }

        ++p->pos;
        sql_expr_t* right = parse_mul(p, out);
        if (!right) return NULL;

        sql_expr_t* e = new_expr(p, SQL_EXPR_BINARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = left->start;
        e->length = right->start + right->length - left->start;
        e->as.binary.op = op;
        e->as.binary.left = left;
        e->as.binary.right = right;
        left = e;
    }

    return left;
}

static int is_compare_symbol(symbol_t s) {
    return s == SYMBOL_EQ || s == SYMBOL_NEQ || s == SYMBOL_LT ||
           s == SYMBOL_LTE || s == SYMBOL_GT || s == SYMBOL_GTE;
}

static sql_binary_op_t compare_op_from_symbol(symbol_t s) {
    switch (s) {
        case SYMBOL_EQ: return SQL_BINARY_EQ;
        case SYMBOL_NEQ: return SQL_BINARY_NEQ;
        case SYMBOL_LT: return SQL_BINARY_LT;
        case SYMBOL_LTE: return SQL_BINARY_LTE;
        case SYMBOL_GT: return SQL_BINARY_GT;
        case SYMBOL_GTE: return SQL_BINARY_GTE;
        default: return SQL_BINARY_EQ;
    }
}

static sql_expr_t* parse_compare(parser_t* p, sql_plan_result_t* out) {
    sql_expr_t* left = parse_add(p, out);
    if (!left) return NULL;

    for (;;) {
        const token_t* t = peek(p);
        sql_binary_op_t op;
        if (t && t->kind == TOKEN_SYMBOL && is_compare_symbol(t->as.symbol)) {
            op = compare_op_from_symbol(t->as.symbol);
            ++p->pos;
        } else if (t && t->kind == TOKEN_KEYWORD && t->as.keyword == KEYWORD_LIKE) {
            op = SQL_BINARY_LIKE;
            ++p->pos;
        } else {
            break;
        }

        sql_expr_t* right = parse_add(p, out);
        if (!right) return NULL;

        sql_expr_t* e = new_expr(p, SQL_EXPR_BINARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = left->start;
        e->length = right->start + right->length - left->start;
        e->as.binary.op = op;
        e->as.binary.left = left;
        e->as.binary.right = right;
        left = e;
    }

    return left;
}

static sql_expr_t* parse_and(parser_t* p, sql_plan_result_t* out) {
    sql_expr_t* left = parse_compare(p, out);
    if (!left) return NULL;

    while (match_keyword(p, KEYWORD_AND)) {
        sql_expr_t* right = parse_compare(p, out);
        if (!right) return NULL;

        sql_expr_t* e = new_expr(p, SQL_EXPR_BINARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = left->start;
        e->length = right->start + right->length - left->start;
        e->as.binary.op = SQL_BINARY_AND;
        e->as.binary.left = left;
        e->as.binary.right = right;
        left = e;
    }

    return left;
}

static sql_expr_t* parse_or(parser_t* p, sql_plan_result_t* out) {
    sql_expr_t* left = parse_and(p, out);
    if (!left) return NULL;

    while (match_keyword(p, KEYWORD_OR)) {
        sql_expr_t* right = parse_and(p, out);
        if (!right) return NULL;

        sql_expr_t* e = new_expr(p, SQL_EXPR_BINARY);
        if (!e) {
            add_error(out, 1, "expression arena exhausted");
            return NULL;
        }
        e->start = left->start;
        e->length = right->start + right->length - left->start;
        e->as.binary.op = SQL_BINARY_OR;
        e->as.binary.left = left;
        e->as.binary.right = right;
        left = e;
    }

    return left;
}

static sql_expr_t* parse_expression(parser_t* p, sql_plan_result_t* out) {
    return parse_or(p, out);
}

static int parse_number_like_int(parser_t* p, sql_plan_result_t* out, int64_t* out_value) {
    const token_t* t = peek(p);
    if (!t) {
        add_error(out, 2, "expected numeric literal");
        return 0;
    }

    if (t->kind == TOKEN_INT) {
        *out_value = t->as.int_value;
        ++p->pos;
        return 1;
    }

    if (t->kind == TOKEN_PLACEHOLDER) {
        sql_text_ref_t ph;
        ph.ptr = p->source + t->start + 1;
        ph.len = t->length - 1;
        ph.quoted = 0;

        const sql_arg_t* a = find_arg(p, ph);
        if (!a) {
            add_error(out, 2, "unknown placeholder");
            return 0;
        }
        if (a->kind != SQL_ARG_INT) {
            add_error(out, 2, "placeholder is not an integer");
            return 0;
        }
        *out_value = a->as.i64;
        ++p->pos;
        return 1;
    }

    add_error(out, 2, "expected integer literal");
    return 0;
}

static int parse_select_item(parser_t* p, sql_plan_result_t* out, sql_select_item_t* item) {
    sql_expr_t* expr = parse_expression(p, out);
    if (!expr) return 0;

    item->expr = expr;
    item->has_alias = 0;

    if (match_keyword(p, KEYWORD_AS)) {
        const token_t* t = peek(p);
        if (!is_ident_like(t)) {
            add_error(out, 2, "expected alias after AS");
            return 0;
        }
        item->alias = make_token_ref(p, t, t->kind == TOKEN_QUOTED_IDENTIFIER);
        item->has_alias = 1;
        ++p->pos;
    }

    return 1;
}

static int parse_select_statement(parser_t* p, sql_plan_result_t* out) {
    if (!expect_keyword(p, KEYWORD_SELECT, out, "expected SELECT")) return 0;

    out->plan.kind = SQL_STMT_SELECT;
    sql_select_plan_t* plan = &out->plan.as.select;
    memset(plan, 0, sizeof(*plan));
    plan->source = p->source;
    plan->source_length = p->source_length;

    if (match_keyword(p, KEYWORD_DISTINCT)) plan->distinct = 1;

    /* select list */
    do {
        sql_select_item_t* item = new_select_item(p);
        if (!item) {
            add_error(out, 1, "select-item arena exhausted");
            return 0;
        }
        if (!parse_select_item(p, out, item)) return 0;
    } while (match_symbol(p, SYMBOL_COMMA));

    plan->select_items = p->select_items_pool;
    plan->select_item_count = p->select_items_used;

    if (!expect_keyword(p, KEYWORD_FROM, out, "expected FROM")) return 0;

    /* table name */
    {
        const token_t* t = peek(p);
        if (!is_ident_like(t)) {
            add_error(out, 2, "expected table name");
            return 0;
        }
        plan->from_table = parse_name_ref(p, out);
        if (out->status != 0) return 0;
    }

    /* optional clauses */
    while (!at_end(p)) {
        if (match_symbol(p, SYMBOL_SEMICOLON)) break;

        if (match_keyword(p, KEYWORD_WHERE)) {
            plan->where_expr = parse_expression(p, out);
            if (!plan->where_expr) return 0;
            continue;
        }

        if (match_keyword(p, KEYWORD_GROUP)) {
            if (!expect_keyword(p, KEYWORD_BY, out, "expected BY after GROUP")) return 0;
            size_t start = p->ptr_pool_used;
            size_t count = 0;

            do {
                sql_expr_t* e = parse_expression(p, out);
                if (!e) return 0;
                sql_expr_t** slot = alloc_expr_ptr_slice(p, 1);
                if (!slot) {
                    add_error(out, 1, "expression-list arena exhausted");
                    return 0;
                }
                *slot = e;
                ++count;
            } while (match_symbol(p, SYMBOL_COMMA));

            plan->group_by = &p->ptr_pool[start];
            plan->group_by_count = count;
            continue;
        }

        if (match_keyword(p, KEYWORD_HAVING)) {
            plan->having_expr = parse_expression(p, out);
            if (!plan->having_expr) return 0;
            continue;
        }

        if (match_keyword(p, KEYWORD_ORDER)) {
            if (!expect_keyword(p, KEYWORD_BY, out, "expected BY after ORDER")) return 0;

            size_t start = p->order_pool_used;
            size_t count = 0;

            do {
                sql_expr_t* e = parse_expression(p, out);
                if (!e) return 0;
                sql_order_item_t* oi = new_order_item(p);
                if (!oi) {
                    add_error(out, 1, "order-item arena exhausted");
                    return 0;
                }
                oi->expr = e;
                oi->descending = 0;

                if (match_keyword(p, KEYWORD_DESC)) oi->descending = 1;
                else (void)match_keyword(p, KEYWORD_ASC);

                ++count;
            } while (match_symbol(p, SYMBOL_COMMA));

            plan->order_by = &p->order_pool[start];
            plan->order_by_count = count;
            continue;
        }

        if (match_keyword(p, KEYWORD_LIMIT)) {
            int64_t v = 0;
            if (!parse_number_like_int(p, out, &v)) return 0;
            plan->has_limit = 1;
            plan->limit = v;

            if (match_keyword(p, KEYWORD_OFFSET)) {
                int64_t off = 0;
                if (!parse_number_like_int(p, out, &off)) return 0;
                plan->offset = off;
            }
            continue;
        }

        if (match_keyword(p, KEYWORD_OFFSET)) {
            int64_t off = 0;
            if (!parse_number_like_int(p, out, &off)) return 0;
            plan->offset = off;
            continue;
        }

        break;
    }

    /* end-of-statement check */
    if (!at_end(p)) {
        const token_t* t = peek(p);
        if (!(t->kind == TOKEN_SYMBOL && t->as.symbol == SYMBOL_SEMICOLON)) {
            add_error(out, 2, "unexpected trailing tokens");
            return 0;
        }
        ++p->pos;
        while (p->pos < p->tv->token_count && p->tv->tokens[p->pos].kind == TOKEN_SYMBOL &&
               p->tv->tokens[p->pos].as.symbol == SYMBOL_SEMICOLON) {
            ++p->pos;
        }
    }

    return 1;
}

static size_t sum_text_arg_bytes(const sql_arg_t* args, size_t arg_count) {
    size_t total = 0;
    for (size_t i = 0; i < arg_count; ++i) {
        if (args[i].kind == SQL_ARG_TEXT) {
            total += args[i].as.text.len + 1;
        }
    }
    return total;
}

static int init_parser(parser_t* p, sql_plan_result_t* out, const tokenization_view_t* view,
                       const sql_arg_t* args, size_t arg_count) {
    memset(p, 0, sizeof(*p));
    memset(out, 0, sizeof(*out));

    if (!view) {
        add_error(out, 1, "tokenization view is NULL");
        return 0;
    }

    if (view->source_length > UINT32_MAX) {
        add_error(out, 1, "source too large");
        return 0;
    }

    p->tv = view;
    p->args = args;
    p->arg_count = arg_count;
    p->out = out;
    p->source_length = view->source_length;

    size_t tok = view->token_count;
    size_t expr_cap = tok * 4 + 64;
    size_t ptr_cap = tok * 8 + 128;
    size_t select_item_cap = tok + 8;
    size_t order_cap = tok + 8;

    size_t arg_text_bytes = sum_text_arg_bytes(args, arg_count);

    size_t arena_bytes =
        align_up(view->source_length + 1, alignof(max_align_t)) +
        align_up(expr_cap * sizeof(sql_expr_t), alignof(sql_expr_t)) +
        align_up(ptr_cap * sizeof(sql_expr_t*), alignof(sql_expr_t*)) +
        align_up(select_item_cap * sizeof(sql_select_item_t), alignof(sql_select_item_t)) +
        align_up(order_cap * sizeof(sql_order_item_t), alignof(sql_order_item_t)) +
        align_up(arg_text_bytes, 1);

    size_t total = sizeof(sql_plan_blob_t) + arena_bytes;
    sql_plan_blob_t* blob = (sql_plan_blob_t*)malloc(total);
    if (!blob) {
        add_error(out, 1, "out of memory");
        return 0;
    }
    blob->capacity = arena_bytes;
    blob->used = 0;
    p->blob = blob;
    out->_owner = blob;

    size_t used = 0;

    uint8_t* source_copy = (uint8_t*)arena_alloc(blob, &used, view->source_length + 1, 1);
    if (!source_copy) {
        free(blob);
        out->_owner = NULL;
        add_error(out, 1, "arena exhausted");
        return 0;
    }
    memcpy(source_copy, view->source, view->source_length);
    source_copy[view->source_length] = '\0';
    p->source = source_copy;

    p->expr_pool = (sql_expr_t*)arena_alloc(blob, &used, expr_cap * sizeof(sql_expr_t), alignof(sql_expr_t));
    p->ptr_pool = (sql_expr_t**)arena_alloc(blob, &used, ptr_cap * sizeof(sql_expr_t*), alignof(sql_expr_t*));
    p->select_items_pool = (sql_select_item_t*)arena_alloc(blob, &used, select_item_cap * sizeof(sql_select_item_t), alignof(sql_select_item_t));
    p->order_pool = (sql_order_item_t*)arena_alloc(blob, &used, order_cap * sizeof(sql_order_item_t), alignof(sql_order_item_t));

    if (!p->expr_pool || !p->ptr_pool || !p->select_items_pool || !p->order_pool) {
        free(blob);
        out->_owner = NULL;
        add_error(out, 1, "arena exhausted");
        return 0;
    }

    p->expr_pool_cap = expr_cap;
    p->ptr_pool_cap = ptr_cap;
    p->select_items_cap = select_item_cap;
    p->order_pool_cap = order_cap;
    blob->used = used;

    return 1;
}

void sql_plan_result_free(sql_plan_result_t* result) {
    if (!result) return;
    free(result->_owner);
    memset(result, 0, sizeof(*result));
}

sql_plan_result_t sql_plan_query(const tokenization_view_t* view,
                                 const sql_arg_t* args,
                                 size_t arg_count) {
    sql_plan_result_t out;
    memset(&out, 0, sizeof(out));

    parser_t p;
    if (!init_parser(&p, &out, view, args, arg_count)) {
        return out;
    }

    if (!parse_select_statement(&p, &out)) {
        sql_plan_result_free(&out);
        return out;
    }

    out.status = 0;
    out.error = NULL;
    out.error_length = 0;
    return out;
}

sql_plan_result_t sql_pipeline_plan(const uint8_t* query,
                                    const sql_arg_t* args,
                                    size_t arg_count) {
    tokenization_result_t tr = tokenize_sql(query);
    if (tr.status != 0) {
        sql_plan_result_t out;
        memset(&out, 0, sizeof(out));
        out.status = tr.status;
        out.error = tr.error;
        out.error_length = tr.error_length;
        tokenization_result_free(&tr);
        return out;
    }

    sql_plan_result_t pr = sql_plan_query(&tr.view, args, arg_count);
    tokenization_result_free(&tr);
    return pr;
}