#include "sql_tokenizer.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

typedef struct tokenization_blob {
    size_t source_length;
    size_t token_capacity;
    max_align_t _align; /* ensures data[] is suitably aligned */
    unsigned char data[];
} tokenization_blob_t;

static inline int is_ascii_space(uint8_t c) {
    return c == ' '  || c == '\t' || c == '\n' ||
           c == '\r' || c == '\f' || c == '\v';
}

static inline int is_ascii_digit(uint8_t c) {
    return c >= '0' && c <= '9';
}

static inline int is_ascii_alpha(uint8_t c) {
    c |= 0x20;
    return c >= 'a' && c <= 'z';
}

static inline int is_ident_start(uint8_t c) {
    return is_ascii_alpha(c) || c == '_' || c >= 0x80;
}

static inline int is_ident_continue(uint8_t c) {
    return is_ident_start(c) || is_ascii_digit(c);
}

static inline uint8_t ascii_upper(uint8_t c) {
    if (c >= 'a' && c <= 'z') return (uint8_t)(c - ('a' - 'A'));
    return c;
}

static int ascii_ieq_n(const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (ascii_upper(a[i]) != ascii_upper(b[i])) return 0;
    }
    return 1;
}

static keyword_t lookup_keyword(const uint8_t* s, size_t n) {
#define KW(name) \
    if (n == sizeof(#name) - 1 && ascii_ieq_n(s, (const uint8_t*)#name, sizeof(#name) - 1)) \
        return KEYWORD_##name;

    SQL_KEYWORD_LIST(KW)

#undef KW
    return KEYWORD__COUNT;
}

static int parse_int64_slice(const uint8_t* s, size_t n, int64_t* out) {
    if (n == 0 || n >= 128) return 0;

    char buf[128];
    memcpy(buf, s, n);
    buf[n] = '\0';

    errno = 0;
    char* endp = NULL;
    long long v = strtoll(buf, &endp, 10);
    if (errno != 0 || endp != buf + n) return 0;

    *out = (int64_t)v;
    return 1;
}

static int parse_float_slice(const uint8_t* s, size_t n, double* out) {
    if (n == 0 || n >= 256) return 0;

    char buf[256];
    memcpy(buf, s, n);
    buf[n] = '\0';

    errno = 0;
    char* endp = NULL;
    double v = strtod(buf, &endp);
    if (errno != 0 || endp != buf + n) return 0;

    *out = v;
    return 1;
}

static token_t* next_token(token_t* tokens, size_t* count) {
    return &tokens[(*count)++];
}

static int add_error(tokenization_result_t* r, int status, const char* msg) {
    r->status = status;
    r->error = msg;
    r->error_length = strlen(msg);
    r->view.source = NULL;
    r->view.source_length = 0;
    r->view.tokens = NULL;
    r->view.token_count = 0;
    return 0;
}

void tokenization_result_free(tokenization_result_t* result) {
    if (!result) return;
    free(result->_owner);
    result->_owner = NULL;
    result->view.source = NULL;
    result->view.source_length = 0;
    result->view.tokens = NULL;
    result->view.token_count = 0;
    result->status = 0;
    result->error = NULL;
    result->error_length = 0;
}

tokenization_result_t tokenize_sql(const uint8_t* query) {
    tokenization_result_t r;
    memset(&r, 0, sizeof(r));

    if (!query) {
        add_error(&r, 1, "query is NULL");
        return r;
    }

    const size_t source_length = strlen((const char*)query);
    if (source_length > UINT32_MAX) {
        add_error(&r, 1, "query too large for 32-bit token offsets");
        return r;
    }

    /*
        One allocation total:
        - token array first
        - source copy after the token array
    */
    const size_t token_capacity = source_length + 1;

    if (token_capacity > (SIZE_MAX - sizeof(tokenization_blob_t) - source_length - 1) / sizeof(token_t)) {
        add_error(&r, 1, "size overflow");
        return r;
    }

    const size_t alloc_size =
        sizeof(tokenization_blob_t) +
        token_capacity * sizeof(token_t) +
        source_length + 1;

    tokenization_blob_t* blob = (tokenization_blob_t*)malloc(alloc_size);
    if (!blob) {
        add_error(&r, 1, "out of memory");
        return r;
    }

    blob->source_length = source_length;
    blob->token_capacity = token_capacity;

    token_t* tokens = (token_t*)blob->data;
    uint8_t* source = blob->data + token_capacity * sizeof(token_t);

    memcpy(source, query, source_length);
    source[source_length] = '\0';

    const uint8_t* p = source;
    const uint8_t* end = source + source_length;
    size_t token_count = 0;

    while (p < end) {
        while (p < end && is_ascii_space(*p)) {
            ++p;
        }
        if (p >= end) break;

        /* comments */
        if (p + 1 < end && p[0] == '-' && p[1] == '-') {
            p += 2;
            while (p < end && *p != '\n') ++p;
            continue;
        }
        if (p + 1 < end && p[0] == '/' && p[1] == '*') {
            p += 2;
            int closed = 0;
            while (p + 1 < end) {
                if (p[0] == '*' && p[1] == '/') {
                    p += 2;
                    closed = 1;
                    break;
                }
                ++p;
            }
            if (!closed) {
                free(blob);
                add_error(&r, 2, "unterminated block comment");
                return r;
            }
            continue;
        }

        if (token_count >= token_capacity) {
            free(blob);
            add_error(&r, 1, "token capacity exceeded");
            return r;
        }

        token_t* tok = next_token(tokens, &token_count);
        memset(tok, 0, sizeof(*tok));
        tok->start = (uint32_t)(p - source);

        /* string literal: '...''...' */
        if (*p == '\'') {
            ++p; /* consume opening quote */
            while (p < end) {
                if (*p == '\'') {
                    if (p + 1 < end && p[1] == '\'') {
                        p += 2; /* escaped quote */
                        continue;
                    }
                    break; /* closing quote */
                }
                ++p;
            }
            if (p >= end) {
                free(blob);
                add_error(&r, 2, "unterminated string literal");
                return r;
            }
            ++p; /* consume closing quote */
            tok->kind = TOKEN_STRING;
            tok->length = (uint32_t)(p - source - tok->start);
            continue;
        }

        /*
            Quoted identifiers:
            "name", `name`, [name]
            Token slice includes the delimiters so the exact lexeme is preserved.
        */
        if (*p == '"' || *p == '`' || *p == '[') {
            const uint8_t open = *p;
            const uint8_t close = (open == '[') ? ']' : open;
            ++p; /* consume open */

            while (p < end) {
                if (*p == close) {
                    if (open == '[' && p + 1 < end && p[1] == ']') {
                        p += 2; /* escaped ] inside [ ... ] */
                        continue;
                    }
                    if ((open == '"' || open == '`') && p + 1 < end && p[1] == close) {
                        p += 2; /* doubled quote/backtick escape */
                        continue;
                    }
                    break; /* closing delimiter */
                }
                ++p;
            }
            if (p >= end) {
                free(blob);
                add_error(&r, 2, "unterminated quoted identifier");
                return r;
            }
            ++p; /* consume close */
            tok->kind = TOKEN_QUOTED_IDENTIFIER;
            tok->length = (uint32_t)(p - source - tok->start);
            continue;
        }

        /* placeholders: @arg_name */
        if (*p == '@') {
            ++p; /* consume '@' */
            if (p >= end || !is_ident_start(*p)) {
                free(blob);
                add_error(&r, 3, "invalid placeholder");
                return r;
            }
            ++p;
            while (p < end && is_ident_continue(*p)) ++p;
            tok->kind = TOKEN_PLACEHOLDER;
            tok->length = (uint32_t)(p - source - tok->start);
            continue;
        }

        /* identifiers / keywords */
        if (is_ident_start(*p)) {
            ++p;
            while (p < end && is_ident_continue(*p)) ++p;

            const size_t len = (size_t)(p - source - tok->start);
            const keyword_t kw = lookup_keyword(source + tok->start, len);
            if (kw != KEYWORD__COUNT) {
                tok->kind = TOKEN_KEYWORD;
                tok->as.keyword = kw;
            } else {
                tok->kind = TOKEN_IDENTIFIER;
            }
            tok->length = (uint32_t)len;
            continue;
        }

        /* numbers: integer / float, including .5 and scientific notation */
        if (is_ascii_digit(*p) || (*p == '.' && p + 1 < end && is_ascii_digit(p[1]))) {
            const uint8_t* nstart = p;
            int is_float = 0;

            if (*p == '.') {
                is_float = 1;
                ++p;
                while (p < end && is_ascii_digit(*p)) ++p;
            } else {
                while (p < end && is_ascii_digit(*p)) ++p;
                if (p < end && *p == '.') {
                    is_float = 1;
                    ++p;
                    while (p < end && is_ascii_digit(*p)) ++p;
                }
            }

            if (p < end && (*p == 'e' || *p == 'E')) {
                const uint8_t* e = p + 1;
                if (e < end && (*e == '+' || *e == '-')) ++e;
                if (e < end && is_ascii_digit(*e)) {
                    is_float = 1;
                    p = e + 1;
                    while (p < end && is_ascii_digit(*p)) ++p;
                }
            }

            const size_t len = (size_t)(p - nstart);
            tok->length = (uint32_t)len;

            if (is_float) {
                tok->kind = TOKEN_FLOAT;
                if (!parse_float_slice(nstart, len, &tok->as.float_value)) {
                    free(blob);
                    add_error(&r, 4, "invalid floating-point literal");
                    return r;
                }
            } else {
                tok->kind = TOKEN_INT;
                if (!parse_int64_slice(nstart, len, &tok->as.int_value)) {
                    free(blob);
                    add_error(&r, 4, "invalid integer literal");
                    return r;
                }
            }
            continue;
        }

        /* operators / punctuation */
        if (p + 1 < end) {
            if (p[0] == '<' && p[1] == '=') {
                tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_LTE; tok->length = 2; p += 2; continue;
            }
            if (p[0] == '>' && p[1] == '=') {
                tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_GTE; tok->length = 2; p += 2; continue;
            }
            if ((p[0] == '<' && p[1] == '>') || (p[0] == '!' && p[1] == '=')) {
                tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_NEQ; tok->length = 2; p += 2; continue;
            }
            if (p[0] == '|' && p[1] == '|') {
                tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_CONCAT; tok->length = 2; p += 2; continue;
            }
            if (p[0] == ':' && p[1] == ':') {
                tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_CAST; tok->length = 2; p += 2; continue;
            }
        }

        switch (*p) {
            case ',': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_COMMA;     tok->length = 1; ++p; continue;
            case ';': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_SEMICOLON;  tok->length = 1; ++p; continue;
            case '.': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_DOT;        tok->length = 1; ++p; continue;
            case '(': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_LPAREN;     tok->length = 1; ++p; continue;
            case ')': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_RPAREN;     tok->length = 1; ++p; continue;
            case '*': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_STAR;       tok->length = 1; ++p; continue;
            case '+': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_PLUS;       tok->length = 1; ++p; continue;
            case '-': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_MINUS;      tok->length = 1; ++p; continue;
            case '/': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_SLASH;      tok->length = 1; ++p; continue;
            case '%': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_PERCENT;    tok->length = 1; ++p; continue;
            case '=': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_EQ;         tok->length = 1; ++p; continue;
            case '<': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_LT;         tok->length = 1; ++p; continue;
            case '>': tok->kind = TOKEN_SYMBOL; tok->as.symbol = SYMBOL_GT;         tok->length = 1; ++p; continue;
            default:
                free(blob);
                add_error(&r, 5, "unknown character");
                return r;
        }
    }

    r.status = 0;
    r.error = NULL;
    r.error_length = 0;
    r.view.source = source;
    r.view.source_length = source_length;
    r.view.tokens = tokens;
    r.view.token_count = token_count;
    r._owner = blob;
    return r;
}