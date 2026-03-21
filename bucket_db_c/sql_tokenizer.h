#ifndef SQL_TOKENIZER_H
#define SQL_TOKENIZER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
    Byte offsets are in the UTF-8 source buffer copy that tokenize_sql() owns.
    The tokenizer expects a NUL-terminated UTF-8 byte string.
*/

typedef enum token_kind {
    TOKEN_IDENTIFIER,
    TOKEN_QUOTED_IDENTIFIER,
    TOKEN_KEYWORD,
    TOKEN_STRING,
    TOKEN_INT,
    TOKEN_FLOAT,
    TOKEN_PLACEHOLDER,
    TOKEN_SYMBOL
} token_kind_t;

typedef enum symbol_kind {
    SYMBOL_COMMA,
    SYMBOL_SEMICOLON,
    SYMBOL_DOT,
    SYMBOL_LPAREN,
    SYMBOL_RPAREN,
    SYMBOL_STAR,
    SYMBOL_PLUS,
    SYMBOL_MINUS,
    SYMBOL_SLASH,
    SYMBOL_PERCENT,
    SYMBOL_EQ,
    SYMBOL_NEQ,      /* != or <> */
    SYMBOL_LT,
    SYMBOL_LTE,
    SYMBOL_GT,
    SYMBOL_GTE,
    SYMBOL_CONCAT,   /* || */
    SYMBOL_CAST      /* :: */
} symbol_t;

/*
    Broad SQL keyword table for CRUD / DDL / GROUP BY / window functions / transactions.
    Intentionally excludes FUNCTION / PROCEDURE / TRIGGER / INDEX tokens.
    Those will be returned as TOKEN_IDENTIFIER.
*/
#define SQL_KEYWORD_LIST(X) \
    X(SELECT) \
    X(INSERT) \
    X(UPDATE) \
    X(DELETE) \
    X(VALUES) \
    X(INTO) \
    X(DISTINCT) \
    X(FROM) \
    X(WHERE) \
    X(GROUP) \
    X(BY) \
    X(HAVING) \
    X(ORDER) \
    X(LIMIT) \
    X(OFFSET) \
    X(FETCH) \
    X(FIRST) \
    X(NEXT) \
    X(ROWS) \
    X(ROW) \
    X(ONLY) \
    X(PARTITION) \
    X(OVER) \
    X(RANGE) \
    X(PRECEDING) \
    X(FOLLOWING) \
    X(CURRENT) \
    X(WINDOW) \
    X(AS) \
    X(AND) \
    X(OR) \
    X(NOT) \
    X(NULL) \
    X(IS) \
    X(IN) \
    X(EXISTS) \
    X(BETWEEN) \
    X(LIKE) \
    X(ESCAPE) \
    X(CASE) \
    X(WHEN) \
    X(THEN) \
    X(ELSE) \
    X(END) \
    X(JOIN) \
    X(INNER) \
    X(LEFT) \
    X(RIGHT) \
    X(FULL) \
    X(OUTER) \
    X(CROSS) \
    X(NATURAL) \
    X(ON) \
    X(USING) \
    X(UNION) \
    X(ALL) \
    X(INTERSECT) \
    X(EXCEPT) \
    X(WITH) \
    X(RECURSIVE) \
    X(BEGIN) \
    X(TRANSACTION) \
    X(COMMIT) \
    X(ROLLBACK) \
    X(SAVEPOINT) \
    X(RELEASE) \
    X(SET) \
    X(READ) \
    X(WRITE) \
    X(ISOLATION) \
    X(LEVEL) \
    X(SERIALIZABLE) \
    X(COMMITTED) \
    X(UNCOMMITTED) \
    X(REPEATABLE) \
    X(CREATE) \
    X(ALTER) \
    X(DROP) \
    X(TABLE) \
    X(VIEW) \
    X(SCHEMA) \
    X(DATABASE) \
    X(COLUMN) \
    X(ADD) \
    X(RENAME) \
    X(TO) \
    X(DEFAULT) \
    X(CONSTRAINT) \
    X(PRIMARY) \
    X(KEY) \
    X(FOREIGN) \
    X(REFERENCES) \
    X(UNIQUE) \
    X(CHECK) \
    X(CASCADE) \
    X(RESTRICT) \
    X(TEMP) \
    X(TEMPORARY) \
    X(REPLACE) \
    X(TRUNCATE) \
    X(IF) \
    X(TRUE) \
    X(FALSE) \
    X(NULLS) \
    X(ASC) \
    X(DESC) \
    X(BEFORE) \
    X(AFTER) \
    X(MATCHED) \
    X(MERGE) \
    X(DO) \
    X(LOCK) \
    X(SHARE)

typedef enum keyword_kind {
#define SQL_KEYWORD_ENUM(name) KEYWORD_##name,
    SQL_KEYWORD_LIST(SQL_KEYWORD_ENUM)
#undef SQL_KEYWORD_ENUM
    KEYWORD__COUNT
} keyword_t;

typedef struct token {
    token_kind_t kind;
    uint32_t start;   /* byte offset into tokenization_view.source */
    uint32_t length;  /* byte length */
    union {
        int64_t int_value;
        double float_value;
        keyword_t keyword;
        symbol_t symbol;
    } as;
} token_t;

typedef struct tokenization_view {
    const uint8_t* source;   /* owned by the result */
    size_t source_length;
    token_t* tokens;         /* owned by the result */
    size_t token_count;
} tokenization_view_t;

typedef struct tokenization_result {
    int status;              /* 0 = ok, non-zero = error */
    const char* error;       /* static string, not owned */
    size_t error_length;
    tokenization_view_t view;

    /* internal ownership pointer, free with tokenization_result_free() */
    void* _owner;
} tokenization_result_t;

tokenization_result_t tokenize_sql(const uint8_t* query);
void tokenization_result_free(tokenization_result_t* result);

#ifdef __cplusplus
}
#endif

#endif /* SQL_TOKENIZER_H */