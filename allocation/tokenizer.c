#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdalign.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#include "allocation.h"
#include "alloc_strings.h"

#define ERROR_MAX_LEN 100
#define MAX_NUMBER_LEN 63
//to internal headers
#define TOKENIZER_PIPE_PART_CALL(fn_call)              \
    {                                    \
        tokenizer_status_t __status = fn_call; \
        if (__status == TOKENIZER_MATCH) continue; \
        if (__status == TOKENIZER_FATAL) break;   \
    }

/* types */

typedef enum punctuation_kind {
    // single char
    PUNC_LPAREN,      // (
    PUNC_RPAREN,      // )
    PUNC_COMMA,       // ,
    PUNC_SEMICOLON,   // ;
    PUNC_DOT,         // .
    PUNC_STAR,        // *
    PUNC_EQUAL,       // =
    PUNC_PLUS,        // +
    PUNC_MINUS,       // -
    PUNC_SLASH,       // /
    PUNC_PERCENT,     // %

    // comparisons
    PUNC_NOT_EQUAL,   // != or <>
    PUNC_LESS,        // <
    PUNC_LESS_EQ,     // <=
    PUNC_GREATER,     // >
    PUNC_GREATER_EQ,  // >=

} punctuation_kind_t;

typedef struct punctuation_entry {
    const char* symbol;
    size_t len;
    punctuation_kind_t kind;
} punctuation_entry_t;

static const punctuation_entry_t punctuations[] = {
    {"!=", 2, PUNC_NOT_EQUAL},
    {"<>", 2, PUNC_NOT_EQUAL},
    {"<=", 2, PUNC_LESS_EQ},
    {">=", 2, PUNC_GREATER_EQ},

    {"(", 1, PUNC_LPAREN},
    {")", 1, PUNC_RPAREN},
    {",", 1, PUNC_COMMA},
    {";", 1, PUNC_SEMICOLON},
    {".", 1, PUNC_DOT},
    {"*", 1, PUNC_STAR},
    {"=", 1, PUNC_EQUAL},
    {"+", 1, PUNC_PLUS},
    {"-", 1, PUNC_MINUS},
    {"/", 1, PUNC_SLASH},
    {"%", 1, PUNC_PERCENT},
    {"<", 1, PUNC_LESS},
    {">", 1, PUNC_GREATER},
};

typedef enum keyword_kind {
    // Core CRUD / DQL
    KW_SELECT,
    KW_INSERT,
    KW_UPDATE,
    KW_DELETE,

    // Clauses
    KW_FROM,
    KW_WHERE,
    KW_GROUP,
    KW_BY,
    KW_HAVING,
    KW_ORDER,
    KW_LIMIT,
    KW_OFFSET,

    // Joins
    KW_JOIN,
    KW_INNER,
    KW_LEFT,
    KW_RIGHT,
    KW_FULL,
    KW_OUTER,
    KW_CROSS,
    KW_NATURAL,
    KW_ON,
    KW_USING,

    // Insert / update helpers
    KW_INTO,
    KW_VALUES,
    KW_SET,

    // Conditions / logic
    KW_AND,
    KW_OR,
    KW_NOT,
    KW_IN,
    KW_EXISTS,
    KW_LIKE,
    KW_BETWEEN,
    KW_IS,
    KW_NULL,
    KW_TRUE,
    KW_FALSE,

    // Set operations
    KW_UNION,
    KW_ALL,
    KW_DISTINCT,

    // Aggregates / functions
    KW_COUNT,
    KW_SUM,
    KW_MIN,
    KW_MAX,
    KW_AVG,

    // Window functions / window clauses
    KW_OVER,
    KW_PARTITION,
    KW_ROWS,
    KW_RANGE,
    KW_PRECEDING,
    KW_FOLLOWING,
    KW_UNBOUNDED,
    KW_CURRENT,
    KW_ROW,
    KW_ROW_NUMBER,
    KW_RANK,
    KW_DENSE_RANK,
    KW_LAG,
    KW_LEAD,

    // Sorting
    KW_ASC,
    KW_DESC,

    // DDL
    KW_CREATE,
    KW_ALTER,
    KW_DROP,
    KW_TABLE,

    // Table constraints
    KW_PRIMARY,
    KW_KEY,
    KW_FOREIGN,
    KW_REFERENCES,
    KW_CONSTRAINT,
    KW_UNIQUE,
    KW_CHECK,
    KW_DEFAULT,

    // Data types
    KW_INT,
    KW_FLOAT,
    KW_TEXT,
    KW_VARCHAR,
    KW_BOOL,

    // Transactions
    KW_BEGIN,
    KW_START,
    KW_TRANSACTION,
    KW_COMMIT,
    KW_ROLLBACK,
    KW_SAVEPOINT,

    // Misc
    KW_AS,
    KW_CASE,
    KW_WHEN,
    KW_THEN,
    KW_ELSE,
    KW_END,

    KW_UNKNOWN
} keyword_kind_t;

typedef struct keyword_entry {
    const char* name;
    keyword_kind_t kind;
} keyword_entry_t;

static const keyword_entry_t keywords[] = {
    {"select", KW_SELECT},
    {"insert", KW_INSERT},
    {"update", KW_UPDATE},
    {"delete", KW_DELETE},

    {"from", KW_FROM},
    {"where", KW_WHERE},
    {"group", KW_GROUP},
    {"by", KW_BY},
    {"having", KW_HAVING},
    {"order", KW_ORDER},
    {"limit", KW_LIMIT},
    {"offset", KW_OFFSET},

    {"join", KW_JOIN},
    {"inner", KW_INNER},
    {"left", KW_LEFT},
    {"right", KW_RIGHT},
    {"full", KW_FULL},
    {"outer", KW_OUTER},
    {"cross", KW_CROSS},
    {"natural", KW_NATURAL},
    {"on", KW_ON},
    {"using", KW_USING},

    {"into", KW_INTO},
    {"values", KW_VALUES},
    {"set", KW_SET},

    {"and", KW_AND},
    {"or", KW_OR},
    {"not", KW_NOT},
    {"in", KW_IN},
    {"exists", KW_EXISTS},
    {"like", KW_LIKE},
    {"between", KW_BETWEEN},
    {"is", KW_IS},
    {"null", KW_NULL},
    {"true", KW_TRUE},
    {"false", KW_FALSE},

    {"union", KW_UNION},
    {"all", KW_ALL},
    {"distinct", KW_DISTINCT},

    {"count", KW_COUNT},
    {"sum", KW_SUM},
    {"min", KW_MIN},
    {"max", KW_MAX},
    {"avg", KW_AVG},

    {"over", KW_OVER},
    {"partition", KW_PARTITION},
    {"rows", KW_ROWS},
    {"range", KW_RANGE},
    {"preceding", KW_PRECEDING},
    {"following", KW_FOLLOWING},
    {"unbounded", KW_UNBOUNDED},
    {"current", KW_CURRENT},
    {"row", KW_ROW},
    {"row_number", KW_ROW_NUMBER},
    {"rank", KW_RANK},
    {"dense_rank", KW_DENSE_RANK},
    {"lag", KW_LAG},
    {"lead", KW_LEAD},

    {"asc", KW_ASC},
    {"desc", KW_DESC},

    {"create", KW_CREATE},
    {"alter", KW_ALTER},
    {"drop", KW_DROP},
    {"table", KW_TABLE},

    {"primary", KW_PRIMARY},
    {"key", KW_KEY},
    {"foreign", KW_FOREIGN},
    {"references", KW_REFERENCES},
    {"constraint", KW_CONSTRAINT},
    {"unique", KW_UNIQUE},
    {"check", KW_CHECK},
    {"default", KW_DEFAULT},

    {"int", KW_INT},
    {"float", KW_FLOAT},
    {"text", KW_TEXT},
    {"varchar", KW_VARCHAR},
    {"bool", KW_BOOL},

    {"begin", KW_BEGIN},
    {"start", KW_START},
    {"transaction", KW_TRANSACTION},
    {"commit", KW_COMMIT},
    {"rollback", KW_ROLLBACK},
    {"savepoint", KW_SAVEPOINT},

    {"as", KW_AS},
    {"case", KW_CASE},
    {"when", KW_WHEN},
    {"then", KW_THEN},
    {"else", KW_ELSE},
    {"end", KW_END},
};


typedef enum {
    TOKENIZER_MATCH,
    TOKENIZER_NO_MATCH,
    TOKENIZER_FATAL
} tokenizer_status_t;

typedef enum token_kind {
    TOKEN_IDENTIFIER,
    TOKEN_KEYWORD,
    TOKEN_STRING_LITERAL,
    TOKEN_PLACEHOLDER,
    TOKEN_FLOAT_LITERAL,
    TOKEN_LONG_LITERAL,
    TOKEN_PUNCTUATION,
} token_kind_t;

typedef union token_value {
    string_t* string;
    float as_float;
    unsigned long long as_long; 
    keyword_kind_t keyword;
    punctuation_kind_t punctuation;
} token_value_t;

typedef struct token {
    token_kind_t token_kind;
    token_value_t token_value;
    struct token* next;
} token_t;

typedef struct tokenize_result {
    token_t* head;
    token_t* tail;
    uint8_t is_error;
    string_t* error_message;
} tokenize_result_t;

/* errors */
static const uint8_t NOT_ENOUGH_SPACE_MSG[] =
    "Not enough space on provided allocator";

static const string_t NOT_ENOUGH_SPACE_STRING = {
    NOT_ENOUGH_SPACE_MSG,
    sizeof(NOT_ENOUGH_SPACE_MSG) - 1
};

static const tokenize_result_t TOKENIZE_ERROR_NOT_ENOUGH_SPACE = {
    .head = NULL,
    .tail = NULL,
    .is_error = 1,
    .error_message = (string_t*)&NOT_ENOUGH_SPACE_STRING
};


/** generic error string creation */
void make_error_string(alloc_area* area, tokenize_result_t* tokenize_result, const char* fmt, ...) {
    tokenize_result->error_message = alloc_string_on_area(area, ERROR_MAX_LEN);
    if (tokenize_result->error_message == NULL) {
        tokenize_result->is_error = 1;
        return;
    }

    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(
        (char*)tokenize_result->error_message->data,
        ERROR_MAX_LEN + 1,
        fmt,
        args
    );
    va_end(args);

    tokenize_result->error_message->len = (written >= ERROR_MAX_LEN) ? ERROR_MAX_LEN : (size_t)written;
    tokenize_result->is_error = 1;
}

void error_no_end_string_literal(
    tokenize_result_t* tokenize_result,
    alloc_area* allocation_space,
    uint8_t string_character,
    size_t string_character_index
    )
{
    return make_error_string(
        allocation_space,
        tokenize_result,
        "There is no end to string literal %c started at %i char",
        string_character,
        string_character_index
    );
}

void error_invalid_charcter_after_string_literal(
    tokenize_result_t* tokenize_result,
    alloc_area* allocation_space,
    uint8_t string_character,
    uint8_t invalid_character,
    size_t invalid_character_index
    )
{
    return make_error_string(
        allocation_space,
        tokenize_result,
        "There is invalid character after string literal %c '%c' at %i char",
        string_character,
        invalid_character,
        invalid_character_index
    );
}

void error_tokenizer_could_not_tokenize_character(
    tokenize_result_t* tokenize_result,
    alloc_area* allocation_space,
    uint8_t string_character,
    size_t string_character_index
    ){
        return make_error_string(
        allocation_space,
        tokenize_result,
        "Tokenizer could not tokenize %c at %i char",
        string_character,
        string_character_index
        );
    }

size_t max_size_of_tokenize_result(string_t* query){
    // all string literals + all tokens + single error
    return (sizeof(uint8_t) + alignof(uint8_t) + ( sizeof(string_t) + alignof(string_t))) * query->len + 
        sizeof(tokenize_result_t) + alignof(tokenize_result_t) + (sizeof(token_t) + alignof(token_t)) * query -> len +
        (ERROR_MAX_LEN + alignof(uint8_t) + sizeof(uint8_t) * 9 ) * sizeof(uint8_t);
}

uint8_t it_is_whitespace(string_t* query, size_t current_index){
    return query->data[current_index] == ' ' ||
            query->data[current_index] == '\n' ||
            query->data[current_index] == '\r' ||
            query->data[current_index] == '\t';
}

void skip_whitespaces(string_t* query, size_t* current_index){
    while(*current_index < query ->len && it_is_whitespace(query, *current_index)){
        *current_index += 1;
    }
};

tokenize_result_t* initialize_tokenize_result(alloc_area* allocation_space){
    tokenize_result_t* result = alloc_on_area(allocation_space, sizeof(tokenize_result_t));
    if (result == NULL) {
        return NULL;
    }

    result->head = NULL;
    result->tail = NULL;
    result->error_message = NULL;
    result->is_error = 0;
    return result;
}

token_t* append_result(tokenize_result_t* tokenize_result, alloc_area* allocation_space){
    token_t* next_token = alloc_on_area(allocation_space, sizeof(token_t));
    
    next_token -> next = NULL;

    if(tokenize_result -> head == NULL){
        tokenize_result -> head = next_token;
        tokenize_result -> tail = next_token;
        return next_token;
    }

    tokenize_result -> tail -> next = next_token;
    tokenize_result -> tail = next_token;

    return next_token;
}

uint8_t it_is_begining_of_single_line_comment(string_t* query, size_t start){
    return (query->data[start] == '-' && (start) + 1 < query->len && query -> data[(start) + 1 ] == '-');
}

uint8_t is_it_begining_of_multi_line_comment(string_t* query, size_t start){
    return query->data[start] == '/' && start + 1 < query->len && query->data[start + 1] == '*';
}

uint8_t it_is_begining_of_comment(string_t* query, size_t start){
    return it_is_begining_of_single_line_comment(query, start) || is_it_begining_of_multi_line_comment(query, start);
}

uint8_t is_it_single_line_comment(string_t* query, size_t* current_index){
    if(it_is_begining_of_single_line_comment(query, *current_index)){
        *current_index += 2;
        while((*current_index) < query->len && query->data[(*current_index)] != '\n')
        {
            *current_index += 1;
        }
        return 1;
    };
    return 0;
}

uint8_t is_it_multi_line_comment(string_t* query, size_t* current_index) {
    if(is_it_begining_of_multi_line_comment(query, *current_index)){
        *current_index += 2;
        while((*current_index) + 1 < query->len &&
            (query->data[*current_index] != '*' ||
             query->data[*current_index + 1] != '/'))
        {
            *current_index += 1;
        }

        if ((*current_index) + 1 >= query->len) {
            return 1;
        }

        *current_index += 2;
        return 1;
    }
    return 0;
}

uint8_t is_it_comment(string_t* query, size_t* current_index){
    if(is_it_single_line_comment(query, current_index)){
        return 1;
    }
    return is_it_multi_line_comment(query, current_index);
}

tokenizer_status_t is_it_string_literal(string_t* query, size_t* current_index, tokenize_result_t* tokenize_result, alloc_area* allocation_space){
    uint8_t first_character = query->data[*current_index];

    if(first_character != '\'' && first_character != '"') {
        return TOKENIZER_NO_MATCH;
    }

    size_t starting_index = *current_index;

    *current_index += 1;
    uint8_t found_end = 0;
    size_t escapes_counter = 0;

    while(*current_index < query->len){
        if(query->data[*current_index] == '\\') {
            escapes_counter+=1;
        }
        if(escapes_counter % 2 == 0 && query->data[*current_index] == first_character){
            found_end = 1;
            break;
        }
        if(query->data[*current_index] != '\\'){
            escapes_counter = 0;
        }
        *current_index += 1;
    }

    if(!found_end){
        error_no_end_string_literal(
            tokenize_result,
            allocation_space,
            first_character,
            starting_index
        );
        return TOKENIZER_FATAL;
    }

    if ((*current_index) + 1 < query->len &&
        !(
            it_is_whitespace(query, (*current_index) + 1) ||
            query->data[(*current_index) + 1] == ';' ||
            query->data[(*current_index) + 1] == ',' ||
            query->data[(*current_index) + 1] == '.' ||
            query->data[(*current_index) + 1] == ')' ||
            it_is_begining_of_comment(query, (*current_index) + 1)
        ))
    {
        error_invalid_charcter_after_string_literal(
            tokenize_result,
            allocation_space,
            first_character,
            query->data[(*current_index) + 1],
            (*current_index) + 1
        );
        return TOKENIZER_FATAL;
    }

    token_t* token = append_result(tokenize_result, allocation_space);
    if (token == NULL) {
        return TOKENIZER_FATAL;
    }

    size_t string_length = *current_index - starting_index - 1;

    token->token_kind = TOKEN_STRING_LITERAL;
    token->token_value.string = alloc_string_on_area(allocation_space, string_length);
    if (token->token_value.string == NULL) {
        return TOKENIZER_FATAL;
    }

    for(size_t char_index = 0; char_index < string_length; char_index += 1){
        token->token_value.string->data[char_index] = query->data[char_index + 1 + starting_index];
    }

    (*current_index)++;
    return TOKENIZER_MATCH;
}

tokenizer_status_t is_it_placeholder(string_t* query, size_t* current_index, tokenize_result_t* tokenize_result, alloc_area* allocation_space)
{
    if(query->data[*current_index] != '$'){
        return TOKENIZER_NO_MATCH;
    }

    *current_index += 1;
    size_t first_index = *current_index;
    
    while (*current_index < query -> len)
    {
        if(
            it_is_whitespace(query, (*current_index) ) ||
            query->data[(*current_index) ] == ';' ||
            query->data[(*current_index) ] == ',' ||
            query->data[(*current_index) ] == '.' ||
            query->data[(*current_index) ] == ')' ||
            it_is_begining_of_comment(query, (*current_index) )
        ){
            break;
        }
        *current_index += 1;
    }
    
    if(*current_index == first_index){
        make_error_string(
            allocation_space,
            tokenize_result,
            "placeholder cannot be empty at %i character",
            first_index - 1
        );
        return TOKENIZER_FATAL;
    }

    size_t string_length = *current_index - first_index;

    token_t* token = append_result(tokenize_result, allocation_space);
    if (token == NULL) {
        return TOKENIZER_FATAL;
    }
    token->token_kind = TOKEN_PLACEHOLDER;
    token->token_value.string = alloc_string_on_area(allocation_space, string_length);
    if (token->token_value.string == NULL) {
        return TOKENIZER_FATAL;
    }

    for(size_t char_index = 0; char_index < string_length; char_index += 1){
        token->token_value.string->data[char_index] = query->data[char_index + first_index];
    }

    return TOKENIZER_MATCH;
}


uint8_t is_digit(char c) {
    return c >= '0' && c <= '9';
}

tokenizer_status_t is_it_number(
    string_t* query,
    size_t* current_index,
    tokenize_result_t* result,
    alloc_area* area
) {
    
    if(!is_digit(query->data[*current_index])){
        return TOKENIZER_NO_MATCH;
    }

    size_t start_index = *current_index;
    uint8_t has_dot = 0;
    *current_index += 1;

    while(
        (is_digit(query->data[*current_index]) || query->data[*current_index] == '.') && (*current_index) < query->len
    ){
        if(query->data[*current_index] == '.' && has_dot){
            make_error_string(area, result, "double dot in number literal not allowed at %i position", *current_index);
            return TOKENIZER_FATAL;
        }
        if(query->data[*current_index] == '.'){
            has_dot = 1;
        }
        *current_index += 1;
    }

    size_t length = *current_index - start_index;

    if(length > MAX_NUMBER_LEN){
        make_error_string(
            area, result, "number literal starting at %i position is to long max length is %i",
            start_index,
            MAX_NUMBER_LEN
        );
        return TOKENIZER_FATAL;
    }

    token_t* token = append_result(result, area);

    char buffer[MAX_NUMBER_LEN + 1];
    memcpy(buffer, &query->data[start_index], length);
    buffer[length] = '\0';

    if (has_dot) {
        token -> token_kind = TOKEN_FLOAT_LITERAL;
        token -> token_value.as_float = strtof(buffer, NULL);
    } else {
        token -> token_kind =  TOKEN_LONG_LITERAL;
        token -> token_value.as_long = strtoll(buffer, NULL, 10);
    }

    return TOKENIZER_MATCH;
}


uint8_t is_alphabetical_or_floor(uint8_t c) {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
            c == '_';
}

uint8_t is_alphanumerical_or_floor(uint8_t c) {
    return is_alphabetical_or_floor(c) || (c >= '0' && c <= '9');
}

size_t keyword_count() {
    return sizeof(keywords) / sizeof(keywords[0]);
}

uint8_t to_lower_ascii(uint8_t c) {
    if (c >= 'A' && c <= 'Z') return c + ('a' - 'A');
    return c;
}

uint8_t strn_eq_ignore_case(uint8_t* a, uint8_t* b, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        if (to_lower_ascii(a[i]) != b[i]) {
            return 0;
        }
    }
    return 1;
}

keyword_entry_t* keyword_lookup(uint8_t* s, size_t len) {
    uint8_t count = keyword_count();
    for (size_t i = 0; i < count; ++i) {
        uint8_t* kw = keywords[i].name;
        if (to_lower_ascii(s[0]) != kw[0]) continue;
        if (strlen(kw) == len &&
            strn_eq_ignore_case(s, kw, len)) {
            return &keywords[i];
        }
    }
    return NULL;
}

tokenizer_status_t is_it_identifier_or_keyword(
    string_t* query,
    size_t* current_index,
    tokenize_result_t* result,
    alloc_area* area
){
    if(!is_alphanumerical_or_floor(query->data[*current_index])){
        return TOKENIZER_NO_MATCH;
    }
    size_t strating_index = *current_index;
    *current_index+=1;
    while(is_alphanumerical_or_floor(query->data[(*current_index)])){
        *current_index+=1;
    }
    size_t len = *current_index - strating_index;

    keyword_entry_t* kw = keyword_lookup(&query->data[strating_index], len);

    token_t* token = append_result(result, area);

    if(kw != NULL){
        token->token_kind = TOKEN_KEYWORD;
        token->token_value.keyword = kw->kind;
        return TOKENIZER_MATCH;
    }

    token->token_kind = TOKEN_IDENTIFIER;
    token->token_value.string = alloc_string_on_area(area, len);

    for(size_t i=0; i<len;i+=1){
        token ->token_value.string->data[i] = query->data[i+strating_index];
    }
    
    return TOKENIZER_MATCH;
}

size_t punctuation_count(void) {
    return sizeof(punctuations) / sizeof(punctuations[0]);
}

punctuation_entry_t* punctuation_lookup(
    string_t* query,
    size_t* current_index) {
    size_t count = punctuation_count();
    size_t remaining_len = (query->len - *current_index);
    for (size_t i = 0; i < count; ++i) {
        const punctuation_entry_t* p = &punctuations[i];
        if (p->len <= remaining_len &&
            strn_eq_ignore_case(&query->data[*current_index], p->symbol, p->len)) {
            return p;
        }
    }
    return NULL;
}

tokenizer_status_t is_it_punctuation(
    string_t* query,
    size_t* current_index,
    tokenize_result_t* result,
    alloc_area* area
){
    punctuation_entry_t* punctuation = punctuation_lookup(query, current_index);

    if(punctuation == NULL){
        return TOKENIZER_NO_MATCH;
    }
    *current_index += punctuation->len;
    token_t* token = append_result(result, area);

    token->token_kind = TOKEN_PUNCTUATION;
    token->token_value.punctuation = punctuation->kind;

    return TOKENIZER_MATCH;
}

tokenize_result_t* tokenize(string_t* query, alloc_area* allocation_space){
    if(allocation_space->capacity - allocation_space->offset < max_size_of_tokenize_result(query)){
        return &TOKENIZE_ERROR_NOT_ENOUGH_SPACE;
    }

    size_t current_index = 0;
    tokenize_result_t* result = initialize_tokenize_result(allocation_space);
    if (result == NULL) {
        return &TOKENIZE_ERROR_NOT_ENOUGH_SPACE;
    }

    while(current_index < query->len){
        skip_whitespaces(query, &current_index);

        if(current_index >= query->len) break;

        TOKENIZER_PIPE_PART_CALL(is_it_string_literal(query, &current_index, result, allocation_space));

        if(is_it_comment(query, &current_index)){
            continue;
        }

        TOKENIZER_PIPE_PART_CALL(is_it_placeholder(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_number(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_identifier_or_keyword(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_punctuation(query, &current_index, result, allocation_space));

        error_tokenizer_could_not_tokenize_character(result, allocation_space, query->data[current_index], current_index);
        break;
    }

    return result;
}


static void print_string(string_t* str) {
    if (!str) {
        printf("NULL");
        return;
    }
    printf("%.*s", (int)str->len, str->data);
}

static const char* token_kind_to_string(token_kind_t kind) {
    switch (kind) {
        case TOKEN_IDENTIFIER: return "IDENTIFIER";
        case TOKEN_KEYWORD: return "KEYWORD";
        case TOKEN_STRING_LITERAL: return "STRING_LITERAL";
        case TOKEN_PLACEHOLDER: return "PLACEHOLDER";
        case TOKEN_LONG_LITERAL: return "LONG_LITERAL";
        case TOKEN_FLOAT_LITERAL: return "FLOAT_LITERAL";
        case TOKEN_PUNCTUATION: return "PUNCTUATION";
        default: return "UNKNOWN";
    }
}


void print_tokenize_result(tokenize_result_t* result) {
    if (!result) {
        printf("Result is NULL\n");
        return;
    }

    if (result->is_error) {
        printf("ERROR: ");
        print_string(result->error_message);
        printf("\n");
    }

    printf("Tokens:\n");

    token_t* current = result->head;
    int index = 0;

    while (current) {
        printf("[%d] %s: ", index, token_kind_to_string(current->token_kind));

        switch (current->token_kind) {
            case TOKEN_STRING_LITERAL:
                print_string(current->token_value.string);
                break;

            case TOKEN_PLACEHOLDER:
                print_string(current->token_value.string);
                break;
            case TOKEN_FLOAT_LITERAL:
                printf("%f", current -> token_value.as_float);
                break;
            case TOKEN_LONG_LITERAL:
                printf("%llu", current -> token_value.as_long);
                break;
            case TOKEN_IDENTIFIER:
                print_string(current->token_value.string);
                break;
            
            case TOKEN_KEYWORD: 
                printf("%i", current->token_value.keyword);
                break;
            
            case TOKEN_PUNCTUATION: 
                printf("%i", current->token_value.punctuation);
                break;

            default:
                printf("?");
                break;
        }

        printf("\n");

        current = current->next;
        index++;
    }
}


int main() {
const uint8_t* query = (const uint8_t*)
        "SELECT a, sum(b) OVER (PARTITION BY grp ORDER BY ts DESC) "
        "FROM tesco "
        "WHERE x >= $min_x "
        "GROUP BY a, grp "
        "HAVING sum(b) > $min_sum "
        "ORDER BY ts DESC "
        "LIMIT $limit OFFSET $offset;";

string_t query_string= {
    query,
    strlen((const char*)query)
};
alloc_area area;
malloc_area(&area, 128000);
print_tokenize_result(
    tokenize(&query_string, &area)
);

return 0;

}