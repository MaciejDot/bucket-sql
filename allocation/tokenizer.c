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

//to internal headers
#define TOKENIZER_PIPE_PART_CALL(fn_call)              \
    {                                    \
        tokenizer_status_t __status = fn_call; \
        if (__status == TOKENIZER_MATCH) continue; \
        if (__status == TOKENIZER_FATAL) break;   \
    }

/* types */
typedef enum {
    TOKENIZER_MATCH,
    TOKENIZER_NO_MATCH,
    TOKENIZER_FATAL
} tokenizer_status_t;

typedef enum token_kind {
    TOKEN_IDENTIFIER,
    TOKEN_KEYWORD,
    TOKEN_STRING_LITERAL,
    TOKEN_PLACEHOLDER
} token_kind_t;

typedef union token_value {
    string_t* string_literal;
    string_t* placeholder;
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
    tokenize_result->error_message = alloc_string_on_area(area, ERROR_MAX_LEN + 1);
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(tokenize_result->error_message->data, ERROR_MAX_LEN, fmt, args);
    va_end(args);
    tokenize_result->error_message->len = (written >= ERROR_MAX_LEN) ? ERROR_MAX_LEN : (size_t)written;
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
    size_t current_index = 0;
    tokenize_result_t* result = alloc_on_area(allocation_space, sizeof(result));

    result -> head = NULL;
    result -> tail = NULL;
    result -> error_message = NULL;
    result -> is_error = 0;
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
        *current_index +=2;
        while((*current_index) + 1 < query->len && 
            (query->data[(*current_index)] != "*" || 
            query->data[(*current_index) + 1] != "/") )
        {
            *current_index += 1;
        }
        *current_index +=2;
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
    
    if(first_character != '\'' || first_character != '"') {
        return TOKENIZER_NO_MATCH;
    }
    
    size_t starting_index = *current_index;

    *current_index += 1;
    uint8_t found_end = 0;

    while(*current_index < query -> len){
        if(query -> data[(*current_index)-1] != '\\' && query -> data[(*current_index)] == first_character){
            found_end = 1;
            break;
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

    if((*current_index) +1 != query->len || (
        it_is_whitespace(query, (*current_index) +1) ||
        query->data[(*current_index) +1] != ';' ||
        query->data[(*current_index) +1] != ',' ||
        query->data[(*current_index) +1] != '.' ||
        query->data[(*current_index) +1] != ')' ||
        it_is_begining_of_comment(query, (*current_index) +1)
    )
    
    
    ){
        error_invalid_charcter_after_string_literal(
            tokenize_result,
            allocation_space,
            first_character,
            query->data[(*current_index) +1],
            (*current_index) +1
        );
        return TOKENIZER_FATAL;
    }

    token_t* token = append_result(tokenize_result, allocation_space);
    
    size_t string_length = *current_index - starting_index - 2; 
    
    token -> token_kind = TOKEN_STRING_LITERAL;
    token -> token_value.string_literal = alloc_string_on_area(allocation_space, string_length);

    for(size_t char_index = 0; char_index < string_length; char_index += 1){
        token -> token_value.string_literal ->data[char_index] = query->data[char_index + 1 + starting_index];
    }
    
    return TOKENIZER_FATAL;
}

/*
    should also signalize termination error for example - not ending string or string end that is like 'sdfjksy ssdf'FROM
    0 - ok - did not found anything
    1 - ok - found smth
    2 - not ok - error parsing should break;
*/
tokenizer_status_t is_it_placeholder(string_t* query, size_t* current_index, tokenize_result_t* tokenize_result, alloc_area* allocation_space)
{
    return TOKENIZER_NO_MATCH;
    // @ 

}

tokenize_result_t* tokenize(string_t* query, alloc_area* allocation_space){
    if(allocation_space->capacity - allocation_space->offset < max_size_of_tokenize_result(query)){
        return &TOKENIZE_ERROR_NOT_ENOUGH_SPACE;
    }

    size_t current_index = 0;
    tokenize_result_t* result = initialize_tokenize_result(allocation_space);

    while(current_index < query->len){
        skip_whitespaces(query, &current_index);
        
        if(is_it_comment(query, &current_index)){
            continue;
        }

        TOKENIZER_PIPE_PART_CALL(is_it_string_literal(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_placeholder(query, &current_index, result, allocation_space));
        /*
        TOKENIZER_PIPE_PART_CALL(is_it_number(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_identifier_or_keyword(query, &current_index, result, allocation_space));
        TOKENIZER_PIPE_PART_CALL(is_it_punctuation(query, &current_index, result, allocation_space));
        5. numbers
        6. identifiers → keyword check
        7. operators / punctuation
        */

        error_tokenizer_could_not_tokenize_character(result, allocation_space, query->data[current_index], current_index);

        break;
    }

    return result;
};


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
                print_string(current->token_value.string_literal);
                break;

            case TOKEN_PLACEHOLDER:
                print_string(current->token_value.placeholder);
                break;

            case TOKEN_IDENTIFIER:
            case TOKEN_KEYWORD:
                // zakładam, że też używasz string_literal dla nich
                print_string(current->token_value.string_literal);
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
        "SELECT * FROMe tab WHERE a= \"classical music where it counts\" /*SELECT * FROM*/;";

string_t query_string= {
    query,
    sizeof(query) - 1
};
alloc_area area;
malloc_area(&area, 128000);

print_tokenize_result(
    tokenize(&query_string, &area)
);

return 0;

}