/**
 * keywords
 */
const keywords = ["SELECT","INSERT","UPDATE","DELETE","VALUES","INTO","DISTINCT","FROM","WHERE","GROUP","BY","HAVING","ORDER","LIMIT","OFFSET","FETCH","FIRST","NEXT","ROWS","ROW","ONLY","PARTITION","OVER","RANGE","PRECEDING","FOLLOWING","CURRENT","WINDOW","AS","AND","OR","NOT","NULL","IS","IN","EXISTS","BETWEEN","LIKE","ESCAPE","CASE","WHEN","THEN","ELSE","END","JOIN","INNER","LEFT","RIGHT","FULL","OUTER","CROSS","NATURAL","ON","USING","UNION","ALL","INTERSECT","EXCEPT","WITH","RECURSIVE","BEGIN","TRANSACTION","COMMIT","ROLLBACK","SAVEPOINT","RELEASE","SET","READ","WRITE","ISOLATION","LEVEL","SERIALIZABLE","COMMITTED","UNCOMMITTED","REPEATABLE","CREATE","ALTER","DROP","TABLE","VIEW","SCHEMA","DATABASE","COLUMN","ADD","RENAME","TO","DEFAULT","CONSTRAINT","PRIMARY","KEY","FOREIGN","REFERENCES","UNIQUE","CHECK","CASCADE","RESTRICT","TEMP","TEMPORARY","REPLACE","TRUNCATE","IF","TRUE","FALSE","NULLS","ASC","DESC","BEFORE","AFTER","MATCHED","MERGE","DO","LOCK","SHARE"]
keywords.sort((a,b) => b.length - a.length)
/**
    TOKEN_IDENTIFIER,
    TOKEN_QUOTED_IDENTIFIER,
    TOKEN_KEYWORD,
    TOKEN_STRING,
    TOKEN_INT,
    TOKEN_FLOAT,
    TOKEN_PLACEHOLDER,
    TOKEN_SYMBOL
**/

/**
 * tokenize('SELECT *, t.FROM FROMe tab AS t WHERE a= "classical music where it counts" /*SELECT * FROM*\/ AND b = c * d;')
 */
/**
 * 
 * @param {string} query 
 */
function tokenize(query){
    let char_index = 0;
    let query_length = query.length;
    let result = [];
    while(char_index < query_length){
        /* in c can be done thru passing references - so it could be smth like
        if(is_it_comment(query, query_length, &char_index)){
            continue;
        }
        */
        const comment_result = is_it_comment(query, query_length, char_index);
        char_index = comment_result.next_char_index;
        if(comment_result.should_continue){
            continue;
        }

        const keyword_result = is_it_keyword(query, query_length, char_index, result);
        char_index = keyword_result.next_char_index;
        if(keyword_result.should_continue){
            continue;
        }

        /* find string literals */
        /* find integer literals */
        /* find float literals */
        /* exponent notation and  */


        const punctuation_result = is_it_punctuation(query, query_length, char_index, result);
        char_index = punctuation_result.next_char_index;
        if(punctuation_result.should_continue){
            continue;
        }

        /**
         * what about custom fields and namings
         * like 
         * SELECT tab.in FROM table1 as tab
         * is it supported
         */


        /* 
            special case here are double / single quuotes then we should look for another quote character
            and save it as string literal
        */

        /**
         * is it int 
         */

        /**
         * is it float
         */


        /* trim thru whitespaces after all checks

        const is_it_whitespace = is_it_punctuation(query, query_length, char_index, result);
        char_index = punctuation_result.next_char_index;
        if(punctuation_result.should_continue){
            continue;
        }
        */

        
        char_index += 1;
    }
    return result
}

const punctuations = [';', ',', '(', ')', "'", '"', '%', '_', '=', '<>', '!=', '<', '>', '<=', '>=', '+', '-', '*', '/', '.']
punctuations.sort((a,b) => b.length - a.length)

/**
 * 
 * @param {string} query 
 * @param {number} query_length 
 * @param {number} char_index 
 */
function is_it_punctuation(query, query_length, char_index, result){
    for(let punctuation of punctuations){
        const punctuation_length = punctuation.length;
        if(query.substring(char_index, char_index + punctuation_length) === punctuation){
            result.push({type: 'punctuation', punctuation })
            return {
                should_continue: true, next_char_index: char_index + punctuation_length,
            }
        }
    }


    return {
        should_continue: false, next_char_index: char_index, punctuation: 0
    }
}


const white_space_characters = new Set([' ', '\xa0', '\n', '\t', '\0'])

function is_it_whitespace(query, query_length, char_index){
    let copied_char_index = char_index;
    while(copied_char_index < query_length){
        if(white_space_characters.has(query[copied_char_index])){
            copied_char_index += 1;
        } else {
            break;
        }
    }
    return { it_is: copied_char_index > char_index, next_char_index: copied_char_index }
}

/**
 * 
 * @param {string} query 
 * @param {number} query_length 
 * @param {number} char_index 
 */
function is_it_keyword(query, query_length, char_index, result){
    for(let keyword of keywords){
        const keyword_length = keyword.length;
        // how would be diffrent IN vs INSERT - check if next character is eof eon space tab or in some cases ()
        // would it qulaify ININSERT as keyword for IN
        if(query.substring(char_index, char_index + keyword_length) === keyword){
            
            if(char_index + keyword_length <  query_length){
                const punctuation_result = is_it_punctuation(query, query_length, char_index + keyword_length, result)
                
                if(punctuation_result.next_char_index === char_index + keyword_length){
                    if(!is_it_whitespace(query, query_length, char_index + keyword_length)){
                        continue;
                    }
                }
            }
            result.push({type:'keyword', keyword})
            return {
                should_continue: true, next_char_index: char_index + keyword_length
            }
        }
    }
    return {
        should_continue: false, next_char_index: char_index
    }
}



function is_it_comment(query, query_length, char_index){
    const result = is_it_single_line_comment(query, query_length, char_index)
    if(result.should_continue){
        return result;
    }
    return is_it_multi_line_comment(query, query_length, char_index)
}



function is_it_single_line_comment(query, query_length, char_index) {
    copied_char_index = char_index;
    if(query[copied_char_index] === '-' && copied_char_index + 1 < query_length && query[copied_char_index + 1] === '-'){
        while(copied_char_index < query.length && query[copied_char_index] !== '\n')
        {
            copied_char_index += 1
        }
        return {should_continue: true, next_char_index: copied_char_index}
    }
    return {should_continue: false, next_char_index: char_index }
}


function is_it_multi_line_comment(query, query_length, char_index) {
    copied_char_index = char_index;
    if(query[copied_char_index] === '/' && copied_char_index + 1 < query_length && query[copied_char_index + 1] === '*'){
        while(copied_char_index < query.length && copied_char_index + 1 < query_length && 
            (query[copied_char_index] !== "*" || 
            query[copied_char_index + 1] !== "/") )
        {
            copied_char_index += 1
        }
        copied_char_index +=2
        return {should_continue: true, next_char_index: copied_char_index}
    }
    return {should_continue: false, next_char_index: char_index}
}


// tokenization result linear table
