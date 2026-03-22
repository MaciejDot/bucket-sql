```bash
clang -std=c11 -O2 -Wall -Wextra -pedantic ./sql_tokenizer.c ./sql_parser.c ./sql_executor.c ./sql_executor_test.c -o sql_exec && ./sql_exec
```