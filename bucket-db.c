/**
 * DB Engine
 * 
 * SQL Dialect
 * 
 * for many buckets (first select bucket by id) then
 * execute query against it - it wont change anything in other
 * buckets and wont hurt performance of other buckets
 */

/**
 * each bucket has its private files
 * data-files
 * buffer-files
 * transaction-files
 */

/**
 * command source can be cli or 
 * udp / tcp connection
 */

/**
 * log operations ??
 */

/**
 * maybe do not manage secure connection  just leave api to interact
 * and then wrap with server
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

int create_bucket(char guid[32]){}
int access_bucket(char guid[32]){}
int has_bucket(char guid[32]){}

char* base_path;
/**
 *
 *    /db
 *      bucket-list.bin
 *    /buckets
 *      /id
 *        /buffor
 *          buffor.bin
 *        /data
 *          data-a.bin
 *          data-b.bin
 */


int initialize(){
    /*READ command line parameters and env variables*/
    /*Initialize db state from file*/
}

uint64_t hash_u64(uint64_t x) {
    x += 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
    x = x ^ (x >> 31);
    return x;
}

// both 0 it is empty?
struct hash_entry {
    uint64_t entry; // entry = entry - 1 if entry is zero it is empty - losing only one possible number of 2^64
    uint64_t next; // 
};

// lock on bucket table recalc ???
// write access to table should be locked
// but many read access should be possible 
uint64_t hash_table_length;
// [[[[[[[[[]]]]]]]]]
// when entry is empty?? - magic number ? - maybe 0 and just - 1 all the results  
uint64_t* hash_table; // keys - entry list begin
uint64_t hash_entry_length;
struct hash_entry* hash_entries;

int init_hash_table(){
    hash_table = malloc(sizeof(uint64_t) * 1024);
}




//hash map [uint64_t]


int main() {

    return 0;
}