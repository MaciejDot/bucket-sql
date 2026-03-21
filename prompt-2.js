/**
 * DATABASE ENGINE (BUCKET DB) {name in progress...}
 * 
 * SUPPORTS BASIC SQL
 * 
 * CRUD OPERATIONS 
 * TRANSACTIONS
 * CREATE?ALTER?DROP TABLE
 * 
 * SELECT should include usage of group by api aggregate functions (COUNT, MAX, MIN ...)
 * 
 * 
 * NO INDEXES
 * NO TRIGGERS
 * 
 * 
 * MAIN FILE (bucket-db.js) RUNS db itself -> it provides very minimalistic api
 * 
 * 
 * SECOUNDARY FILES
 * 
 * shell.js - provides cli api to interact with
 * server.js - provides udp/tcp server to interact thru (handles user/password for db and ssl if setup)
 * whisper.js - provides protocol for communication between replicas/ shards
 * gate.js - provides service to be balancer above cluster (as mongos)
 * 
 * dockerfile for single db
 * dockerfile for gate
 * 
 * database is persistent (its not only in memory)
 * 
 * database above sql provides api for creating/deleting/getting buckets
 * (small dbs with its own schema) - so api for getting given data could look like
 * 
 * SELECT name, id FROM table_tab;
 * 
 * WHy like this?
 * 
 * many applications use sql but there is little connection if any between users and their data
 * so for these kind of datasets when user has its own private data that is consumed by him and
 * some small number of others if any (for example training log/ medical log and so on)
 * 
 * so there is no need to put it in single dataset for many users - also sharding is pretty easy 
 * sharding will be just hash of bucket handled by gate.js
 * whisper protocol would be also easy between small buckets.
 * 
 * so the folder structure for data of main file would be something like this
 * 
 * buffer-global-operation.bin
 * set-bucket-ids.bin
 * buckets/
 *  {id}/
 *      buffer-bucket.bin
 *      data-{random}.bin
 *      data-{random}.bin
 * 
 * all buckets should be decorated with vector clocks - if in whisper protocol
 * 
 * there should be functions for serialization / deserialization of data in given files kind
 * to be easly editable 
 * serialization/ deserialization into byte format
 * format should have header with info what is in file how many data rows or definitions what is kind of file
 * version of protocol and so on
 * 
 * smth like this
 * 
 * {how many rows 8 bytes}
 * {length_of_data column type column type name column type kind column length}
 * ....
 * 
 * and now for example for variable length columns
 * {column_length , column_data}
 * 
 * result of execution should be simmilar as serialization data (do not use json as it is overhead)
 * 
 * the program should handle multiple connection concurrent executions (do not use workers or webGL - just do not set up things like currentBucketId and so on)
 * 
 * all function, args, variable names should be in snake case
 * 
 * not only data is persisted but also table definitions
 */

/** execute command against bucket */
export function execute(bucket_id, command, args){}
/** execute query against bucket */
export function query(bucket_id, query, args){}
/** global for creating new bucket*/
export function create_bucket(bucket_id){}
/** to ask if bucket exist */
export function has_bucket(bucket_id){}
/** to dlete bucket*/
export function delete_bucket(bucket_id){}
/**helper for testing */
export function deserialize_to_json(raw_result){}




/*initialize*/

const data_folder_path = './data';//from env
const data_buffer_global_operations = `${data_folder_path}/buffer-global-operation.bin`;
const data_bucket_ids = `${data_folder_path}/set-bucket-ids.bin`;

/*create files if not exist*/

function create_bucket_folder(id){}
function get_bucket_folder(id){}


/* no need to put them out/into memory lets assume that if rem could not hold full set of bucket
ids there is something wrong with app */
const all_buckets = new Set()
const all_buckets_locks = new Map()
const all_buckets_buffer = new Uint32Array();

/*
    but for each bucket data must be LRU cached
    lets make another assumption (at this stage of impl)
    if bucket is being used we put it whole into RAM
*/

class LRU_CACHE_BUCKETS_DATA {
    constructor(max_size_mb){}
    get_bucket(id){}
    put_bucket(id, data){}
}

const lru_cached_buckets = new LRU_CACHE_BUCKETS_DATA();

function load_bucket_into_memory (id) {
    // get data from get_bucket_folder;
    const data = ''
    lru_cached_buckets.put_bucket()
}

function create_bucket(id){}
function create_bucket_and_load_it_into_memory(id){}

/*tokenizer*/
tokenizer(command, args){

}
/*planner*/
planner(tokenized){}
execute(plan){}
/**
 * for now dont bother with whisper protocol - please fill out based on file
 */