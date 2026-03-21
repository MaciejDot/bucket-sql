/**
 * DATABASE ENGINE (BUCKET DB) {name in progress...}
 * 
 * SUPPORTS BASIC SQL
 * 
 * CRUD OPERATIONS
 * TRANSACTIONS
 * CREATE?ALTER?DROP TABLE
 * 
 * NO INDEXES
 * NO TRIGGERS
 * 
 * MAIN FILE (bucket-db.js) RUNS db itself -> it provides very minimalistic api
 * 
 * execute(string command, string[] args) -> byte[]
 * query(string query, string[] args) -> byte[]
 * execute_bucket(string command, string[] args) -> byte
 * bulk_whisper_execut(byte[] data) -> int (for whisper portocol)
 * bulk_whisper_provide(byte[] data) -> byte[] (for whisper portocol)
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
 * USE BUCKET 64746cca-75c1-4629-9570-dff69bfb6323;
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
 */

export function execute(command, args){}
export function query(query,args){}
export function execute_bucket(command, args){}
export function bulk_whisper_execute(data){}
export function bulk_whisper_provide(data){}

/*initialize*/

const data_folder_path = '/data';//from env
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





