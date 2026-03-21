import { 
    execute, 
    query, 
    create_bucket, 
    deserialize_to_json 
} from './bucket-db-2.js'


async function run_test() {
    const bucket_id = '9c49fd19-4673-481b-aa83-25c6d5c37438'
    const warmup = await query(bucket_id, `
        SELECT * FROM users;
    `)
    const warmup_de = deserialize_to_json(warmup)
    console.log('\nwarmup:')
    console.log(warmup_de)

    // query users
    const users_raw = await query(bucket_id, `
        SELECT * FROM users;
    `)
    const users = deserialize_to_json(users_raw)
    console.log('\nUSERS:')
    console.log(users)
    const agg_raw = await query(bucket_id, `
        SELECT user_id, COUNT(id) as cnt
        FROM logs
        GROUP BY user_id;
    `)

    const agg = deserialize_to_json(agg_raw)
    console.log('\nAGGREGATION:')
    console.log(agg)
}

run_test().catch(err => {
    console.error('ERROR:', err)
})