import { 
    execute, 
    query, 
    create_bucket, 
    deserialize_to_json 
} from './bucket-db-2.js'

function generate_uuid() {
    return crypto.randomUUID()
}

async function run_test() {
    const bucket_id = generate_uuid()
    console.log('bucket_id:', bucket_id)

    // create bucket
    await create_bucket(bucket_id)
    console.log('bucket created')

    // create tables
    await execute(bucket_id, `
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT,
            age INT
        );
    `)

    await execute(bucket_id, `
        CREATE TABLE logs (
            id INT PRIMARY KEY,
            user_id INT,
            action TEXT
        );
    `)

    console.log('tables created')

    // insert data into users
    await execute(bucket_id, `
        INSERT INTO users (id, name, age) VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35);
    `)

    // insert data into logs
    let start = performance.now()
    await execute(bucket_id, `
        INSERT INTO logs (id, user_id, action) VALUES
        (1, 1, 'login'),
        (2, 2, 'logout'),
        (3, 1, 'update_profile'),
        ${new Array(10_000).fill(0).map((_,i)=> `(${i+4}, 1, 'dd')`).join(',')};
    `)
    let full = performance.now() - start;
    console.log('data inserted')
    console.log(full)

    // query users
    const users_raw = await query(bucket_id, `
        SELECT * FROM users;
    `)

    const users = deserialize_to_json(users_raw)
    console.log('\nUSERS:')
    console.log(users)

    // example with aggregation
    start = performance.now()
    const agg_raw = await query(bucket_id, `
        SELECT user_id, COUNT(id) as cnt
        FROM logs
        GROUP BY user_id;
    `)
    full = performance.now() -start;

    const agg = deserialize_to_json(agg_raw)
    console.log('\nAGGREGATION:')
    console.log(agg)
    console.log(full)
}

run_test().catch(err => {
    console.error('ERROR:', err)
})