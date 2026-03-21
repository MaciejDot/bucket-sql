import { randomUUID } from 'crypto'
import { create_bucket, execute, query, deserialize_to_json, has_bucket } from './bucket-db/index.js'

async function main() {
  const bucket_id = randomUUID().replace(/-/g, '_')
  console.log('bucket_id:', bucket_id)

  await create_bucket(bucket_id)
  console.log('bucket created?', await has_bucket(bucket_id))

  await execute(bucket_id, 'CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);')
  await execute(bucket_id, 'CREATE TABLE logs (id INT PRIMARY KEY, user_id INT, action TEXT);')
  console.log('tables created')

  await execute(bucket_id, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35);")
  await execute(bucket_id, `INSERT INTO logs (id, user_id, action) VALUES (1, 1, 'login'), (2, 2, 'logout'), (3, 1, 'update_profile'),${new Array(10_000).fill(0).map((_,i)=> `(${i+4}, 1, 'dd')`).join(',')};`)
  console.log('data inserted')

  const users_raw = await query(bucket_id, 'SELECT * FROM users;')
  const users = deserialize_to_json(users_raw)
  console.log('\nUSERS:')
  console.log(users)

  const logs_raw = await query(bucket_id, 'SELECT * FROM logs;')
  const logs = deserialize_to_json(logs_raw)
  console.log('\nLOGS:')
  console.log(logs)

  const agg_raw = await query(bucket_id, 'SELECT user_id, COUNT(id) AS cnt FROM logs GROUP BY user_id;')
  const agg = deserialize_to_json(agg_raw)
  console.log('\nAGGREGATION:')
  console.log(agg)
}

main().catch((error) => {
  console.error('TEST FAILED:', error)
  process.exitCode = 1
})
