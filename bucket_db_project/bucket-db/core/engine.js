import { tokenize } from '../sql/tokenizer.js'
import { parse } from '../sql/parser.js'
import { plan } from '../sql/planner.js'
import { execute_plan } from '../sql/executor.js'
import { encode_query_result, encode_command_result, encode_error_result } from '../serialization/result_codec.js'
import { measure_ms } from '../utils/timing.js'
import { open_bucket, create_bucket, delete_bucket, has_bucket, persist_bucket } from '../storage/bucket_manager.js'
import { promise_mutex } from './locks.js'

const bucket_locks = new Map()

function get_lock(bucket_id) {
  if (!bucket_locks.has(bucket_id)) bucket_locks.set(bucket_id, new promise_mutex())
  return bucket_locks.get(bucket_id)
}

function log_timing(kind, text, timings) {
  console.log(`[timing][${kind}] tokenize=${timings.tokenize_ms.toFixed(3)}ms parse=${timings.parse_ms.toFixed(3)}ms plan=${timings.plan_ms.toFixed(3)}ms execute=${timings.execute_ms.toFixed(3)}ms total=${timings.total_ms.toFixed(3)}ms | ${text}`)
}

async function run_pipeline(bucket_id, text, args = []) {
  const total_start = performance.now()
  const t1 = measure_ms(() => tokenize(text))
  const t2 = measure_ms(() => parse(t1.value))
  const t3 = measure_ms(() => plan(t2.value))
console.log(t1,t2,t3)
  const bucket = await open_bucket(bucket_id)
  const lock = get_lock(bucket_id)
  const execute_start = performance.now()
  const result = await lock.run(async () => execute_plan(bucket, t3.value))
  await persist_bucket(bucket_id, bucket)
  const execute_end = performance.now()

  const timings = {
    tokenize_ms: t1.elapsed_ms,
    parse_ms: t2.elapsed_ms,
    plan_ms: t3.elapsed_ms,
    execute_ms: execute_end - execute_start,
    total_ms: performance.now() - total_start,
  }
  return { result, timings }
}

export async function execute(bucket_id, command, args = []) {
  try {
    const { result, timings } = await run_pipeline(bucket_id, command, args)
    log_timing('execute', command, timings)
    return encode_command_result(result)
  } catch (error) {
    log_timing('execute', command, { tokenize_ms: 0, parse_ms: 0, plan_ms: 0, execute_ms: 0, total_ms: 0 })
    return encode_error_result(error)
  }
}

export async function query(bucket_id, query_text, args = []) {
  try {
    const { result, timings } = await run_pipeline(bucket_id, query_text, args)
    log_timing('query', query_text, timings)
    return encode_query_result(result)
  } catch (error) {
    log_timing('query', query_text, { tokenize_ms: 0, parse_ms: 0, plan_ms: 0, execute_ms: 0, total_ms: 0 })
    return encode_error_result(error)
  }
}

export { create_bucket, delete_bucket, has_bucket }
