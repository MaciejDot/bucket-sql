import fs from 'fs/promises'
import path from 'path'
import { bucket_root_path, bucket_ids_path } from '../config/constants.js'
import { save_bucket_state, load_bucket_state, remove_bucket_state, bucket_path } from './persistence.js'

const all_buckets = new Set()

async function ensure_root() {
  await fs.mkdir(bucket_root_path, { recursive: true })
}

async function sync_bucket_ids_file() {
  await ensure_root()
  const data = JSON.stringify([...all_buckets])
  await fs.writeFile(bucket_ids_path, data)
}

async function load_bucket_ids_file() {
  await ensure_root()
  try {
    const raw = await fs.readFile(bucket_ids_path, 'utf8')
    const ids = JSON.parse(raw)
    all_buckets.clear()
    for (const id of ids) all_buckets.add(String(id))
  } catch {
    all_buckets.clear()
  }
}

await load_bucket_ids_file()

function validate_bucket_id(bucket_id) {
  const id = String(bucket_id || '').trim()
  if (!id) throw new Error('bucket_id is required')
  if (!/^[a-zA-Z0-9._-]+$/.test(id)) throw new Error('bucket_id contains invalid characters')
  return id
}

export async function create_bucket(bucket_id) {
  const id = validate_bucket_id(bucket_id)
  await ensure_root()
  all_buckets.add(id)
  await fs.mkdir(bucket_path(id), { recursive: true })
  const bucket_state = { tables: new Map() }
  await save_bucket_state(id, bucket_state)
  await sync_bucket_ids_file()
  return true
}

export async function has_bucket(bucket_id) {
  const id = validate_bucket_id(bucket_id)
  return all_buckets.has(id)
}

export async function delete_bucket(bucket_id) {
  const id = validate_bucket_id(bucket_id)
  all_buckets.delete(id)
  await sync_bucket_ids_file()
  await remove_bucket_state(id)
  return true
}

export async function open_bucket(bucket_id) {
  const id = validate_bucket_id(bucket_id)
  if (!all_buckets.has(id)) {
    const exists_on_disk = await has_bucket_on_disk(id)
    if (!exists_on_disk) throw new Error(`bucket not found: ${id}`)
    all_buckets.add(id)
  }
  const bucket_state = await load_bucket_state(id)
  return bucket_state
}

async function has_bucket_on_disk(bucket_id) {
  try {
    await fs.access(bucket_path(bucket_id))
    return true
  } catch {
    return false
  }
}

export async function persist_bucket(bucket_id, bucket_state) {
  const id = validate_bucket_id(bucket_id)
  all_buckets.add(id)
  await save_bucket_state(id, bucket_state)
  await sync_bucket_ids_file()
}

export { validate_bucket_id, all_buckets }
