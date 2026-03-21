import fs from 'fs/promises'
import path from 'path'

const data_folder_path = process.env.DATA_FOLDER_PATH || './data'
const data_bucket_root_path = path.join(data_folder_path, 'buckets')
const data_buffer_global_operations = path.join(data_folder_path, 'buffer-global-operation.bin')
const data_bucket_ids = path.join(data_folder_path, 'set-bucket-ids.bin')

const storage_version = 1
const snapshot_magic = 'BDB1'
const result_magic = 'RSLT'

const all_buckets = new Set()
const all_buckets_locks = new Map()
const all_buckets_buffer = new Uint32Array(0)

let storage_initialized = false
let storage_init_promise = null

function is_node_buffer(value) {
  return typeof Buffer !== 'undefined' && Buffer.isBuffer(value)
}

function to_u8(value) {
  if (value instanceof Uint8Array) return value
  if (is_node_buffer(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  return new Uint8Array(value)
}

function concat_u8(chunks) {
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.length
  }
  return out
}

function utf8_encode(text) {
  return new TextEncoder().encode(String(text))
}

function utf8_decode(bytes) {
  return new TextDecoder().decode(bytes)
}

class binary_writer {
  constructor() {
    this.chunks = []
  }

  write_u8(value) {
    const out = new Uint8Array(1)
    out[0] = value & 0xff
    this.chunks.push(out)
  }

  write_bool(value) {
    this.write_u8(value ? 1 : 0)
  }

  write_u16(value) {
    const out = new Uint8Array(2)
    new DataView(out.buffer).setUint16(0, value, true)
    this.chunks.push(out)
  }

  write_u32(value) {
    const out = new Uint8Array(4)
    new DataView(out.buffer).setUint32(0, value >>> 0, true)
    this.chunks.push(out)
  }

  write_i32(value) {
    const out = new Uint8Array(4)
    new DataView(out.buffer).setInt32(0, value | 0, true)
    this.chunks.push(out)
  }

  write_u64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setBigUint64(0, BigInt(value), true)
    this.chunks.push(out)
  }

  write_i64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setBigInt64(0, BigInt(value), true)
    this.chunks.push(out)
  }

  write_f64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setFloat64(0, Number(value), true)
    this.chunks.push(out)
  }

  write_bytes(bytes) {
    const u8 = to_u8(bytes)
    this.write_u32(u8.length)
    this.chunks.push(u8)
  }

  write_raw_bytes(bytes) {
    this.chunks.push(to_u8(bytes))
  }

  write_string(value) {
    const bytes = utf8_encode(value)
    this.write_u32(bytes.length)
    this.chunks.push(bytes)
  }

  write_value(value) {
    if (value === null || value === undefined) {
      this.write_u8(0)
      return
    }
    if (typeof value === 'boolean') {
      this.write_u8(1)
      this.write_bool(value)
      return
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value) && Number.isSafeInteger(value)) {
        this.write_u8(2)
        this.write_i64(value)
      } else {
        this.write_u8(3)
        this.write_f64(value)
      }
      return
    }
    if (typeof value === 'bigint') {
      this.write_u8(2)
      this.write_i64(value)
      return
    }
    if (typeof value === 'string') {
      this.write_u8(4)
      this.write_string(value)
      return
    }
    if (value instanceof Uint8Array || is_node_buffer(value)) {
      this.write_u8(5)
      const u8 = to_u8(value)
      this.write_u32(u8.length)
      this.chunks.push(u8)
      return
    }
    if (value instanceof Date) {
      this.write_u8(6)
      this.write_string(value.toISOString())
      return
    }
    if (Array.isArray(value)) {
      this.write_u8(7)
      this.write_u32(value.length)
      for (const item of value) this.write_value(item)
      return
    }
    if (typeof value === 'object') {
      const keys = Object.keys(value)
      this.write_u8(8)
      this.write_u32(keys.length)
      for (const key of keys) {
        this.write_string(key)
        this.write_value(value[key])
      }
      return
    }
    this.write_u8(4)
    this.write_string(String(value))
  }

  to_u8() {
    return concat_u8(this.chunks)
  }
}

class binary_reader {
  constructor(bytes) {
    this.bytes = to_u8(bytes)
    this.view = new DataView(this.bytes.buffer, this.bytes.byteOffset, this.bytes.byteLength)
    this.offset = 0
  }

  read_u8() {
    const value = this.view.getUint8(this.offset)
    this.offset += 1
    return value
  }

  read_bool() {
    return this.read_u8() !== 0
  }

  read_u16() {
    const value = this.view.getUint16(this.offset, true)
    this.offset += 2
    return value
  }

  read_u32() {
    const value = this.view.getUint32(this.offset, true)
    this.offset += 4
    return value
  }

  read_i32() {
    const value = this.view.getInt32(this.offset, true)
    this.offset += 4
    return value
  }

  read_u64() {
    const value = this.view.getBigUint64(this.offset, true)
    this.offset += 8
    return value
  }

  read_i64() {
    const value = this.view.getBigInt64(this.offset, true)
    this.offset += 8
    const as_number = Number(value)
    return Number.isSafeInteger(as_number) ? as_number : value
  }

  read_f64() {
    const value = this.view.getFloat64(this.offset, true)
    this.offset += 8
    return value
  }

  read_bytes() {
    const length = this.read_u32()
    const start = this.offset
    const end = start + length
    this.offset = end
    return this.bytes.slice(start, end)
  }

  read_string() {
    return utf8_decode(this.read_bytes())
  }

  read_value() {
    const tag = this.read_u8()
    if (tag === 0) return null
    if (tag === 1) return this.read_bool()
    if (tag === 2) return this.read_i64()
    if (tag === 3) return this.read_f64()
    if (tag === 4) return this.read_string()
    if (tag === 5) return this.read_bytes()
    if (tag === 6) return new Date(this.read_string())
    if (tag === 7) {
      const length = this.read_u32()
      const out = []
      for (let i = 0; i < length; i += 1) out.push(this.read_value())
      return out
    }
    if (tag === 8) {
      const length = this.read_u32()
      const out = {}
      for (let i = 0; i < length; i += 1) {
        const key = this.read_string()
        out[key] = this.read_value()
      }
      return out
    }
    throw new Error(`unknown value tag: ${tag}`)
  }
}

function clone_value(value) {
  if (typeof structuredClone === 'function') return structuredClone(value)
  return JSON.parse(JSON.stringify(value))
}

function normalize_bucket_id(bucket_id) {
  if (bucket_id === null || bucket_id === undefined) throw new Error('bucket_id is required')
  const text = String(bucket_id).trim()
  if (!text) throw new Error('bucket_id is empty')
  if (!/^[a-zA-Z0-9._-]+$/.test(text)) throw new Error('bucket_id contains invalid characters')
  return text
}

function get_bucket_folder(bucket_id) {
  return path.join(data_bucket_root_path, normalize_bucket_id(bucket_id))
}

async function create_bucket_folder(bucket_id) {
  await fs.mkdir(get_bucket_folder(bucket_id), { recursive: true })
}

async function file_exists(file_path) {
  try {
    await fs.access(file_path)
    return true
  } catch {
    return false
  }
}

function get_bucket_snapshot_path(bucket_id) {
  return path.join(get_bucket_folder(bucket_id), 'bucket_snapshot.bin')
}

function get_bucket_state(bucket_id) {
  return lru_cached_buckets.get_bucket(bucket_id)
}

function empty_bucket_state(bucket_id) {
  return {
    bucket_id,
    tables: new Map(),
    transaction_state: null,
    dirty: false,
    last_accessed_at: Date.now(),
  }
}

function clone_bucket_state(bucket_state) {
  const tables = new Map()
  for (const [table_name, table_data] of bucket_state.tables.entries()) {
    tables.set(table_name, {
      table_name: table_data.table_name,
      columns: table_data.columns.map((col) => ({ ...col, default_value: clone_value(col.default_value) })),
      rows: table_data.rows.map((row) => row.map((cell) => clone_value(cell))),
      auto_increment_next: table_data.auto_increment_next,
    })
  }
  return {
    bucket_id: bucket_state.bucket_id,
    tables,
    transaction_state: null,
    dirty: bucket_state.dirty,
    last_accessed_at: Date.now(),
  }
}

function estimate_bucket_size(bucket_state) {
  try {
    return serialize_bucket_snapshot(bucket_state).length
  } catch {
    return 1024
  }
}

class lru_cache_buckets_data {
  constructor(max_size_mb = 64) {
    this.max_size_bytes = Math.max(1, Number(max_size_mb) || 64) * 1024 * 1024
    this.map = new Map()
    this.total_size_bytes = 0
  }

  get_bucket(bucket_id) {
    const key = normalize_bucket_id(bucket_id)
    const cached = this.map.get(key)
    if (cached) {
      this.map.delete(key)
      this.map.set(key, cached)
      cached.last_accessed_at = Date.now()
      return cached
    }
    return null
  }

  async put_bucket(bucket_id, bucket_data) {
    const key = normalize_bucket_id(bucket_id)
    const existing = this.map.get(key)
    if (existing) {
      this.total_size_bytes -= existing._estimated_size_bytes || 0
      this.map.delete(key)
    }
    bucket_data._estimated_size_bytes = estimate_bucket_size(bucket_data)
    this.total_size_bytes += bucket_data._estimated_size_bytes
    this.map.set(key, bucket_data)
    await this.evict_if_needed()
  }

  async evict_if_needed() {
    for (const [key, bucket_data] of this.map) {
      if (this.total_size_bytes <= this.max_size_bytes) break
      if (bucket_data.transaction_state) continue
      if (bucket_data.dirty) {
        await save_bucket_state_to_disk(key, bucket_data)
        bucket_data.dirty = false
      }
      this.total_size_bytes -= bucket_data._estimated_size_bytes || 0
      this.map.delete(key)
    }
  }
}

const lru_cached_buckets = new lru_cache_buckets_data()

function get_bucket_lock(bucket_id) {
  const key = normalize_bucket_id(bucket_id)
  if (!all_buckets_locks.has(key)) {
    all_buckets_locks.set(key, new promise_mutex())
  }
  return all_buckets_locks.get(key)
}

class promise_mutex {
  constructor() {
    this.tail = Promise.resolve()
  }

  async run(fn) {
    const previous = this.tail
    let release_tail
    this.tail = new Promise((resolve) => {
      release_tail = resolve
    })
    await previous
    try {
      return await fn()
    } finally {
      release_tail()
    }
  }
}

const global_mutex = new promise_mutex()

async function ensure_storage_initialized() {
  if (storage_initialized) return
  if (!storage_init_promise) {
    storage_init_promise = (async () => {
      await fs.mkdir(data_folder_path, { recursive: true })
      await fs.mkdir(data_bucket_root_path, { recursive: true })
      if (!(await file_exists(data_bucket_ids))) {
        await write_bucket_ids_file()
      }
      await read_bucket_ids_file()
      storage_initialized = true
    })()
  }
  await storage_init_promise
}

async function read_bucket_ids_file() {
  if (!(await file_exists(data_bucket_ids))) return
  const raw = await fs.readFile(data_bucket_ids)
  if (raw.length === 0) return
  const reader = new binary_reader(raw)
  const magic = reader.read_string()
  const version = reader.read_u16()
  if (magic !== 'BIDS' || version !== storage_version) return
  const count = reader.read_u32()
  all_buckets.clear()
  for (let i = 0; i < count; i += 1) {
    all_buckets.add(reader.read_string())
  }
}

async function write_bucket_ids_file() {
  const writer = new binary_writer()
  writer.write_string('BIDS')
  writer.write_u16(storage_version)
  writer.write_u32(all_buckets.size)
  for (const bucket_id of all_buckets) writer.write_string(bucket_id)
  await fs.writeFile(data_bucket_ids, writer.to_u8())
}

function serialize_column_def(column_def) {
  return {
    column_name: String(column_def.column_name),
    column_type: String(column_def.column_type || 'text').to_lower_case ? String(column_def.column_type || 'text').to_lower_case() : String(column_def.column_type || 'text').toLowerCase(),
    nullable: column_def.nullable !== false,
    primary_key: !!column_def.primary_key,
    unique: !!column_def.unique,
    has_default: Object.prototype.hasOwnProperty.call(column_def, 'default_value'),
    default_value: column_def.default_value,
  }
}

function serialize_table_def(table_data) {
  return {
    table_name: table_data.table_name,
    columns: table_data.columns.map(serialize_column_def),
    rows: table_data.rows.map((row) => row.map((cell) => clone_value(cell))),
    auto_increment_next: table_data.auto_increment_next || 1,
  }
}

function deserialize_table_def(raw_table) {
  return {
    table_name: raw_table.table_name,
    columns: raw_table.columns.map((col) => ({
      column_name: col.column_name,
      column_type: col.column_type,
      nullable: col.nullable,
      primary_key: col.primary_key,
      unique: col.unique,
      default_value: col.has_default ? clone_value(col.default_value) : undefined,
      has_default: col.has_default,
    })),
    rows: raw_table.rows.map((row) => row.map((cell) => clone_value(cell))),
    auto_increment_next: raw_table.auto_increment_next || 1,
  }
}

function serialize_bucket_snapshot(bucket_state) {
  const writer = new binary_writer()
  writer.write_string(snapshot_magic)
  writer.write_u16(storage_version)
  writer.write_u8(1)
  writer.write_u32(bucket_state.tables.size)
  for (const table_data of bucket_state.tables.values()) {
    const raw_table = serialize_table_def(table_data)
    writer.write_string(raw_table.table_name)
    writer.write_u32(raw_table.columns.length)
    writer.write_u64(raw_table.auto_increment_next)
    writer.write_u32(raw_table.rows.length)
    for (const column_def of raw_table.columns) {
      writer.write_string(column_def.column_name)
      writer.write_string(column_def.column_type)
      writer.write_bool(column_def.nullable)
      writer.write_bool(column_def.primary_key)
      writer.write_bool(column_def.unique)
      writer.write_bool(column_def.has_default)
      if (column_def.has_default) writer.write_value(column_def.default_value)
    }
    for (const row of raw_table.rows) {
      for (const cell of row) writer.write_value(cell)
    }
  }
  return writer.to_u8()
}

function deserialize_bucket_snapshot(raw_bytes, bucket_id) {
  const reader = new binary_reader(raw_bytes)
  const magic = reader.read_string()
  const version = reader.read_u16()
  const file_kind = reader.read_u8()
  if (magic !== snapshot_magic || version !== storage_version || file_kind !== 1) {
    return empty_bucket_state(bucket_id)
  }
  const table_count = reader.read_u32()
  const tables = new Map()
  for (let t = 0; t < table_count; t += 1) {
    const table_name = reader.read_string()
    const column_count = reader.read_u32()
    const auto_increment_next = Number(reader.read_u64())
    const row_count = reader.read_u32()
    const columns = []
    for (let c = 0; c < column_count; c += 1) {
      const column_name = reader.read_string()
      const column_type = reader.read_string()
      const nullable = reader.read_bool()
      const primary_key = reader.read_bool()
      const unique = reader.read_bool()
      const has_default = reader.read_bool()
      const default_value = has_default ? reader.read_value() : undefined
      columns.push({ column_name, column_type, nullable, primary_key, unique, default_value, has_default })
    }
    const rows = []
    for (let r = 0; r < row_count; r += 1) {
      const row = []
      for (let c = 0; c < column_count; c += 1) row.push(reader.read_value())
      rows.push(row)
    }
    tables.set(table_name, { table_name, columns, rows, auto_increment_next })
  }
  return {
    bucket_id,
    tables,
    transaction_state: null,
    dirty: false,
    last_accessed_at: Date.now(),
  }
}

async function save_bucket_state_to_disk(bucket_id, bucket_state) {
  await create_bucket_folder(bucket_id)
  const snapshot_path = get_bucket_snapshot_path(bucket_id)
  const raw = serialize_bucket_snapshot(bucket_state)
  await fs.writeFile(snapshot_path, raw)
}

async function load_bucket_into_memory(bucket_id) {
  await ensure_storage_initialized()
  const key = normalize_bucket_id(bucket_id)
  const cached = lru_cached_buckets.get_bucket(key)
  if (cached) return cached
  await create_bucket_folder(key)
  const snapshot_path = get_bucket_snapshot_path(key)
  let bucket_state = empty_bucket_state(key)
  if (await file_exists(snapshot_path)) {
    const raw = await fs.readFile(snapshot_path)
    bucket_state = deserialize_bucket_snapshot(raw, key)
  }
  await lru_cached_buckets.put_bucket(key, bucket_state)
  return bucket_state
}

async function persist_bucket_state(bucket_id, bucket_state) {
  bucket_state.dirty = false
  await save_bucket_state_to_disk(bucket_id, bucket_state)
  await lru_cached_buckets.put_bucket(bucket_id, bucket_state)
}

function get_table(bucket_state, table_name) {
  const table = bucket_state.tables.get(String(table_name))
  if (!table) throw new Error(`table not found: ${table_name}`)
  return table
}

function table_has_column(table_data, column_name) {
  return table_data.columns.some((col) => col.column_name === column_name)
}

function get_column_index(table_data, column_name) {
  const index = table_data.columns.findIndex((col) => col.column_name === column_name)
  if (index < 0) throw new Error(`column not found: ${column_name}`)
  return index
}

function cast_scalar_value(column_def, value) {
  if (value === null || value === undefined) return null
  const column_type = String(column_def.column_type || 'text').toLowerCase()
  if (column_type === 'int' || column_type === 'integer' || column_type === 'bigint') return Number(value)
  if (column_type === 'float' || column_type === 'double' || column_type === 'real' || column_type === 'decimal') return Number(value)
  if (column_type === 'bool' || column_type === 'boolean') return !!value
  if (column_type === 'json') return clone_value(value)
  if (column_type === 'blob') return to_u8(value)
  if (column_type === 'datetime' || column_type === 'timestamp') return value instanceof Date ? value.toISOString() : String(value)
  return String(value)
}

function default_for_column(column_def) {
  if (Object.prototype.hasOwnProperty.call(column_def, 'default_value')) return clone_value(column_def.default_value)
  return null
}

function row_to_object(table_data, row) {
  const out = {}
  for (let i = 0; i < table_data.columns.length; i += 1) {
    out[table_data.columns[i].column_name] = clone_value(row[i])
  }
  return out
}

function object_to_row(table_data, row_object) {
  const row = []
  for (const column_def of table_data.columns) {
    let value = row_object[column_def.column_name]
    if (value === undefined) {
      value = default_for_column(column_def)
      if (value === undefined) value = null
    }
    row.push(cast_scalar_value(column_def, value))
  }
  return row
}

function validate_row_constraints(table_data, row, exclude_row_index = -1) {
  for (let i = 0; i < table_data.columns.length; i += 1) {
    const column_def = table_data.columns[i]
    const value = row[i]
    if ((value === null || value === undefined) && column_def.nullable === false) {
      throw new Error(`column is not nullable: ${column_def.column_name}`)
    }
  }
  const unique_columns = table_data.columns.filter((col) => col.primary_key || col.unique)
  for (const column_def of unique_columns) {
    const index = get_column_index(table_data, column_def.column_name)
    const value = row[index]
    for (let row_index = 0; row_index < table_data.rows.length; row_index += 1) {
      if (row_index === exclude_row_index) continue
      if (table_data.rows[row_index][index] === value) throw new Error(`duplicate value for unique column: ${column_def.column_name}`)
    }
  }
}

function create_table_plan(table_name, columns) {
  return { type: 'create_table', table_name, columns }
}

function drop_table_plan(table_name) {
  return { type: 'drop_table', table_name }
}

function alter_table_add_column_plan(table_name, column_def) {
  return { type: 'alter_table_add_column', table_name, column_def }
}

function insert_plan(table_name, columns, rows) {
  return { type: 'insert', table_name, columns, rows }
}

function update_plan(table_name, assignments, where_expr) {
  return { type: 'update', table_name, assignments, where_expr }
}

function delete_plan(table_name, where_expr) {
  return { type: 'delete', table_name, where_expr }
}

function select_plan(select_items, table_name, where_expr, group_by, having_expr, order_by, limit, offset) {
  return { type: 'select', select_items, table_name, where_expr, group_by, having_expr, order_by, limit, offset }
}

function begin_plan() { return { type: 'begin' } }
function commit_plan() { return { type: 'commit' } }
function rollback_plan() { return { type: 'rollback' } }

function create_bucket_plan(bucket_id) { return { type: 'create_bucket', bucket_id } }
function delete_bucket_plan(bucket_id) { return { type: 'delete_bucket', bucket_id } }
function has_bucket_plan(bucket_id) { return { type: 'has_bucket', bucket_id } }

function is_keyword(token, keyword) {
  return token && token.type === 'keyword' && token.value === keyword
}

function is_identifier(token) {
  return token && (token.type === 'identifier' || token.type === 'keyword')
}

function tokenizer(command, args = []) {
  let start = performance.now()
  const text = String(command || '').trim()
  const tokens = []
  let i = 0
  while (i < text.length) {
    const ch = text[i]
    if (/\s/.test(ch)) {
      i += 1
      continue
    }
    if (ch === '-' && text[i + 1] === '-') {
      while (i < text.length && text[i] !== '\n') i += 1
      continue
    }
    if (ch === '/' && text[i + 1] === '*') {
      i += 2
      while (i < text.length && !(text[i] === '*' && text[i + 1] === '/')) i += 1
      i += 2
      continue
    }
    if (ch === '\'' || ch === '"') {
      const quote = ch
      i += 1
      let value = ''
      while (i < text.length) {
        const current = text[i]
        if (current === '\\') {
          value += text[i + 1] || ''
          i += 2
          continue
        }
        if (current === quote) {
          if (text[i + 1] === quote) {
            value += quote
            i += 2
            continue
          }
          i += 1
          break
        }
        value += current
        i += 1
      }
      tokens.push({ type: 'string', value })
      continue
    }
    if (ch === '?' ) {
      tokens.push({ type: 'placeholder', value: null })
      i += 1
      continue
    }
    if (/[0-9]/.test(ch) || (ch === '.' && /[0-9]/.test(text[i + 1]))) {
      let start = i
      i += 1
      while (i < text.length && /[0-9eE+\-.]/.test(text[i])) i += 1
      const raw = text.slice(start, i)
      tokens.push({ type: 'number', value: Number(raw) })
      continue
    }
    if (/[A-Za-z_]/.test(ch)) {
      let start = i
      i += 1
      while (i < text.length && /[A-Za-z0-9_$]/.test(text[i])) i += 1
      const raw = text.slice(start, i)
      const lower = raw.toLowerCase()
      const keywords = new Set([
        'select', 'from', 'where', 'group', 'by', 'having', 'order', 'asc', 'desc', 'limit', 'offset', 'insert', 'into',
        'values', 'update', 'set', 'delete', 'create', 'table', 'drop', 'alter', 'add', 'column', 'begin', 'commit',
        'rollback', 'as', 'and', 'or', 'not', 'null', 'is', 'in', 'count', 'max', 'min', 'sum', 'avg', 'row_number',
        'rank', 'dense_rank', 'over', 'partition', 'primary', 'key', 'unique', 'default', 'true', 'false', 'exists',
      ])
      tokens.push({ type: keywords.has(lower) ? 'keyword' : 'identifier', value: keywords.has(lower) ? lower : raw })
      continue
    }
    const two_char = text.slice(i, i + 2)
    const three_char = text.slice(i, i + 3)
    if (['>=', '<=', '<>', '!=', '||', '&&'].includes(two_char)) {
      tokens.push({ type: 'operator', value: two_char })
      i += 2
      continue
    }
    if (['...', '::'].includes(three_char)) {
      tokens.push({ type: 'operator', value: three_char })
      i += 3
      continue
    }
    if ('(),;.*+-/%=<>&|!'.includes(ch) || ch === '.') {
      tokens.push({ type: 'punctuation', value: ch })
      i += 1
      continue
    }
    throw new Error(`unexpected character in SQL: ${ch}`)
  }
  tokens.push({ type: 'eof', value: null })
  console.log('tokenization took', performance.now() -start)
  return { tokens, args, text }
}

function planner(tokenized) {
  const parser = new sql_parser(tokenized.tokens, tokenized.args, tokenized.text)
  return parser.parse_statement()
}

class sql_parser {
  constructor(tokens, args, text) {
    this.tokens = tokens
    this.args = Array.isArray(args) ? args : []
    this.text = text
    this.pos = 0
    this.placeholder_index = 0
  }

  current() {
    return this.tokens[this.pos]
  }

  next() {
    const token = this.tokens[this.pos]
    this.pos += 1
    return token
  }

  match_value(value) {
    const token = this.current()
    if (token && token.value === value) {
      this.pos += 1
      return true
    }
    return false
  }

  match_type(type) {
    const token = this.current()
    if (token && token.type === type) {
      this.pos += 1
      return token
    }
    return null
  }

  expect_value(value) {
    const token = this.next()
    if (!token || token.value !== value) throw new Error(`expected ${value}`)
    return token
  }

  expect_type(type) {
    const token = this.next()
    if (!token || token.type !== type) throw new Error(`expected ${type}`)
    return token
  }

  consume_placeholder_value() {
    const value = this.args[this.placeholder_index]
    this.placeholder_index += 1
    return value
  }

  parse_statement(){
    let start = performance.now();
    const result = this._parse_statement()
    console.log('parsing took', performance.now() - start)
    return result;
  }

  _parse_statement() {
    const token = this.current()
    if (!token) throw new Error('empty command')
    if (is_keyword(token, 'create')) return this.parse_create()
    if (is_keyword(token, 'drop')) return this.parse_drop()
    if (is_keyword(token, 'alter')) return this.parse_alter()
    if (is_keyword(token, 'insert')) return this.parse_insert()
    if (is_keyword(token, 'update')) return this.parse_update()
    if (is_keyword(token, 'delete')) return this.parse_delete()
    if (is_keyword(token, 'select')) return this.parse_select()
    if (is_keyword(token, 'begin')) return begin_plan()
    if (is_keyword(token, 'commit')) return commit_plan()
    if (is_keyword(token, 'rollback')) return rollback_plan()
    if (is_keyword(token, 'has_bucket')) {
      this.next()
      const bucket_id = this.parse_identifier_like()
      return has_bucket_plan(bucket_id)
    }
    if (is_keyword(token, 'create_bucket')) {
      this.next()
      const bucket_id = this.parse_identifier_like()
      return create_bucket_plan(bucket_id)
    }
    if (is_keyword(token, 'delete_bucket')) {
      this.next()
      const bucket_id = this.parse_identifier_like()
      return delete_bucket_plan(bucket_id)
    }
    throw new Error(`unsupported statement: ${token.value}`)
  }

  parse_create() {
    this.expect_value('create')
    if (is_keyword(this.current(), 'bucket')) {
      this.next()
      const bucket_id = this.parse_identifier_like()
      return create_bucket_plan(bucket_id)
    }
    this.expect_value('table')
    const table_name = this.parse_identifier_like()
    this.expect_value('(')
    const columns = []
    while (!this.match_value(')')) {
      const column_name = this.parse_identifier_like()
      const column_type = this.parse_identifier_like()
      let nullable = true
      let primary_key = false
      let unique = false
      let has_default = false
      let default_value = undefined
      let continue_loop = true
      while (continue_loop) {
        continue_loop = false
        if (is_keyword(this.current(), 'primary')) {
          this.next(); this.expect_value('key')
          primary_key = true
          nullable = false
          continue_loop = true
        } else if (is_keyword(this.current(), 'unique')) {
          this.next()
          unique = true
          continue_loop = true
        } else if (is_keyword(this.current(), 'not')) {
          this.next(); this.expect_value('null')
          nullable = false
          continue_loop = true
        } else if (is_keyword(this.current(), 'default')) {
          this.next()
          default_value = this.parse_expression()
          has_default = true
          continue_loop = true
        }
      }
      columns.push({ column_name, column_type, nullable, primary_key, unique, has_default, default_value })
      this.match_value(',')
    }
    return create_table_plan(table_name, columns)
  }

  parse_drop() {
    this.expect_value('drop')
    this.expect_value('table')
    const table_name = this.parse_identifier_like()
    return drop_table_plan(table_name)
  }

  parse_alter() {
    this.expect_value('alter')
    this.expect_value('table')
    const table_name = this.parse_identifier_like()
    this.expect_value('add')
    if (is_keyword(this.current(), 'column')) this.next()
    const column_name = this.parse_identifier_like()
    const column_type = this.parse_identifier_like()
    let nullable = true
    let primary_key = false
    let unique = false
    let has_default = false
    let default_value = undefined
    let continue_loop = true
    while (continue_loop) {
      continue_loop = false
      if (is_keyword(this.current(), 'primary')) {
        this.next(); this.expect_value('key')
        primary_key = true
        nullable = false
        continue_loop = true
      } else if (is_keyword(this.current(), 'unique')) {
        this.next()
        unique = true
        continue_loop = true
      } else if (is_keyword(this.current(), 'not')) {
        this.next(); this.expect_value('null')
        nullable = false
        continue_loop = true
      } else if (is_keyword(this.current(), 'default')) {
        this.next()
        default_value = this.parse_expression()
        has_default = true
        continue_loop = true
      }
    }
    return alter_table_add_column_plan(table_name, { column_name, column_type, nullable, primary_key, unique, has_default, default_value })
  }

  parse_insert() {
    this.expect_value('insert')
    this.expect_value('into')
    const table_name = this.parse_identifier_like()
    let columns = null
    if (this.match_value('(')) {
      columns = []
      while (!this.match_value(')')) {
        columns.push(this.parse_identifier_like())
        this.match_value(',')
      }
    }
    this.expect_value('values')
    const rows = []
    do {
      this.expect_value('(')
      const row = []
      while (!this.match_value(')')) {
        row.push(this.parse_expression())
        this.match_value(',')
      }
      rows.push(row)
    } while (this.match_value(','))
    return insert_plan(table_name, columns, rows)
  }

  parse_update() {
    this.expect_value('update')
    const table_name = this.parse_identifier_like()
    this.expect_value('set')
    const assignments = []
    while (true) {
      const column_name = this.parse_identifier_like()
      this.expect_value('=')
      const value_expr = this.parse_expression()
      assignments.push({ column_name, value_expr })
      if (!this.match_value(',')) break
    }
    let where_expr = null
    if (is_keyword(this.current(), 'where')) {
      this.next()
      where_expr = this.parse_expression()
    }
    return update_plan(table_name, assignments, where_expr)
  }

  parse_delete() {
    this.expect_value('delete')
    this.expect_value('from')
    const table_name = this.parse_identifier_like()
    let where_expr = null
    if (is_keyword(this.current(), 'where')) {
      this.next()
      where_expr = this.parse_expression()
    }
    return delete_plan(table_name, where_expr)
  }

  parse_select() {
    this.expect_value('select')
    const select_items = []
    do {
      select_items.push(this.parse_select_item())
    } while (this.match_value(','))
    this.expect_value('from')
    const table_name = this.parse_identifier_like()
    let where_expr = null
    let group_by = []
    let having_expr = null
    let order_by = []
    let limit = null
    let offset = 0
    while (true) {
      if (is_keyword(this.current(), 'where')) {
        this.next()
        where_expr = this.parse_expression()
        continue
      }
      if (is_keyword(this.current(), 'group')) {
        this.next(); this.expect_value('by')
        group_by = []
        do {
          group_by.push(this.parse_expression())
        } while (this.match_value(','))
        continue
      }
      if (is_keyword(this.current(), 'having')) {
        this.next()
        having_expr = this.parse_expression()
        continue
      }
      if (is_keyword(this.current(), 'order')) {
        this.next(); this.expect_value('by')
        order_by = []
        do {
          const expr = this.parse_expression()
          let direction = 'asc'
          if (is_keyword(this.current(), 'asc')) {
            this.next(); direction = 'asc'
          } else if (is_keyword(this.current(), 'desc')) {
            this.next(); direction = 'desc'
          }
          order_by.push({ expr, direction })
        } while (this.match_value(','))
        continue
      }
      if (is_keyword(this.current(), 'limit')) {
        this.next()
        limit = Number(this.parse_expression_value_only())
        if (is_keyword(this.current(), 'offset')) {
          this.next()
          offset = Number(this.parse_expression_value_only())
        }
        continue
      }
      break
    }
    return select_plan(select_items, table_name, where_expr, group_by, having_expr, order_by, limit, offset)
  }

  parse_select_item() {
    if (this.match_value('*')) return { type: 'star' }
    const expr = this.parse_expression()
    let alias = null
    if (is_keyword(this.current(), 'as')) {
      this.next()
      alias = this.parse_identifier_like()
    } else if (is_identifier(this.current()) && this.current().type === 'identifier') {
      // allow implicit alias only for simple expressions with no trailing operator
      const lookahead = this.tokens[this.pos + 1]
      if (lookahead && ['eof', ',', 'from', 'where', 'group', 'having', 'order', 'limit', ')'].includes(lookahead.value)) {
        alias = this.next().value
      }
    }
    return { type: 'expr', expr, alias }
  }

  parse_expression_value_only() {
    const expr = this.parse_expression()
    if (expr.type === 'literal') return expr.value
    if (expr.type === 'param') return expr.value
    throw new Error('expected literal expression')
  }

  parse_expression() {
    return this.parse_or()
  }

  parse_or() {
    let left = this.parse_and()
    while (is_keyword(this.current(), 'or') || this.match_value('||')) {
      if (this.tokens[this.pos - 1].value !== '||') this.next()
      const right = this.parse_and()
      left = { type: 'binary', operator: 'or', left, right }
    }
    return left
  }

  parse_and() {
    let left = this.parse_not()
    while (is_keyword(this.current(), 'and') || this.match_value('&&')) {
      if (this.tokens[this.pos - 1].value !== '&&') this.next()
      const right = this.parse_not()
      left = { type: 'binary', operator: 'and', left, right }
    }
    return left
  }

  parse_not() {
    if (is_keyword(this.current(), 'not') || this.match_value('!')) {
      if (this.tokens[this.pos - 1].value !== '!') this.next()
      return { type: 'unary', operator: 'not', operand: this.parse_not() }
    }
    return this.parse_compare()
  }

  parse_compare() {
    let left = this.parse_add()
    while (true) {
      const token = this.current()
      if (!token) break
      const op_map = {
        '=': 'eq', '!=': 'ne', '<>': 'ne', '<': 'lt', '>': 'gt', '<=': 'le', '>=': 'ge',
      }
      if (token.type === 'punctuation' && op_map[token.value]) {
        this.next()
        const right = this.parse_add()
        left = { type: 'binary', operator: op_map[token.value], left, right }
        continue
      }
      if (is_keyword(token, 'is')) {
        this.next()
        const is_not = is_keyword(this.current(), 'not') ? (this.next(), true) : false
        if (is_keyword(this.current(), 'null')) {
          this.next()
          left = { type: 'binary', operator: is_not ? 'is_not_null' : 'is_null', left, right: { type: 'literal', value: null } }
          continue
        }
        throw new Error('unsupported IS clause')
      }
      break
    }
    return left
  }

  parse_add() {
    let left = this.parse_mul()
    while (true) {
      const token = this.current()
      if (!token || token.value !== '+' && token.value !== '-') break
      this.next()
      const right = this.parse_mul()
      left = { type: 'binary', operator: token.value === '+' ? 'add' : 'sub', left, right }
    }
    return left
  }

  parse_mul() {
    let left = this.parse_unary()
    while (true) {
      const token = this.current()
      if (!token || token.value !== '*' && token.value !== '/' && token.value !== '%') break
      this.next()
      const right = this.parse_unary()
      left = { type: 'binary', operator: token.value === '*' ? 'mul' : token.value === '/' ? 'div' : 'mod', left, right }
    }
    return left
  }

  parse_unary() {
    const token = this.current()
    if (token && token.value === '-') {
      this.next()
      return { type: 'unary', operator: 'neg', operand: this.parse_unary() }
    }
    return this.parse_primary()
  }

  parse_primary() {
    const token = this.current()
    if (!token) throw new Error('unexpected end of input')
    if (token.type === 'number') {
      this.next()
      return { type: 'literal', value: token.value }
    }
    if (token.type === 'string') {
      this.next()
      return { type: 'literal', value: token.value }
    }
    if (token.type === 'placeholder') {
      this.next()
      return { type: 'param', value: this.consume_placeholder_value() }
    }
    if (is_keyword(token, 'null')) {
      this.next()
      return { type: 'literal', value: null }
    }
    if (is_keyword(token, 'true')) {
      this.next()
      return { type: 'literal', value: true }
    }
    if (is_keyword(token, 'false')) {
      this.next()
      return { type: 'literal', value: false }
    }
    if (token.value === '(') {
      this.next()
      const expr = this.parse_expression()
      this.expect_value(')')
      return expr
    }
    if (token.type === 'identifier' || token.type === 'keyword') {
      const identifier = this.parse_identifier_path()
      if (this.match_value('(')) {
        const args = []
        if (!this.match_value(')')) {
          do {
            args.push(this.parse_expression())
          } while (this.match_value(','))
          this.expect_value(')')
        }
        let over_clause = null
        if (is_keyword(this.current(), 'over')) {
          this.next()
          this.expect_value('(')
          const partition_by = []
          const order_by = []
          if (is_keyword(this.current(), 'partition')) {
            this.next(); this.expect_value('by')
            do {
              partition_by.push(this.parse_expression())
            } while (this.match_value(','))
          }
          if (is_keyword(this.current(), 'order')) {
            this.next(); this.expect_value('by')
            do {
              const expr = this.parse_expression()
              let direction = 'asc'
              if (is_keyword(this.current(), 'asc')) { this.next(); direction = 'asc' }
              else if (is_keyword(this.current(), 'desc')) { this.next(); direction = 'desc' }
              order_by.push({ expr, direction })
            } while (this.match_value(','))
          }
          this.expect_value(')')
          over_clause = { partition_by, order_by }
        }
        return { type: 'call', name: identifier, args, over_clause }
      }
      return { type: 'identifier', name: identifier }
    }
    throw new Error(`unexpected token: ${token.value}`)
  }

  parse_identifier_like() {
    const token = this.next()
    if (!token || !(token.type === 'identifier' || token.type === 'keyword')) throw new Error('expected identifier')
    return token.value
  }

  parse_identifier_path() {
    let name = this.parse_identifier_like()
    while (this.match_value('.')) {
      name += '.' + this.parse_identifier_like()
    }
    return name
  }
}

function is_aggregate_function(name) {
  return ['count', 'max', 'min', 'sum', 'avg'].includes(String(name).toLowerCase())
}

function is_window_function(name) {
  return ['row_number', 'rank', 'dense_rank'].includes(String(name).toLowerCase())
}

function expr_contains_aggregate(expr) {
  if (!expr) return false
  if (expr.type === 'call') return is_aggregate_function(expr.name) || expr.args.some(expr_contains_aggregate)
  if (expr.type === 'binary') return expr_contains_aggregate(expr.left) || expr_contains_aggregate(expr.right)
  if (expr.type === 'unary') return expr_contains_aggregate(expr.operand)
  return false
}

function expr_contains_window(expr) {
  if (!expr) return false
  if (expr.type === 'call') return !!expr.over_clause || expr.args.some(expr_contains_window)
  if (expr.type === 'binary') return expr_contains_window(expr.left) || expr_contains_window(expr.right)
  if (expr.type === 'unary') return expr_contains_window(expr.operand)
  return false
}

function eval_expr(expr, row_ctx, group_ctx = null) {
  if (!expr) return null
  switch (expr.type) {
    case 'literal': return clone_value(expr.value)
    case 'param': return clone_value(expr.value)
    case 'identifier': return get_identifier_value(row_ctx, expr.name)
    case 'unary': {
      const value = eval_expr(expr.operand, row_ctx, group_ctx)
      if (expr.operator === 'neg') return value === null ? null : -Number(value)
      if (expr.operator === 'not') return !truthy(value)
      throw new Error(`unknown unary operator: ${expr.operator}`)
    }
    case 'binary': {
      const left = eval_expr(expr.left, row_ctx, group_ctx)
      const right = eval_expr(expr.right, row_ctx, group_ctx)
      return eval_binary(expr.operator, left, right)
    }
    case 'call': {
      const fn_name = String(expr.name).toLowerCase()
      if (expr.over_clause) throw new Error('window function must be evaluated separately')
      if (is_aggregate_function(fn_name)) {
        if (!group_ctx) throw new Error(`aggregate function used outside grouping: ${fn_name}`)
        return evaluate_aggregate(fn_name, expr.args, group_ctx.rows)
      }
      if (fn_name === 'coalesce') {
        for (const arg of expr.args) {
          const value = eval_expr(arg, row_ctx, group_ctx)
          if (value !== null && value !== undefined) return value
        }
        return null
      }
      throw new Error(`unsupported function: ${fn_name}`)
    }
    default:
      throw new Error(`unknown expression type: ${expr.type}`)
  }
}

function eval_binary(operator, left, right) {
  switch (operator) {
    case 'or': return truthy(left) || truthy(right)
    case 'and': return truthy(left) && truthy(right)
    case 'eq': return compare_values(left, right) === 0
    case 'ne': return compare_values(left, right) !== 0
    case 'lt': return compare_values(left, right) < 0
    case 'gt': return compare_values(left, right) > 0
    case 'le': return compare_values(left, right) <= 0
    case 'ge': return compare_values(left, right) >= 0
    case 'add': return arithmetic(left, right, (a, b) => a + b)
    case 'sub': return arithmetic(left, right, (a, b) => a - b)
    case 'mul': return arithmetic(left, right, (a, b) => a * b)
    case 'div': return arithmetic(left, right, (a, b) => a / b)
    case 'mod': return arithmetic(left, right, (a, b) => a % b)
    case 'is_null': return left === null || left === undefined
    case 'is_not_null': return !(left === null || left === undefined)
    default: throw new Error(`unknown operator: ${operator}`)
  }
}

function arithmetic(left, right, fn) {
  if (left === null || left === undefined || right === null || right === undefined) return null
  return fn(Number(left), Number(right))
}

function compare_values(left, right) {
  if (left === right) return 0
  if (left === null || left === undefined) return -1
  if (right === null || right === undefined) return 1
  if (typeof left === 'number' && typeof right === 'number') return left < right ? -1 : left > right ? 1 : 0
  const left_text = String(left)
  const right_text = String(right)
  return left_text < right_text ? -1 : left_text > right_text ? 1 : 0
}

function truthy(value) {
  return !(value === null || value === undefined || value === false || value === 0 || value === '')
}

function get_identifier_value(row_ctx, name) {
  if (name in row_ctx.row) return row_ctx.row[name]
  const short_name = name.split('.').pop()
  if (short_name in row_ctx.row) return row_ctx.row[short_name]
  if (row_ctx.alias_map && name in row_ctx.alias_map) return row_ctx.alias_map[name]
  if (row_ctx.alias_map && short_name in row_ctx.alias_map) return row_ctx.alias_map[short_name]
  return null
}

function evaluate_aggregate(fn_name, args, rows) {
  const values = []
  if (fn_name === 'count' && args.length === 1 && args[0].type === 'star') return rows.length
  for (const row_ctx of rows) {
    const value = args.length ? eval_expr(args[0], row_ctx, { rows }) : null
    if (value !== null && value !== undefined) values.push(value)
  }
  if (fn_name === 'count') return args.length ? values.length : rows.length
  if (fn_name === 'max') return values.length ? values.reduce((a, b) => compare_values(a, b) >= 0 ? a : b) : null
  if (fn_name === 'min') return values.length ? values.reduce((a, b) => compare_values(a, b) <= 0 ? a : b) : null
  if (fn_name === 'sum') return values.reduce((a, b) => Number(a) + Number(b), 0)
  if (fn_name === 'avg') return values.length ? values.reduce((a, b) => Number(a) + Number(b), 0) / values.length : null
  throw new Error(`unknown aggregate function: ${fn_name}`)
}

function build_row_contexts(table_data) {
  return table_data.rows.map((row, row_index) => ({
    row: row_to_object(table_data, row),
    row_index,
    raw_row: row,
  }))
}

function apply_where_filter(rows, where_expr) {
  if (!where_expr) return rows
  return rows.filter((row_ctx) => truthy(eval_expr(where_expr, row_ctx, null)))
}

function group_rows(rows, group_by_exprs) {
  if (!group_by_exprs || group_by_exprs.length === 0) return [{ group_key: '__all__', rows }]
  const groups = new Map()
  for (const row_ctx of rows) {
    const key_values = group_by_exprs.map((expr) => JSON.stringify(eval_expr(expr, row_ctx, null)))
    const key = key_values.join('\u0001')
    if (!groups.has(key)) groups.set(key, { group_key: key, rows: [] })
    groups.get(key).rows.push(row_ctx)
  }
  return [...groups.values()]
}

function evaluate_window_function(expr, rows, current_index) {
  const fn_name = String(expr.name).toLowerCase()
  const partition_key = expr.over_clause && expr.over_clause.partition_by.length
    ? JSON.stringify(expr.over_clause.partition_by.map((part_expr) => part_expr ? part_expr : null).map((part_expr) => part_expr))
    : '__all__'
  // The actual partitioning is done by caller; this helper is only for the current partition.
  if (fn_name === 'row_number') return current_index + 1
  if (fn_name === 'rank' || fn_name === 'dense_rank') {
    let rank = 1
    let dense_rank = 1
    let last_sort_key = null
    for (let i = 0; i <= current_index; i += 1) {
      const sort_key = JSON.stringify(rows[i]._window_sort_key)
      if (i === 0) {
        last_sort_key = sort_key
        continue
      }
      if (sort_key !== last_sort_key) {
        dense_rank += 1
        rank = i + 1
        last_sort_key = sort_key
      }
    }
    return fn_name === 'rank' ? rank : dense_rank
  }
  throw new Error(`unsupported window function: ${fn_name}`)
}

function sort_rows(rows, order_by) {
  if (!order_by || order_by.length === 0) return rows
  const sorted = [...rows]
  sorted.sort((a, b) => {
    for (const order_item of order_by) {
      const left = eval_expr(order_item.expr, a, null)
      const right = eval_expr(order_item.expr, b, null)
      const cmp = compare_values(left, right)
      if (cmp !== 0) return order_item.direction === 'desc' ? -cmp : cmp
    }
    return 0
  })
  return sorted
}

function serialize_command_result(command_result) {
  const writer = new binary_writer()
  writer.write_string(result_magic)
  writer.write_u16(storage_version)
  writer.write_u8(2)
  writer.write_value(command_result.status || 'ok')
  writer.write_value(command_result.message || '')
  writer.write_u64(command_result.affected_rows || 0)
  writer.write_value(command_result.inserted_rows || [])
  writer.write_value(command_result.extra || null)
  return writer.to_u8()
}

function serialize_query_result(query_result) {
  const writer = new binary_writer()
  writer.write_string(result_magic)
  writer.write_u16(storage_version)
  writer.write_u8(1)
  writer.write_u32(query_result.columns.length)
  writer.write_u64(query_result.rows.length)
  for (const column of query_result.columns) {
    writer.write_string(column.column_name)
    writer.write_string(column.column_type || 'text')
  }
  for (const row of query_result.rows) {
    for (const cell of row) writer.write_value(cell)
  }
  return writer.to_u8()
}

function serialize_error_result(error) {
  const writer = new binary_writer()
  writer.write_string(result_magic)
  writer.write_u16(storage_version)
  writer.write_u8(3)
  writer.write_string(error.code || 'error')
  writer.write_string(error.message || String(error))
  return writer.to_u8()
}

function deserialize_to_json(raw_result) {
  const reader = new binary_reader(raw_result)
  const magic = reader.read_string()
  const version = reader.read_u16()
  const kind = reader.read_u8()
  if (magic !== result_magic || version !== storage_version) {
    throw new Error('invalid result buffer')
  }
  if (kind === 1) {
    const column_count = reader.read_u32()
    const row_count = Number(reader.read_u64())
    const columns = []
    for (let i = 0; i < column_count; i += 1) {
      columns.push({ column_name: reader.read_string(), column_type: reader.read_string() })
    }
    const rows = []
    for (let r = 0; r < row_count; r += 1) {
      const row = {}
      for (const column of columns) row[column.column_name] = reader.read_value()
      rows.push(row)
    }
    return { status: 'ok', kind: 'query', columns, rows }
  }
  if (kind === 2) {
    return {
      status: reader.read_value(),
      kind: 'command',
      message: reader.read_value(),
      affected_rows: Number(reader.read_u64()),
      inserted_rows: reader.read_value(),
      extra: reader.read_value(),
    }
  }
  if (kind === 3) {
    return { status: 'error', code: reader.read_string(), message: reader.read_string() }
  }
  throw new Error(`unknown result kind: ${kind}`)
}

async function create_bucket(bucket_id) {
  await ensure_storage_initialized()
  const key = normalize_bucket_id(bucket_id)
  return global_mutex.run(async () => {
    if (all_buckets.has(key)) return true
    await create_bucket_folder(key)
    const bucket_state = empty_bucket_state(key)
    all_buckets.add(key)
    await write_bucket_ids_file()
    await persist_bucket_state(key, bucket_state)
    return true
  })
}

async function has_bucket(bucket_id) {
  await ensure_storage_initialized()
  const key = normalize_bucket_id(bucket_id)
  return all_buckets.has(key)
}

async function delete_bucket(bucket_id) {
  await ensure_storage_initialized()
  const key = normalize_bucket_id(bucket_id)
  return global_mutex.run(async () => {
    const bucket_path = get_bucket_folder(key)
    all_buckets.delete(key)
    await write_bucket_ids_file()
    await fs.rm(bucket_path, { recursive: true, force: true })
    lru_cached_buckets.map.delete(key)
    return true
  })
}

async function create_bucket_and_load_it_into_memory(bucket_id) {
  await create_bucket(bucket_id)
  return load_bucket_into_memory(bucket_id)
}

async function execute_plan(bucket_id, plan) {
    let start = performance.now()
    const result = await _execute_plan(bucket_id, plan);
    console.log('plan execution took', performance.now() - start)
    return result;
}

async function _execute_plan(bucket_id, plan) {
  const bucket_state = await load_bucket_into_memory(bucket_id)
  const lock = get_bucket_lock(bucket_id)
  return lock.run(async () => {
    bucket_state.last_accessed_at = Date.now()
    if (plan.type === 'begin') {
      if (bucket_state.transaction_state) throw new Error('transaction already active')
      bucket_state.transaction_state = { snapshot: clone_bucket_state(bucket_state) }
      return serialize_command_result({ status: 'ok', message: 'transaction started', affected_rows: 0 })
    }
    if (plan.type === 'commit') {
      if (!bucket_state.transaction_state) throw new Error('no active transaction')
      bucket_state.transaction_state = null
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: 'transaction committed', affected_rows: 0 })
    }
    if (plan.type === 'rollback') {
      if (!bucket_state.transaction_state) throw new Error('no active transaction')
      const restored = bucket_state.transaction_state.snapshot
      bucket_state.tables = restored.tables
      bucket_state.transaction_state = null
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: 'transaction rolled back', affected_rows: 0 })
    }
    if (plan.type === 'create_table') {
      if (bucket_state.tables.has(plan.table_name)) throw new Error(`table already exists: ${plan.table_name}`)
      const columns = plan.columns.map((col) => ({
        column_name: col.column_name,
        column_type: String(col.column_type || 'text').toLowerCase(),
        nullable: col.nullable !== false,
        primary_key: !!col.primary_key,
        unique: !!col.unique,
        default_value: col.has_default ? clone_value(col.default_value) : undefined,
        has_default: !!col.has_default,
      }))
      if (columns.filter((col) => col.primary_key).length > 1) throw new Error('only one primary key is supported')
      bucket_state.tables.set(plan.table_name, {
        table_name: plan.table_name,
        columns,
        rows: [],
        auto_increment_next: 1,
      })
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: `table created: ${plan.table_name}`, affected_rows: 0 })
    }
    if (plan.type === 'drop_table') {
      if (!bucket_state.tables.has(plan.table_name)) throw new Error(`table not found: ${plan.table_name}`)
      bucket_state.tables.delete(plan.table_name)
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: `table dropped: ${plan.table_name}`, affected_rows: 0 })
    }
    if (plan.type === 'alter_table_add_column') {
      const table_data = get_table(bucket_state, plan.table_name)
      if (table_has_column(table_data, plan.column_def.column_name)) throw new Error(`column already exists: ${plan.column_def.column_name}`)
      const column_def = {
        column_name: plan.column_def.column_name,
        column_type: String(plan.column_def.column_type || 'text').toLowerCase(),
        nullable: plan.column_def.nullable !== false,
        primary_key: !!plan.column_def.primary_key,
        unique: !!plan.column_def.unique,
        default_value: plan.column_def.has_default ? clone_value(plan.column_def.default_value) : undefined,
        has_default: !!plan.column_def.has_default,
      }
      table_data.columns.push(column_def)
      for (const row of table_data.rows) row.push(column_def.has_default ? clone_value(column_def.default_value) : null)
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: `column added: ${column_def.column_name}`, affected_rows: 0 })
    }
    if (plan.type === 'insert') {
      const table_data = get_table(bucket_state, plan.table_name)
      const affected_rows = []
      const input_columns = plan.columns || table_data.columns.map((col) => col.column_name)
      for (const row_exprs of plan.rows) {
        const row_object = {}
        if (plan.columns) {
          if (plan.columns.length !== row_exprs.length) throw new Error('insert column count does not match values count')
          for (let i = 0; i < plan.columns.length; i += 1) row_object[input_columns[i]] = eval_expr(row_exprs[i], { row: {}, row_index: -1 }, null)
        } else {
          if (table_data.columns.length !== row_exprs.length) throw new Error('insert value count does not match table column count')
          for (let i = 0; i < table_data.columns.length; i += 1) row_object[table_data.columns[i].column_name] = eval_expr(row_exprs[i], { row: {}, row_index: -1 }, null)
        }
        const row = object_to_row(table_data, row_object)
        validate_row_constraints(table_data, row)
        table_data.rows.push(row)
        affected_rows.push(row_to_object(table_data, row))
      }
      if (table_data.columns.some((col) => col.primary_key)) {
        const primary_index = table_data.columns.findIndex((col) => col.primary_key)
        if (primary_index >= 0) {
          const values = new Set()
          for (const row of table_data.rows) {
            const value = row[primary_index]
            if (values.has(value)) throw new Error('duplicate primary key')
            values.add(value)
          }
        }
      }
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: 'rows inserted', affected_rows: affected_rows.length, inserted_rows: affected_rows })
    }
    if (plan.type === 'update') {
      const table_data = get_table(bucket_state, plan.table_name)
      const rows = build_row_contexts(table_data)
      const filtered = apply_where_filter(rows, plan.where_expr)
      let affected = 0
      for (const row_ctx of filtered) {
        const row_index = row_ctx.row_index
        const current_row = table_data.rows[row_index]
        const updated = [...current_row]
        const row_object = row_to_object(table_data, updated)
        const eval_ctx = { row: row_object, row_index }
        for (const assignment of plan.assignments) {
          const col_idx = get_column_index(table_data, assignment.column_name)
          updated[col_idx] = cast_scalar_value(table_data.columns[col_idx], eval_expr(assignment.value_expr, eval_ctx, null))
          row_object[assignment.column_name] = updated[col_idx]
        }
        validate_row_constraints(table_data, updated, row_index)
        table_data.rows[row_index] = updated
        affected += 1
      }
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: 'rows updated', affected_rows: affected })
    }
    if (plan.type === 'delete') {
      const table_data = get_table(bucket_state, plan.table_name)
      const rows = build_row_contexts(table_data)
      const filtered = apply_where_filter(rows, plan.where_expr)
      const delete_indexes = new Set(filtered.map((r) => r.row_index))
      const new_rows = []
      for (let i = 0; i < table_data.rows.length; i += 1) {
        if (!delete_indexes.has(i)) new_rows.push(table_data.rows[i])
      }
      const affected = table_data.rows.length - new_rows.length
      table_data.rows = new_rows
      bucket_state.dirty = true
      await persist_bucket_state(bucket_id, bucket_state)
      return serialize_command_result({ status: 'ok', message: 'rows deleted', affected_rows: affected })
    }
    if (plan.type === 'select') {
      const table_data = get_table(bucket_state, plan.table_name)
      let rows = build_row_contexts(table_data)
      rows = apply_where_filter(rows, plan.where_expr)
      const select_contains_aggregate = plan.select_items.some((item) => item.type === 'expr' && expr_contains_aggregate(item.expr)) || plan.group_by.length > 0 || !!plan.having_expr
      const select_contains_window = plan.select_items.some((item) => item.type === 'expr' && expr_contains_window(item.expr))
      let output_rows = []
      let output_columns = []
      if (select_contains_aggregate || plan.group_by.length > 0) {
        const groups = group_rows(rows, plan.group_by)
        for (const group of groups) {
          const representative = group.rows[0] || { row: {} }
          const group_ctx = { rows: group.rows }
          const having_ok = plan.having_expr ? truthy(eval_expr(plan.having_expr, representative, group_ctx)) : true
          if (!having_ok) continue
          const out_row = []
          for (const item of plan.select_items) {
            if (item.type === 'star') {
              const star_row = representative.row || {}
              for (const column of table_data.columns) out_row.push(clone_value(star_row[column.column_name]))
              continue
            }
            const value = eval_select_item(item, representative, group_ctx)
            out_row.push(value)
          }
          output_rows.push(out_row)
        }
      } else {
        output_rows = rows.map((row_ctx) => plan.select_items.flatMap((item) => item.type === 'star' ? table_data.columns.map((col) => clone_value(row_ctx.row[col.column_name])) : [eval_select_item(item, row_ctx, null)]))
      }

      output_columns = infer_select_columns(plan.select_items, table_data)

      if (select_contains_window) {
        output_rows = evaluate_window_output_rows(plan, output_rows, output_columns, rows, table_data)
      }
      const result_rows_objects = output_rows.map((row) => {
        const obj = {}
        for (let i = 0; i < output_columns.length; i += 1) obj[output_columns[i].column_name] = row[i]
        return obj
      })
      if (plan.order_by && plan.order_by.length) {
        result_rows_objects.sort((a, b) => {
          for (const order_item of plan.order_by) {
            const left = eval_expr(order_item.expr, { row: a, row_index: -1 }, null)
            const right = eval_expr(order_item.expr, { row: b, row_index: -1 }, null)
            const cmp = compare_values(left, right)
            if (cmp !== 0) return order_item.direction === 'desc' ? -cmp : cmp
          }
          return 0
        })
      }
      let sliced_rows = result_rows_objects
      if (typeof plan.offset === 'number' && plan.offset > 0) sliced_rows = sliced_rows.slice(plan.offset)
      if (typeof plan.limit === 'number' && plan.limit >= 0) sliced_rows = sliced_rows.slice(0, plan.limit)
      const final_rows = sliced_rows.map((row) => output_columns.map((col) => row[col.column_name]))
      return serialize_query_result({ columns: output_columns, rows: final_rows })
    }
    if (plan.type === 'create_bucket') {
      await create_bucket(plan.bucket_id)
      return serialize_command_result({ status: 'ok', message: `bucket created: ${plan.bucket_id}`, affected_rows: 0 })
    }
    if (plan.type === 'delete_bucket') {
      await delete_bucket(plan.bucket_id)
      return serialize_command_result({ status: 'ok', message: `bucket deleted: ${plan.bucket_id}`, affected_rows: 0 })
    }
    if (plan.type === 'has_bucket') {
      const exists = await has_bucket(plan.bucket_id)
      return serialize_command_result({ status: 'ok', message: String(exists), affected_rows: 0, extra: exists })
    }
    throw new Error(`unsupported plan: ${plan.type}`)
  })
}

function infer_select_columns(select_items, table_data) {
  const columns = []
  let expr_index = 0
  for (const item of select_items) {
    if (item.type === 'star') {
      for (const column of table_data.columns) columns.push({ column_name: column.column_name, column_type: column.column_type })
      continue
    }
    const alias = item.alias || infer_alias(item.expr, expr_index)
    columns.push({ column_name: alias, column_type: 'text' })
    expr_index += 1
  }
  return columns
}

function infer_alias(expr, expr_index) {
  if (expr.type === 'identifier') return expr.name.split('.').pop()
  if (expr.type === 'call') return expr.name.toLowerCase()
  return `expr_${expr_index + 1}`
}

function eval_select_item(item, row_ctx, group_ctx) {
  if (item.type === 'star') throw new Error('star item must be expanded before evaluation')
  const expr = item.expr
  if (expr.type === 'call' && expr.over_clause) {
    return null
  }
  return eval_expr(expr, row_ctx, group_ctx)
}

function evaluate_window_output_rows(plan, output_rows, output_columns, source_rows, table_data) {
  const row_objects = output_rows.map((row) => {
    const obj = {}
    for (let i = 0; i < output_columns.length; i += 1) obj[output_columns[i].column_name] = row[i]
    return obj
  })
  const decorated = row_objects.map((row, index) => ({ row, index }))
  const window_items = plan.select_items
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => item.type === 'expr' && item.expr.type === 'call' && item.expr.over_clause)
  if (!window_items.length) return output_rows

  for (const { item, index } of window_items) {
    const fn_name = String(item.expr.name).toLowerCase()
    const partitions = new Map()
    for (let i = 0; i < decorated.length; i += 1) {
      const source_row = source_rows[i] || { row: decorated[i].row }
      const partition_key = item.expr.over_clause.partition_by.length
        ? JSON.stringify(item.expr.over_clause.partition_by.map((part_expr) => eval_expr(part_expr, source_row, null)))
        : '__all__'
      if (!partitions.has(partition_key)) partitions.set(partition_key, [])
      partitions.get(partition_key).push({ index: i, row_ctx: source_row })
    }
    for (const partition_rows of partitions.values()) {
      const ordered = item.expr.over_clause.order_by.length
        ? sort_rows(partition_rows.map((part) => part.row_ctx), item.expr.over_clause.order_by).map((row_ctx) => ({ row_ctx }))
        : partition_rows.map((part) => ({ row_ctx: part.row_ctx }))
      if (fn_name === 'row_number') {
        for (let i = 0; i < ordered.length; i += 1) {
          const target_row = ordered[i].row_ctx
          const target_index = source_rows.indexOf(target_row)
          if (target_index >= 0) row_objects[target_index][infer_alias(item.expr, index)] = i + 1
        }
        continue
      }
      if (fn_name === 'rank' || fn_name === 'dense_rank') {
        let current_rank = 1
        let current_dense_rank = 1
        let last_key = null
        for (let i = 0; i < ordered.length; i += 1) {
          const row_ctx = ordered[i].row_ctx
          const sort_key = JSON.stringify(item.expr.over_clause.order_by.map((order_item) => eval_expr(order_item.expr, row_ctx, null)))
          if (i === 0) {
            last_key = sort_key
          } else if (sort_key !== last_key) {
            current_dense_rank += 1
            current_rank = i + 1
            last_key = sort_key
          }
          const value = fn_name === 'rank' ? current_rank : current_dense_rank
          const target_index = source_rows.indexOf(row_ctx)
          if (target_index >= 0) row_objects[target_index][infer_alias(item.expr, index)] = value
        }
      }
    }
  }
  return row_objects.map((row) => output_columns.map((col) => row[col.column_name]))
}

async function run_command(bucket_id, command, args = []) {
  await ensure_storage_initialized()
  const tokenized = typeof command === 'string' ? tokenizer(command, args) : command
  const plan = typeof command === 'object' && command && command.type ? command : planner(tokenized)
  return execute_plan(bucket_id, plan)
}

async function execute(bucket_id, command, args = []) {
  try {
    return await run_command(bucket_id, command, args)
  } catch (error) {
    return serialize_error_result({ code: 'execute_error', message: error.message || String(error) })
  }
}
async function query(bucket_id, query_text, args = []) {
    const start = performance.now();
    const result = await _query(bucket_id, query_text, args);
    console.log('query took', performance.now() - start)
    return result;
}
async function _query(bucket_id, query_text, args = []) {
  try {
    return await run_command(bucket_id, query_text, args)
  } catch (error) {
    return serialize_error_result({ code: 'query_error', message: error.message || String(error) })
  }
}

// eager bootstrap for environments that support top-level async work through module import
ensure_storage_initialized().catch(() => {})

export {
  execute,
  query,
  create_bucket,
  has_bucket,
  delete_bucket,
  deserialize_to_json,
  tokenizer,
  planner,
  create_bucket_folder,
  get_bucket_folder,
  load_bucket_into_memory,
  create_bucket_and_load_it_into_memory,
  lru_cached_buckets,
  all_buckets,
  all_buckets_locks,
  all_buckets_buffer,
  data_folder_path,
  data_buffer_global_operations,
  data_bucket_ids,
}
