import fs from 'fs/promises'
import path from 'path'
import { bucket_root_path, snapshot_magic, storage_version } from '../config/constants.js'
import { binary_reader, binary_writer } from '../utils/binary.js'
import { deep_clone } from '../utils/binary.js'

async function ensure_dir(dir_path) {
  await fs.mkdir(dir_path, { recursive: true })
}

function bucket_folder(bucket_id) {
  return path.join(bucket_root_path, bucket_id)
}

function bucket_snapshot_file(bucket_id) {
  return path.join(bucket_folder(bucket_id), 'bucket_snapshot.bin')
}

function serialize_cell(writer, value) {
  writer.push_value(value)
}

function deserialize_cell(reader) {
  return reader.read_value()
}

export async function save_bucket_state(bucket_id, bucket_state) {
  await ensure_dir(bucket_root_path)
  await ensure_dir(bucket_folder(bucket_id))

  const writer = new binary_writer()
  writer.push_string(snapshot_magic)
  writer.push_u32(storage_version)
  writer.push_u32(bucket_state.tables.size)

  for (const table of bucket_state.tables.values()) {
    writer.push_string(table.table_name)
    writer.push_u32(table.columns.length)
    writer.push_u32(table.rows.length)
    for (const column of table.columns) {
      writer.push_string(column.name)
      writer.push_string(column.type_name)
      writer.push_u8(column.nullable ? 1 : 0)
      writer.push_u8(column.primary_key ? 1 : 0)
      writer.push_u8(column.unique ? 1 : 0)
      writer.push_u8(column.has_default ? 1 : 0)
      if (column.has_default) writer.push_value(column.default_value)
    }
    for (const row of table.rows) {
      for (const cell of row) serialize_cell(writer, cell)
    }
  }

  await fs.writeFile(bucket_snapshot_file(bucket_id), writer.to_uint8array())
}

export async function load_bucket_state(bucket_id) {
  await ensure_dir(bucket_root_path)
  await ensure_dir(bucket_folder(bucket_id))
  const file_path = bucket_snapshot_file(bucket_id)
  try {
    const raw = await fs.readFile(file_path)
    const reader = new binary_reader(raw)
    const magic = reader.read_string()
    const version = reader.read_u32()
    if (magic !== snapshot_magic || version !== storage_version) {
      return { tables: new Map() }
    }
    const table_count = reader.read_u32()
    const tables = new Map()
    for (let ti = 0; ti < table_count; ti += 1) {
      const table_name = reader.read_string()
      const column_count = reader.read_u32()
      const row_count = reader.read_u32()
      const columns = []
      for (let ci = 0; ci < column_count; ci += 1) {
        const name = reader.read_string()
        const type_name = reader.read_string()
        const nullable = !!reader.read_u8()
        const primary_key = !!reader.read_u8()
        const unique = !!reader.read_u8()
        const has_default = !!reader.read_u8()
        const default_value = has_default ? reader.read_value() : null
        columns.push({ name, type_name, nullable, primary_key, unique, has_default, default_value })
      }
      const rows = []
      for (let ri = 0; ri < row_count; ri += 1) {
        const row = []
        for (let ci = 0; ci < column_count; ci += 1) row.push(deserialize_cell(reader))
        rows.push(row)
      }
      tables.set(table_name, { table_name, columns, rows, auto_increment_next: 1 })
    }
    return { tables }
  } catch {
    return { tables: new Map() }
  }
}

export function bucket_path(bucket_id) {
  return bucket_folder(bucket_id)
}

export function bucket_snapshot_path(bucket_id) {
  return bucket_snapshot_file(bucket_id)
}

export async function remove_bucket_state(bucket_id) {
  await fs.rm(bucket_folder(bucket_id), { recursive: true, force: true })
}
