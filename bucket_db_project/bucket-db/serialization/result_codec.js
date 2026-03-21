import { binary_reader, binary_writer } from '../utils/binary.js'
import { result_magic, storage_version } from '../config/constants.js'

export function encode_query_result(result) {
  const writer = new binary_writer()
  writer.push_string(result_magic)
  writer.push_u32(storage_version)
  writer.push_u8(1)
  writer.push_u32(result.columns.length)
  writer.push_u32(result.rows.length)
  for (const column of result.columns) {
    writer.push_string(column.name)
    writer.push_string(column.type_name || 'text')
  }
  for (const row of result.rows) {
    for (const cell of row) writer.push_value(cell)
  }
  return writer.to_uint8array()
}

export function encode_command_result(result) {
  const writer = new binary_writer()
  writer.push_string(result_magic)
  writer.push_u32(storage_version)
  writer.push_u8(2)
  writer.push_string(result.status || 'ok')
  writer.push_string(result.message || '')
  writer.push_u32(result.affected_rows || 0)
  writer.push_value(result.extra ?? null)
  return writer.to_uint8array()
}

export function encode_error_result(error) {
  const writer = new binary_writer()
  writer.push_string(result_magic)
  writer.push_u32(storage_version)
  writer.push_u8(3)
  writer.push_string(error.code || 'error')
  writer.push_string(error.message || String(error))
  return writer.to_uint8array()
}

export function deserialize_to_json(raw_result) {
  const reader = new binary_reader(raw_result)
  const magic = reader.read_string()
  const version = reader.read_u32()
  const kind = reader.read_u8()
  if (magic !== result_magic || version !== storage_version) throw new Error('invalid result buffer')
  if (kind === 1) {
    const column_count = reader.read_u32()
    const row_count = reader.read_u32()
    const columns = []
    for (let i = 0; i < column_count; i += 1) {
      columns.push({ name: reader.read_string(), type_name: reader.read_string() })
    }
    const rows = []
    for (let r = 0; r < row_count; r += 1) {
      const row = {}
      for (const column of columns) row[column.name] = reader.read_value()
      rows.push(row)
    }
    return { status: 'ok', kind: 'query', columns, rows }
  }
  if (kind === 2) {
    return {
      status: reader.read_string(),
      kind: 'command',
      message: reader.read_string(),
      affected_rows: reader.read_u32(),
      extra: reader.read_value(),
    }
  }
  if (kind === 3) {
    return { status: 'error', code: reader.read_string(), message: reader.read_string() }
  }
  throw new Error('unknown result kind')
}
