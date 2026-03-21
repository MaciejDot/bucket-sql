import { clone_value } from './clone.js'

export class binary_writer {
  constructor() {
    this.chunks = []
  }

  push_u8(value) {
    const out = new Uint8Array(1)
    out[0] = value & 0xff
    this.chunks.push(out)
  }

  push_u32(value) {
    const out = new Uint8Array(4)
    new DataView(out.buffer).setUint32(0, value >>> 0, true)
    this.chunks.push(out)
  }

  push_i32(value) {
    const out = new Uint8Array(4)
    new DataView(out.buffer).setInt32(0, value | 0, true)
    this.chunks.push(out)
  }

  push_u64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setBigUint64(0, BigInt(value), true)
    this.chunks.push(out)
  }

  push_i64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setBigInt64(0, BigInt(value), true)
    this.chunks.push(out)
  }

  push_f64(value) {
    const out = new Uint8Array(8)
    new DataView(out.buffer).setFloat64(0, Number(value), true)
    this.chunks.push(out)
  }

  push_bytes(bytes) {
    const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
    this.push_u32(u8.length)
    this.chunks.push(u8)
  }

  push_raw_bytes(bytes) {
    const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
    this.chunks.push(u8)
  }

  push_string(value) {
    const bytes = new TextEncoder().encode(String(value))
    this.push_u32(bytes.length)
    this.chunks.push(bytes)
  }

  push_value(value) {
    if (value === null || value === undefined) {
      this.push_u8(0)
      return
    }
    if (typeof value === 'boolean') {
      this.push_u8(1)
      this.push_u8(value ? 1 : 0)
      return
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value) && Number.isSafeInteger(value)) {
        this.push_u8(2)
        this.push_i64(value)
      } else {
        this.push_u8(3)
        this.push_f64(value)
      }
      return
    }
    if (typeof value === 'bigint') {
      this.push_u8(2)
      this.push_i64(value)
      return
    }
    if (typeof value === 'string') {
      this.push_u8(4)
      this.push_string(value)
      return
    }
    if (value instanceof Uint8Array) {
      this.push_u8(5)
      this.push_u32(value.length)
      this.chunks.push(value)
      return
    }
    if (value instanceof Date) {
      this.push_u8(6)
      this.push_string(value.toISOString())
      return
    }
    if (Array.isArray(value)) {
      this.push_u8(7)
      this.push_u32(value.length)
      for (const item of value) this.push_value(item)
      return
    }
    if (typeof value === 'object') {
      const keys = Object.keys(value)
      this.push_u8(8)
      this.push_u32(keys.length)
      for (const key of keys) {
        this.push_string(key)
        this.push_value(value[key])
      }
      return
    }
    this.push_u8(4)
    this.push_string(String(value))
  }

  to_uint8array() {
    const total = this.chunks.reduce((sum, chunk) => sum + chunk.length, 0)
    const out = new Uint8Array(total)
    let offset = 0
    for (const chunk of this.chunks) {
      out.set(chunk, offset)
      offset += chunk.length
    }
    return out
  }
}

export class binary_reader {
  constructor(bytes) {
    this.bytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
    this.view = new DataView(this.bytes.buffer, this.bytes.byteOffset, this.bytes.byteLength)
    this.offset = 0
  }

  read_u8() {
    const value = this.view.getUint8(this.offset)
    this.offset += 1
    return value
  }

  read_u32() {
    const value = this.view.getUint32(this.offset, true)
    this.offset += 4
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

  read_string() {
    const len = this.read_u32()
    const start = this.offset
    const end = start + len
    this.offset = end
    return new TextDecoder().decode(this.bytes.slice(start, end))
  }

  read_bytes() {
    const len = this.read_u32()
    const start = this.offset
    const end = start + len
    this.offset = end
    return this.bytes.slice(start, end)
  }

  read_value() {
    const tag = this.read_u8()
    if (tag === 0) return null
    if (tag === 1) return this.read_u8() !== 0
    if (tag === 2) return this.read_i64()
    if (tag === 3) return this.read_f64()
    if (tag === 4) return this.read_string()
    if (tag === 5) return this.read_bytes()
    if (tag === 6) return new Date(this.read_string())
    if (tag === 7) {
      const count = this.read_u32()
      const out = []
      for (let i = 0; i < count; i += 1) out.push(this.read_value())
      return out
    }
    if (tag === 8) {
      const count = this.read_u32()
      const out = {}
      for (let i = 0; i < count; i += 1) {
        const key = this.read_string()
        out[key] = this.read_value()
      }
      return out
    }
    return null
  }
}

export function deep_clone(value) {
  return clone_value(value)
}
