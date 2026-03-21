export class lru_cache_buckets_data {
  constructor(max_size_mb = 64) {
    this.max_size_bytes = Math.max(1, Number(max_size_mb) || 64) * 1024 * 1024
    this.map = new Map()
    this.total_size_bytes = 0
  }

  get_bucket(id) {
    const bucket = this.map.get(id)
    if (!bucket) return null
    this.map.delete(id)
    this.map.set(id, bucket)
    bucket.last_accessed_at = Date.now()
    return bucket
  }

  put_bucket(id, bucket_data, estimated_size_bytes = 1024) {
    const existing = this.map.get(id)
    if (existing) this.total_size_bytes -= existing._estimated_size_bytes || 0
    bucket_data._estimated_size_bytes = estimated_size_bytes
    this.map.set(id, bucket_data)
    this.total_size_bytes += estimated_size_bytes
  }

  delete_bucket(id) {
    const existing = this.map.get(id)
    if (!existing) return
    this.total_size_bytes -= existing._estimated_size_bytes || 0
    this.map.delete(id)
  }
}
