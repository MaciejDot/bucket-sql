import { deep_clone } from '../../utils/binary.js'

export function is_aggregate_function(name) {
  const lower = String(name).toLowerCase()
  return lower === 'count' || lower === 'max' || lower === 'min' || lower === 'sum' || lower === 'avg'
}

export function evaluate_aggregate(name, arg_expr, row_contexts, eval_expr) {
  const lower = String(name).toLowerCase()
  if (lower === 'count') {
    if (!arg_expr || arg_expr.type === 'star') return row_contexts.length
    let count = 0
    for (const row_ctx of row_contexts) {
      const value = eval_expr(arg_expr, row_ctx, { rows: row_contexts })
      if (value !== null && value !== undefined) count += 1
    }
    return count
  }

  const values = []
  for (const row_ctx of row_contexts) {
    const value = eval_expr(arg_expr, row_ctx, { rows: row_contexts })
    if (value !== null && value !== undefined) values.push(value)
  }
  if (!values.length) return null

  if (lower === 'max') return values.reduce((a, b) => compare(a, b) >= 0 ? a : b)
  if (lower === 'min') return values.reduce((a, b) => compare(a, b) <= 0 ? a : b)
  if (lower === 'sum') return values.reduce((acc, v) => acc + Number(v), 0)
  if (lower === 'avg') return values.reduce((acc, v) => acc + Number(v), 0) / values.length
  return null
}

function compare(a, b) {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  return String(a).localeCompare(String(b))
}
