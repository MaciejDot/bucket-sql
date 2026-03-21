export function is_window_function(name) {
  const lower = String(name).toLowerCase()
  return lower === 'row_number' || lower === 'rank' || lower === 'dense_rank'
}

export function apply_window_functions(rows, select_items, eval_expr, table) {
  const output = rows.map((row) => ({ ...row }))
  for (const item of select_items) {
    if (!item.expr || item.expr.type !== 'call' || !item.expr.over_clause) continue
    const fn_name = String(item.expr.name).toLowerCase()
    const alias = item.alias || fn_name
    const partitions = new Map()

    for (let i = 0; i < rows.length; i += 1) {
      const row_ctx = rows[i]
      const key = item.expr.over_clause.partition_by.length
        ? JSON.stringify(item.expr.over_clause.partition_by.map((e) => eval_expr(e, row_ctx, null)))
        : '__all__'
      if (!partitions.has(key)) partitions.set(key, [])
      partitions.get(key).push({ row_ctx, index: i })
    }

    for (const partition_rows of partitions.values()) {
      const ordered = item.expr.over_clause.order_by.length
        ? [...partition_rows].sort((a, b) => {
            for (const order_expr of item.expr.over_clause.order_by) {
              const va = eval_expr(order_expr, a.row_ctx, null)
              const vb = eval_expr(order_expr, b.row_ctx, null)
              if (va < vb) return -1
              if (va > vb) return 1
            }
            return 0
          })
        : partition_rows

      let current_rank = 1
      let current_dense_rank = 1
      let previous_key = null
      for (let i = 0; i < ordered.length; i += 1) {
        const current_row = ordered[i]
        let value = i + 1
        if (fn_name === 'row_number') value = i + 1
        if (fn_name === 'rank' || fn_name === 'dense_rank') {
          const sort_key = JSON.stringify(item.expr.over_clause.order_by.map((order_expr) => eval_expr(order_expr, current_row.row_ctx, null)))
          if (i === 0) {
            previous_key = sort_key
          } else if (sort_key !== previous_key) {
            current_dense_rank += 1
            current_rank = i + 1
            previous_key = sort_key
          }
          value = fn_name === 'rank' ? current_rank : current_dense_rank
        }
        output[current_row.index][alias] = value
      }
    }
  }
  return output
}
