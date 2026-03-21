import { bucket_db_error } from '../core/errors.js'
import { clone_value } from '../utils/clone.js'
import { evaluate_aggregate, is_aggregate_function } from './functions/aggregates.js'
import { apply_window_functions, is_window_function } from './functions/window.js'

function compare_values(a, b) {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  return String(a).localeCompare(String(b))
}

function truthy(value) {
  return !(value === null || value === undefined || value === false || value === 0 || value === '')
}

function get_column_value(table, row, name) {
  const short_name = String(name).split('.').pop()
  const index = table.columns.findIndex((col) => col.name === short_name || col.name === name)
  if (index < 0) return null
  return row[index]
}

export function eval_expr(expr, row_ctx, group_ctx = null) {
  if (!expr) return null

  if (expr.type === 'literal') return clone_value(expr.value)
  if (expr.type === 'identifier') return get_column_value(row_ctx.table, row_ctx.row, expr.name)
  if (expr.type === 'star') return '*'

  if (expr.type === 'unary') {
    const v = eval_expr(expr.operand, row_ctx, group_ctx)
    if (expr.operator === 'neg') return v === null ? null : -Number(v)
    if (expr.operator === 'not') return !truthy(v)
    throw new bucket_db_error(`unknown unary operator ${expr.operator}`, 'executor_error')
  }

  if (expr.type === 'binary') {
    const left = eval_expr(expr.left, row_ctx, group_ctx)
    const right = eval_expr(expr.right, row_ctx, group_ctx)
    switch (expr.operator) {
      case 'or': return truthy(left) || truthy(right)
      case 'and': return truthy(left) && truthy(right)
      case 'eq': return compare_values(left, right) === 0
      case 'ne': return compare_values(left, right) !== 0
      case 'lt': return compare_values(left, right) < 0
      case 'gt': return compare_values(left, right) > 0
      case 'le': return compare_values(left, right) <= 0
      case 'ge': return compare_values(left, right) >= 0
      case 'is_null': return left === null || left === undefined
      case 'is_not_null': return !(left === null || left === undefined)
      case 'add': return left === null || right === null ? null : Number(left) + Number(right)
      case 'sub': return left === null || right === null ? null : Number(left) - Number(right)
      case 'mul': return left === null || right === null ? null : Number(left) * Number(right)
      case 'div': return left === null || right === null ? null : Number(right) === 0 ? null : Number(left) / Number(right)
      case 'mod': return left === null || right === null ? null : Number(left) % Number(right)
      default:
        throw new bucket_db_error(`unknown operator ${expr.operator}`, 'executor_error')
    }
  }

  if (expr.type === 'call') {
    const fn_name = String(expr.name).toLowerCase()
    if (expr.over_clause) throw new bucket_db_error('window function must be handled by projection stage', 'executor_error')
    if (is_aggregate_function(fn_name)) {
      if (!group_ctx) throw new bucket_db_error(`aggregate outside group: ${fn_name}`, 'executor_error')
      const arg = expr.args[0]
      return evaluate_aggregate(fn_name, arg, group_ctx.rows, (e, ctx) => eval_expr(e, ctx, group_ctx))
    }
    if (fn_name === 'coalesce') {
      for (const arg of expr.args) {
        const v = eval_expr(arg, row_ctx, group_ctx)
        if (v !== null && v !== undefined) return v
      }
      return null
    }
    if (fn_name === 'row_number') return null
    if (fn_name === 'rank' || fn_name === 'dense_rank') return null
    throw new bucket_db_error(`unsupported function ${fn_name}`, 'executor_error')
  }

  throw new bucket_db_error(`unknown expression type ${expr.type}`, 'executor_error')
}

function row_matches_where(table, row, where_expr) {
  if (!where_expr) return true
  return truthy(eval_expr(where_expr, { table, row }, null))
}

function infer_alias(expr, index) {
  if (expr.type === 'identifier') return expr.name.split('.').pop()
  if (expr.type === 'call') return expr.name.toLowerCase()
  return `expr_${index + 1}`
}

function build_result_columns(table, plan) {
  const cols = []
  for (let i = 0; i < plan.select_items.length; i += 1) {
    const item = plan.select_items[i]
    if (item.is_star) {
      for (const col of table.columns) cols.push({ name: col.name, type_name: col.type_name })
    } else {
      cols.push({ name: item.alias || infer_alias(item.expr, i), type_name: 'text' })
    }
  }
  return cols
}

function project_row(table, plan, row, row_index, group_rows = null) {
  const out = []
  for (const item of plan.select_items) {
    if (item.is_star) {
      for (const cell of row) out.push(clone_value(cell))
      continue
    }
    const expr = item.expr
    if (expr.type === 'call' && expr.over_clause) {
      out.push(null)
      continue
    }
    if (expr.type === 'call' && is_aggregate_function(String(expr.name).toLowerCase())) {
      out.push(evaluate_aggregate(String(expr.name).toLowerCase(), expr.args[0], group_rows, (e, ctx) => eval_expr(e, ctx, { rows: group_rows })))
      continue
    }
    out.push(eval_expr(expr, { table, row, row_index }, null))
  }
  return out
}


function group_rows(table, filtered_rows, plan) {
  if (!plan.group_by || !plan.group_by.length) {
    return [{ key: '__all__', rows: filtered_rows }]
  }
  return [...Object.entries(Object.groupBy(filtered_rows, (row_ctx) => {
    return plan.group_by.map((expr) => eval_expr(expr, row_ctx, null)).join('\u0001')
  }))].map(([key, rows]) => ({ key, rows }));
}

function rows_from_group(table, plan, group, row_index, select_columns) {
  const representative = group.rows[0]
  const out = []
  for (const item of plan.select_items) {
    if (item.is_star) {
      for (const cell of representative.row) out.push(clone_value(cell))
      continue
    }
    const expr = item.expr
    if (expr.type === 'call' && is_aggregate_function(String(expr.name).toLowerCase())) {
      out.push(evaluate_aggregate(String(expr.name).toLowerCase(), expr.args[0], group.rows, (e, ctx) => eval_expr(e, ctx, { rows: group.rows })))
      continue
    }
    out.push(eval_expr(expr, { table, row: representative.row, row_index }, { rows: group.rows }))
  }
  return out
}

function sort_result_rows(query_rows, order_by, columns) {
  if (!order_by || !order_by.length) return query_rows
  return [...query_rows].sort((a, b) => {
    for (const item of order_by) {
      const ai = columns.findIndex((c) => c.name === item.alias || c.name === item.name)
      const av = ai >= 0 ? a[ai] : null
      const bv = ai >= 0 ? b[ai] : null
      const cmp = compare_values(av, bv)
      if (cmp !== 0) return item.desc ? -cmp : cmp
    }
    return 0
  })
}

function get_order_value(expr, row_obj, table) {
  if (expr.type === 'identifier') return row_obj[expr.name.split('.').pop()]
  if (expr.type === 'literal') return expr.value
  return null
}

function apply_window_columns(table, plan, base_rows, projected_rows, row_ctxs) {
  const window_items = plan.select_items.filter((item) => item.expr && item.expr.type === 'call' && item.expr.over_clause)
  if (!window_items.length) return projected_rows

  const rows_by_group = new Map()
  for (let i = 0; i < row_ctxs.length; i += 1) {
    const row_ctx = row_ctxs[i]
    for (const item of window_items) {
      const partition_key = item.expr.over_clause.partition_by.length
        ? JSON.stringify(item.expr.over_clause.partition_by.map((expr) => eval_expr(expr, row_ctx, null)))
        : '__all__'
      if (!rows_by_group.has(partition_key)) rows_by_group.set(partition_key, [])
      rows_by_group.get(partition_key).push({ row_ctx, index: i })
    }
  }

  for (const item of window_items) {
    const alias = item.alias || String(item.expr.name).toLowerCase()
    const fn_name = String(item.expr.name).toLowerCase()
    const group_map = new Map()
    for (let i = 0; i < row_ctxs.length; i += 1) {
      const row_ctx = row_ctxs[i]
      const partition_key = item.expr.over_clause.partition_by.length
        ? JSON.stringify(item.expr.over_clause.partition_by.map((expr) => eval_expr(expr, row_ctx, null)))
        : '__all__'
      if (!group_map.has(partition_key)) group_map.set(partition_key, [])
      group_map.get(partition_key).push({ row_ctx, index: i })
    }

    for (const partition_rows of group_map.values()) {
      const ordered = item.expr.over_clause.order_by.length
        ? [...partition_rows].sort((a, b) => {
            for (const order_expr of item.expr.over_clause.order_by) {
              const av = get_order_value(order_expr, a.row_ctx.row_obj, table)
              const bv = get_order_value(order_expr, b.row_ctx.row_obj, table)
              const cmp = compare_values(av, bv)
              if (cmp !== 0) return cmp
            }
            return 0
          })
        : partition_rows

      let current_rank = 1
      let current_dense_rank = 1
      let previous_key = null

      for (let i = 0; i < ordered.length; i += 1) {
        const item_row = ordered[i]
        let value = i + 1
        if (fn_name === 'rank' || fn_name === 'dense_rank') {
          const sort_key = JSON.stringify(item.expr.over_clause.order_by.map((order_expr) => eval_expr(order_expr, item_row.row_ctx, null)))
          if (i === 0) {
            previous_key = sort_key
          } else if (sort_key !== previous_key) {
            current_dense_rank += 1
            current_rank = i + 1
            previous_key = sort_key
          }
          value = fn_name === 'rank' ? current_rank : current_dense_rank
        }
        projected_rows[item_row.index][alias] = value
      }
    }
  }
  return projected_rows
}

export function execute_plan(bucket_state, plan) {
  const tables = bucket_state.tables
  if (plan.type === 'create_table') {
    if (tables.has(plan.table_name)) throw new bucket_db_error(`table exists: ${plan.table_name}`, 'executor_error')
    const columns = plan.columns.map((column) => ({ ...column }))
    if (columns.filter((c) => c.primary_key).length > 1) throw new bucket_db_error('only one primary key supported', 'executor_error')
    tables.set(plan.table_name, { table_name: plan.table_name, columns, rows: [], auto_increment_next: 1 })
    return { kind: 'command', status: 'ok', message: 'table created', affected_rows: 0 }
  }

  if (plan.type === 'drop_table') {
    if (!tables.has(plan.table_name)) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')
    tables.delete(plan.table_name)
    return { kind: 'command', status: 'ok', message: 'table dropped', affected_rows: 0 }
  }

  if (plan.type === 'alter_table_add_column') {
    const table = tables.get(plan.table_name)
    if (!table) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')
    const column = { ...plan.column }
    if (table.columns.some((c) => c.name === column.name)) throw new bucket_db_error(`column exists: ${column.name}`, 'executor_error')
    table.columns.push(column)
    for (const row of table.rows) row.push(column.has_default ? clone_value(column.default_value) : null)
    return { kind: 'command', status: 'ok', message: 'column added', affected_rows: 0 }
  }

  if (plan.type === 'insert') {
    const table = tables.get(plan.table_name)
    if (!table) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')
    let affected = 0

    for (const row_exprs of plan.rows) {
      const row = new Array(table.columns.length).fill(null)
      const row_ctx = { table, row }
      const cols = plan.columns || table.columns.map((c) => c.name)
      if (plan.columns && plan.columns.length !== row_exprs.length) throw new bucket_db_error('insert column count mismatch', 'executor_error')
      if (!plan.columns && row_exprs.length !== table.columns.length) throw new bucket_db_error('insert value count mismatch', 'executor_error')

      for (let i = 0; i < row_exprs.length; i += 1) {
        const col_name = cols[i]
        const col_index = table.columns.findIndex((c) => c.name === col_name)
        if (col_index < 0) throw new bucket_db_error(`unknown column: ${col_name}`, 'executor_error')
        row[col_index] = eval_expr(row_exprs[i], row_ctx, null)
      }

      for (let i = 0; i < table.columns.length; i += 1) {
        const col = table.columns[i]
        if (row[i] === null || row[i] === undefined) {
          if (col.has_default) row[i] = clone_value(col.default_value)
          else if (!col.nullable) throw new bucket_db_error(`column not nullable: ${col.name}`, 'executor_error')
        }
      }

      for (const existing of table.rows) {
        for (let i = 0; i < table.columns.length; i += 1) {
          const col = table.columns[i]
          if (col.primary_key || col.unique) {
            if (compare_values(existing[i], row[i]) === 0) throw new bucket_db_error(`duplicate value for unique column: ${col.name}`, 'executor_error')
          }
        }
      }

      table.rows.push(row)
      affected += 1
    }

    return { kind: 'command', status: 'ok', message: 'rows inserted', affected_rows: affected }
  }

  if (plan.type === 'update') {
    const table = tables.get(plan.table_name)
    if (!table) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')
    let affected = 0
    for (let r = 0; r < table.rows.length; r += 1) {
      const row = table.rows[r]
      const row_ctx = { table, row, row_index: r }
      if (!row_matches_where(table, row, plan.where_expr)) continue
      for (const assignment of plan.assignments) {
        const index = table.columns.findIndex((c) => c.name === assignment.column_name)
        if (index < 0) throw new bucket_db_error(`unknown column: ${assignment.column_name}`, 'executor_error')
        row[index] = eval_expr(assignment.expr, row_ctx, null)
      }
      affected += 1
    }
    return { kind: 'command', status: 'ok', message: 'rows updated', affected_rows: affected }
  }

  if (plan.type === 'delete') {
    const table = tables.get(plan.table_name)
    if (!table) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')
    const remaining = []
    let affected = 0
    for (let r = 0; r < table.rows.length; r += 1) {
      const row = table.rows[r]
      if (row_matches_where(table, row, plan.where_expr)) {
        affected += 1
      } else {
        remaining.push(row)
      }
    }
    table.rows = remaining
    return { kind: 'command', status: 'ok', message: 'rows deleted', affected_rows: affected }
  }

  if (plan.type === 'select') {
    const table = tables.get(plan.table_name)
    if (!table) throw new bucket_db_error(`table not found: ${plan.table_name}`, 'executor_error')

    const row_ctxs = table.rows
      .filter((row) => row_matches_where(table, row, plan.where_expr))
      .map((row) => {
        const row_obj = {}
        table.columns.forEach((col, idx) => { row_obj[col.name] = row[idx] })
        return ({ table, row, row_obj })
      })
    
    /*const row_ctxs = [];
    for (let r = 0; r < table.rows.length; r += 1) {
      const row = table.rows[r]
      if (row_matches_where(table, row, plan.where_expr)) {
        const row_obj = {}
        table.columns.forEach((col, idx) => { row_obj[col.name] = row[idx] })
        row_ctxs.push({ table, row, row_index: r, row_obj })
      }
    }*/

    const has_grouping = (plan.group_by && plan.group_by.length > 0) || plan.select_items.some((item) => item.expr && item.expr.type === 'call' && is_aggregate_function(String(item.expr.name).toLowerCase())) || !!plan.having_expr
    const columns = build_result_columns(table, plan)
    const result_rows = []

    if (has_grouping) {
      const groups = group_rows(table, row_ctxs, plan)
      for (const group of groups) {
        const representative = group.rows[0]
        if (!representative) continue
        if (plan.having_expr) {
          const hv = eval_expr(plan.having_expr, { table, row: representative.row, row_index: representative.row_index }, { rows: group.rows })
          if (!truthy(hv)) continue
        }
        const out_row = []
        for (const item of plan.select_items) {
          if (item.is_star) {
            for (const cell of representative.row) out_row.push(clone_value(cell))
          } else if (item.expr.type === 'call' && is_aggregate_function(String(item.expr.name).toLowerCase())) {
            out_row.push(evaluate_aggregate(String(item.expr.name).toLowerCase(), item.expr.args[0], group.rows, (e, ctx) => eval_expr(e, ctx, { rows: group.rows })))
          } else {
            out_row.push(eval_expr(item.expr, { table, row: representative.row, row_index: representative.row_index }, { rows: group.rows }))
          }
        }
        result_rows.push(out_row)
      }
    } else {
      for (const row_ctx of row_ctxs) {
        const out_row = []
        for (const item of plan.select_items) {
          if (item.is_star) {
            for (const cell of row_ctx.row) out_row.push(clone_value(cell))
          } else if (item.expr.type === 'call' && item.expr.over_clause) {
            out_row.push(null)
          } else {
            out_row.push(eval_expr(item.expr, { table, row: row_ctx.row, row_index: row_ctx.row_index }, null))
          }
        }
        result_rows.push(out_row)
      }
    }

    let projected_rows = result_rows.map((row) => {
      const obj = {}
      for (let i = 0; i < columns.length; i += 1) obj[columns[i].name] = row[i]
      return obj
    })

    const has_window = plan.select_items.some((item) => item.expr && item.expr.type === 'call' && item.expr.over_clause)
    if (has_window) {
      projected_rows = apply_window_functions(
        projected_rows,
        plan.select_items,
        (expr, row_ctx, group_ctx) => eval_expr(expr, row_ctx, group_ctx),
        table
      )
    }

    if (plan.order_by && plan.order_by.length) {
      projected_rows.sort((a, b) => {
        for (const order of plan.order_by) {
          const av = eval_expr(order.expr, { table, row: columns.map((c) => a[c.name]), row_obj: a }, null)
          const bv = eval_expr(order.expr, { table, row: columns.map((c) => b[c.name]), row_obj: b }, null)
          const cmp = compare_values(av, bv)
          if (cmp !== 0) return order.desc ? -cmp : cmp
        }
        return 0
      })
    }

    if (typeof plan.offset === 'number' && plan.offset > 0) projected_rows = projected_rows.slice(plan.offset)
    if (typeof plan.limit === 'number') projected_rows = projected_rows.slice(0, plan.limit)

    const rows = projected_rows.map((row_obj) => columns.map((col) => row_obj[col.name] ?? null))
    return { kind: 'query', columns, rows }
  }

  if (plan.type === 'begin' || plan.type === 'commit' || plan.type === 'rollback') {
    return { kind: 'command', status: 'ok', message: plan.type, affected_rows: 0 }
  }

  if (plan.type === 'create_bucket' || plan.type === 'delete_bucket' || plan.type === 'has_bucket') {
    return { kind: 'command', status: 'ok', message: plan.type, affected_rows: 0 }
  }

  throw new bucket_db_error(`unsupported plan: ${plan.type}`, 'executor_error')
}
