import { bucket_db_error } from '../core/errors.js'
import { literal_node, identifier_node, star_node, binary_node, unary_node, call_node } from './ast.js'

export function parse(tokens) {
  const p = new parser_state(tokens)
  return p.parse_statement()
}

class parser_state {
  constructor(tokens) {
    this.tokens = tokens
    this.pos = 0
  }

  current() {
    return this.tokens[this.pos]
  }

  next() {
    return this.tokens[this.pos++]
  }

  match(value) {
    const token = this.current()
    if (token && token.value === value) {
      this.pos += 1
      return true
    }
    return false
  }

  match_keyword(value) {
    const token = this.current()
    if (token && token.type === 'keyword' && token.value === value) {
      this.pos += 1
      return true
    }
    return false
  }

  expect(value) {
    const token = this.next()
    if (!token || token.value !== value) throw new bucket_db_error(`expected '${value}', got '${token ? token.value : 'eof'}'`, 'parse_error')
    return token
  }

  expect_keyword(value) {
    const token = this.next()
    if (!token || token.type !== 'keyword' || token.value !== value) throw new bucket_db_error(`expected keyword '${value}'`, 'parse_error')
    return token
  }

  parse_statement() {
    const token = this.current()
    if (!token) throw new bucket_db_error('empty command', 'parse_error')
    if (token.type === 'keyword' && token.value === 'create') return this.parse_create()
    if (token.type === 'keyword' && token.value === 'drop') return this.parse_drop()
    if (token.type === 'keyword' && token.value === 'alter') return this.parse_alter()
    if (token.type === 'keyword' && token.value === 'insert') return this.parse_insert()
    if (token.type === 'keyword' && token.value === 'update') return this.parse_update()
    if (token.type === 'keyword' && token.value === 'delete') return this.parse_delete()
    if (token.type === 'keyword' && token.value === 'select') return this.parse_select()
    if (token.type === 'keyword' && token.value === 'begin') return { type: 'begin' }
    if (token.type === 'keyword' && token.value === 'commit') return { type: 'commit' }
    if (token.type === 'keyword' && token.value === 'rollback') return { type: 'rollback' }
    if (token.type === 'identifier' && token.value === 'create_bucket') {
      this.next()
      return { type: 'create_bucket', bucket_id: this.parse_identifier_like() }
    }
    if (token.type === 'identifier' && token.value === 'delete_bucket') {
      this.next()
      return { type: 'delete_bucket', bucket_id: this.parse_identifier_like() }
    }
    if (token.type === 'identifier' && token.value === 'has_bucket') {
      this.next()
      return { type: 'has_bucket', bucket_id: this.parse_identifier_like() }
    }
    throw new bucket_db_error(`unsupported statement: ${token.value}`, 'parse_error')
  }

  parse_create() {
    this.expect_keyword('create')
    if (this.match_keyword('bucket')) {
      return { type: 'create_bucket', bucket_id: this.parse_identifier_like() }
    }
    this.expect_keyword('table')
    const table_name = this.parse_identifier_like()
    this.expect('(')
    const columns = []
    while (!this.match(')')) {
      columns.push(this.parse_column_def())
      this.match(',')
    }
    return { type: 'create_table', table_name, columns }
  }

  parse_drop() {
    this.expect_keyword('drop')
    this.expect_keyword('table')
    return { type: 'drop_table', table_name: this.parse_identifier_like() }
  }

  parse_alter() {
    this.expect_keyword('alter')
    this.expect_keyword('table')
    const table_name = this.parse_identifier_like()
    this.expect_keyword('add')
    this.match_keyword('column')
    const column = this.parse_column_def()
    return { type: 'alter_table_add_column', table_name, column }
  }

  parse_insert() {
    this.expect_keyword('insert')
    this.expect_keyword('into')
    const table_name = this.parse_identifier_like()
    let columns = null
    if (this.match('(')) {
      columns = []
      while (!this.match(')')) {
        columns.push(this.parse_identifier_like())
        this.match(',')
      }
    }
    this.expect_keyword('values')
    const rows = []
    do {
      this.expect('(')
      const row = []
      while (!this.match(')')) {
        row.push(this.parse_expression())
        this.match(',')
      }
      rows.push(row)
    } while (this.match(','))
    return { type: 'insert', table_name, columns, rows }
  }

  parse_update() {
    this.expect_keyword('update')
    const table_name = this.parse_identifier_like()
    this.expect_keyword('set')
    const assignments = []
    do {
      const column_name = this.parse_identifier_like()
      this.expect('=')
      assignments.push({ column_name, expr: this.parse_expression() })
    } while (this.match(','))
    let where_expr = null
    if (this.match_keyword('where')) where_expr = this.parse_expression()
    return { type: 'update', table_name, assignments, where_expr }
  }

  parse_delete() {
    this.expect_keyword('delete')
    this.expect_keyword('from')
    const table_name = this.parse_identifier_like()
    let where_expr = null
    if (this.match_keyword('where')) where_expr = this.parse_expression()
    return { type: 'delete', table_name, where_expr }
  }

  parse_select() {
    this.expect_keyword('select')
    const select_items = []
    do {
      select_items.push(this.parse_select_item())
    } while (this.match(','))
    this.expect_keyword('from')
    const table_name = this.parse_identifier_like()
    let where_expr = null
    let group_by = []
    let having_expr = null
    let order_by = []
    let limit = null
    let offset = 0

    while (true) {
      if (this.match_keyword('where')) {
        where_expr = this.parse_expression()
        continue
      }
      if (this.match_keyword('group')) {
        this.expect_keyword('by')
        do {
          group_by.push(this.parse_expression())
        } while (this.match(','))
        continue
      }
      if (this.match_keyword('having')) {
        having_expr = this.parse_expression()
        continue
      }
      if (this.match_keyword('order')) {
        this.expect_keyword('by')
        do {
          const expr = this.parse_expression()
          let desc = false
          if (this.match_keyword('desc')) desc = true
          else this.match_keyword('asc')
          order_by.push({ expr, desc })
        } while (this.match(','))
        continue
      }
      if (this.match_keyword('limit')) {
        limit = Number(this.expect_number_like())
        if (this.match_keyword('offset')) offset = Number(this.expect_number_like())
        continue
      }
      break
    }

    return { type: 'select', table_name, select_items, where_expr, group_by, having_expr, order_by, limit, offset }
  }

  parse_select_item() {
    if (this.match('*')) return { is_star: true, expr: star_node(), alias: null }
    const expr = this.parse_expression()
    let alias = null
    if (this.match_keyword('as')) alias = this.parse_identifier_like()
    return { is_star: false, expr, alias }
  }

  parse_column_def() {
    const name = this.parse_identifier_like()
    const type_name = this.parse_identifier_like()
    let nullable = true
    let primary_key = false
    let unique = false
    let has_default = false
    let default_value = null

    while (true) {
      if (this.match_keyword('primary')) {
        this.expect_keyword('key')
        primary_key = true
        nullable = false
        continue
      }
      if (this.match_keyword('unique')) {
        unique = true
        continue
      }
      if (this.match_keyword('not')) {
        this.expect_keyword('null')
        nullable = false
        continue
      }
      if (this.match_keyword('default')) {
        default_value = this.parse_literal_value()
        has_default = true
        continue
      }
      break
    }

    return { name, type_name, nullable, primary_key, unique, has_default, default_value }
  }

  parse_literal_value() {
    const token = this.next()
    if (!token) throw new bucket_db_error('unexpected eof', 'parse_error')
    if (token.type === 'string') return token.value
    if (token.type === 'number') return token.value
    if (token.type === 'keyword' && token.value === 'null') return null
    if (token.type === 'keyword' && token.value === 'true') return true
    if (token.type === 'keyword' && token.value === 'false') return false
    throw new bucket_db_error(`unsupported literal: ${token.value}`, 'parse_error')
  }

  parse_expression() {
    return this.parse_or()
  }

  parse_or() {
    let left = this.parse_and()
    while (this.match_keyword('or') || this.match('||')) {
      left = binary_node('or', left, this.parse_and())
    }
    return left
  }

  parse_and() {
    let left = this.parse_compare()
    while (this.match_keyword('and') || this.match('&&')) {
      left = binary_node('and', left, this.parse_compare())
    }
    return left
  }

  parse_compare() {
    let left = this.parse_add()
    while (true) {
      if (this.match('=')) left = binary_node('eq', left, this.parse_add())
      else if (this.match('!=' ) || this.match('<>')) left = binary_node('ne', left, this.parse_add())
      else if (this.match('<')) left = binary_node('lt', left, this.parse_add())
      else if (this.match('>')) left = binary_node('gt', left, this.parse_add())
      else if (this.match('<=')) left = binary_node('le', left, this.parse_add())
      else if (this.match('>=')) left = binary_node('ge', left, this.parse_add())
      else if (this.match_keyword('is')) {
        const is_not = this.match_keyword('not')
        this.expect_keyword('null')
        left = binary_node(is_not ? 'is_not_null' : 'is_null', left, literal_node(null))
      } else break
    }
    return left
  }

  parse_add() {
    let left = this.parse_mul()
    while (true) {
      if (this.match('+')) left = binary_node('add', left, this.parse_mul())
      else if (this.match('-')) left = binary_node('sub', left, this.parse_mul())
      else break
    }
    return left
  }

  parse_mul() {
    let left = this.parse_unary()
    while (true) {
      if (this.match('*')) left = binary_node('mul', left, this.parse_unary())
      else if (this.match('/')) left = binary_node('div', left, this.parse_unary())
      else if (this.match('%')) left = binary_node('mod', left, this.parse_unary())
      else break
    }
    return left
  }

  parse_unary() {
    if (this.match('-')) return unary_node('neg', this.parse_unary())
    if (this.match_keyword('not')) return unary_node('not', this.parse_unary())
    return this.parse_primary()
  }

  parse_primary() {
    const token = this.current()
    if (!token) throw new bucket_db_error('unexpected eof', 'parse_error')
    if (token.type === 'number') { this.next(); return literal_node(token.value) }
    if (token.type === 'string') { this.next(); return literal_node(token.value) }
    if (token.type === 'placeholder') { this.next(); return literal_node(null) }
    if (token.type === 'keyword' && token.value === 'null') { this.next(); return literal_node(null) }
    if (token.type === 'keyword' && token.value === 'true') { this.next(); return literal_node(true) }
    if (token.type === 'keyword' && token.value === 'false') { this.next(); return literal_node(false) }
    if (token.value === '(') { this.next(); const expr = this.parse_expression(); this.expect(')'); return expr }

    if (token.type === 'identifier' || token.type === 'keyword') {
      const name = this.parse_identifier_like()
      if (this.match('(')) {
        const args = []
        if (!this.match(')')) {
          do {
            if (this.match('*')) {
              args.push(star_node())
            } else {
              args.push(this.parse_expression())
            }
          } while (this.match(','))
          this.expect(')')
        }
        let over_clause = null
        if (this.match_keyword('over')) {
          this.expect('(')
          const partition_by = []
          const order_by = []
          if (this.match_keyword('partition')) {
            this.expect_keyword('by')
            do {
              partition_by.push(this.parse_expression())
            } while (this.match(','))
          }
          if (this.match_keyword('order')) {
            this.expect_keyword('by')
            do {
              order_by.push(this.parse_expression())
            } while (this.match(','))
          }
          this.expect(')')
          over_clause = { partition_by, order_by }
        }
        return call_node(name, args, over_clause)
      }
      return identifier_node(name)
    }

    throw new bucket_db_error(`unexpected token: ${token.value}`, 'parse_error')
  }

  parse_identifier_like() {
    const token = this.next()
    if (!token || !(token.type === 'identifier' || token.type === 'keyword')) throw new bucket_db_error('expected identifier', 'parse_error')
    return token.value
  }

  expect_number_like() {
    const token = this.next()
    if (!token || token.type !== 'number') throw new bucket_db_error('expected number', 'parse_error')
    return token.value
  }
}
