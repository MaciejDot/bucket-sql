import { bucket_db_error } from '../core/errors.js'

const keywords = new Set([
  'select','from','where','group','by','having','order','asc','desc','limit','offset',
  'insert','into','values','update','set','delete','create','table','drop','alter','add',
  'column','begin','commit','rollback','as','and','or','not','null','is','count',
  'max','min','sum','avg','row_number','rank','dense_rank','over','partition','primary',
  'key','unique','default','true','false'
])

export function tokenize(sql_text) {
  const tokens = []
  let i = 0

  while (i < sql_text.length) {
    const ch = sql_text[i]
    if (/\s/.test(ch)) { i += 1; continue }

    if (ch === '-' && sql_text[i + 1] === '-') {
      while (i < sql_text.length && sql_text[i] !== '\n') i += 1
      continue
    }

    if (ch === '/' && sql_text[i + 1] === '*') {
      i += 2
      while (i < sql_text.length && !(sql_text[i] === '*' && sql_text[i + 1] === '/')) i += 1
      i += 2
      continue
    }

    if (ch === '\'' || ch === '"') {
      const quote = ch
      i += 1
      let value = ''
      while (i < sql_text.length) {
        const current = sql_text[i]
        if (current === '\\') {
          value += sql_text[i + 1] || ''
          i += 2
          continue
        }
        if (current === quote) {
          if (sql_text[i + 1] === quote) {
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

    if (ch === '?') {
      tokens.push({ type: 'placeholder', value: '?' })
      i += 1
      continue
    }

    if (/[0-9]/.test(ch) || (ch === '.' && /[0-9]/.test(sql_text[i + 1]))) {
      const start = i
      i += 1
      while (i < sql_text.length && /[0-9eE+\-.]/.test(sql_text[i])) i += 1
      const value = Number(sql_text.slice(start, i))
      tokens.push({ type: 'number', value })
      continue
    }

    if (/[A-Za-z_]/.test(ch)) {
      const start = i
      i += 1
      while (i < sql_text.length && /[A-Za-z0-9_$]/.test(sql_text[i])) i += 1
      const raw = sql_text.slice(start, i)
      const lower = raw.toLowerCase()
      tokens.push({ type: keywords.has(lower) ? 'keyword' : 'identifier', value: keywords.has(lower) ? lower : raw })
      continue
    }

    const two = sql_text.slice(i, i + 2)
    if (['>=', '<=', '<>', '!=', '&&', '||'].includes(two)) {
      tokens.push({ type: 'operator', value: two })
      i += 2
      continue
    }

    if ('(),;.*+-/%=<>&|!'.includes(ch)) {
      tokens.push({ type: 'punctuation', value: ch })
      i += 1
      continue
    }

    throw new bucket_db_error(`unexpected character: ${ch}`, 'tokenizer_error')
  }

  tokens.push({ type: 'eof', value: null })
  return tokens
}
