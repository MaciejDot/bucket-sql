export function literal_node(value) { return { type: 'literal', value } }
export function identifier_node(name) { return { type: 'identifier', name } }
export function star_node() { return { type: 'star' } }
export function binary_node(operator, left, right) { return { type: 'binary', operator, left, right } }
export function unary_node(operator, operand) { return { type: 'unary', operator, operand } }
export function call_node(name, args = [], over_clause = null) { return { type: 'call', name, args, over_clause } }
