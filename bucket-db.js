// bucket-db.js
// Core engine for a small persistent bucketed SQL-ish database.
// Node.js ESM module.

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

/* -------------------------------------------------------------------------- */
/* Configuration / storage                                                    */
/* -------------------------------------------------------------------------- */

const data_folder_path = process.env.DATA_DIR || "./data";
const data_buffer_global_operations = path.join(
  data_folder_path,
  "buffer-global-operation.bin"
);
const data_bucket_ids = path.join(data_folder_path, "set-bucket-ids.bin");
const buckets_root_path = path.join(data_folder_path, "buckets");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function atomicWrite(filePath, bytes) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.tmp-${process.pid}-${Date.now()}-${crypto
    .randomBytes(4)
    .toString("hex")}`;
  fs.writeFileSync(tmpPath, bytes);
  fs.renameSync(tmpPath, filePath);
}

function safeJsonParse(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function readJsonFile(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath);
    if (!raw || raw.length === 0) return fallback;
    return safeJsonParse(raw.toString("utf8"), fallback);
  } catch {
    return fallback;
  }
}

function writeJsonFile(filePath, value) {
  atomicWrite(filePath, Buffer.from(JSON.stringify(value, null, 2), "utf8"));
}

function deepClone(value) {
  if (typeof structuredClone === "function") return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function encodeResult(value) {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function decodeBuffer(data) {
  if (Buffer.isBuffer(data)) return safeJsonParse(data.toString("utf8"), null);
  if (data instanceof Uint8Array)
    return safeJsonParse(Buffer.from(data).toString("utf8"), null);
  if (typeof data === "string") return safeJsonParse(data, null);
  return null;
}

/* -------------------------------------------------------------------------- */
/* State                                                                      */
/* -------------------------------------------------------------------------- */

const all_buckets = new Set();
const all_buckets_locks = new Map(); // legacy placeholder; sync engine does not need it
const all_buckets_buffer = new Uint32Array(0); // legacy placeholder; bucket ids are strings

let currentBucketId = null;

/**
 * Transaction stacks are per bucket.
 * Each stack entry is a full snapshot of the bucket state.
 * Mutating statements clone the top snapshot (or live state), modify the clone,
 * then replace the top snapshot or live cache atomically.
 */
const transactionStacks = new Map();

/* -------------------------------------------------------------------------- */
/* Bucket cache (LRU)                                                         */
/* -------------------------------------------------------------------------- */

class LRU_CACHE_BUCKETS_DATA {
  constructor(max_size_mb = 128) {
    this.maxBytes = Math.max(1, max_size_mb) * 1024 * 1024;
    this.totalBytes = 0;
    this.cache = new Map(); // id -> { data, size }
  }

  _estimateSize(data) {
    return Buffer.byteLength(JSON.stringify(data), "utf8");
  }

  get_bucket(id) {
    const entry = this.cache.get(id);
    if (!entry) return null;
    this.cache.delete(id);
    this.cache.set(id, entry);
    return entry.data;
  }

  put_bucket(id, data) {
    const size = this._estimateSize(data);

    if (this.cache.has(id)) {
      const old = this.cache.get(id);
      this.totalBytes -= old.size;
      this.cache.delete(id);
    }

    while (this.totalBytes + size > this.maxBytes && this.cache.size > 0) {
      const oldestKey = this.cache.keys().next().value;
      const oldest = this.cache.get(oldestKey);
      this.totalBytes -= oldest.size;
      this.cache.delete(oldestKey);
    }

    this.cache.set(id, { data, size });
    this.totalBytes += size;
  }

  delete_bucket(id) {
    const old = this.cache.get(id);
    if (!old) return;
    this.totalBytes -= old.size;
    this.cache.delete(id);
  }

  clear() {
    this.cache.clear();
    this.totalBytes = 0;
  }
}

const lru_cached_buckets = new LRU_CACHE_BUCKETS_DATA(
  Number(process.env.BUCKET_DB_LRU_MB || 128)
);

/* -------------------------------------------------------------------------- */
/* Filesystem helpers                                                         */
/* -------------------------------------------------------------------------- */

function assertSafeBucketId(id) {
  if (typeof id !== "string" || !id.trim()) {
    throw new Error("Invalid bucket id.");
  }
  const safe = id.trim();
  // UUIDs, slugs, and simple ids are allowed. No slashes / traversal.
  if (!/^[A-Za-z0-9._:@-]+$/.test(safe)) {
    throw new Error(`Unsafe bucket id: ${id}`);
  }
  return safe;
}

function normalizeDbIdentifier(name) {
  return String(name).trim().toLowerCase();
}

function get_bucket_folder(id) {
  const safeId = assertSafeBucketId(id);
  return path.join(buckets_root_path, safeId);
}

function create_bucket_folder(id) {
  const folder = get_bucket_folder(id);
  ensureDir(folder);
  return folder;
}

function bucketSnapshotPath(id) {
  return path.join(get_bucket_folder(id), "buffer-bucket.bin");
}

function bucketLatestJournalPath(id) {
  const folder = get_bucket_folder(id);
  return path.join(
    folder,
    `data-${Date.now()}-${crypto.randomBytes(4).toString("hex")}.bin`
  );
}

function isBucketKnown(id) {
  return all_buckets.has(assertSafeBucketId(id));
}

function persistGlobalMetadata() {
  const payload = {
    currentBucketId,
    buckets: [...all_buckets],
    updatedAt: new Date().toISOString(),
  };
  writeJsonFile(data_buffer_global_operations, payload);
  writeJsonFile(data_bucket_ids, [...all_buckets]);
}

function loadGlobalMetadata() {
  ensureDir(data_folder_path);
  ensureDir(buckets_root_path);

  const meta = readJsonFile(data_buffer_global_operations, null);
  const ids = readJsonFile(data_bucket_ids, null);

  if (Array.isArray(ids)) {
    ids.forEach((id) => {
      try {
        all_buckets.add(assertSafeBucketId(id));
      } catch {
        // ignore malformed ids
      }
    });
  } else if (meta && Array.isArray(meta.buckets)) {
    meta.buckets.forEach((id) => {
      try {
        all_buckets.add(assertSafeBucketId(id));
      } catch {
        // ignore malformed ids
      }
    });
  }

  if (meta && typeof meta.currentBucketId === "string") {
    try {
      const candidate = assertSafeBucketId(meta.currentBucketId);
      currentBucketId = all_buckets.has(candidate) ? candidate : null;
    } catch {
      currentBucketId = null;
    }
  }

  // Fallback: if metadata exists but no current bucket, keep null.
  if (!currentBucketId && all_buckets.size > 0) {
    currentBucketId = null;
  }
}

function normalizeTableSchema(table) {
  if (!table || typeof table !== "object") {
    return { columns: [], rows: [] };
  }
  const columns = Array.isArray(table.columns) ? table.columns : [];
  const rows = Array.isArray(table.rows) ? table.rows : [];

  const normalizedColumns = columns.map((c) => ({
    name: normalizeDbIdentifier(c.name),
    type: String(c.type || "TEXT").toUpperCase(),
    nullable: c.nullable !== undefined ? Boolean(c.nullable) : true,
    default:
      c.default !== undefined
        ? c.default
        : null,
    primary: Boolean(c.primary),
    unique: Boolean(c.unique || c.primary),
    autoIncrement: Boolean(c.autoIncrement),
  }));

  const normalizedRows = rows.map((row) => {
    const clean = {};
    for (const col of normalizedColumns) {
      clean[col.name] = Object.prototype.hasOwnProperty.call(row || {}, col.name)
        ? row[col.name]
        : null;
    }
    return clean;
  });

  return {
    columns: normalizedColumns,
    rows: normalizedRows,
  };
}

function normalizeBucketState(state) {
  const base = state && typeof state === "object" ? state : {};
  const tables = base.tables && typeof base.tables === "object" ? base.tables : {};
  const normalizedTables = {};

  for (const [tableName, table] of Object.entries(tables)) {
    normalizedTables[normalizeDbIdentifier(tableName)] = normalizeTableSchema(table);
  }

  return {
    version: Number(base.version || 1),
    createdAt: base.createdAt || new Date().toISOString(),
    updatedAt: base.updatedAt || new Date().toISOString(),
    tables: normalizedTables,
  };
}

function loadBucketStateFromDisk(id) {
  const folder = get_bucket_folder(id);
  const snapshotPath = bucketSnapshotPath(id);

  if (fs.existsSync(snapshotPath)) {
    const raw = fs.readFileSync(snapshotPath);
    return normalizeBucketState(safeJsonParse(raw.toString("utf8"), null));
  }

  // fallback to latest journal file
  if (fs.existsSync(folder)) {
    const entries = fs
      .readdirSync(folder)
      .filter((f) => /^data-.*\.bin$/i.test(f))
      .map((f) => {
        const full = path.join(folder, f);
        const stat = fs.statSync(full);
        return { file: full, mtimeMs: stat.mtimeMs };
      })
      .sort((a, b) => b.mtimeMs - a.mtimeMs);

    if (entries.length > 0) {
      const raw = fs.readFileSync(entries[0].file);
      return normalizeBucketState(safeJsonParse(raw.toString("utf8"), null));
    }
  }

  return normalizeBucketState({ version: 1, tables: {} });
}

function persistBucketState(id, state) {
  const safeId = assertSafeBucketId(id);
  const normalized = normalizeBucketState(state);
  normalized.updatedAt = new Date().toISOString();

  create_bucket_folder(safeId);
  atomicWrite(bucketSnapshotPath(safeId), encodeResult(normalized));
  atomicWrite(bucketLatestJournalPath(safeId), encodeResult(normalized));

  all_buckets.add(safeId);
  lru_cached_buckets.put_bucket(safeId, normalized);
  persistGlobalMetadata();

  return normalized;
}

function load_bucket_into_memory(id) {
  const safeId = assertSafeBucketId(id);

  const cached = lru_cached_buckets.get_bucket(safeId);
  if (cached) return cached;

  if (!isBucketKnown(safeId)) {
    throw new Error(`Bucket not found: ${safeId}`);
  }

  const state = loadBucketStateFromDisk(safeId);
  lru_cached_buckets.put_bucket(safeId, state);
  return state;
}

function create_bucket(id) {
  const safeId = assertSafeBucketId(id);
  if (isBucketKnown(safeId)) return load_bucket_into_memory(safeId);

  create_bucket_folder(safeId);
  const state = normalizeBucketState({
    version: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    tables: {},
  });

  all_buckets.add(safeId);
  lru_cached_buckets.put_bucket(safeId, state);
  persistBucketState(safeId, state);
  return state;
}

function create_bucket_and_load_it_into_memory(id) {
  create_bucket(id);
  return load_bucket_into_memory(id);
}

function drop_bucket(id) {
  const safeId = assertSafeBucketId(id);
  if (!isBucketKnown(safeId)) return false;

  lru_cached_buckets.delete_bucket(safeId);
  all_buckets.delete(safeId);

  transactionStacks.delete(safeId);

  if (currentBucketId === safeId) currentBucketId = null;

  const folder = get_bucket_folder(safeId);
  fs.rmSync(folder, { recursive: true, force: true });

  persistGlobalMetadata();
  return true;
}

/* -------------------------------------------------------------------------- */
/* Transaction helpers                                                        */
/* -------------------------------------------------------------------------- */

function getTransactionStack(bucketId) {
  const safeId = assertSafeBucketId(bucketId);
  if (!transactionStacks.has(safeId)) transactionStacks.set(safeId, []);
  return transactionStacks.get(safeId);
}

function hasActiveTransaction(bucketId) {
  const stack = transactionStacks.get(assertSafeBucketId(bucketId));
  return !!(stack && stack.length > 0);
}

function getActiveBucketId() {
  if (!currentBucketId) {
    throw new Error("No active bucket. Use `USE BUCKET <id>;` first.");
  }
  return currentBucketId;
}

function getWorkingState(bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  const stack = transactionStacks.get(safeId);
  if (stack && stack.length > 0) return stack[stack.length - 1];
  return load_bucket_into_memory(safeId);
}

function mutateWorkingState(mutator, bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  const stack = transactionStacks.get(safeId);
  const inTx = stack && stack.length > 0;

  const base = getWorkingState(safeId);
  const draft = deepClone(base);

  const result = mutator(draft);

  if (inTx) {
    stack[stack.length - 1] = draft;
  } else {
    lru_cached_buckets.put_bucket(safeId, draft);
    persistBucketState(safeId, draft);
  }

  return result;
}

function beginTransaction(bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  const stack = getTransactionStack(safeId);
  stack.push(deepClone(getWorkingState(safeId)));
  return { ok: true, depth: stack.length };
}

function commitTransaction(bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  const stack = getTransactionStack(safeId);
  if (stack.length === 0) throw new Error("No active transaction to COMMIT.");

  const committed = stack.pop();
  if (stack.length > 0) {
    stack[stack.length - 1] = committed;
    return { ok: true, depth: stack.length };
  }

  // Top-level transaction commit: persist to live store.
  lru_cached_buckets.put_bucket(safeId, committed);
  persistBucketState(safeId, committed);
  return { ok: true, depth: 0 };
}

function rollbackTransaction(bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  const stack = getTransactionStack(safeId);
  if (stack.length === 0) throw new Error("No active transaction to ROLLBACK.");

  stack.pop();
  return { ok: true, depth: stack.length };
}

/* -------------------------------------------------------------------------- */
/* Tokenizer                                                                  */
/* -------------------------------------------------------------------------- */

const KEYWORDS = new Set([
  "SELECT",
  "FROM",
  "WHERE",
  "INSERT",
  "INTO",
  "VALUES",
  "UPDATE",
  "SET",
  "DELETE",
  "CREATE",
  "DROP",
  "ALTER",
  "TABLE",
  "BUCKET",
  "USE",
  "BEGIN",
  "TRANSACTION",
  "COMMIT",
  "ROLLBACK",
  "PRIMARY",
  "KEY",
  "NOT",
  "NULL",
  "DEFAULT",
  "UNIQUE",
  "IF",
  "EXISTS",
  "NOT",
  "AND",
  "OR",
  "ORDER",
  "BY",
  "ASC",
  "DESC",
  "LIMIT",
  "ADD",
  "COLUMN",
  "RENAME",
  "TO",
  "SHOW",
  "LIST",
  "AS",
  "LIKE",
  "IS",
  "IN",
  "DISTINCT",
  "COUNT",
  "CURRENT",
]);

function tokenizer(command, args = []) {
  const sql = String(command || "");
  const tokens = [];
  let i = 0;
  let paramIndex = 0;

  const push = (type, value) => tokens.push({ type, value });

  const isWordChar = (ch) => /[A-Za-z0-9_:@-]/.test(ch);
  const isNumberWord = (w) =>
    /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(w);

  while (i < sql.length) {
    const ch = sql[i];

    // whitespace
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }

    // line comment --
    if (ch === "-" && sql[i + 1] === "-") {
      i += 2;
      while (i < sql.length && sql[i] !== "\n") i += 1;
      continue;
    }

    // block comment /* ... */
    if (ch === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i < sql.length && !(sql[i] === "*" && sql[i + 1] === "/")) i += 1;
      i += 2;
      continue;
    }

    // strings
    if (ch === "'" || ch === '"') {
      const quote = ch;
      i += 1;
      let value = "";
      while (i < sql.length) {
        const cur = sql[i];
        if (cur === quote) {
          if (sql[i + 1] === quote) {
            value += quote;
            i += 2;
            continue;
          }
          i += 1;
          break;
        }
        value += cur;
        i += 1;
      }
      push("string", value);
      continue;
    }

    // placeholders
    if (ch === "?") {
      push("param", { index: paramIndex, value: args[paramIndex] });
      paramIndex += 1;
      i += 1;
      continue;
    }

    // punctuation / symbols
    if ("(),;.*".includes(ch)) {
      push("symbol", ch);
      i += 1;
      continue;
    }

    // operators
    if ("=<>!".includes(ch)) {
      const two = sql.slice(i, i + 2);
      if (["<=", ">=", "<>", "!="].includes(two)) {
        push("operator", two);
        i += 2;
      } else {
        push("operator", ch);
        i += 1;
      }
      continue;
    }

    // words / numbers / ids (including UUID-like values)
    if (isWordChar(ch) || /\d/.test(ch)) {
      let start = i;
      while (i < sql.length && isWordChar(sql[i])) i += 1;
      const word = sql.slice(start, i);
      const upper = word.toUpperCase();

      if (KEYWORDS.has(upper)) {
        push("keyword", upper);
      } else if (isNumberWord(word)) {
        push("number", Number(word));
      } else {
        push("identifier", word);
      }
      continue;
    }

    throw new Error(`Unexpected character in SQL: "${ch}"`);
  }

  push("eof", null);
  return tokens;
}

/* -------------------------------------------------------------------------- */
/* Parser                                                                     */
/* -------------------------------------------------------------------------- */

function planner(tokenized) {
  const tokens = Array.isArray(tokenized) ? tokenized : tokenizer(String(tokenized || ""));
  let i = 0;

  const peek = (offset = 0) => tokens[i + offset] || { type: "eof", value: null };
  const next = () => tokens[i++] || { type: "eof", value: null };
  const eof = () => peek().type === "eof";

  const is = (type, value = undefined) =>
    peek().type === type && (value === undefined || peek().value === value);

  const isKw = (kw) => peek().type === "keyword" && peek().value === kw;
  const consumeKw = (kw) => {
    if (!isKw(kw)) return false;
    i += 1;
    return true;
  };
  const expectKw = (kw) => {
    if (!consumeKw(kw)) {
      throw new Error(`Expected keyword ${kw}, got ${peek().value || peek().type}`);
    }
  };
  const consumeSymbol = (sym) => {
    if (is("symbol", sym)) {
      i += 1;
      return true;
    }
    return false;
  };
  const expectSymbol = (sym) => {
    if (!consumeSymbol(sym)) {
      throw new Error(`Expected "${sym}", got ${peek().value || peek().type}`);
    }
  };
  const consumeOperator = (op) => {
    if (is("operator", op)) {
      i += 1;
      return true;
    }
    return false;
  };

  const parseName = () => {
    const t = next();
    if (t.type !== "identifier" && t.type !== "keyword") {
      throw new Error(`Expected identifier, got ${t.value || t.type}`);
    }
    let name = String(t.value);
    while (consumeSymbol(".")) {
      const part = next();
      if (part.type !== "identifier" && part.type !== "keyword") {
        throw new Error(`Expected identifier after ".", got ${part.value || part.type}`);
      }
      name += `.${part.value}`;
    }
    return name;
  };

  const parseBucketId = () => {
    const t = next();
    if (t.type === "string" || t.type === "identifier" || t.type === "number") {
      return String(t.value);
    }
    throw new Error(`Expected bucket id, got ${t.value || t.type}`);
  };

  const parseLiteral = () => {
    const t = next();
    if (t.type === "string" || t.type === "number") return { type: "literal", value: t.value };
    if (t.type === "param") return { type: "param", index: t.value.index, value: t.value.value };
    if (t.type === "keyword" && t.value === "NULL") return { type: "literal", value: null };
    if (t.type === "keyword" && t.value === "TRUE") return { type: "literal", value: true };
    if (t.type === "keyword" && t.value === "FALSE") return { type: "literal", value: false };
    throw new Error(`Expected literal, got ${t.value || t.type}`);
  };

  const parseOperand = () => {
    if (consumeSymbol("(")) {
      const expr = parseExpression();
      expectSymbol(")");
      return expr;
    }
    const t = peek();
    if (t.type === "string" || t.type === "number" || t.type === "param") {
      return parseLiteral();
    }
    if (t.type === "keyword" && ["NULL", "TRUE", "FALSE"].includes(t.value)) {
      return parseLiteral();
    }
    if (t.type === "identifier" || t.type === "keyword") {
      const name = parseName();
      return { type: "column", name };
    }
    throw new Error(`Unexpected token in expression: ${t.value || t.type}`);
  };

  const parseComparison = () => {
    let left = parseOperand();

    if (consumeKw("IS")) {
      const negated = consumeKw("NOT");
      expectKw("NULL");
      return { type: "isNull", expr: left, negated };
    }

    if (is("operator")) {
      const op = next().value;
      const right = parseOperand();
      return { type: "comparison", op, left, right };
    }

    if (consumeKw("LIKE")) {
      const right = parseOperand();
      return { type: "comparison", op: "LIKE", left, right };
    }

    return left;
  };

  const parseUnary = () => {
    if (consumeKw("NOT")) return { type: "not", expr: parseUnary() };
    return parseComparison();
  };

  const parseAnd = () => {
    let node = parseUnary();
    while (consumeKw("AND")) {
      node = { type: "and", left: node, right: parseUnary() };
    }
    return node;
  };

  const parseExpression = () => {
    let node = parseAnd();
    while (consumeKw("OR")) {
      node = { type: "or", left: node, right: parseAnd() };
    }
    return node;
  };

  const parseOrderBy = () => {
    expectKw("ORDER");
    expectKw("BY");
    const items = [];
    while (!eof()) {
      const name = parseName();
      let direction = "ASC";
      if (consumeKw("ASC")) direction = "ASC";
      else if (consumeKw("DESC")) direction = "DESC";
      items.push({ name, direction });
      if (!consumeSymbol(",")) break;
    }
    return items;
  };

  const parseLimit = () => {
    expectKw("LIMIT");
    const t = next();
    if (t.type === "number") return Number(t.value);
    if (t.type === "param") return { type: "param", index: t.value.index, value: t.value.value };
    throw new Error("LIMIT must be a number or parameter.");
  };

  const parseColumnDefinition = () => {
    const name = normalizeDbIdentifier(parseName());
    let type = "TEXT";

    const typeToken = next();
    if (typeToken.type !== "identifier" && typeToken.type !== "keyword") {
      throw new Error(`Expected column type for ${name}.`);
    }
    type = String(typeToken.value).toUpperCase();

    // skip length/precision specs, e.g. VARCHAR(255), DECIMAL(10,2)
    if (consumeSymbol("(")) {
      let depth = 1;
      while (!eof() && depth > 0) {
        const t = next();
        if (t.type === "symbol" && t.value === "(") depth += 1;
        else if (t.type === "symbol" && t.value === ")") depth -= 1;
      }
    }

    const def = {
      name,
      type,
      nullable: true,
      default: null,
      primary: false,
      unique: false,
      autoIncrement: false,
    };

    while (!eof() && !is("symbol", ",") && !is("symbol", ")")) {
      if (consumeKw("PRIMARY")) {
        expectKw("KEY");
        def.primary = true;
        def.unique = true;
        def.nullable = false;
        continue;
      }
      if (consumeKw("NOT")) {
        expectKw("NULL");
        def.nullable = false;
        continue;
      }
      if (consumeKw("NULL")) {
        def.nullable = true;
        continue;
      }
      if (consumeKw("UNIQUE")) {
        def.unique = true;
        continue;
      }
      if (consumeKw("DEFAULT")) {
        def.default = parseLiteral();
        continue;
      }
      if (consumeKw("AUTO_INCREMENT") || consumeKw("AUTOINCREMENT")) {
        def.autoIncrement = true;
        continue;
      }
      break;
    }

    return def;
  };

  const parseCreateTable = () => {
    expectKw("TABLE");
    let ifNotExists = false;
    if (consumeKw("IF")) {
      expectKw("NOT");
      expectKw("EXISTS");
      ifNotExists = true;
    }
    const tableName = normalizeDbIdentifier(parseName());
    expectSymbol("(");

    const columns = [];
    const tablePrimaryKeys = [];

    while (!eof() && !is("symbol", ")")) {
      if (consumeKw("PRIMARY")) {
        expectKw("KEY");
        expectSymbol("(");
        while (!eof() && !is("symbol", ")")) {
          tablePrimaryKeys.push(normalizeDbIdentifier(parseName()));
          consumeSymbol(",");
        }
        expectSymbol(")");
      } else {
        columns.push(parseColumnDefinition());
      }
      if (!consumeSymbol(",")) break;
    }

    expectSymbol(")");

    for (const pk of tablePrimaryKeys) {
      const col = columns.find((c) => c.name === pk);
      if (!col) throw new Error(`PRIMARY KEY references unknown column: ${pk}`);
      col.primary = true;
      col.unique = true;
      col.nullable = false;
    }

    return { type: "create_table", tableName, columns, ifNotExists };
  };

  const parseDropTable = () => {
    expectKw("TABLE");
    let ifExists = false;
    if (consumeKw("IF")) {
      expectKw("EXISTS");
      ifExists = true;
    }
    const tableName = normalizeDbIdentifier(parseName());
    return { type: "drop_table", tableName, ifExists };
  };

  const parseAlterTable = () => {
    expectKw("TABLE");
    const tableName = normalizeDbIdentifier(parseName());

    if (consumeKw("ADD")) {
      consumeKw("COLUMN");
      const column = parseColumnDefinition();
      return { type: "alter_table_add_column", tableName, column };
    }

    if (consumeKw("DROP")) {
      consumeKw("COLUMN");
      const columnName = normalizeDbIdentifier(parseName());
      return { type: "alter_table_drop_column", tableName, columnName };
    }

    if (consumeKw("RENAME")) {
      expectKw("TO");
      const newName = normalizeDbIdentifier(parseName());
      return { type: "alter_table_rename", tableName, newName };
    }

    throw new Error("Unsupported ALTER TABLE operation.");
  };

  const parseInsert = () => {
    expectKw("INTO");
    const tableName = normalizeDbIdentifier(parseName());

    let columns = null;
    if (consumeSymbol("(")) {
      columns = [];
      while (!eof() && !is("symbol", ")")) {
        columns.push(normalizeDbIdentifier(parseName()));
        consumeSymbol(",");
      }
      expectSymbol(")");
    }

    expectKw("VALUES");

    const values = [];
    while (!eof()) {
      expectSymbol("(");
      const row = [];
      while (!eof() && !is("symbol", ")")) {
        row.push(parseLiteral());
        consumeSymbol(",");
      }
      expectSymbol(")");
      values.push(row);
      if (!consumeSymbol(",")) break;
    }

    return { type: "insert", tableName, columns, values };
  };

  const parseUpdate = () => {
    const tableName = normalizeDbIdentifier(parseName());
    expectKw("SET");

    const assignments = [];
    while (!eof()) {
      const col = normalizeDbIdentifier(parseName());
      if (!consumeOperator("=")) {
        throw new Error("Expected '=' in UPDATE assignment.");
      }
      const value = parseExpression();
      assignments.push({ column: col, value });
      if (!consumeSymbol(",")) break;
    }

    let where = null;
    if (consumeKw("WHERE")) where = parseExpression();

    return { type: "update", tableName, assignments, where };
  };

  const parseDelete = () => {
    expectKw("FROM");
    const tableName = normalizeDbIdentifier(parseName());
    let where = null;
    if (consumeKw("WHERE")) where = parseExpression();
    return { type: "delete", tableName, where };
  };

  const parseSelect = () => {
    const columns = [];

    if (consumeSymbol("*")) {
      columns.push({ type: "star" });
    } else {
      while (!eof()) {
        if (consumeSymbol("*")) {
          columns.push({ type: "star" });
        } else {
          columns.push({ type: "column", name: normalizeDbIdentifier(parseName()) });
        }
        if (!consumeSymbol(",")) break;
      }
    }

    expectKw("FROM");
    const tableName = normalizeDbIdentifier(parseName());

    let where = null;
    let orderBy = [];
    let limit = null;

    while (!eof()) {
      if (consumeKw("WHERE")) {
        where = parseExpression();
        continue;
      }
      if (isKw("ORDER")) {
        orderBy = parseOrderBy();
        continue;
      }
      if (isKw("LIMIT")) {
        limit = parseLimit();
        continue;
      }
      break;
    }

    return { type: "select", tableName, columns, where, orderBy, limit };
  };

  const parseShowOrList = () => {
    if (consumeKw("BUCKETS")) return { type: "list_buckets" };
    throw new Error("Unsupported SHOW/LIST command.");
  };

  const parseStatement = () => {
    if (eof()) return null;

    if (consumeSymbol(";")) return null;

    if (consumeKw("USE")) {
      expectKw("BUCKET");
      return { type: "use_bucket", bucketId: parseBucketId() };
    }

    if (consumeKw("BEGIN")) {
      consumeKw("TRANSACTION");
      return { type: "begin" };
    }

    if (consumeKw("COMMIT")) return { type: "commit" };
    if (consumeKw("ROLLBACK")) return { type: "rollback" };

    if (consumeKw("CREATE")) {
      if (isKw("BUCKET")) {
        expectKw("BUCKET");
        let ifNotExists = false;
        if (consumeKw("IF")) {
          expectKw("NOT");
          expectKw("EXISTS");
          ifNotExists = true;
        }
        return { type: "create_bucket", bucketId: parseBucketId(), ifNotExists };
      }
      if (isKw("TABLE")) return parseCreateTable();
      throw new Error("Unsupported CREATE target.");
    }

    if (consumeKw("DROP")) {
      if (isKw("BUCKET")) {
        expectKw("BUCKET");
        let ifExists = false;
        if (consumeKw("IF")) {
          expectKw("EXISTS");
          ifExists = true;
        }
        return { type: "drop_bucket", bucketId: parseBucketId(), ifExists };
      }
      if (isKw("TABLE")) return parseDropTable();
      throw new Error("Unsupported DROP target.");
    }

    if (consumeKw("ALTER")) return parseAlterTable();

    if (consumeKw("INSERT")) return parseInsert();

    if (consumeKw("UPDATE")) return parseUpdate();

    if (consumeKw("DELETE")) return parseDelete();

    if (consumeKw("SELECT")) return parseSelect();

    if (consumeKw("SHOW") || consumeKw("LIST")) return parseShowOrList();

    throw new Error(`Unknown SQL statement starting with ${peek().value || peek().type}`);
  };

  const statements = [];
  while (!eof()) {
    const stmt = parseStatement();
    if (stmt) statements.push(stmt);
    consumeSymbol(";");
    while (consumeSymbol(";")) {
      // allow repeated semicolons
    }
  }

  return statements;
}

/* -------------------------------------------------------------------------- */
/* Expression evaluation                                                      */
/* -------------------------------------------------------------------------- */

function resolveNode(node, row, args = []) {
  if (!node) return null;

  switch (node.type) {
    case "literal":
      return node.value;
    case "param":
      return args[node.index];
    case "column": {
      const key = normalizeDbIdentifier(node.name.split(".").pop());
      return row[key];
    }
    case "not":
      return !truthy(resolveNode(node.expr, row, args));
    case "and":
      return truthy(resolveNode(node.left, row, args)) && truthy(resolveNode(node.right, row, args));
    case "or":
      return truthy(resolveNode(node.left, row, args)) || truthy(resolveNode(node.right, row, args));
    case "isNull": {
      const v = resolveNode(node.expr, row, args);
      const isNull = v === null || v === undefined;
      return node.negated ? !isNull : isNull;
    }
    case "comparison": {
      const left = resolveNode(node.left, row, args);
      const right = resolveNode(node.right, row, args);
      switch (node.op) {
        case "=":
          return left === right;
        case "!=":
        case "<>":
          return left !== right;
        case "<":
          return compareValues(left, right) < 0;
        case "<=":
          return compareValues(left, right) <= 0;
        case ">":
          return compareValues(left, right) > 0;
        case ">=":
          return compareValues(left, right) >= 0;
        case "LIKE":
          return likeMatch(left, right);
        default:
          throw new Error(`Unsupported operator: ${node.op}`);
      }
    }
    default:
      // bare expression / bare column
      return resolveNode({ type: "column", name: String(node.name || "") }, row, args);
  }
}

function truthy(value) {
  return !!value;
}

function compareValues(a, b) {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;

  const an = Number(a);
  const bn = Number(b);
  const aNum = Number.isFinite(an) && String(a).trim() !== "";
  const bNum = Number.isFinite(bn) && String(b).trim() !== "";
  if (aNum && bNum) return an - bn;

  return String(a).localeCompare(String(b));
}

function likeMatch(value, pattern) {
  const text = String(value ?? "");
  const p = String(pattern ?? "");
  const escaped = p
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  const re = new RegExp(`^${escaped}$`, "i");
  return re.test(text);
}

/* -------------------------------------------------------------------------- */
/* Schema / row helpers                                                       */
/* -------------------------------------------------------------------------- */

function getTable(state, tableName) {
  const key = normalizeDbIdentifier(tableName);
  const table = state.tables[key];
  if (!table) throw new Error(`Table not found: ${tableName}`);
  return table;
}

function createEmptyTable() {
  return { columns: [], rows: [] };
}

function inferColumnMap(table) {
  const map = new Map();
  for (const col of table.columns) map.set(col.name, col);
  return map;
}

function coerceValueForType(value, column) {
  if (value === undefined) return null;
  if (value === null) return null;

  const type = String(column?.type || "TEXT").toUpperCase();

  if (type === "TEXT" || type === "STRING" || type === "CHAR" || type === "VARCHAR") {
    return String(value);
  }

  if (
    type === "INT" ||
    type === "INTEGER" ||
    type === "SMALLINT" ||
    type === "BIGINT" ||
    type === "TINYINT"
  ) {
    const n = Number(value);
    if (!Number.isFinite(n) || !Number.isInteger(n)) {
      throw new Error(`Expected integer for column ${column.name}`);
    }
    return n;
  }

  if (type === "FLOAT" || type === "DOUBLE" || type === "REAL" || type === "NUMERIC" || type === "DECIMAL") {
    const n = Number(value);
    if (!Number.isFinite(n)) {
      throw new Error(`Expected number for column ${column.name}`);
    }
    return n;
  }

  if (type === "BOOLEAN" || type === "BOOL") {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value !== 0;
    if (typeof value === "string") {
      const s = value.trim().toLowerCase();
      if (["1", "true", "yes", "on"].includes(s)) return true;
      if (["0", "false", "no", "off"].includes(s)) return false;
    }
    return Boolean(value);
  }

  if (type === "JSON") {
    if (typeof value === "object") return deepClone(value);
    if (typeof value === "string") {
      try {
        return JSON.parse(value);
      } catch {
        // keep as string if not valid JSON
        return value;
      }
    }
    return value;
  }

  if (type === "DATE" || type === "DATETIME" || type === "TIMESTAMP") {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "string") {
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) throw new Error(`Invalid date for column ${column.name}`);
      return d.toISOString();
    }
    if (typeof value === "number") {
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) throw new Error(`Invalid date for column ${column.name}`);
      return d.toISOString();
    }
    throw new Error(`Invalid date for column ${column.name}`);
  }

  if (type === "UUID") {
    return String(value);
  }

  // Fallback: preserve value if possible.
  return value;
}

function normalizeRowForStorage(table, row) {
  const out = {};
  const map = inferColumnMap(table);

  for (const column of table.columns) {
    let value = Object.prototype.hasOwnProperty.call(row, column.name) ? row[column.name] : undefined;
    if (value === undefined) {
      if (column.autoIncrement) {
        const existing = table.rows
          .map((r) => Number(r[column.name]))
          .filter((n) => Number.isFinite(n));
        value = existing.length > 0 ? Math.max(...existing) + 1 : 1;
      } else if (column.default !== null && column.default !== undefined) {
        value = resolveNode(column.default, {}, []);
      } else if (column.nullable) {
        value = null;
      } else {
        throw new Error(`Column ${column.name} requires a value.`);
      }
    }

    value = coerceValueForType(value, column);
    out[column.name] = value;
  }

  // Drop extra fields unless explicitly present in schema (keeps storage small)
  for (const key of Object.keys(row)) {
    if (!map.has(key)) continue;
    if (!(key in out)) out[key] = row[key];
  }

  return out;
}

function enforceTableConstraints(table, candidateRow, ignoreRowIndex = -1) {
  const uniqueColumns = table.columns.filter((c) => c.unique || c.primary);

  for (const col of uniqueColumns) {
    const value = candidateRow[col.name];
    if ((value === null || value === undefined) && !col.nullable) {
      throw new Error(`Column ${col.name} cannot be NULL.`);
    }

    const duplicate = table.rows.findIndex((row, idx) => {
      if (idx === ignoreRowIndex) return false;
      return row[col.name] === value;
    });

    if (duplicate !== -1) {
      throw new Error(`Unique constraint violation on column ${col.name}`);
    }
  }

  for (const col of table.columns) {
    if (!col.nullable && (candidateRow[col.name] === null || candidateRow[col.name] === undefined)) {
      throw new Error(`Column ${col.name} cannot be NULL.`);
    }
  }
}

function applyDefaultAndCoerce(table, partialRow, indexToIgnore = -1) {
  const row = {};
  for (const col of table.columns) {
    row[col.name] = Object.prototype.hasOwnProperty.call(partialRow, col.name)
      ? partialRow[col.name]
      : undefined;
  }
  const normalized = normalizeRowForStorage(table, row);
  enforceTableConstraints(table, normalized, indexToIgnore);
  return normalized;
}

/* -------------------------------------------------------------------------- */
/* Statement execution                                                        */
/* -------------------------------------------------------------------------- */

function executePlan(plan, args = []) {
  const statements = Array.isArray(plan) ? plan : [plan];
  let last = null;

  for (const stmt of statements) {
    if (!stmt) continue;
    last = executeStatement(stmt, args);
  }

  return last;
}

function executeStatement(stmt, args = []) {
  switch (stmt.type) {
    case "use_bucket": {
      const bucketId = assertSafeBucketId(stmt.bucketId);
      if (!isBucketKnown(bucketId)) {
        throw new Error(`Bucket not found: ${bucketId}`);
      }
      currentBucketId = bucketId;
      persistGlobalMetadata();
      return { ok: true, kind: "use_bucket", bucketId };
    }

    case "create_bucket": {
      const bucketId = assertSafeBucketId(stmt.bucketId);
      if (isBucketKnown(bucketId)) {
        if (stmt.ifNotExists) {
          return { ok: true, kind: "create_bucket", bucketId, created: false };
        }
        throw new Error(`Bucket already exists: ${bucketId}`);
      }
      create_bucket(bucketId);
      return { ok: true, kind: "create_bucket", bucketId, created: true };
    }

    case "drop_bucket": {
      const bucketId = assertSafeBucketId(stmt.bucketId);
      if (!isBucketKnown(bucketId)) {
        if (stmt.ifExists) return { ok: true, kind: "drop_bucket", bucketId, dropped: false };
        throw new Error(`Bucket not found: ${bucketId}`);
      }
      drop_bucket(bucketId);
      return { ok: true, kind: "drop_bucket", bucketId, dropped: true };
    }

    case "list_buckets": {
      return {
        ok: true,
        kind: "list_buckets",
        buckets: [...all_buckets].sort(),
      };
    }

    case "begin": {
      const bucketId = getActiveBucketId();
      return beginTransaction(bucketId);
    }

    case "commit": {
      const bucketId = getActiveBucketId();
      return commitTransaction(bucketId);
    }

    case "rollback": {
      const bucketId = getActiveBucketId();
      return rollbackTransaction(bucketId);
    }

    case "create_table": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const key = normalizeDbIdentifier(stmt.tableName);
        if (state.tables[key]) {
          if (stmt.ifNotExists) return { ok: true, kind: "create_table", table: key, created: false };
          throw new Error(`Table already exists: ${stmt.tableName}`);
        }

        const seen = new Set();
        const columns = stmt.columns.map((c) => {
          if (seen.has(c.name)) throw new Error(`Duplicate column: ${c.name}`);
          seen.add(c.name);
          return {
            name: normalizeDbIdentifier(c.name),
            type: String(c.type || "TEXT").toUpperCase(),
            nullable: Boolean(c.nullable),
            default: c.default || null,
            primary: Boolean(c.primary),
            unique: Boolean(c.unique || c.primary),
            autoIncrement: Boolean(c.autoIncrement),
          };
        });

        state.tables[key] = {
          columns,
          rows: [],
        };

        return { ok: true, kind: "create_table", table: key, created: true };
      }, bucketId);
    }

    case "drop_table": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const key = normalizeDbIdentifier(stmt.tableName);
        if (!state.tables[key]) {
          if (stmt.ifExists) return { ok: true, kind: "drop_table", table: key, dropped: false };
          throw new Error(`Table not found: ${stmt.tableName}`);
        }
        delete state.tables[key];
        return { ok: true, kind: "drop_table", table: key, dropped: true };
      }, bucketId);
    }

    case "alter_table_add_column": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const table = getTable(state, stmt.tableName);
        const col = {
          name: normalizeDbIdentifier(stmt.column.name),
          type: String(stmt.column.type || "TEXT").toUpperCase(),
          nullable: Boolean(stmt.column.nullable),
          default: stmt.column.default || null,
          primary: Boolean(stmt.column.primary),
          unique: Boolean(stmt.column.unique || stmt.column.primary),
          autoIncrement: Boolean(stmt.column.autoIncrement),
        };

        if (table.columns.some((c) => c.name === col.name)) {
          throw new Error(`Column already exists: ${col.name}`);
        }

        if (!col.nullable && col.default === null && !col.autoIncrement) {
          // Adding a NOT NULL column without default would invalidate existing rows.
          throw new Error(
            `Cannot add NOT NULL column "${col.name}" without DEFAULT or AUTO_INCREMENT.`
          );
        }

        table.columns.push(col);

        for (const row of table.rows) {
          if (col.autoIncrement) {
            const existing = table.rows
              .map((r) => Number(r[col.name]))
              .filter((n) => Number.isFinite(n));
            row[col.name] = existing.length > 0 ? Math.max(...existing) + 1 : 1;
          } else if (col.default !== null) {
            row[col.name] = resolveNode(col.default, {}, []);
          } else {
            row[col.name] = null;
          }
          row[col.name] = coerceValueForType(row[col.name], col);
        }

        return {
          ok: true,
          kind: "alter_table_add_column",
          table: normalizeDbIdentifier(stmt.tableName),
          column: col.name,
        };
      }, bucketId);
    }

    case "alter_table_drop_column": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const table = getTable(state, stmt.tableName);
        const colName = normalizeDbIdentifier(stmt.columnName);
        const idx = table.columns.findIndex((c) => c.name === colName);
        if (idx === -1) throw new Error(`Column not found: ${colName}`);
        table.columns.splice(idx, 1);
        for (const row of table.rows) {
          delete row[colName];
        }
        return {
          ok: true,
          kind: "alter_table_drop_column",
          table: normalizeDbIdentifier(stmt.tableName),
          column: colName,
        };
      }, bucketId);
    }

    case "alter_table_rename": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const oldKey = normalizeDbIdentifier(stmt.tableName);
        const newKey = normalizeDbIdentifier(stmt.newName);
        if (!state.tables[oldKey]) throw new Error(`Table not found: ${stmt.tableName}`);
        if (state.tables[newKey]) throw new Error(`Table already exists: ${stmt.newName}`);
        state.tables[newKey] = state.tables[oldKey];
        delete state.tables[oldKey];
        return { ok: true, kind: "alter_table_rename", from: oldKey, to: newKey };
      }, bucketId);
    }

    case "insert": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const table = getTable(state, stmt.tableName);
        const inserted = [];

        const providedColumns = stmt.columns
          ? stmt.columns.map((c) => normalizeDbIdentifier(c))
          : table.columns.map((c) => c.name);

        for (const rowLiteralList of stmt.values) {
          const partial = {};
          for (let idx = 0; idx < rowLiteralList.length; idx += 1) {
            const colName = providedColumns[idx];
            if (!colName) throw new Error("Too many values in INSERT row.");
            const node = rowLiteralList[idx];
            partial[colName] = resolveNode(node, {}, args);
          }

          const normalized = applyDefaultAndCoerce(table, partial);
          table.rows.push(normalized);
          inserted.push(normalized);
        }

        return {
          ok: true,
          kind: "insert",
          table: normalizeDbIdentifier(stmt.tableName),
          inserted: inserted.length,
          rows: inserted,
        };
      }, bucketId);
    }

    case "update": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const table = getTable(state, stmt.tableName);
        let affected = 0;

        for (let rowIndex = 0; rowIndex < table.rows.length; rowIndex += 1) {
          const row = table.rows[rowIndex];
          if (stmt.where && !truthy(resolveNode(stmt.where, row, args))) continue;

          const updated = deepClone(row);
          for (const assignment of stmt.assignments) {
            const colName = normalizeDbIdentifier(assignment.column);
            const column = table.columns.find((c) => c.name === colName);
            if (!column) throw new Error(`Column not found: ${colName}`);
            const raw = resolveNode(assignment.value, updated, args);
            updated[colName] = coerceValueForType(raw, column);
          }

          enforceTableConstraints(table, updated, rowIndex);
          table.rows[rowIndex] = updated;
          affected += 1;
        }

        return {
          ok: true,
          kind: "update",
          table: normalizeDbIdentifier(stmt.tableName),
          affected,
        };
      }, bucketId);
    }

    case "delete": {
      const bucketId = getActiveBucketId();
      return mutateWorkingState((state) => {
        const table = getTable(state, stmt.tableName);
        const original = table.rows.length;
        table.rows = table.rows.filter((row) => {
          if (!stmt.where) return false; // DELETE without WHERE deletes all, handled below
          return !truthy(resolveNode(stmt.where, row, args));
        });

        if (!stmt.where) table.rows = [];
        const affected = original - table.rows.length;

        return {
          ok: true,
          kind: "delete",
          table: normalizeDbIdentifier(stmt.tableName),
          affected,
        };
      }, bucketId);
    }

    case "select": {
      const bucketId = getActiveBucketId();
      const state = getWorkingState(bucketId);
      const table = getTable(state, stmt.tableName);

      let rows = table.rows.map((r) => deepClone(r));

      if (stmt.where) {
        rows = rows.filter((row) => truthy(resolveNode(stmt.where, row, args)));
      }

      if (stmt.orderBy && stmt.orderBy.length > 0) {
        rows.sort((a, b) => {
          for (const item of stmt.orderBy) {
            const colName = normalizeDbIdentifier(item.name);
            const cmp = compareValues(a[colName], b[colName]);
            if (cmp !== 0) return item.direction === "DESC" ? -cmp : cmp;
          }
          return 0;
        });
      }

      if (stmt.limit !== null && stmt.limit !== undefined) {
        const limitValue =
          typeof stmt.limit === "object" && stmt.limit.type === "param"
            ? Number(args[stmt.limit.index])
            : Number(stmt.limit);
        rows = rows.slice(0, Number.isFinite(limitValue) ? limitValue : 0);
      }

      const allColumns = table.columns.map((c) => c.name);
      let projected;

      if (stmt.columns.length === 1 && stmt.columns[0].type === "star") {
        projected = rows.map((r) => {
          const out = {};
          for (const col of allColumns) out[col] = r[col] ?? null;
          return out;
        });
      } else {
        projected = rows.map((r) => {
          const out = {};
          for (const col of stmt.columns) {
            const colName = normalizeDbIdentifier(col.name.split(".").pop());
            if (!allColumns.includes(colName)) {
              throw new Error(`Column not found: ${colName}`);
            }
            out[colName] = r[colName] ?? null;
          }
          return out;
        });
      }

      return {
        ok: true,
        kind: "select",
        table: normalizeDbIdentifier(stmt.tableName),
        rows: projected,
        count: projected.length,
      };
    }

    case "list_buckets":
      return {
        ok: true,
        kind: "list_buckets",
        buckets: [...all_buckets].sort(),
      };

    default:
      throw new Error(`Unsupported statement: ${stmt.type}`);
  }
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

function runSQL(command, args = []) {
  const tokens = tokenizer(command, args);
  const plan = planner(tokens);
  return executePlan(plan, args);
}

export function execute(command, args = []) {
  try {
    const result = runSQL(command, args);
    return encodeResult(result);
  } catch (error) {
    return encodeResult({ ok: false, error: String(error?.message || error) });
  }
}

export function query(sql, args = []) {
  try {
    const result = runSQL(sql, args);
    return encodeResult(result);
  } catch (error) {
    return encodeResult({ ok: false, error: String(error?.message || error) });
  }
}

export function execute_bucket(command, args = []) {
  try {
    runSQL(command, args);
    return 1;
  } catch {
    return 0;
  }
}

/**
 * Whisper replication protocol:
 * - bulk_whisper_execute(Buffer|Uint8Array|string JSON):
 *     { type: "snapshot", bucketId, state }
 *     { type: "ops", ops: [{ command, args }] }
 *     { type: "sql", sql, args }
 *
 * Returns: integer count of applied operations, or 0 on failure.
 */
export function bulk_whisper_execute(data) {
  try {
    const msg = decodeBuffer(data);
    if (!msg || typeof msg !== "object") return 0;

    if (msg.type === "snapshot") {
      const bucketId = assertSafeBucketId(msg.bucketId);
      const state = normalizeBucketState(msg.state);
      all_buckets.add(bucketId);
      persistBucketState(bucketId, state);
      return 1;
    }

    if (msg.type === "sql") {
      runSQL(msg.sql, Array.isArray(msg.args) ? msg.args : []);
      return 1;
    }

    if (msg.type === "ops" && Array.isArray(msg.ops)) {
      let applied = 0;
      for (const op of msg.ops) {
        if (!op) continue;
        if (op.type === "snapshot") {
          const bucketId = assertSafeBucketId(op.bucketId);
          const state = normalizeBucketState(op.state);
          all_buckets.add(bucketId);
          persistBucketState(bucketId, state);
          applied += 1;
          continue;
        }
        if (typeof op.command === "string") {
          runSQL(op.command, Array.isArray(op.args) ? op.args : []);
          applied += 1;
          continue;
        }
        if (typeof op.sql === "string") {
          runSQL(op.sql, Array.isArray(op.args) ? op.args : []);
          applied += 1;
        }
      }
      return applied;
    }

    return 0;
  } catch {
    return 0;
  }
}

export function bulk_whisper_provide(data) {
  try {
    const msg = decodeBuffer(data);

    // Default: provide global metadata plus active bucket snapshot (if any)
    if (!msg || typeof msg !== "object") {
      const payload = {
        type: "metadata",
        currentBucketId,
        buckets: [...all_buckets].sort(),
        activeSnapshot: currentBucketId ? load_bucket_into_memory(currentBucketId) : null,
      };
      return encodeResult(payload);
    }

    if (msg.type === "snapshot") {
      const bucketId = assertSafeBucketId(msg.bucketId || currentBucketId);
      if (!bucketId || !isBucketKnown(bucketId)) {
        return encodeResult({ ok: false, error: "Bucket not found." });
      }
      const payload = {
        type: "snapshot",
        bucketId,
        state: load_bucket_into_memory(bucketId),
      };
      return encodeResult(payload);
    }

    if (msg.type === "metadata") {
      return encodeResult({
        type: "metadata",
        currentBucketId,
        buckets: [...all_buckets].sort(),
      });
    }

    if (msg.type === "bucket" && msg.bucketId) {
      const bucketId = assertSafeBucketId(msg.bucketId);
      return encodeResult({
        type: "snapshot",
        bucketId,
        state: load_bucket_into_memory(bucketId),
      });
    }

    return encodeResult({ ok: false, error: "Unsupported whisper provide request." });
  } catch (error) {
    return encodeResult({ ok: false, error: String(error?.message || error) });
  }
}

/* -------------------------------------------------------------------------- */
/* Optional helpers for shell/server/gate wrappers                             */
/* -------------------------------------------------------------------------- */

export function getCurrentBucketId() {
  return currentBucketId;
}

export function setCurrentBucketId(bucketId) {
  const safeId = assertSafeBucketId(bucketId);
  if (!isBucketKnown(safeId)) throw new Error(`Bucket not found: ${safeId}`);
  currentBucketId = safeId;
  persistGlobalMetadata();
  return currentBucketId;
}

export function listBuckets() {
  return [...all_buckets].sort();
}

export function snapshotBucket(bucketId = getActiveBucketId()) {
  const safeId = assertSafeBucketId(bucketId);
  return deepClone(load_bucket_into_memory(safeId));
}

export function importBucketSnapshot(bucketId, state) {
  const safeId = assertSafeBucketId(bucketId);
  const normalized = normalizeBucketState(state);
  all_buckets.add(safeId);
  persistBucketState(safeId, normalized);
  return true;
}

/* -------------------------------------------------------------------------- */
/* Initialize                                                                 */
/* -------------------------------------------------------------------------- */

function bootstrap() {
  ensureDir(data_folder_path);
  ensureDir(buckets_root_path);
  loadGlobalMetadata();

  // Ensure metadata files exist even on a fresh install.
  if (!fs.existsSync(data_bucket_ids)) writeJsonFile(data_bucket_ids, [...all_buckets]);
  if (!fs.existsSync(data_buffer_global_operations)) {
    writeJsonFile(data_buffer_global_operations, {
      currentBucketId,
      buckets: [...all_buckets],
      updatedAt: new Date().toISOString(),
    });
  }

  // Keep legacy placeholder vars referenced so linters do not prune them.
  void all_buckets_locks;
  void all_buckets_buffer;
}

bootstrap();