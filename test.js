import { execute, query } from "./bucket-db.js";

function decode(buffer) {
  return JSON.parse(Buffer.from(buffer).toString("utf8"));
}

const BUCKET_ID = crypto.randomUUID();

console.log("=== STEP 1: CREATE + INSERT ===");

// create bucket
console.log(
  decode(execute(`CREATE BUCKET IF NOT EXISTS ${BUCKET_ID};`))
);

// select bucket
console.log(
  decode(execute(`USE BUCKET ${BUCKET_ID};`))
);

// create table
console.log(
  decode(
    execute(`
      CREATE TABLE users (
        id INT PRIMARY KEY,
        name TEXT,
        age INT
      );
    `)
  )
);

// insert data
console.log(
  decode(
    execute(`
      INSERT INTO users (id, name, age) VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35);
    `)
  )
);

console.log("\n=== SIMULATE RESTART ===\n");

// simulate restart (important: clear module cache)
const modulePath = new URL("./bucket-db.js", import.meta.url).pathname;
delete import.meta.cache?.[modulePath]; // safe fallback

const db = await import("./bucket-db.js");

function decode2(buffer) {
  return JSON.parse(Buffer.from(buffer).toString("utf8"));
}

console.log("=== STEP 2: READ AFTER RESTART ===");

// use same bucket again
console.log(
  decode2(db.execute(`USE BUCKET ${BUCKET_ID};`))
);

// query with WHERE
const result = decode2(
  db.query(`
    SELECT id, name FROM users
    WHERE age > 28;
  `)
);

console.log("QUERY RESULT:");
console.log(result);