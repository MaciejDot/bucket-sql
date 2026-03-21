export function measure_ms(fn) {
  const start = performance.now()
  const value = fn()
  const end = performance.now()
  return { value, elapsed_ms: end - start }
}

export async function measure_ms_async(fn) {
  const start = performance.now()
  const value = await fn()
  const end = performance.now()
  return { value, elapsed_ms: end - start }
}
