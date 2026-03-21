export class promise_mutex {
  constructor() {
    this.tail = Promise.resolve()
  }

  async run(fn) {
    const previous = this.tail
    let release_tail
    this.tail = new Promise((resolve) => {
      release_tail = resolve
    })
    await previous
    try {
      return await fn()
    } finally {
      release_tail()
    }
  }
}
