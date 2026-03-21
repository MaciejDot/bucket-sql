export class bucket_db_error extends Error {
  constructor(message, code = 'bucket_db_error') {
    super(message)
    this.name = 'bucket_db_error'
    this.code = code
  }
}
