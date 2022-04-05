// const store = require('data-store')({ path: process.cwd() + `${storageDir}/data.db` })
const fs = require('graceful-fs')

class FileDB {
  constructor(storageDir) {
    this.storageDir = storageDir
    this.dbPath = `${storageDir}/data.db`
    this.store = require('data-store')({ path: this.dbPath })
  }

  get(key) {
    return this.store.get(key)
  }

  put(key, value) {
    this.store.set(key, value)
  }

  del(key) {
    this.store.del(key)
  }
}

module.exports = FileDB