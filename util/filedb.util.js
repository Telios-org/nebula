const fs = require('graceful-fs')

class FileDB {
  constructor(storageDir) {
    this.storageDir = storageDir
    this.dbPath = `${storageDir}/data.db`
    this.cache = null
    
    if (!fs.existsSync(storageDir)) {
      fs.mkdirSync(storageDir)
      fs.writeFileSync(this.dbPath, JSON.stringify({}))
    }
  }

  async get(key) {
    let data

    if(this.cache) {
      data = this.cache
    } else {
      data = await this._read()
      this.cache = data
    }
    return data[key]
  }

  async put(key, value) {
    let data

    if(this.cache) {
      data = this.cache
    } else {
      data = await this._read()
    }

    data[key] = value
    this.cache = data
    await this._write(data)
  }

  async _read() {
    return new Promise((resolve, reject) => {
      fs.readFile(this.dbPath, (err, data) => {
        if(err) return reject(err)
        return resolve(JSON.parse(data.toString()))
      })
    })
  }

  async _write(json) {
    return new Promise((resolve, reject) => {
      fs.writeFile(this.dbPath, JSON.stringify(json), (err) => {
        if(err) return reject(err)
        return resolve(JSON.stringify(json))
      })
    })
  }
}

module.exports = FileDB