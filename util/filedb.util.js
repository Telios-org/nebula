const fs = require('fs')

class FileDB {
  constructor(storageDir) {
    this.locked = false
    this.storageDir = storageDir
    this.dbPath = `${storageDir}/data.db`
    this.cache = null
    
    if (!fs.existsSync(storageDir)) {
      fs.mkdirSync(storageDir)
    }
    
    fs.writeFileSync(this.dbPath, JSON.stringify({}))
  }

  get(key) {
    let data

    if(this.locked) {
      return
    }

    if(this.cache) {
      data = this.cache
    } else {
      data = this.read()
      this.cache = data
    }
    
    return data[key]
  }

  put(key, value) {
    let data

    if(this.locked) {
      console.log('PUT BUSY', key, value)
      return
    }

    if(this.cache) {
      data = this.cache
    } else {
      data = this.read()
    }

    data[key] = value
    this.cache = data
    this.write(data)
  }

  read() {
    this.locked = true
    const data = fs.readFileSync(this.dbPath)
    this.locked = false
    return JSON.parse(data.toString())
  }

  write(json) {
    this.locked = true
    fs.writeFileSync(this.dbPath, JSON.stringify(json))
    this.locked = false
  }
}

module.exports = FileDB