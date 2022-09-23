const hypercore = require('hypercore');

class Hypercore {
  constructor(storage, key, opts) {
    this.storage = storage
    this.opts = opts

    if(key && typeof key === 'object') {
      this.opts = key
    }

    return new hypercore(this.storage, key, opts)
  }
}

module.exports = Hypercore