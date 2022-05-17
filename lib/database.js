const fs = require('fs')
const Hypercore = require('./core')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')
const { DB } = require('hyperbeedeebee')
const HyperbeeMessages = require('hyperbee/lib/messages.js')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null
    
    this.storageName = opts.storageName
    this.opts = opts
    this.keyPair = opts.keyPair
    this.autobee = null
    this.bee = null
    this.metadb = null
    this.acl = opts.acl
    this.peerPubKey = key
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/Database` : storage
    this.storageIsString = typeof storage === 'string' ? true : false
    this.joinSwarm = typeof opts.joinSwarm === 'boolean' ? opts.joinSwarm : true
    this.storageMaxBytes = opts.storageMaxBytes
    this.stat = opts.stat
    this.localDB = opts.localDB
    this.fileStatPath = opts.fileStatPath
    this.blind = opts.blind

    this._statTimeout = null

    // Init local Autobase
    this.localInput = new Hypercore(
      this.storageIsString ? `${this.storage}/local-writer` : this.storage, 
      null, 
      { 
        encryptionKey: this.encryptionKey,
        storageNamespace: `${this.storageName}:local-writer` 
      }
    )
    
    this.localOutput = new Hypercore(
      this.storageIsString ? `${this.storage}/local-index` : this.storage, 
      null, 
      { 
        encryptionKey: this.encryptionKey,
        storageNamespace: `${this.storageName}:local-index`
      }
    )

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage, { storageNamespace: `${this.storageName}:meta-local` })
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey, { storageNamespace: `${this.storageName}:meta-remote` })
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage, null, { storageNamespace: `${this.storageName}:meta-index` })

    this.collections = {}
    this.cores = [
      this.localInput,
      this.localOutput,
      this.localMetaCore,
      this.remoteMetaCore,
      this.metaIndex
    ]
    this.connections = []

    // Init local search index
    if(opts.fts) {
      this.fts = new HyperFTS(this.storageIsString ? `${this.storage}/fts` : this.storage, this.encryptionKey)
    }
  }

  async ready() {
    let remotePeers

    this.connections = [] // reset connections

    if(this.opts.fts && !this.blind) {
      await this.fts.ready()
    }

    await this._joinSwarm(this.localMetaCore, { server: true, client: true })

    await this._joinSwarm(this.localInput, { server: true, client: true })
  
    this.autobee = new Autobee({
      inputs: [this.localInput], 
      defaultInput: this.localInput, 
      outputs: this.localOutput,
      keyEncoding: 'bson',
      valueEncoding: 'bson'
    })

    await this.autobee.ready()

    this.bee = new DB(this.autobee)

    this.metadb = new Autobee({
      inputs: [this.localMetaCore], 
      defaultInput: this.localMetaCore, 
      outputs: this.metaIndex,
      valueEncoding: 'json'
    })

    const metaStream = this.metadb.createReadStream({ live: true })

    const peerMap = new Map()

    metaStream.on('data', async data => {
      if(this.storageMaxBytes) {
        await this._updateStatBytes()
      }

      if(data && data.value && data.value.toString().indexOf('hyperbee') === -1) {
        const op = HyperbeeMessages.Node.decode(data.value)
        const node = {
          key: op.key.toString('utf8'),
          value: JSON.parse(op.value.toString('utf8')),
          seq: data.seq
        }

        if(node.key === '__peers' && !node.value[this.keyPair.publicKey.toString('hex')]) {
          for(const peer in node.value) {
            const p = peerMap.get(node.value[peer].writer)

            if(!p && node.value[peer].writer) {
              peerMap.set(node.value[peer].writer, node.value[peer])
              this.addRemotePeer(node.value[peer])
            }
          }
        }
      }
    })

    await this.metadb.ready()
    await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
    
    if(this.peerPubKey) {
      await this.metadb.addInput(this.remoteMetaCore)
      remotePeers = await this.metadb.get('__peers')
    }
    
    if(!remotePeers) {
      await this.metadb.put('__peers', {
        [this.keyPair.publicKey.toString('hex')]: {
          writer: this.localInput.key.toString('hex'),
          meta: this.localMetaCore.key.toString('hex')
        }
      })
    }
  }

  async addRemotePeer(peer) {
    const peerWriter = new Hypercore(
      this.storageIsString ? `${this.storage}/peers/${peer.writer}` : this.storage, 
      peer.writer,
      {
        encryptionKey: this.encryptionKey,
        storageNamespace: `${this.storageName}:peers:${peer.writer}` 
      }
    )

    peerWriter.on('append', async () => {
      if(this.storageMaxBytes) await this._updateStatBytes()
      this.emit('collection-update')
    })

    this.cores.push(peerWriter)

    if(!this.blind) {
      await this.autobee.addInput(peerWriter)
    }

    await this._joinSwarm(peerWriter, { server: true, client: true })
  }

  async collection(name) {
    const _collection = await this.bee.collection(name)
    const self = this
    const collection = new Proxy(_collection, {
      get (target, prop) {
        if(prop === 'insert') {
          return async (doc, opts) => {
            const _doc = {...doc, author: self.keyPair.publicKey.toString('hex') }
            return await _collection.insert(_doc)
          }
        }

        if(prop === 'update') {
          return async (query, data, opts) => {
            let _data = {...data }
            _data = {..._data, author: self.keyPair.publicKey.toString('hex') }
            return await _collection.update(query, _data, opts)
          }
        }

        if(prop === 'remove') {
          return async (query) => {
            if(self.fts) {
              await self.fts.deIndex({ db:_collection, name, query })
            }
            await _collection.remove(query)
          }
        }
        return _collection[prop]
      }
    })

    collection.ftsIndex = async (props, docs) => {
      if(!this.opts.fts) throw('Full text search is currently disabled because the option was set to false')
      await this.fts.index({ name, props, docs })
    }

    collection.search = async (query, opts) => {
      if(!this.opts.fts) throw('Full text search is currently disabled because the option was set to false')
      
      return this.fts.search({ db:collection, name, query, opts })
    }

    this.collections[name] = collection
    return collection
  }

  async addInput(key) {
    const core = new Hypercore( this.storageIsString ? `${this.storage}/${key.toString('hex')}` : this.storage, 
      key,
      { 
        encryptionKey: this.encryptionKey, 
        storageNamespace: `${this.storageName}:${key.toString('hex')}` 
      }
    )

    await core.ready()

    this.cores.push(core)

    await this.autobee.addInput(core)

    await this._joinSwarm(core, { server: true, client: true })
  }

  async removeInput(key) {
    // TODO
  }

  async close() {
    if(this.opts.fts) {
      await this.fts.close()
    }

    for await(const core of this.cores) {
      await core.close()
    }

    for await(const conn of this.connections) {
      await conn.destroy()
    }
  }

  // TODO: Figure out how to multiplex these connections
  async _joinSwarm(core, { server, client }) {
    if(!this.storageMaxBytes || this.stat.total_bytes < this.storageMaxBytes) {
      const swarm = new Hyperswarm()
    
      await core.ready()

      if(this.joinSwarm) {
        try {
          swarm.on('connection', async (socket, info) => {
            socket.pipe(core.replicate(info.client)).pipe(socket)
          })
      
          const topic = core.discoveryKey;
          const discovery = swarm.join(topic, { server, client })

          this.connections.push(discovery)
      
          if (server) {
            discovery.flushed().then(() => {
              this.emit('connected')
            })
            
          }

          swarm.flush().then(() => {
            this.emit('connected')
          })

        } catch(e) {
          this.emit('disconnected')
        }
      }
    }
  }

  async _leaveSwarm() {
    for await(const conn of this.connections) {
      await conn.destroy()
    }
  }

  async _updateStatBytes(fileBytes) {
    return new Promise(async (resolve, reject) => {
      let totalBytes = 0

      if(this._statTimeout && !fileBytes) {
        return resolve(this.stat.total_bytes)
      }

      this._statTimeout = setTimeout(() => {
        clearTimeout(this._statTimeout)
        this._statTimeout = null
      }, 200)

      for(const core of this.cores) {
        totalBytes += core.byteLength
      }

      this.stat.core_bytes = totalBytes
      
      if(fileBytes) {
        this.stat.file_bytes += fileBytes
        this.stat.total_bytes = totalBytes + this.stat.file_bytes
      }

      this.localDB.put('stat', { ...this.stat })

      if(this.stat.total_bytes >= this.storageMaxBytes) {
        // Shut down replication
        await this._leaveSwarm()
      }

      return resolve(totalBytes)
    })
  }
}

module.exports = Database


