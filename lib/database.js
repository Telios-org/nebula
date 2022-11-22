const { rmdir } = require('fs').promises
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobase = require('autobase')
const Autodeebee = require('../node_modules/hyperbeedeebee/autodeebee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')
const { DB } = require('hyperbeedeebee')
const BSON = require('bson')
const debounce = require('debounce')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null
    
    this.storageName = opts.storageName
    this.opts = opts
    this.keyPair = opts.keyPair
    this.autobee = null
    this.bee = null
    this.metaAutobee = null
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
    this.syncingCoreCount = 0
    this.broadcast = opts.broadcast
    this._inputs = new Map()
    this._dbVersion = opts.dbVersion
    this.localInput = {
      key: Buffer.alloc(32)
    }

    this._statTimeout = null

    // Init local Autobase
    if(!this.blind) {
      this.localInput = new Hypercore(
        this.storageIsString ? `${this.storage}/local-writer` : this.storage, 
        null, 
        { 
          encryptionKey: this.encryptionKey
        }
      )
    
      this.localOutput = new Hypercore(
        this.storageIsString ? `${this.storage}/local-index` : this.storage, 
        null, 
        { 
          encryptionKey: this.encryptionKey
        }
      )

      const base1 = new Autobase({
        inputs: [this.localInput],
        localOutput: this.localOutput,
        localInput: this.localInput
      })

      this.autobee = new Autodeebee(base1)
    }

    // Init Meta Autobase
    if(this.peerPubKey) {
      
    }
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey)

    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage)
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage)

    const base2 = new Autobase({
      inputs: [this.localMetaCore], 
      localOutput: this.metaIndex,
      localInput: this.localMetaCore 
    })

    try {
      this.metaAutobee = new Autodeebee(base2)
    } catch(e) {

    }

    this.addInput(this.remoteMetaCore, 'meta')

    this.collections = {}
    this.connections = []
    this.coresLocal = new Map()
    this.coresRemote = new Map()

    // Init local search index
    if(opts.fts) {
      this.fts = new HyperFTS(this.storageIsString ? `${this.storage}/fts` : this.storage, this.encryptionKey)
    }
  }

  async ready() {
    if(this.localDB) {
      this.lastWriterSeq = this.localDB.get('lastWSeq')
    }

    this.connections = [] // reset connections
    
    if(this.opts.fts && !this.blind) {
      await this.fts.ready()
    }

    if(!this.blind) {
      this.autobeeVersion = this.autobee.version()
      await this.autobee.ready()
      
      await this._joinSwarm(this.localInput, { server: true, client: true })

      if(!this.bee) {
        this.bee = new DB(this.autobee)
      }
    }
    
    this.metaAutobeeVersion = this.metaAutobee.version()
    await this.metaAutobee.ready()
    
    await this._joinSwarm(this.localMetaCore, { server: true, client: true })
    
    if(!this.metadb) {
      this.mdb = new DB(this.metaAutobee)

      this.metadb = await this.mdb.collection('metadb')
    }

   if(this.peerPubKey) {
      try {
        this._handleCoreSyncStatus(1)
        
        let remoteMetaDidSync = false

        this.remoteMetaCore.on('append', debounce(async () => {
          if(!remoteMetaDidSync) {
            this._handleCoreSyncStatus(-1)
            remoteMetaDidSync = true
          }
          await this._getDiff(this.metaAutobee, this.metaAutobeeVersion)
          if(this.storageMaxBytes) await this._updateStatBytes()
          this.metaAutobeeVersion = this.metaAutobee.version()
        }, 500))

        await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })

        // Download blocks from remote peer
        this.remoteMetaCore.download({ start: 0, end: -1 })
      } catch(err) {
        // No results
      }
    }

    if(!this.peerPubKey && this.broadcast) {
      const peerInfo = {
        __version: this._dbVersion,
        blacklisted: false,
        peerPubKey: this.keyPair.publicKey.toString('hex'),
        blind: this.blind,
        cores : {
          writer: !this.blind ? this.localInput.key.toString('hex') : null,
          meta: this.localMetaCore.key.toString('hex')
        }
      }

      try {
        const doc = await this.metadb.findOne({ peerPubKey: this.keyPair.publicKey.toString('hex')})
      } catch(err) {
        await this.metadb.insert(peerInfo)
      } 
    }

    try {
      const docs = await this.metadb.find()
      
      for await(const doc of docs) {
        if(doc.cores && !doc.blacklisted && doc.__version === this._dbVersion) {
          const peer = {
            blind: doc.blind,
            peerPubKey: doc.peerPubKey, 
            ...doc.cores
          }
          await this.addRemotePeer(peer)
        }
      }
    } catch(err) {
      console.log(err)
    }

    this.opened = true
  }

  async addRemotePeer(peer) {
    if(!this._inputs.get(peer.writer) &&
      !this._inputs.get(peer.meta) &&
      peer.peerPubKey !== this.keyPair.publicKey.toString('hex')) {

      if(peer.meta && !peer.writer) {
        this._handleCoreSyncStatus(1)
      } else {
        this._handleCoreSyncStatus(2)
      }

      if(peer.writer) {
        let peerWriterDidSync = false
        const peerWriter = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.writer}` : this.storage, 
          peer.writer,
          {
            encryptionKey: this.encryptionKey
          }
        )

        if(!this.blind) {
          await this.addInput(peerWriter, 'autobee')
        }

        peerWriter.update().then(() => {
          peerWriter.on('append', debounce(async () => {
            if(!peerWriterDidSync) this._handleCoreSyncStatus(-1)
            
            peerWriterDidSync = true

            if(!this.blind) {
              this._getDiff(this.autobee, this.autobeeVersion, 'autob', peerWriter.length)
              if(this.storageMaxBytes) this._updateStatBytes()
            }
          }, 500))
        })

        await this._joinSwarm(peerWriter, { server: true, client: true, peer })
        this._inputs.set(peer.writer, peerWriter)

        // Download blocks from remote peer
        peerWriter.download({ start: 0, end: -1 })
      }

      if(peer.meta) {
        let peerMetaDidSync = false
        const peerMeta = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, 
          peer.meta
        )

        await this.addInput(peerMeta, 'meta')

        peerMeta.on('append', debounce(async () => {
          if(!peerMetaDidSync) this._handleCoreSyncStatus(-1)
          
          peerMetaDidSync = true

          await this._getDiff(this.metaAutobee, this.metaAutobeeVersion, 'meta')
          if(this.storageMaxBytes) this._updateStatBytes()
          this.metaAutobeeVersion = this.metaAutobee.version()
        }, 500))

        await this._joinSwarm(peerMeta, { server: true, client: true, peer })
        this._inputs.set(peer.meta, peerMeta)
        // Download blocks from remote peer
        peerMeta.download({ start: 0, end: -1 })
      }
    }
  }

  async removeRemotePeer(peer) {
    try {
      if(peer.peerPubKey !== this.keyPair.publicKey.toString('hex')) {

        if(!this.blind && !peer.blind) {
          const core = this._inputs.get(peer.writer)
          
          if(core) {
            await this.autobee.removeInput(core)
            await core.close()

            // Remove Hypercores from disk
            await rmdir(`${this.storage}/peers/${peer.writer}`, {
              recursive: true,
              force: true,
            })
          }
        }

        const metaCore = this._inputs.get(peer.meta)

        if(metaCore) {
          await this.metaAutobee.removeInput(metaCore)
          await metaCore.close()

          // Remove Hypercores from disk
          await rmdir(`${this.storage}/peers/${peer.meta}`, {
            recursive: true,
            force: true,
          })
        }
      }
    } catch(e) {}
  }

  async addInput(core, type) {
    if(!type) return

    if(!core.key) {
      await core.ready()
    }
    
    if(type === 'autobee') {
      await this.autobee.addInput(core)
    }

    if(type === 'meta') {
      await this.metaAutobee.addInput(core)
    }
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
            await _collection.delete(query)
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

  // TODO: Figure out how to multiplex these connections
  async _joinSwarm(core, { server, client, peer }) {
    
    if(!this.storageMaxBytes || this.stat.total_bytes < this.storageMaxBytes) {
      const swarm = new Hyperswarm()
      
      try {
        await core.ready()
      } catch(err) {
        console.log(err)
      }

      if(this.joinSwarm) {
        let connected = false
        try {
          swarm.on('connection', async (socket, info) => {
            connected = true

            socket.on('error', err => {})

            if(peer && peer.peerPubKey) {
              this.emit('peer-connected', peer)

              socket.on('close', () => {
                this.emit('peer-disconnected', peer)
              })
            }

            socket.pipe(core.replicate(info.client)).pipe(socket)
          })
      
          const topic = core.discoveryKey;
          const discovery = swarm.join(topic, { server, client })

          this.connections.push(swarm)
      
          if(server) {
            discovery.flushed().then(() => {
              this.emit('connected')
            })
          }

          swarm.flush().then(() => {
            this.emit('connected')
          })

          // Refresh if no connection has been made within 10s
          const refreshInt = setInterval(() => {
            if(!connected) {
              discovery.refresh({ client, server })
            } else {
              clearInterval(refreshInt)
            }
          }, 10000)
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

      totalBytes += this.metaIndex.byteLength
      totalBytes += this.localMetaCore.byteLength

      if(!this.blind) {
        totalBytes += this.localInput.byteLength
        totalBytes += this.localOutput.byteLength
      }

      if(this._inputs.size) {
        for (const [key, value] of this._inputs.entries()) {
          const core = this._inputs.get(key)
          totalBytes += core.byteLength
        }
      }

      this.stat.core_bytes = totalBytes
      
      if(fileBytes) {
        this.stat.file_bytes += fileBytes
        this.stat.total_bytes = totalBytes + this.stat.file_bytes
      }

      this.localDB.put('stat', { ...this.stat })

      if(this.storageMaxBytes && this.stat.total_bytes >= this.storageMaxBytes) {
        // Shut down replication
        await this._leaveSwarm()
      }

      return resolve(totalBytes)
    })
  }

  async close() {
    if(this.opts.fts) {
      await this.fts.close()
    }

    if(this.autobee) {
      await this.autobee.close()
    }

    await this.metaAutobee.close()

    for await(const conn of this.connections) {
      await conn.destroy()
    }


    if(this.remoteMetaCore) {
      for (const session of this.remoteMetaCore.sessions) {
        if(session) {
          await session.close()
        }
      }
    }

    if(this.metaIndex) {
      for (const session of this.metaIndex.sessions) {
        if(session) {
          await session.close()
        }
      }
    }

    if(this.localOutput) {
      for (const session of this.localOutput.sessions) {
        if(session) {
          await session.close()
        }
      }
    }

    // Only way found to force-close autobase and prevent file lock errors when
    // trying to re-instantiate
    try {
      await this.remoteMetaCore.sessions[0].close()
      while(this.remoteMetaCore.sessions[0].opened) {
        await this.remoteMetaCore.sessions[0].close()
      }
    } catch(e) {}

    try {
      await this.metaIndex.sessions[0].close()
      while(this.metaIndex.sessions[0].opened) {
        await this.metaIndex.sessions[0].close()
      }
    } catch(e) {}

    try {
      await this.localOutput.sessions[0].close()
      while(this.localOutput.sessions[0].opened) {
        await this.localOutput.sessions[0].close()
      }
    } catch(e) {}

    if(this._inputs.size) {
      for await(const [key, value] of this._inputs.entries()) {
        const core = this._inputs.get(key)
        
        if(core && core.opened) {
          await core.close()
        }
        
        try {
          await core.sessions[0].close()

          while(core.sessions[0].opened) {
            await core.sessions[0].close()
          }
        } catch(e) {

        }
      }
    }

    
    this.metadb = null
    this.mdb = null
    this.bee = null
    this._inputs = new Map()

    this._inputs = []

    this.collections = {}
    this.connections = []
    this.coresLocal = new Map()
    this.coresRemote = new Map()
    this.recordsSet = new Set()
    this.removeAllListeners()
    this.opened = false
  }

  async _getDiff(bee, version, type, length) {
    let diffStream = bee.createDiffStream(version)
    let lastSeq = 0

    for await(const data of diffStream) {
      let node
      // New Record
      if(data && data.left && data.left.key.toString().indexOf('\x00doc') > -1 && !data.right) {
        node = await this._buildNode(data.left, 'create')
      }

      // Recored Updated
      if(data && data.left && data.left.key.toString().indexOf('\x00doc') > -1 && data.right && data.right.key.toString().indexOf('\x00doc') > -1) {
        node = await this._buildNode(data.left, 'update')
      }

      // Deleted Record
      if(data && !data.left && data.right && data.right.key.toString().indexOf('\x00doc') > -1) {
        node = await this._buildNode(data.right, 'del')
      }

      if(data && data.right && data.right.key.toString().indexOf('\x00doc') > -1 || data && data.left && data.left.key.toString().indexOf('\x00doc') > -1) {
        if(type === 'autob') {
          lastSeq = node.seq
          this.autobeeVersion = bee.version()
        }
      }

      if(node && node.collection === 'metadb' || 
        node && node.collection !== 'metadb' && !this.lastWriterSeq ||
        node && node.collection !== 'metadb' && this.lastWriterSeq && this.lastWriterSeq < node.seq
      ) {
        this.emit('collection-update', node)
      }
    }

    if(this.localDB) {
      this.localDB.put('lastWSeq', lastSeq)
    }

    diffStream = null
  }

  async _buildNode(data, event) {
    const _data = BSON.deserialize(data.value)
    const collection = data.key.toString().split('\x00doc')[0]
    let node = { collection, type: event, value: _data, seq: data.seq }

    if(collection === 'metadb' && _data.cores && _data.__version === this._dbVersion) {  
      const peer = {
        blind: _data.blind,
        peerPubKey: _data.peerPubKey, 
        ..._data.cores
      }

      if(_data.blacklisted) {
        await this.removeRemotePeer(peer)
      } else {
        await this.addRemotePeer(peer)
      }
    }

    return node
  }

  _handleCoreSyncStatus(count) {
    this.syncingCoreCount += count
    if(this.syncingCoreCount === 0) {
      this.emit('remote-cores-downloaded')
    }
  }
}

module.exports = Database