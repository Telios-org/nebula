const { rmdir } = require('fs').promises
// const Hypercore = require('./core')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobase = require('autobase')
const Autodeebee = require('./autodeebee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')
const { DB } = require('hyperbeedeebee')
const DHT = require('@hyperswarm/dht')
const blake = require('blakejs')
const BSON = require('bson')

process.on('uncaughtException', (err) => {
  //throw err
  if(err.message.indexOf('PEER_NOT_FOUND') === -1 && err.message.indexOf('connection reset') === -1 && err.message.indexOf('PeerDiscovery') === -1)
    console.log(err)
})

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
    this.inputs = new Map()
    this._inputs = []
    this.recordsSet = new Set()
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
      this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey)
    }

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
    this.connections = [] // reset connections
    
    if(this.opts.fts && !this.blind) {
      await this.fts.ready()
    }
    
    if(!this.blind) {
      await this.autobee.ready()
      
      await this._joinSwarm(this.localInput, { server: true, client: true })
      
      if(!this.bee) {
        this.bee = new DB(this.autobee)
      }
    }

    await this.metaAutobee.ready()

    this.metaAutobeeVersion = this.metaAutobee.version()
  
    await this._joinSwarm(this.localMetaCore, { server: true, client: true })
    
    if(!this.metadb) {
      this.mdb = new DB(this.metaAutobee)

      this.metadb = await this.mdb.collection('metadb')
    }

    let localPeer

   if(this.peerPubKey) {
      try {
        this.remoteMetaCore.on('append', async () => {
          this._getDiff(this.metaAutobee, this.metaAutobeeVersion)
          setTimeout(() => {
            //this.metaAutobeeVersion = this.metaAutobee.version()
          })
          if(this.storageMaxBytes) await this._updateStatBytes()
        })

        await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
        
        //await this.metaAutobee.addInput(this.remoteMetaCore)
        await this.addInput(this.remoteMetaCore, 'meta')
        // this.inputs.set(remoteMetaCore.key.toString('hex'), this.remoteMetaCore)
        // Download blocks from remote peer
        this.remoteMetaCore.download({ start: 0, end: -1 })

        // this.coresLocal.set(this.remoteMetaCore.key.toString('hex'), this.remoteMetaCore)
        localPeer = await this.metadb.findOne({ peerPubKey: this.keyPair.publicKey.toString('hex')})
      } catch(err) {
        // No results
      }
    }

    if(!localPeer && this.broadcast) {
      const peerInfo = {
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
        const docs = await this.metadb.find()
        
        this.metaAutobeeVersion = this.metaAutobee.version()
        
        for await(const doc of docs) {
          if(doc.cores && !doc.blacklisted) {
            const peer = {
              blind: doc.blind,
              peerPubKey: doc.peerPubKey, 
              ...doc.cores
            }
            
            await this.addRemotePeer(peer)
          }
        }

      } catch(err) {
        await this.metadb.insert(peerInfo)
      } 
    }

    // if(!this.blind) {
    //   this.coresLocal.set(this.localInput.key.toString('hex'), this.localInput)
    //   this.coresLocal.set(this.localOutput.key.toString('hex'), this.localOutput)
    // }

    // this.coresLocal.set(this.localMetaCore.key.toString('hex'), this.localMetaCore)
    // this.coresLocal.set(this.metaIndex.key.toString('hex'), this.metaIndex)

    this.opened = true
  }

  async addRemotePeer(peer) {
    
    // this.emit('debug', {
    //   '!this.inputs.get(peer.writer)': !this.inputs.get(peer.writer),
    //   '!this.inputs.get(peer.meta)': !this.inputs.get(peer.meta),
    //   'peer.peerPubKey !== this.keyPair.publicKey.toString(hex)': peer.peerPubKey !== this.keyPair.publicKey.toString('hex')
    // })

    // this.emit('debug', peer)

    if(!this.inputs.get(peer.writer) &&
      !this.inputs.get(peer.meta) &&
      peer.peerPubKey !== this.keyPair.publicKey.toString('hex')) {

      // this.emit('debug', peer)

      this.inputs.set(peer.writer, {})
      this.inputs.set(peer.meta, {})

      this.syncingCoreCount += 2

      if(peer.writer) {
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

        peerWriter.on('append', async () => {
          if(!this.blind) {
            this.autobeeVersion = this.autobee.version()
            this._getDiff(this.autobee, this.autobeeVersion)
          }
          if(this.storageMaxBytes) await this._updateStatBytes()
        })

        await this._joinSwarm(peerWriter, { server: true, client: true, peer })
        this.emit('debug', `add writer ${peer.writer}`)
        this.inputs.set(peer.writer, peerWriter)

        // Download blocks from remote peer
        peerWriter.download({ start: 0, end: -1 })

        peerWriter.update().then(() => {
          this.syncingCoreCount -= 1
        })
      } else {
        this.syncingCoreCount -= 1
      }

      if(peer.meta) {
        const peerMeta = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, 
          peer.meta
        )

        await this.addInput(peerMeta, 'meta')

        peerMeta.on('append', async () => {
          this.metaAutobeeVersion = this.metaAutobee.version()
          this._getDiff(this.metaAutobee, this.metaAutobeeVersion)
          if(this.storageMaxBytes) await this._updateStatBytes()
        })

        await this._joinSwarm(peerMeta, { server: true, client: true, peer })
        // this.emit('debug', peer.meta)
        this.inputs.set(peer.meta, peerMeta)

        // Download blocks from remote peer
        peerMeta.download({ start: 0, end: -1 })

        peerMeta.update().then(() => {
          this.syncingCoreCount -= 1
          if(this.syncingCoreCount === 0) {
            this.emit('remote-cores-downloaded')
          }
        })

        
      }

      // Notify the peers about this drive's cores for bi-directional syncing
      if(peer.peerPubKey && this.peerPubKey && this.broadcast) {
      //   const noisePublicKey =  blake.blake2bHex(this.peerPubKey, null, 32)
      //   const topic = blake.blake2bHex(noisePublicKey, null, 32)
      //   const node = new DHT()
      //   const noiseSocket = node.connect(Buffer.from(topic, 'hex'))

      //   // This sync only needs to be anounced from peers that aren't the master drive.
      //   // Basically any drive that was initialized with a public key.
        
      //   if(this.peerPubKey) {    
      //     const data = JSON.stringify({
      //       type: 'sync',
      //       meta: {
      //         blind: this.blind,
      //         drivePubKey: this.peerPubKey,
      //         peerPubKey: this.keyPair.publicKey.toString('hex'),
      //         writer: !this.blind ? this.localInput.key.toString('hex') : null,
      //         meta: this.localMetaCore.key.toString('hex')
      //       }
      //     })

      //     noiseSocket.on('open', function () {
      //       console.log('GOT HERE')
      //       noiseSocket.end(data)
            
      //       setTimeout(async () => {
      //         try {
      //           await node.close()
      //         } catch(err) {

      //         }
      //       }, 5000)
      //     })
      //   }
      }
    }
  }

  async removeRemotePeer(peer) {
    try {
      if(peer.peerPubKey !== this.keyPair.publicKey.toString('hex')) {

        if(!this.blind && !peer.blind) {
          const core = this.inputs.get(peer.writer)
          
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

        const metaCore = this.inputs.get(peer.meta)

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

    // if(!core.key) {
    //   await core.ready()
    // }
    
    if(type === 'autobee') {
      this.autobee.addInput(core)
    }

    if(type === 'meta') {
      this.metaAutobee.addInput(core)
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
      
          if (server) {
            discovery.flushed().then(() => {
              this.emit('connected')
            })
          }

          swarm.flush().then(() => {
            this.emit('connected')
          })

          //Refresh if no connection has been made within 10s
          setTimeout(() => {
            if(!connected) {
              discovery.refresh({ client, server })
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

      if(this.inputs.size) {
        for (const [key, value] of this.inputs.entries()) {
          const core = this.inputs.get(key)
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

    if(this.inputs.size) {
      for await(const [key, value] of this.inputs.entries()) {
        const core = this.inputs.get(key)
        
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

    // for(const input of this._inputs) {
    //   await input.close()
    //   try {
    //     await input.sessions[0].close()
    //   } catch(e) {

    //   }
    // }
    
    this.metadb = null
    this.mdb = null
    this.bee = null
    this.inputs = new Map()

    this._inputs = []

    this.collections = {}
    this.connections = []
    this.coresLocal = new Map()
    this.coresRemote = new Map()
    this.recordsSet = new Set()
    this.removeAllListeners()
    this.opened = true
  }

  async _getDiff(bee, version) {
    const diffStream = bee.createDiffStream(version)

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

      if(node) {
        this.emit('collection-update', node)
        if(!this.blind) {
          this.autobeeVersion = this.autobee.version()
        }
        this.metaAutobeeVersion = this.metaAutobee.version()
      }
    }
  }

  async _buildNode(data, event) {
    const collection = data.key.toString().split('\x00doc')[0]
    const _data = BSON.deserialize(data.value)
    let node = { collection, type: event, value: _data }
    const hash = blake.blake2bHex(JSON.stringify(node))

    if(this.recordsSet.has(hash)) return null

    this.recordsSet.add(hash)

    if(collection === 'metadb' && _data.cores) {  
      const peer = {
        blind: _data.blind,
        peerPubKey: _data.peerPubKey, 
        ..._data.cores
      }

      if(_data.blacklisted) {
        this.emit('debug', _data)
        await this.removeRemotePeer(peer)
      } else {
        await this.addRemotePeer(peer)
      }
    }

    return node
  }
}

module.exports = Database