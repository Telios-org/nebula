const { rmdir } = require('fs').promises
const Hypercore = require('./core')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')
const { DB } = require('hyperbeedeebee')
const HyperbeeMessages = require('hyperbee/lib/messages.js')
const DHT = require('@hyperswarm/dht')

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
        
        if(node.key.indexOf('__peer:') > -1) {          
          const peer = {
            blind: node.value.blind,
            peerPubKey: node.value.peerPubKey, 
            ...node.value.cores 
          }

          if(!node.value.blacklisted) {
            await this.addRemotePeer(peer)
          } else {
            await this.removeRemotePeer(peer)
          }
        }
      }
    })

    await this.metadb.ready()
    await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
    
    let localPeer

    if(this.peerPubKey) {
      await this.metadb.addInput(this.remoteMetaCore)
      localPeer = await this.metadb.get(`__peer:${this.keyPair.publicKey.toString('hex')}`)
    }
    
    if(!localPeer) {
      await this.metadb.put(`__peer:${this.keyPair.publicKey.toString('hex')}`, {
        blacklisted: false,
        peerPubKey: this.keyPair.publicKey.toString('hex'),
        blind: this.blind,
        cores : {
          writer: this.localInput.key.toString('hex'),
          meta: this.localMetaCore.key.toString('hex')
        }
      })
    }

    this.coresLocal.set(this.localInput.key.toString('hex'), this.localInput)
    this.coresLocal.set(this.localOutput.key.toString('hex'), this.localOutput)
    this.coresLocal.set(this.localMetaCore.key.toString('hex'), this.localMetaCore)
    this.coresLocal.set(this.remoteMetaCore.key.toString('hex'), this.remoteMetaCore)
    this.coresLocal.set(this.metaIndex.key.toString('hex'), this.metaIndex)
  }

  async addRemotePeer(peer) {
    if(!this.coresRemote.get(peer.writer) &&
      !this.coresRemote.get(peer.meta) &&
      peer.writer !== this.localInput.key.toString('hex') &&
      peer.meta !== this.localMetaCore.key.toString('hex')) {

      this.coresRemote.set(peer.writer, {})
      this.coresRemote.set(peer.meta, {})

      if(!peer.blind) {
        const peerWriter = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.writer}` : this.storage, 
          peer.writer,
          {
            encryptionKey: this.encryptionKey,
            storageNamespace: `${this.storageName}:peers:${peer.writer}` 
          }
        )

        this.coresRemote.set(peer.writer, peerWriter)

        await this._joinSwarm(peerWriter, { server: true, client: true, peer })

        // Download blocks from remote peer
        peerWriter.download({ start: 0, end: -1 })

        if(!peer.blind) {
          await this.autobee.addInput(peerWriter)
        }
      }

      const peerMeta = new Hypercore(
        this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, 
        peer.meta,
        {
          storageNamespace: `${this.storageName}:peers:${peer.meta}` 
        }
      )

      this.coresRemote.set(peer.meta, peerMeta)

      peerMeta.on('append', async () => {
        if(this.storageMaxBytes) await this._updateStatBytes()
        this.emit('collection-update')
      })

      await this._joinSwarm(peerMeta, { server: true, client: true, peer })

      // Download blocks from remote peer
      peerMeta.download({ start: 0, end: -1 })

      await this.metadb.addInput(peerMeta)

      // Notify the peer that was just added about this drive's cores for bi-directional syncing
      if(peer.peerPubKey) {
        const noisePublicKey = peer.peerPubKey
        const node = new DHT()
        const noiseSocket = node.connect(Buffer.from(noisePublicKey, 'hex'))

        // This sync only needs to be anounced from peers that aren't the master drive.
        // Basically any drive that was initialized with a public key.
        if(this.peerPubKey) {
          const data = JSON.stringify({
            type: 'sync',
            meta: {
              blind: this.blind,
              drivePubKey: this.peerPubKey,
              peerPubKey: this.keyPair.publicKey.toString('hex'),
              writer: this.localInput.key.toString('hex'),
              meta: this.localMetaCore.key.toString('hex')
            }
          })

          noiseSocket.on('open', function () {
            noiseSocket.end(data)
            
            setTimeout(() => {
              try {
                node.close()
              } catch(err) {

              }
            }, 5000)
          })
        }
      }
    }
  }

  async removeRemotePeer(peer) {
    const peerWriter = this.coresRemote.get(peer.writer)
    const peerMeta = this.coresRemote.get(peer.meta)

    if(peerWriter) {
      if(!this.blind && !peer.blind) {
        await this.autobee.removeInput(peerWriter)
      }

      // Close Hypercores
      await peerWriter.close()

      // Remove Hypercores from disk
      await rmdir(`${this.storage}/peers/${peer.writer}`, {
        recursive: true,
        force: true,
      })

      // this.coresRemote.delete(peer.writer)
      this.coresRemote.set(peer.writer, null)
    }

    if(peerMeta) {
      await this.metadb.removeInput(peerMeta)

      // Close Hypercores
      await peerMeta.close()

      // Remove Hypercores from disk
      await rmdir(`${this.storage}/peers/${peer.meta}`, {
        recursive: true,
        force: true,
      })

      // this.coresRemote.delete(peer.meta)
      this.coresRemote.set(peer.meta, null)
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

  // TODO: Figure out how to multiplex these connections
  async _joinSwarm(core, { server, client, peer }) {
    if(!this.storageMaxBytes || this.stat.total_bytes < this.storageMaxBytes) {
      const swarm = new Hyperswarm()
    
      await core.ready()

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

          this.connections.push(discovery)
      
          if (server) {
            discovery.flushed().then(() => {
              this.emit('connected')
            })
          }

          swarm.flush().then(() => {
            this.emit('connected')
          })

          // Refresh if no connection has been made within 10s
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

      for (const [key, value] of this.coresRemote.entries()) {
        const core = this.coresRemote.get(key)
        totalBytes += core.byteLength
      }

      for (const [key, value] of this.coresLocal.entries()) {
        const core = this.coresLocal.get(key)
        totalBytes += core.byteLength
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

    for await(const [key, value] of this.coresRemote.entries()) {
      const core = this.coresRemote.get(key)

      if(core) {
        await core.close()
      }
    }

    for await(const [key, value] of this.coresLocal.entries()) {
      const core = this.coresLocal.get(key)

      if(core) {
        await core.close()
      }
    }

    for await(const conn of this.connections) {
      await conn.destroy()
    }

    this.collections = {}
    this.connections = []
    this.coresLocal = new Map()
    this.coresRemote = new Map()
    this.removeAllListeners()
  }
}

module.exports = Database


