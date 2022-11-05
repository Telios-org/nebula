const { rmdir } = require('fs').promises
// const Hypercore = require('./core')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobase = require('autobase')
const Autodeebee = require('./autodeebee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')
const { DB } = require('hyperbeedeebee')
const HyperbeeMessages = require('hyperbee/lib/messages.js')
const DHT = require('@hyperswarm/dht')
const blake = require('blakejs')

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
    }

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage)
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey)
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage, null)

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
    console.log('db 1')
    if(this.opts.fts && !this.blind) {
      await this.fts.ready()
    }
    console.log('db 2')
    await this._joinSwarm(this.localMetaCore, { server: true, client: true })
    console.log('db 3', this.localMetaCore.key.toString('hex'))
    if(!this.blind) {
      await this._joinSwarm(this.localInput, { server: true, client: true })
      console.log('db 4')
      const base1 = new Autobase({
        inputs: [this.localInput],
        localOutput: this.localOutput,
        localInput: this.localInput
      })
    
      this.autobee = new Autodeebee(base1)

      this.bee = new DB(this.autobee)
    }

    console.log('db 5')
    const base2 = new Autobase({
      inputs: [this.localMetaCore], 
      localInput: this.localMetaCore, 
      localOutput: this.metaIndex
    })
  
    this.metaAutobee = new Autodeebee(base2)

    const mdb = new DB(this.metaAutobee)

    this.metadb = await mdb.collection('metadb')

    console.log('db 6')

    const metaStream = this.metaAutobee.createReadStream({ live: true })
    console.log('db 7')
    // metaStream.on('data', async data => {
    //   if(this.storageMaxBytes) {
    //     await this._updateStatBytes()
    //   }
      
    //   if(data && data.value && data.value.toString().indexOf('hyperbee') === -1) {
    //     const op = HyperbeeMessages.Node.decode(data.value)
    //     const node = {
    //       key: op.key.toString('utf8'),
    //       value: JSON.parse(op.value.toString('utf8')),
    //       seq: data.seq
    //     }
        
    //     if(node.key.indexOf('__peer:') > -1) {          
    //       const peer = {
    //         blind: node.value.blind,
    //         peerPubKey: node.value.peerPubKey, 
    //         ...node.value.cores 
    //       }

    //       if(!node.value.blacklisted) {
    //         console.log('ADD REMOTE PEER')
    //         await this.addRemotePeer(peer)
    //       } else {
    //         await this.removeRemotePeer(peer)
    //       }
    //     }
    //   }
    // })

    console.log('db 8')
    await this.metaAutobee.ready()
    console.log('db 9')
    await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
    console.log('db 10')
    
    let localPeer

    if(this.peerPubKey) {
      try {
        console.log('db 11')
        console.log('ADD REMOTE REMOTE INPUT')
        await this.metaAutobee.addInput(this.remoteMetaCore)
        console.log('db 12')
        localPeer = await this.metadb.findOne(`__peer:${this.keyPair.publicKey.toString('hex')}`)
        console.log('db 13')
      } catch(err) {
        // handle error
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
      console.log('db 14')
      await this.metadb.insert({ [`__peer:${this.keyPair.publicKey.toString('hex')}`]: peerInfo })
      console.log('db 15')
    }

    if(!this.blind) {
      this.coresLocal.set(this.localInput.key.toString('hex'), this.localInput)
      this.coresLocal.set(this.localOutput.key.toString('hex'), this.localOutput)
    }

    console.log('db 15')

    await this.metaIndex.ready()

    console.log('db 16')
    this.coresLocal.set(this.localMetaCore.key.toString('hex'), this.localMetaCore)
    this.coresLocal.set(this.remoteMetaCore.key.toString('hex'), this.remoteMetaCore)
    this.coresLocal.set(this.metaIndex.key.toString('hex'), this.metaIndex)
  }

  async addRemotePeer(peer) {
    console.log('ADD REMOTE PEER')
    if(!this.coresRemote.get(peer.writer) &&
      !this.coresRemote.get(peer.meta) &&
      peer.writer !== this.localInput.key.toString('hex') &&
      peer.meta !== this.localMetaCore.key.toString('hex')) {

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
          await this.autobee.addInput(peerWriter)
        }

        this.coresRemote.set(peer.writer, peerWriter)

        peerWriter.on('append', async () => {
          console.log('ADD REMOTE PEER - APPEND')
          if(this.storageMaxBytes) await this._updateStatBytes()
          this.emit('collection-update')
        })

        await this._joinSwarm(peerWriter, { server: true, client: true, peer })

        // Download blocks from remote peer
        peerWriter.download({ start: 0, end: -1 })

        peerWriter.update().then(() => {
          console.log('ADD REMOTE PEER - UPDATE')
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

        this.coresRemote.set(peer.meta, peerMeta)

        peerMeta.on('append', async () => {
          if(this.storageMaxBytes) await this._updateStatBytes()
        })

        await this._joinSwarm(peerMeta, { server: true, client: true, peer })

        // Download blocks from remote peer
        peerMeta.download({ start: 0, end: -1 })

        peerMeta.update().then(() => {
          this.syncingCoreCount -= 1
          if(this.syncingCoreCount === 0) {
            this.emit('remote-cores-downloaded')
          }
        })

        await this.metaAutobee.addInput(peerMeta)
      }

      // Notify the peers about this drive's cores for bi-directional syncing
      if(peer.peerPubKey && this.peerPubKey && this.broadcast) {
        const noisePublicKey =  blake.blake2bHex(this.peerPubKey, null, 32)
        const topic = blake.blake2bHex(noisePublicKey, null, 32)
        const node = new DHT()
        const noiseSocket = node.connect(Buffer.from(topic, 'hex'))

        // This sync only needs to be anounced from peers that aren't the master drive.
        // Basically any drive that was initialized with a public key.
        if(this.peerPubKey) {
          const data = JSON.stringify({
            type: 'sync',
            meta: {
              blind: this.blind,
              drivePubKey: this.peerPubKey,
              peerPubKey: this.keyPair.publicKey.toString('hex'),
              writer: !this.blind ? this.localInput.key.toString('hex') : null,
              meta: this.localMetaCore.key.toString('hex')
            }
          })

          noiseSocket.on('open', function () {
            console.log('SOCKET OPENED!!!')
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

      this.coresRemote.set(peer.writer, null)
    }

    if(peerMeta) {
      await this.metaAutobee.removeInput(peerMeta)

      // Close Hypercores
      await peerMeta.close()

      // Remove Hypercores from disk
      await rmdir(`${this.storage}/peers/${peer.meta}`, {
        recursive: true,
        force: true,
      })

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
        console.log('CLOSED', key)
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


