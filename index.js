const fs = require('fs')
const EventEmitter = require('events')
const getDirName = require('path').dirname
const path = require('path')
const Database = require('./lib/database')
const Hyperbee = require('hyperbee')
const Hypercore = require('./lib/core')
const pump = require('pump')
const Crypto = require('./lib/crypto')
const Swarm = require('./lib/swarm')
const stream = require('stream')
const blake = require('blakejs')
const Hyperswarm = require('hyperswarm')
const DHT = require('@hyperswarm/dht')
const HyperbeeMessages = require('hyperbee/lib/messages.js')
const MemoryStream = require('memorystream')
const { v4: uuidv4 } = require('uuid')
const FixedChunker = require('./util/fixedChunker.js')
const RequestChunker = require('./util/requestChunker.js')
const WorkerKeyPairs = require('./util/workerKeyPairs.js')
const isOnline = require('is-online')
const BSON = require('bson')
const FileDB = require('./util/filedb.util')

const HASH_OUTPUT_LENGTH = 32 // bytes
const MAX_PLAINTEXT_BLOCK_SIZE = 65536
const MAX_ENCRYPTED_BLOCK_SIZE = 65553
const FILE_TIMEOUT = 5000 // How long to wait for the on data event when downloading a file from a remote drive.
const FILE_RETRY_ATTEMPTS = 2 // Fail to fetch file after 3 attempts
const FILE_BATCH_SIZE = 10 // How many parallel requests are made in each file request batch


class Drive extends EventEmitter {
  constructor(
    drivePath, 
    peerPubKey, // Key used to clone and seed drive. Should only be shared with trusted sources
    { 
      storage, 
      keyPair, // ed25519 keypair to listen on
      writable, 
      swarmOpts, 
      encryptionKey, 
      fileTimeout, 
      fileRetryAttempts, 
      checkNetworkStatus, 
      joinSwarm,
      fullTextSearch, // Initialize a corestore to support full text search indexes.
      blind, // Set to true if blind mirroring another drive (you don't have the encryption key)
      storageMaxBytes, // Max size this drive will store in bytes before turning off replication/file syncing
      syncFiles = true,
      includeFiles,
      broadcast = true // Tell the other peer drives about this drive
    }
  ) {
    super()

    this.storage = storage
    this.encryptionKey = encryptionKey
    this.database = null
    this.db = null;
    this.drivePath = drivePath
    this.swarmOpts = swarmOpts
    this.publicKey = null
    this.peerPubKey = peerPubKey 
    this.peerWriterKey = null
    this.keyPair = keyPair ? keyPair : DHT.keyPair()
    this.writable = writable
    this.fullTextSearch = fullTextSearch 
    this.fileTimeout = fileTimeout || FILE_TIMEOUT
    this.fileRetryAttempts = fileRetryAttempts-1 || FILE_RETRY_ATTEMPTS-1
    this.requestQueue = new RequestChunker(null, FILE_BATCH_SIZE)
    this.checkNetworkStatus = checkNetworkStatus
    this.joinSwarm = typeof joinSwarm === 'boolean' ? joinSwarm : true
    this.peers = new Set()
    this.network = {
      internet: false,
      drive: false
    }

    this.blind = blind ? blind : false
    this.storageMaxBytes = storageMaxBytes || false
    this.syncFiles = syncFiles
    this.includeFiles = includeFiles
    this.opened = false
    this.broadcast = broadcast

    // When using custom storage, transform drive path into beginning of the storage namespace
    this.storageName = drivePath.slice(drivePath.lastIndexOf('/') + 1, drivePath.length)
  
    this._localCore = null
    this._localHB = null // Optional datastore for storing encrypted data locally. This will not sync with peers or be replicated.
    this._swarm = null
    this._workerKeyPairs = new WorkerKeyPairs(FILE_BATCH_SIZE)
    this._collections = {}
    this._filesDir = path.join(drivePath, `./Files`)
    this._localDB = null // Local Key value datastore only
    this._lastSeq = null
    this._checkInternetInt = null
    this._checkInternetInProgress = false
    this._fileStatPath = this.drivePath + '/Files/file_stat.txt'
    this._stat = {
      file_bytes: 0,
      core_bytes: 0,
      total_bytes: 0
    }

    if (!fs.existsSync(drivePath)) {
      fs.mkdirSync(drivePath)
    }

    if (!fs.existsSync(this._filesDir)) {
      fs.mkdirSync(this._filesDir)
    }

    this.requestQueue.on('process-queue', async files => {
      this.requestQueue.reset()

      await this.fetchFileBatch(files, (stream, file) => {
        return new Promise((resolve, reject) => {
          fs.mkdirSync(getDirName(this._filesDir + file.path), { recursive: true })

          const writeStream = fs.createWriteStream(this._filesDir + file.path)

          pump(stream, writeStream, (err) => {
            if (err) reject(err)

            setTimeout(async () => {
              if(this.opened) {
                this.emit('file-sync', file)
                const filePath = file.encrypted ? `/${file.uuid}` : file.path
                await this._localHB.put(filePath, {})
              }
            })

            resolve()
          })
        })
      })
    })

    // Periodically check this drive's connection to the internet.
    // When the internet is down, emit a network status updated event.
    if(this.checkNetworkStatus) {
      this._checkInternetInt = setInterval(async () => {
        if(!this._checkInternetInProgress) {
          this._checkInternetInProgress = true
          await this._checkInternet();
          this._checkInternetInProgress = false
        }
      }, 1500)
    }
  }

  async ready() {
    const uncaughtCount = process.listenerCount('uncaughtException')

    if(uncaughtCount === 0) {
      process.on('uncaughtException', (err) => {
        //throw err
      })
    }

    await this._bootstrap()

    const stat = this._localDB.get('stat')

    if(!stat) {
      await this.database._updateStatBytes(0)
      this._localDB.put('stat', { ...this._stat })
    } else {
      this._stat = stat
    }

    this.publicKey = this.database.localMetaCore.key.toString('hex')

    if(!this.blind) {
      this.peerWriterKey = this.database.localInput.key.toString('hex')
    }

    if (this.peerPubKey) {
      this.discoveryKey = createTopicHash(this.peerPubKey).toString('hex')
    } else {
      this.discoveryKey = createTopicHash(this.publicKey).toString('hex')
    }

    // Data here can only be read by peer drives
    // that are sharing the same drive secret
    if(!this.blind) {
      this._collections.files = await this.database.collection('file')

      // This drastically speeds up queries and is necessary for sorting by fields
      await this._collections.files.createIndex(['path'])
    }

    if (this.keyPair && this.joinSwarm) {
      await this.connect()
    }

    
    const stream = this.metadb.createReadStream({ live: true })
    
    stream.on('data', async data => {
      if(data.value.toString().indexOf('hyperbee') === -1) {
        const op = HyperbeeMessages.Node.decode(data.value)
        const node = {
          key: op.key.toString('utf8'),
          value: JSON.parse(op.value.toString('utf8')),
          seq: data.seq
        }

        if (node.key !== '__peers') {
          await this._update(node)
        }
      }
    })

    if(!this.blind) {
      const cStream = this.database.autobee.createReadStream({ live: true, tail: true }) // Collections stream

      cStream.on('data', async data => {
        if(data.value.toString().indexOf('hyperbee') === -1) {
          const op = HyperbeeMessages.Node.decode(data.value)
          const key = op.key.toString('utf8')
          let collection
          let value

          if(key.split('-').length === 1) return

          collection = key.split('-')[0]

          if(!op.value) {
            const val = await this._getPrevVal(data.clock)

            if(val) {
              if(val._id) {
                const node = {
                  collection,
                  type: 'del',
                  value: {
                    ...val,
                    _id: val._id.toString('hex')
                  }
                }

                this.emit('collection-update', node )
              }
            } 
          }

          try {
            value = BSON.deserialize(op.value)
          } catch(err) {
            // womp womp
          }

          if(value && value._id && value.author !== this.keyPair.publicKey.toString('hex')) {
            let type

            if(value.deleted) {
              type = 'del'
            } else {
              type = data.clock.size > 1 ? 'update' : 'create'
            }

            const node = {
              collection,
              type,
              value: {
                ...value,
                _id: value._id.toString('hex')
              }
            }

            if(this.storageMaxBytes) await this.database._updateStatBytes()

            this.emit('collection-update', node)
          }
        }
      })
    }

    this.opened = true
  }

  // Connect to the Hyperswarm network
  async connect() {
    if (this._swarm) {
      await this._swarm.close()
    }

    this._swarm = new Swarm({
      keyPair: this.keyPair,
      blind: this.blind,
      workerKeyPairs: this._workerKeyPairs.keyPairs,
      topic: this.discoveryKey,
      publicKey: this.peerPubKey || this.publicKey,
      isServer: this.swarmOpts.server,
      isClient: this.swarmOpts.client,
      acl: this.swarmOpts.acl,
    })

    this._swarm.on('peer-connected', socket => {
      socket.write(JSON.stringify({
        type: 'sync',
        meta: {
          drivePubKey: this.peerPubKey || this.publicKey,
          peerPubKey: this.keyPair.publicKey.toString('hex'),
          blind: this.blind,
          writer: this.peerWriterKey,
          meta: this.publicKey
        }
      }))
    })

    if(this.checkNetworkStatus) {
      this._swarm.on('disconnected', () => {
        if(this.network.drive) {
          this.network.drive = false
          this.emit('network-updated', { drive: this.network.drive })
        }
      })

      this._swarm.on('connected', () => {
        if(!this.network.drive) {
          this.network.drive = true
          this.emit('network-updated', { drive: this.network.drive })
        }
      })
    }


    this._swarm.on('message', async (peerPubKey, data) => {
      try {
        const msg = JSON.parse(data.toString())
        if(msg && msg.type === 'sync') {
          const drivePubKey = this.peerPubKey || this.publicKey

          if(msg.meta.drivePubKey === drivePubKey) {
            await this.addPeer(msg.meta)
          } else {
            // ACCESS DENIED
          }
        }

        this.emit('message', peerPubKey, data)
      } catch(err) {
        console.log(err)
      }
    })

    this._swarm.on('file-requested', socket => {
      socket.once('data', async data => {
        const fileHash = data.toString('utf-8')
        const file = await this.metadb.get(fileHash)

        if (!file || file.value.deleted) {
          let err = new Error()
          err.message = 'Requested file was not found on drive'
          socket.destroy(err)
        } else {
          const readStream = fs.createReadStream(path.join(this.drivePath, `./Files${file.value.path}`))

          pump(readStream, socket, (err) => {
            // handle done
          })
        }
      })
      
      socket.on('error', (err) => {
        // handle errors
      })
    })

    this._swarm.ready()
  }

  async addPeer(peer) {
    await this.database.metadb.put(`__peer:${peer.publicKey}`, {
      blacklisted: false,
      peerPubKey: peer.publicKey,
      blind: peer.blind,
      cores: {
        writer: peer.writer,
        meta: peer.meta
      }
    })

    await this.database.addRemotePeer(peer)
  }

  // Remove Peer
  async removePeer(peer) {
    await this.database.metadb.put(`__peer:${peer.publicKey}`, {
      blacklisted: true,
      peerPubKey: peer.publicKey,
      blind: peer.blind,
      cores: {
        writer: peer.writer,
        meta: peer.meta
      }
    })
  }

  async writeFile(path, readStream, opts = {}) {
    let filePath = path
    let dest
    const uuid = uuidv4()

    if (filePath[0] === '/') {
      filePath = filePath.slice(1, filePath.length)
    }

    if (opts.encrypted) {
      dest = `${this._filesDir}/${uuid}`
    } else {
      fs.mkdirSync(getDirName(this._filesDir + path), { recursive: true })
      dest = this._filesDir + path
    }

    return new Promise(async (resolve, reject) => {
      const pathSeg = filePath.split('/')
      let fullFile = pathSeg[pathSeg.length - 1]
      let fileName
      let fileExt

      if (fullFile.indexOf('.') > -1) {
        fileName = fullFile.split('.')[0]
        fileExt = fullFile.split('.')[1]
      }

      const writeStream = fs.createWriteStream(dest)

      if (opts.encrypted && !opts.skipEncryption) {
        const fixedChunker = new FixedChunker(readStream, MAX_PLAINTEXT_BLOCK_SIZE)
        const { key, header, file } = await Crypto.encryptStream(fixedChunker, writeStream)

        await this.database._updateStatBytes(file.size)

        await this.metadb.put(file.hash, {
          uuid,
          size: file.size,
          hash: file.hash,
          path: `/${uuid}`,
          peer_key: this.keyPair.publicKey.toString('hex'),
          discovery_key: this.discoveryKey,
          custom_data: opts.customData
        })

        const fileMeta = {
          uuid,
          name: fileName,
          size: file.size,
          mimetype: fileExt,
          encrypted: true,
          key: key.toString('hex'),
          header: header.toString('hex'),
          hash: file.hash,
          path: filePath,
          peer_key: this.keyPair.publicKey.toString('hex'),
          discovery_key: this.discoveryKey,
          custom_data: opts.customData
        }

        await this._collections.files.insert({ ...fileMeta, updatedAt: new Date().toISOString() })

        this.emit('file-add', fileMeta)

        resolve({
          key: key.toString('hex'),
          header: header.toString('hex'),
          ...fileMeta
        })
      } else {
        let bytes = 0
        const hash = blake.blake2bInit(HASH_OUTPUT_LENGTH, null)
        const calcHash = new stream.Transform({
          transform
        })

        function transform(chunk, encoding, callback) {
          bytes += chunk.byteLength

          blake.blake2bUpdate(hash, chunk)
          callback(null, chunk)
        }

        pump(readStream, calcHash, writeStream, async () => {
          setTimeout(async () => {
            const _hash = Buffer.from(blake.blake2bFinal(hash)).toString('hex')

            if (bytes > 0) {
              await this.database._updateStatBytes(bytes)

              await this.metadb.put(_hash, {
                uuid,
                size: bytes,
                hash: _hash,
                path,
                peer_key: this.keyPair.publicKey.toString('hex'),
                discovery_key: this.discoveryKey,
                custom_data: opts.customData
              })

              const fileMeta = {
                uuid,
                name: fileName,
                size: bytes,
                mimetype: fileExt,
                hash: _hash,
                path: filePath,
                peer_key: this.keyPair.publicKey.toString('hex'),
                discovery_key: this.discoveryKey,
                custom_data: opts.customData
              }

              await this._collections.files.insert({ ...fileMeta, updatedAt: new Date().toISOString() })

              this.emit('file-add', fileMeta)
              resolve(fileMeta)
            } else {
              reject('No bytes were written.')
            }
          })
        })
      }
    })
  }

  async readFile(path) {
    let file
    let filePath = path

    if (filePath[0] === '/') {
      filePath = filePath.slice(1, filePath.length)
    }

    try {
      file = await this._collections.files.findOne({ path: filePath })

      const stream = fs.createReadStream(`${this._filesDir}/${file.uuid}`)

      // If key then decipher file
      if (file.encrypted && file.key && file.header) {
        const fixedChunker = new FixedChunker(stream, MAX_ENCRYPTED_BLOCK_SIZE)
        return Crypto.decryptStream(fixedChunker, file.key, file.header)
      } else {
        return stream
      }
    } catch (err) {
      throw err
    }
  }

  decryptFileStream(stream, key, header) {
    const fixedChunker = new FixedChunker(stream, MAX_ENCRYPTED_BLOCK_SIZE)
    return Crypto.decryptStream(fixedChunker, key, header)
  }

  // TODO: Implement this
  fetchFileByHash(fileHash) {
  }

  async fetchFileByDriveHash(discoveryKey, fileHash, opts = {}) {
    const keyPair = opts.keyPair || this.keyPair
    const memStream = new MemoryStream()
    const topic = blake.blake2bHex(discoveryKey, null, HASH_OUTPUT_LENGTH)


    if (!fileHash || typeof fileHash !== 'string') {
      return reject('File hash is required before making a request.')
    }

    if (!discoveryKey || typeof discoveryKey !== 'string') {
      return reject('Discovery key cannot be null and must be a string.')
    }

    try {
      await this._initFileSwarm(memStream, topic, fileHash, 0, { keyPair })
    } catch(e) {      
      setTimeout(() => {
        memStream.destroy(e)
      })
      return memStream
    }

    if (opts.key && opts.header) {
      return this.decryptFileStream(memStream, opts.key, opts.header)
    }

    return memStream
  }

  async fetchFileBatch(files, cb) {
    const batches = new RequestChunker(files, FILE_BATCH_SIZE)

    for (let batch of batches) {
      const requests = []

      for (let file of batch) {
        if(typeof file.size === 'number') await this.database._updateStatBytes(file.size)
        const stat = this._localDB.get('stat')
        if(stat.total_bytes <= this.storageMaxBytes || !this.storageMaxBytes) {
          requests.push(new Promise(async (resolve, reject) => {
            if (file.discovery_key) {
              try {
                const keyPair = this._workerKeyPairs.getKeyPair()
                const stream = await this.fetchFileByDriveHash(file.discovery_key, file.hash, { key: file.key, header: file.header, keyPair })

                await cb(stream, file)
                
                return resolve()
              } catch(err) {
                return reject(err)
              }
            } else {
              // TODO: Fetch files by hash
              return reject()
            }
          }))
        }
      }

      try {
        await Promise.all(requests)
      } catch(err) {
        // Could not download some files. Will try again.
      }

      this.requestQueue.queue = []
    }
  }

  async _initFileSwarm(stream, topic, fileHash, attempts, { keyPair }) {
    return new Promise((resolve, reject) => {
      if(!this.opened) throw ('Drive is closed.')

      if (attempts > this.fileRetryAttempts) {
        const err = new Error('Unable to make a connection or receive data within the allotted time.')
        err.fileHash = fileHash
        this._workerKeyPairs.release(keyPair.publicKey.toString('hex'))
        stream.destroy(err)
        return reject(err)
      }

      const swarm = new Hyperswarm({ keyPair })

      let connected = false
      let receivedData = false
      let streamError = false

      swarm.join(Buffer.from(topic, 'hex'), { server: false, client: true })

      swarm.on('connection', async (socket, info) => {
        receivedData = false

        if (!connected) {
          connected = true

          // Tell the host drive which file we want
          socket.write(fileHash)

          socket.on('data', (data) => {
            resolve()
            stream.write(data)
            receivedData = true
          })

          socket.once('end', () => {
            if (receivedData) {
              this._workerKeyPairs.release(keyPair.publicKey.toString('hex'))
              stream.end()
              swarm.destroy()
            }
          })

          socket.once('error', (err) => {
            stream.destroy(err)
            streamError = true
            reject(err)
          })
        }
      })

      setTimeout(async () => {
        if (!connected || streamError || !receivedData) {
          attempts += 1

          await swarm.leave(topic)
          await swarm.destroy()

          try {
            await this._initFileSwarm(stream, topic, fileHash, attempts, { keyPair })
            resolve()
          } catch(e) {
            reject(e)
          }
        }
      }, this.fileTimeout)
    })
  }

  async _checkInternet() {
    return new Promise((resolve, reject) => {
      isOnline().then((isOnline) => {
        if(!isOnline && this.network.internet) {
          this.network.internet = false
          this.emit('network-updated', { internet: this.network.internet })
        }

        if(isOnline && !this.network.internet) {
          this.network.internet = true
          this.emit('network-updated', { internet: this.network.internet })
        }

        resolve()
      })
    })
  }

  async unlink(filePath) {
    let fp = filePath

    if (fp[0] === '/') {
      fp = filePath.slice(1, fp.length)
    }

    try {
      const file = await this._collections.files.findOne({ path: fp })

      if (!file) return

      fs.unlinkSync(path.join(this._filesDir, file.encrypted ? `/${file.uuid}` : file.path))

      await this._collections.files.update({ _id: file._id} , { uuid: file.uuid, deleted: true, updatedAt: new Date().toISOString() })

      await this.database._updateStatBytes(-Math.abs(file.size))

      await this.metadb.put(file.hash, {
        path: file.encrypted ? `/${file.uuid}` : file.path,
        discovery_key: file.discovery_key,
        size: file.size,
        deleted: true
      })

      this.emit('file-unlink', file.value)
    } catch (err) {
      throw err
    }
  }

  async destroyHyperfile(path) {
    const filePath = await this.bee.get(path)
    const file = await this.bee.get(filePath.value.hash)
    await this._clearStorage(file.value)
  }

  async _bootstrap() {
    this._localCore = new Hypercore(path.join(this.drivePath, `./LocalCore`), { storageNamespace: `${this.storageName}:local-core` })
    await this._localCore.ready()
    this._localHB = new Hyperbee(this._localCore, { keyEncoding: 'utf8', valueEncoding: 'json' })
    
    this._localDB = new FileDB(`${this.drivePath}/LocalDS`)

    this.database = new Database(this.storage || this.drivePath, {
      localDB: this._localDB,
      keyPair: this.keyPair,
      storageName: this.storageName,
      encryptionKey: this.encryptionKey,
      peerPubKey: this.peerPubKey,
      acl: this.swarmOpts && this.swarmOpts.acl ? this.swarmOpts.acl : null,
      joinSwarm: this.joinSwarm,
      fts: this.fullTextSearch,
      blind: this.blind,
      stat: this._stat,
      storageMaxBytes: this.storageMaxBytes,
      fileStatPath: this._fileStatPath,
      broadcast: this.broadcast
    })

    this.database.on('disconnected', () => {
      if(this.network.drive) {
        this.network.drive = false
        this.emit('network-updated', { drive: this.network.drive })
      }
    })

    this.database.on('connected', () => {
      if(!this.network.drive) {
        this.network.drive = true
        this.emit('network-updated', { drive: this.network.drive })
      }
    })

    if(this.checkNetworkStatus) {
      this.database.on('disconnected', () => {
        if(this.network.drive) {
          this.network.drive = false
          this.emit('network-updated', { drive: this.network.drive })
        }
      })
    }


    // If this drive can't decipher the data inside the remote hypercore's then just listen for when those cores are updated.
    if(this.blind) {
      this.database.on('collection-update', () => {
        this.emit('collection-update')
      })
    }

    this.database.on('remote-cores-downloaded', () => {
      this.emit('remote-cores-downloaded')
    })

    this.database.on('peer-connected', (peer) => {
      if(!this.peers.has(peer.peerPubKey)) {
        this.emit('peer-connected', peer)
        this.peers.add(peer.peerPubKey)
      }
    })

    this.database.on('peer-disconnected', (peer) => {
      if(this.peers.has(peer.peerPubKey)) {
        this.emit('peer-disconnected', peer)
        this.peers.delete(peer.peerPubKey)
      }
    })

    await this.database.ready()

    this.db = this.database
    this.metadb = this.database.metadb
  }

  async _update(data) {

    this.emit('sync', data.value)

    const fileHash = await this._localHB.get(data.value.path)

    if (
      !data.value.deleted &&
      data.value.peer_key !== this.keyPair.publicKey.toString('hex') &&
      !fileHash
    ) {

      if (this.syncFiles && !this.includeFiles && data.value.hash || this.includeFiles && this.includeFiles.indexOf(data.value.path) > -1 && data.value.hash) {
        try {
          const stat = this._localDB.get('stat')
          if(stat.total_bytes <= this.storageMaxBytes || !this.storageMaxBytes) {
            this.requestQueue.addFile(data.value)
          }
        } catch (err) {
          throw err
        }
      } else {
        if(data.value.path) {
          const filePath = data.value.encrypted ? `/${data.value.uuid}` : data.value.path
          await this._localHB.put(filePath, {})
        }
      }
    }

    if (data.value.deleted &&
      data.value.peer_key !== this.keyPair.publicKey.toString('hex')) {
      try {
        let filePath = path.join(this._filesDir, `${data.value.path}`)
        
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath)

          const _file = await this._localHB.get(data.value.path)
          
          if(_file) {
            await this._localHB.del(data.value.path)
          }

          await this.database._updateStatBytes(-Math.abs(data.value.size))

          setTimeout(() => {
            this.emit('file-unlink', data.value)
          })
        }
      } catch (err) {
        console.log(err)
        throw err
      }
    }
  }

  async _getPrevVal(map) {
    for (let [key, value] of map) {
      try {
        const data = await this.database.autobee.autobase.view.get(value)
        const op = HyperbeeMessages.Node.decode(data)
        
        if(!op || !op.value || !op.key.toString('hex')) continue

        const val = BSON.deserialize(op.value)

        if(val && val.version !== "1.0" && !val.name && !val.fields && !val.opts) {
          return val
        }
      } catch(err) {
        // womp womp
      }
    }
  }

  // Deprecated
  info() {
    const bytes = getTotalSize(this.drivePath)
    return {
      size: bytes
    }
  }

  async stat() {
    return this._localDB.get('stat')
  }

  /**
   * Close drive and disconnect from all Hyperswarm topics
   */
  async close() {
    if(this.joinSwarm) {
      await this._swarm.close()
    }

    if(this._localCore) {
      await this._localCore.close()
    }

    await this.database.close()

    clearInterval(this._checkInternetInt)

    this.network = {
      internet: false,
      drive: false
    }

    this.emit('network-updated', this.network)

    if(this.checkNetworkStatus) {
      clearInterval(this._checkInternetInt)

      this.network = {
        internet: false,
        drive: false
      }

      this.emit('network-updated', this.network)
    }

    this.removeAllListeners()
    this.requestQueue.removeAllListeners()
    this._swarm.removeAllListeners()

    this.opened = false
  }
}

function createTopicHash(topic) {
  const crypto = require('crypto')

  return crypto.createHash('sha256')
    .update(topic)
    .digest()
}

async function auditFile(stream, remoteHash) {
  return new Promise((resolve, reject) => {
    let hash = blake.blake2bInit(HASH_OUTPUT_LENGTH, null)

    stream.on('error', err => reject(err))
    stream.on('data', chunk => {
      blake.blake2bUpdate(hash, chunk)
    })
    stream.on('end', () => {
      const localHash = Buffer.from(blake.blake2bFinal(hash)).toString('hex')

      if (localHash === remoteHash)
        return resolve()

      reject('Hashes do not match')
    })
  })
}


const getAllFiles = function (dirPath, arrayOfFiles) {
  files = fs.readdirSync(dirPath)

  arrayOfFiles = arrayOfFiles || []

  files.forEach(function (file) {
    if (fs.statSync(dirPath + "/" + file).isDirectory()) {
      arrayOfFiles = getAllFiles(dirPath + "/" + file, arrayOfFiles)
    } else {
      arrayOfFiles.push(path.join(dirPath, file))
    }
  })

  return arrayOfFiles
}

const getTotalSize = function (directoryPath) {
  const arrayOfFiles = getAllFiles(directoryPath)

  let totalSize = 0

  arrayOfFiles.forEach(function (filePath) {
    totalSize += fs.statSync(filePath).size
  })

  return totalSize
}

module.exports = Drive
