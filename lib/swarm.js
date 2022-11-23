const EventEmitter = require('events')
const Hyperswarm = require('hyperswarm')
const blake = require('blakejs')

class Swarm extends EventEmitter {
  constructor({ acl, keyPair, workerKeyPairs, publicKey, topic, ephemeral, isServer, isClient, blind }) {
    super()

    this.fileSwarm = new Hyperswarm({  // Networking for streaming file data
      firewall(remotePublicKey) {
        if (workerKeyPairs[remotePublicKey.toString('hex')]) {
          return true;
        }
        return false;
      }
    })
    this.keyPair = keyPair
    this.blind = blind
    this.publicKey = publicKey
    this.topic = blake.blake2bHex(publicKey, null, 32)
    this.fileTopic = blake.blake2bHex(topic, null, 32)
    this.ephemeral = ephemeral
    this.closing = false
    this.fileDiscovery = null
    this.serverDiscovery = null
    this.isServer = isServer
    this.isClient = isClient

    
    this.server = new Hyperswarm({
      keyPair,
      firewall(remotePublicKey) {
        // validate if you want a connection from remotePublicKey
        if (acl && acl.length) {
          return acl.indexOf(remotePublicKey.toString('hex')) === -1;
        }

        return false;
      }
    })
  }

  async ready() {
    this.fileSwarm.on('connection', async (socket, info) => {
      this.emit('file-requested', socket)

      socket.on('error', (err) => {})
    })


    this.server.on('connection', (socket, info) => {
      const peerPubKey = socket.remotePublicKey.toString('hex')
      this.emit('peer-connected', socket)

      socket.on('error', (err) => {})

      socket.on('data', data => {
        this.emit('message', peerPubKey, data)
      })
    })

    try {
      const serverDiscoveryTopic = blake.blake2bHex(this.topic, null, 32)
      this.serverDiscovery = this.server.join(Buffer.from(serverDiscoveryTopic, 'hex'), { server: true, client: true })
      await this.serverDiscovery.flushed()
      await this.server.listen()
    } catch(e) {
      this.emit('disconnected')
    }
    
    try {
      this.fileDiscovery = this.fileSwarm.join(Buffer.from(this.fileTopic, 'hex'), { server: true, client: false })
      this.fileDiscovery.flushed().then(() => {
        this.emit('connected')
      })
    } catch(e) {
      this.emit('disconnected')
    }
  }

  connect(publicKey) {
    const noiseSocket = this.node.connect(Buffer.from(publicKey, 'hex'))

    noiseSocket.on('error', (err) => {})

    noiseSocket.on('open', function () {
      // noiseSocket fully open with the other peer
      this.emit('onPeerConnected', noiseSocket)
    });
  }

  async close() {
    const promises = []

    if(this.fileDiscovery)
      promises.push(this.fileDiscovery.destroy())

    if(this.server) {
      promises.push(this.serverDiscovery.destroy())
      promises.push(this.server.leave(this.keyPair.publicKey))
    }

    try {
      await Promise.all(promises)

      this.removeAllListeners()
      this.fileSwarm.removeAllListeners()
    } catch (err) {
      throw err
    }
  }
}

module.exports = Swarm
