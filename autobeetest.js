(async() => {
  const RAM = require('random-access-memory')
  const Hypercore = require('hypercore')
  const Hyperswarm = require('hyperswarm')
  const Autobase = require('autobase')
  const HyperbeeDeeBee = require('hyperbeedeebee')
  const Autodeebee = require('./lib/autodeebee')
  const { DB } = HyperbeeDeeBee
  const BSON = require('bson')


  const encKey = Buffer.alloc(32, 'hello world')
    

  class Bee {
    constructor() {
      this.firstUser = new Hypercore('./t1', null, { encryptionKey: encKey })
      this.firstOutput = new Hypercore('./t2', null, { encryptionKey: encKey })

      this.inputs = [this.firstUser]

      const base = new Autobase({
        inputs: this.inputs,
        localInput: this.firstUser,
        localOutput: this.firstOutput
      })

      this.bee = new Autodeebee(base)
    }

    async ready() {
      await this.firstUser.ready()
      await this.firstOutput.ready()

      console.log('GOT HERE 1')
      await this.bee.ready()
      console.log('GOT HERE 2')

      this.db = new DB(this.bee)
    }

    async addInput(core) {
      await this.bee.addInput(core)
    }

    async close() {
      await this.bee.close()

      // for (const session of this.firstUser.sessions) {
      //   if(session) {
      //     await session.close()
      //   }
      // }

      for (const session of this.firstOutput.sessions) {
        if(session) {
          await session.close()
        }
      }

      try {
        if(this.firstOutput.sessions.length)
          await this.firstOutput.sessions[0].close()
      console.log(this.firstOutput.sessions)
      } catch(er){
        console.log(er)
      }

      // this.db = null
      // this.bee = null
    }
  }

  // // WHY CANT I OPEN AND CLOSE AUTOBEE?
  // async function getBee () {
    
  //   const firstUser = new Hypercore('./t1', null, { encryptionKey: encKey })
  //   const firstOutput = new Hypercore('./t2', null, { encryptionKey: encKey })
    
  //   const inputs = [firstUser]
    
  //   await _joinSwarm(firstUser, { server: true, client: true })
  //   await _joinSwarm(firstOutput, { server: true, client: true })
    

  //   const base1 = new Autobase({
  //     inputs,
  //     localInput: firstUser,
  //     localOutput: firstOutput
  //   })

  //   const bee = new Autodeebee(base1)

  //   await bee.ready()

  //   return bee
  // }


  

  try {
    let bee = new Bee()
    await bee.ready()
    let db = bee.db

    let collection = db.collection('documents')
    await collection.createIndex(['example'])
    const doc = await collection.insert({ example: 'Hello World!' })

    await collection.createIndex(['example'])

    console.log('doc', doc)
    const otherDoc = await collection.findOne({ _id: doc._id })
    console.log('otherDoc', otherDoc)

    const t3 = new Hypercore('./t3', null, { encryptionKey: encKey })

    await bee.addInput(t3)

    await bee.close()
    console.log('close')

    let bee2 = new Bee()
    await bee2.ready()
    db2 = bee2.db
    console.log('ready')

    const collection2 = db2.collection('documents')
    await collection2.createIndex(['example'])
    let doc2 = await collection2.insert({ example: 'Hello Worlderadfasdfasfasfd!' })
    console.log(doc2)

    await bee2.close()
    console.log('close')

    const bee3 = new Bee()
    await bee3.ready()
    console.log('ready')
    
  } catch(err) {
    console.log(err)
  }



  function getNode(data) {
    const collection = data.key.toString().split('\x00doc')[0]
      try {
        const node = BSON.deserialize(data.value)
        console.log('NODE', { collection, ...node })
      } catch(err) {

      }
  }

  function getDiff(version) {
    const diffStream = bee.createDiffStream(version)

    diffStream.on('data', data => {
      // New Record
      if(data && data.left && data.left.key.toString().indexOf('\x00doc') > -1 && !data.right) {
        getNode(data.left)
      }

      // Recored Updated
      if(data && data.left && data.left.key.toString().indexOf('\x00doc') > -1 && data.right && data.right.key.toString().indexOf('\x00doc') > -1) {
        getNode(data.left)
      }

      // Deleted Record
      if(data && !data.left && data.right && data.right.key.toString().indexOf('\x00doc') > -1) {
        getNode(data.right)
      }
    })
  }

  async function _joinSwarm(core, { server, client, peer }) {
    
      const swarm = new Hyperswarm()
    
      try {
        await core.ready()
      } catch(err) {
        console.log(err)
      }

      //if(this.joinSwarm) {
        let connected = false
        try {
          swarm.on('connection', async (socket, info) => {
            connected = true

            

            socket.pipe(core.replicate(info.client)).pipe(socket)
          })
      
          const topic = core.discoveryKey;
          const discovery = swarm.join(topic, { server, client })

          // this.connections.push(swarm)
      
          if (server) {
           
            discovery.flushed().then(() => {
              //this.emit('connected')
            })
          }

          swarm.flush().then(() => {
            //this.emit('connected')
          })

          //Refresh if no connection has been made within 10s
          setTimeout(() => {
            if(!connected) {
              discovery.refresh({ client, server })
            }
          }, 10000)
        } catch(e) {
          //this.emit('disconnected')
        }
      //}
    
  }
})()