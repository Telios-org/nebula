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

      this.base = new Autobase({
        inputs: this.inputs,
        localInput: this.firstUser,
        localOutput: this.firstOutput
      })

      this.bee = new Autodeebee(this.base)
    }

    async ready() {
      // this.db = new DB(this.bee)


        
        await this.firstUser.ready()
        await this.firstOutput.ready()
      

      // await _joinSwarm(this.firstUser, { server: true, client: true })
      // await _joinSwarm(this.firstOutput, { server: true, client: true })

      await this.bee.ready()
    }

    async close() {
      await this.firstUser.close()
      await this.firstOutput.close()
      await this.bee.close()
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
    let db = bee.bee

    // const collection = db.collection('documents')
    // await collection.createIndex(['example'])
    // const doc = await collection.insert({ example: 'Hello World!' })

    // await collection.createIndex(['example'])

    // console.log('doc', doc)
    // const otherDoc = await collection.findOne({ _id: doc._id })
    // console.log('otherDoc', otherDoc)

    await db.put('foo', 'bar')

    const val = await db.get(Buffer.from('foo'))
    console.log(val)

    await bee.close()
    console.log('close')

    await bee.ready()
    console.log('ready')

    await db.put('foo', 'test')

    // const collection2 = db.collection('documents')
    // await collection2.createIndex(['example'])
    // const doc2 = await collection2.insert({ example: 'Hello World!' })
    // console.log(doc2)


    await bee.close()
    console.log('close')
    await bee.ready()
    console.log('ready')
    await bee.close()
    console.log('close')
    await bee.ready()
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