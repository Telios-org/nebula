(async() => {
  const Database = require('./lib/database')
  const Hypercore = require('hypercore')
  const DHT = require('@hyperswarm/dht')

  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  
    let database = new Database('./db', {
      keyPair,
      encryptionKey,
      broadcast: true,
      blind: false
    })

    // database.on('collection-update', data => {
    //   console.log(data)
    // })

    await database.ready()

    // let collection = await database.collection('example')
    // await collection.createIndex(['foo'])

    // await collection.insert({ foo: 'bar' })
    console.log('ready 1')

    let t4 = new Hypercore('./t4', null, { encryptionKey })

    await database.addInput(t4, 'autobee')
    
    await database.close()
    console.log('close 1')

    let database2 = new Database('./db', {
      keyPair,
      encryptionKey,
      broadcast: true,
      blind: false
    })

    await database2.ready()
    
    t4 = new Hypercore('./t4', null, { encryptionKey })
    await database2.addInput(t4, 'autobee')

    console.log('ready 2')
    
    const collection2 = await database2.collection('example')

    await collection2.createIndex(['foo'])
    const doc = await collection2.insert({ foo: 'bar' })

    console.log(doc)

    await database2.close()
    console.log('closed 2')

    let database3 = new Database('./db', {
      keyPair,
      encryptionKey,
      broadcast: true,
      blind: false
    })
    await database3.ready()

    t4 = new Hypercore('./t4', null, { encryptionKey })
    await database3.addInput(t4, 'autobee')

    const collection3 = await database3.collection('example')
    await collection3.createIndex(['foo'])

    const doc3 = await collection3.insert({ foo: 'bar' })

    console.log(doc3)

    console.log('ready 3')
})()