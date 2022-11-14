(async() => {
  const Database = require('./lib/database_test')
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

    let collection = await database.collection('example')
    // await collection.createIndex(['foo'])

   await collection.insert({ foo: 'bar' })
    console.log('ready')
    await database.close()
    console.log('close')

    await database.ready()
    console.log('ready')
    collection = await database.collection('example')

    await collection.insert({ foo: 'bar' })
    // await collection.createIndex(['foo'])

    console.log('ready')

    // await database.close()
    // console.log('closed')


    // await database.ready()

    // const collection3 = await database.collection('example')
    // await collection3.createIndex(['foo'])

    // console.log('ready')
})()