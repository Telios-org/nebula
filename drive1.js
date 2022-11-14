(async() => {
  const Drive = require('./index')

  const encKey = Buffer.alloc(32, 'hello world')

  const keyPair = {
    publicKey: '45ccb55e617351613c35da062aa926b14b839ec4db624244e264ff8fd5e353f7',
    secretKey: 'cbb8cce4c36387a869dd7b89bfb15694cc3ac612ce1f19dc738a7c00f81b995f45ccb55e617351613c35da062aa926b14b839ec4db624244e264ff8fd5e353f7'
  }

  const peer1 = new Drive(__dirname + '/d1', null, {
    keyPair,
    checkNetworkStatus: true,
    fullTextSearch: true,
    encryptionKey: encKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  peer1.on('collection-update', data => {
    console.log('PEER1 DATA', data)
  })

  await peer1.ready()

  console.log(peer1.publicKey)
  console.log({ keyPair: {
    publicKey: peer1.keyPair.publicKey.toString('hex'),
    secretKey: peer1.keyPair.secretKey.toString('hex')
  }})


  const collection = await peer1.db.collection('example')

  console.log(await collection.find())

  // setTimeout(async () => {
  //   const collection = await peer1.db.collection('example')
  //   await collection.insert({ hello: 'world' })
  // }, 2000)
})()