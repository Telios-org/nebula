(async() => {
  const Drive = require('./index')

  const encKey = Buffer.alloc(32, 'hello world')

  const keyPair = {
    publicKey: 'ede4b97a6f2f898b2ce1d991e5b60dda2afae28ccb989c1102ab39a7446076c4',
    secretKey: '4c8c2b7e686d837003e6ab6cae401d33e3aa15cd9a803c604773f314de3cdde3ede4b97a6f2f898b2ce1d991e5b60dda2afae28ccb989c1102ab39a7446076c4'
  }

  let peer2 = new Drive(__dirname + '/d2', 'e825322fe96340a9acad7b7a59aa00fbca23b6dca3293ad88465cc99aa283b47', {
    keyPair,
    checkNetworkStatus: true,
    fullTextSearch: true,
    blind: true,
    encryptionKey: encKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await peer2.ready()

  // const collection = await peer2.db.collection('example')

  // await collection.insert({ foo: 'bar' })

  console.log({ keyPair: {
    publicKey: peer2.keyPair.publicKey.toString('hex'),
    secretKey: peer2.keyPair.secretKey.toString('hex')
  }})

  peer2.on('collection-update', data => {
    console.log('PEER2 DATA', data)
  })

  
})()