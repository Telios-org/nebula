(async() => {
  const Drive = require('./index')

  const encKey = Buffer.alloc(32, 'hello world')

  const keyPair = {
    publicKey: '814396628d44182e6c8f738351c549330ea6b4bbe3c2804ec1e57690899d68d4',
    secretKey: 'ecc63947ab27e82311dea8fb764e2bbd9bd8f22a8f158ceed85ff96dbfcdc5b6814396628d44182e6c8f738351c549330ea6b4bbe3c2804ec1e57690899d68d4'
  }

  let peer3 = new Drive(__dirname + '/d3', '72e4c159ab329d7fba68f20bf99600909e687e3f4c4312d8a530215d792f6071', {
    //keyPair,
    checkNetworkStatus: true,
    fullTextSearch: true,
    encryptionKey: encKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await peer3.ready()

  const collection = await peer3.db.collection('example')

  await collection.insert({ foo: 'bar' })

  console.log({ keyPair: {
    publicKey: peer3.keyPair.publicKey.toString('hex'),
    secretKey: peer3.keyPair.secretKey.toString('hex')
  }})

  peer3.on('collection-update', data => {
    console.log('PEER3 DATA', data)
  })

  
})()