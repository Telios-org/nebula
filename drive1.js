(async() => {
  const Drive = require('./index')

  const encKey = Buffer.alloc(32, 'hello world')

  // 1 - Y
  // 2 - Y
  // 4 - Y
  // 5 - Y

  const keyPair = {
    publicKey: Buffer.from('20a932724b820709452be98ac4e1f0b551c96892e4a203c51fd57535e6b79422', 'hex'),
    secretKey: Buffer.from('1d6a752c92d9c0cc4bacb830adcae26650a02c88af53b41565dc0182881552cc20a932724b820709452be98ac4e1f0b551c96892e4a203c51fd57535e6b79422', 'hex')
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


  // const collection = await peer1.db.collection('example')

  // console.log(await collection.find())

  setInterval(async () => {
    const collection = await peer1.db.collection('example')
    await collection.insert({ hello: 'world' })
    console.log('inserted')
  }, 5000)
})()