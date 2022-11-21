(async() => {
  const Drive = require('./index')

  const encKey = Buffer.alloc(32, 'hello world')

  // 1 - Y
  // 2 - Y
  // 3 - Y
  // 4 - Y
  // 5 - Y

  const keyPair = {
    publicKey: Buffer.from('aac649cf612a815df45cfc311c3baab6c8a6729a6eac50c6a2ecd45640766e10', 'hex'),
    secretKey: Buffer.from('b71ed8956af0e6d5db89e633663a27b81375701acb72e63a4594a6a6f6bd09a3aac649cf612a815df45cfc311c3baab6c8a6729a6eac50c6a2ecd45640766e10', 'hex')
  }

  let peer2 = new Drive(__dirname + '/d2', '6b52356d5839554eaa8d8729fcc52d1e4cd32056336e9cb7aa6d89297d3a3928', {
    keyPair,
    checkNetworkStatus: true,
    fullTextSearch: true,
    blind: false,
    encryptionKey: encKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  peer2.on('collection-update', data => {
    console.log('PEER2 DATA', data)
  })


  await peer2.ready()

  // const collection = await peer2.db.collection('example')
  // console.log(await collection.find())
  // await collection.insert({ foo: 'bar' })

  console.log({ keyPair: {
    publicKey: peer2.keyPair.publicKey.toString('hex'),
    secretKey: peer2.keyPair.secretKey.toString('hex')
  }})
  
})()