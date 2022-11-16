const tape = require('tape')
const _test = require('tape-promise').default
const test = _test(tape)
const fs = require('fs')
const path = require('path')
const del = require('del')
const Drive = require('..')
const DHT = require('@hyperswarm/dht')
const ram = require('random-access-memory')
const BSON = require('bson')

const keyPair = DHT.keyPair()
const keyPair2 = DHT.keyPair()
const keyPair3 = DHT.keyPair()
const keyPair4 = DHT.keyPair()

let drive
let drive2
let drive3
let drive4
let drive5
let drive6

let hyperFiles = []

const encryptionKey = Buffer.alloc(32, 'hello world')

test('Drive - Create', async t => {
  t.plan(9)

  let networkCount = 0

  await cleanup()

  await peerCleanup()

  drive = new Drive(__dirname + '/drive', null, {
    keyPair,
    encryptionKey,
    checkNetworkStatus: true,
    fullTextSearch: false,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  drive.on('network-updated', async data => {
    networkCount += 1

    t.ok(data)

    if(networkCount == 2) {
      await drive.close()
      
    }
  })

  await drive.ready()

  t.ok(drive.publicKey, `Drive has public key ${drive.publicKey}`)
  t.ok(drive.keyPair, `Drive has peer keypair`)
  t.ok(drive.db, `Drive has Database`)
  t.ok(drive.drivePath, `Drive has path ${drive.drivePath}`)
  t.equals(true, drive.opened, `Drive is open`)
})

test('Drive - Upload Local Encrypted File', async t => {
  t.plan(28)

  try {

   drive = new Drive(__dirname + '/drive', null, {
      keyPair,
      encryptionKey,
      checkNetworkStatus: true,
      fullTextSearch: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await drive.ready()

    const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
    const file = await drive.writeFile('/email/rawEmailEncrypted.eml', readStream, { encrypted: true, customData: { foo: 'bar' } })

    hyperFiles.push(file)

    t.ok(file.key, `File was encrypted with key`)
    t.ok(file.header, `File was encrypted with header`)
    t.ok(file.hash, `Hash of file was returned ${file.hash}`)
    t.ok(file.size, `Size of file in bytes was returned ${file.size}`)
    t.equals(file.custom_data.foo, 'bar', `File has custom data`)

    for (let i = 0; i < 20; i++) {
      const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
      const file = await drive.writeFile(`/email/rawEmailEncrypted${i}.eml`, readStream, { encrypted: true, customData: { foo: 'bar' } })
      t.ok(file)
    }

    const stat = await drive.stat()

    t.ok(stat.file_bytes)
    t.ok(stat.core_bytes)
    t.ok(stat.total_bytes)
  } catch (e) {
    t.error(e)
  }
})

test('Drive - Read Local Encrypted File', async t => {
  t.plan(2)

  const origFile = fs.readFileSync(path.join(__dirname, '/data/email.eml'), { encoding: 'utf-8' })
  const stream = await drive.readFile('/email/rawEmailEncrypted.eml')
  let decrypted = ''

  stream.on('data', chunk => {
    decrypted += chunk.toString('utf-8')
  })

  stream.on('end', () => {
    t.ok(decrypted.length, 'Returned encrypted data')
    t.equals(origFile.length, decrypted.length, 'Decrypted file matches original')
  })
})

test('Drive - Create Seed Peer', async t => {
  t.plan(22)

  drive2 = new Drive(__dirname + '/drive2', drive.publicKey, {
    keyPair: keyPair2,
    encryptionKey: drive.encryptionKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive2.ready()

  drive2.on('file-sync', async (file) => {
    t.ok(file.uuid, `File has synced from remote peer`)
  })

  const readStream = fs.createReadStream(path.join(__dirname, '/data/test.doc'))
  const file = await drive.writeFile('/email/test.doc', readStream)

  hyperFiles.push(file)
})

test('Drive - Add/remove docs from synced drives', async t => {
  t.plan(3)

  try {
    const encKey = Buffer.alloc(32, 'hello world')

    const peer1 = new Drive(__dirname + '/peer1', null, {
      keyPair: DHT.keyPair(),
      checkNetworkStatus: true,
      fullTextSearch: true,
      broadcast: true,
      encryptionKey: encKey,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer1.ready()

    const peer2 = new Drive(__dirname + '/peer2', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      checkNetworkStatus: true,
      fullTextSearch: true,
      broadcast: true,
      encryptionKey: encKey,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer2.ready()

    const collection1 = await peer1.db.collection('example')
    await collection1.createIndex(['foo2'])

    const collection2 = await peer2.db.collection('example')
    await collection2.createIndex(['foo2'])

    peer2.on('collection-update', async data => {
      if(data.collection !== 'metadb' && data.type === 'del') {
        const docs = await collection2.find({ foo2: 'bar2' })
        t.equals(0, docs.length, 'Peer2 returns 0 results')
      }

      
      if(data.collection !== 'metadb' && data.type !== 'del') {
        await collection2.delete({ foo2: 'bar2' })
        t.ok(true, 'Peer2 deleted synced document from peer1')
      }
    })

    peer1.on('collection-update', async data => {
      if(data.collection !== 'metadb' && data.type === 'del') {
        const docs = await collection1.find({ foo2: 'bar2' })
        t.equals(0, docs.length, 'Peer1 returns 0 results')
      }
    })

    await collection1.insert({ foo0: 'bar0' })
    await collection1.insert({ foo1: 'bar1' })
    const doc = await collection1.insert({ foo2: 'bar2' })
    await collection1.ftsIndex(['foo2'], [doc])

    t.teardown(async () => {
      try {
        await closeCores([peer1, peer2])
        await peerCleanup()
      } catch(err) {
        console.log(err)
      }
    })
  } catch(err) {
    console.log(err)
  }
})

test('Drive - Sync Remote Database Updates from blind peer', async t => {
  t.plan(1)
  
  try {
    const encKey = Buffer.alloc(32, 'hello world')
    const p1KP = DHT.keyPair()
    let peer1 = new Drive(__dirname + '/peer1', null, {
      keyPair: p1KP,
      checkNetworkStatus: true,
      fullTextSearch: true,
      broadcast: true,
      encryptionKey: encKey,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer1.ready()

    // BLIND PEER DOES NOT HAVE ENCRYPTION KEY
    const peer2 = new Drive(__dirname + '/peer2', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      blind: true,
      syncFiles: false,
      broadcast: true,
      includeFiles: ['/test.doc'],
      swarmOpts: {
        server: true,
        client: true
      }
    })
    
    await peer2.ready()

    // Sync remote updates from offline peer1 via blind peer2
    const peer3 = new Drive(__dirname + '/peer3', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer1.close()
    await peer3.ready()

    const collection2 = await peer3.db.collection('example')
    await collection2.insert({ hello: 'world' })
    
    await peer3.close()

    peer1 = new Drive(__dirname + '/peer1', null, {
      keyPair: p1KP,
      checkNetworkStatus: true,
      fullTextSearch: true,
      broadcast: true,
      encryptionKey: encKey,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    peer1.on('collection-update', async data => {
      if(data.value.hello) {
        const col = await peer1.db.collection('example')
        const docs = await col.find(data.value)
        t.equals(docs[0].hello, 'world', 'peer 1 retrieved update from peer3')
      }
    })

    await peer1.ready()
      
    t.teardown(async () => {
      try {
        await closeCores([peer1, peer2])
        await peerCleanup()
      } catch(err) {
        console.log(err)
      }
    })
  } catch(err) {
    console.log(err)
  }
})

test('Drive - Sync Remote Database Updates', async t => {
  t.plan(4)
  
  try {
    const encKey = Buffer.alloc(32, 'hello world')

    const peer1 = new Drive(__dirname + '/peer1', null, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer1.ready()

    const peer2 = new Drive(__dirname + '/peer2', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer2.ready()

    const peer3 = new Drive(__dirname + '/peer3', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    peer1.on('collection-update', async data => {
      if(data.value.hello) {
        t.ok(data.value.hello, 'peer 1 has value hello')
      }
    })

    peer2.on('collection-update', data => {
      if(data.value.foo) {
        t.ok(data.value.foo, 'peer 2 has value foo')
      }

      if(data.value.hello) {
        t.ok(data.value.hello, 'peer 2 has value hello')
      }
    })

    peer3.on('collection-update', async data => {
      if(data.value.foo) {
        t.ok(data.value.foo, 'peer 3 has value foo')
      }
    })

    await peer3.ready()

    setTimeout(async () => {
      const collection1 = await peer1.db.collection('example')
      await collection1.insert({ foo: 'bar' })
      
      const collection3 = await peer3.db.collection('example')
      await collection3.insert({ hello: 'world' })
    }, 5000)

    t.teardown(async () => {
      try {
        await closeCores([peer1, peer2, peer3])
        await peerCleanup()
      } catch(err) {
        t.fail(err)
      }
    })
  } catch(err) {
    t.fail(err)
  }
})

test('Drive - Remove remote peer', async t => {
  t.plan(1)
  try {
    const encKey = Buffer.alloc(32, 'hello world')

    const peer1 = new Drive(__dirname + '/peer1', null, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    peer1.on('collection-update', async data => {
      if(data.value.baz) {
        t.fail('Should not sync data from removed peer!')
      }
    })
    await peer1.ready()
    const p2Kp = DHT.keyPair()
    let peer2 = new Drive(__dirname + '/peer2', peer1.publicKey, {
      keyPair: p2Kp,
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    await peer2.ready()

    const collection2 = await peer2.db.collection('example')

    const peer3 = new Drive(__dirname + '/peer3', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    peer3.on('collection-update', async data => {
      if(data.value.baz) {
        t.fail('Should not sync data from removed peer!')
      }
    })
    await peer3.ready()
    
    const peer4 = new Drive(__dirname + '/peer4', peer1.publicKey, {
      keyPair: DHT.keyPair(),
      encryptionKey: encKey,
      broadcast: true,
      swarmOpts: {
        server: true,
        client: true
      }
    })

    peer4.on('collection-update', async data => {
      if(data.value.baz) {
        t.fail('Should not sync data from removed peer!')
      }
    })

    await peer4.ready()

    pause(2000)

    await peer1.removePeer({
      blind: peer2.blind,
      publicKey: peer2.keyPair.publicKey.toString('hex'),
      writer: peer2.peerWriterKey, 
      meta: peer2.publicKey
    })

    setTimeout(async () => {
      await collection2.insert({ baz: 'foo' })
      t.ok(true)
    }, 10000)

    t.teardown(async () => {
      try {
        await closeCores([peer1, peer2, peer3, peer4])
        await peerCleanup()
      } catch(err) {
        console.log(err)
      }
    })
  } catch(err) {
    console.log(err)
  }
})

test('Drive - Create Seed Peer with Max Storage of 12mb', async t => {
  t.plan(4)
  
  drive6 = new Drive(__dirname + '/drive6', drive.publicKey, {
    keyPair: DHT.keyPair(),
    encryptionKey: drive.encryptionKey,
    storageMaxBytes: 1000 * 1000 * 12, // max size = 12mb
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive6.ready()

  drive6.on('file-sync', async (file) => {
    t.ok(file.uuid, `File has synced from remote peer`)
  })
})

// test('Drive - Fetch Files from Remote Drive', async t => {
//   t.plan(4)

//   drive3 = new Drive(__dirname + '/drive3', null, {
//     keyPair: keyPair3,
//     fileRetryAttempts: 10,
//     swarmOpts: {
//       server: true,
//       client: true
//     }
//   })

//   await drive3.ready()

//   await drive3.fetchFileBatch(hyperFiles, (stream, file) => {
//     return new Promise((resolve, reject) => {
//       let content = ''

//       stream.on('data', chunk => {
//         content += chunk.toString()
//       })

//       stream.once('end', () => {
//         t.ok(file.hash, `File has hash ${file.hash}`)
//         t.ok(content.length, `File downloaded from remote peer`)
//         resolve()
//       })
//     })
//   })
// })

// test('Drive - Fail to Fetch Files from Remote Drive', async t => {
//   t.plan(2)

//   drive4 = new Drive(__dirname + '/drive4', null, {
//     keyPair: keyPair3,
//     swarmOpts: {
//       server: true,
//       client: true
//     }
//   })
//   drive5 = new Drive(__dirname + '/drive5', null, {
//     keyPair: keyPair4,
//     swarmOpts: {
//       server: true,
//       client: true
//     }
//   })

  
//   await drive4.ready()
//   await drive5.ready()

//   const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
//   const file = await drive4.writeFile('/email/rawEmailEncrypted2.eml', readStream, { encrypted: true })
//   console.log('drive 4 write file')
//   await drive4.unlink(file.path)
//   console.log('drive 4 unlink file')
//   await drive5.fetchFileBatch([file], (stream, file) => {
//     return new Promise((resolve, reject) => {
//       let content = ''

//       stream.on('data', chunk => {
//         console.log('got data')
//         content += chunk.toString()
//       })

//       stream.on('error', (err) => {
//         console.log('stream error!')
//         t.ok(err.message, `Error has message: ${err.message}`)
//         t.equals(file.hash, err.fileHash, `Failed file hash matches the has in the request`)
//         resolve()
//       })
//     })
//   })
// })

// test('Drive - Unlink Local File', async t => {
//   t.plan(3)

//     const drive2 = new Drive(__dirname + '/drive2', drive.publicKey, {
//     keyPair: keyPair2,
//     encryptionKey: drive.encryptionKey,
//     swarmOpts: {
//       server: true,
//       client: true
//     }
//   })

//   await drive2.ready()

//   const drive1Size = drive.info().size
//   const drive2Size = drive2.info().size

  

//   drive.on('file-unlink', file => {
//     t.ok(drive1Size > drive.info().size, `Drive1 size before: ${drive1Size} > size after: ${drive.info().size}`)
//   })

//   drive2.on('file-unlink', file => {
//     t.ok(drive2Size > drive2.info().size, `Drive2 size before: ${drive2Size} > size after: ${drive2.info().size}`)
//   })

//   await drive.unlink('/email/rawEmailEncrypted.eml')
// })

test('Drive - Receive messages', async t => {
  t.plan(1)

  drive.on('message', (publicKey, data) => {
    const msg = JSON.parse(data.toString())
    t.ok(msg, 'Drive can receive messages.')
  })

  const node = new DHT()
  const noiseSocket = node.connect(keyPair.publicKey)

  noiseSocket.on('open', function () {
    noiseSocket.end(JSON.stringify({
      type: 'newMail',
      meta: 'meta mail message'
    }))
  })
})

test('Drive - Close drives', async t => {
  t.plan(1)

  const promises = []
  
  promises.push(drive.close())
  promises.push(drive2.close())
  promises.push(drive6.close())

  try {
    await Promise.all(promises)
    await cleanup()
    t.ok(1)
  } catch(err) {
    t.fail(err)
  }
})

test.onFinish(async () => {
  process.exit(0)
})


async function cleanup() {
  if (fs.existsSync(path.join(__dirname, '/drive'))) {
    await del([
      path.join(__dirname, '/drive')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive2'))) {
    await del([
      path.join(__dirname, '/drive2')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive3'))) {
    await del([
      path.join(__dirname, '/drive3')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive4'))) {
    await del([
      path.join(__dirname, '/drive4')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive5'))) {
    await del([
      path.join(__dirname, '/drive5')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive6'))) {
    await del([
      path.join(__dirname, '/drive6')
    ])
  }
}

async function peerCleanup() {
  if (fs.existsSync(path.join(__dirname, '/peer1'))) {
    await del([
      path.join(__dirname, '/peer1')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/peer2'))) {
    await del([
      path.join(__dirname, '/peer2')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/peer3'))) {
    await del([
      path.join(__dirname, '/peer3')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/peer4'))) {
    await del([
      path.join(__dirname, '/peer4')
    ])
  }
}

async function closeCores(cores) {
  const promises = []

  for(const core of cores) {
    promises.push(new Promise((resolve, reject) => {
      setTimeout(async () => {
        try {
          await core.close()
          resolve()
        } catch(err) {
          console.log(err)
          reject(err)
        }
      })
    }))
  }

  return Promise.all(promises)
}

async function pause(ms) {
  return new Promise((res, rej) => {
    setTimeout(() => {
      res()
    }, ms)
  })
}