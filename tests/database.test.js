const tape = require('tape')
const _test = require('tape-promise').default
const test = _test(tape)
const Database = require('../lib/database')
const ram = require('random-access-memory')
const DHT = require('@hyperswarm/dht')
const { v4: uuidv4 } = require('uuid')

test('Database - Create new db', async t => {
  t.plan(1)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey
    })

    await database.ready()

    t.ok(database.localMetaCore.key.toString('hex'))
  } catch (err) {
    console.log('ERROR: ', err)
    t.error(err)
  }
})

test('Database - Test put/get', async t => {
  t.plan(2)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey,
      fts: true
    })

    await database.ready()
    
    const collection = await database.collection('foobar')
    
    await collection.insert({ hello: "world" })

    const docs = await collection.find({
      hello: 'world'
    })

    t.equals(docs[0].hello, 'world')

    await collection.update({_id: docs[0]._id }, { hello: "baz" })

    const val = await collection.findOne({
      hello: 'baz'
    })

    t.equals(val.hello, 'baz')
  } catch (err) {
    t.error(err)
  }
})

test('Database - Full text search', async t => {
  const corpus = [
    {
      title: 'Painting 1',
      text_body: "In your world you can create anything you desire."
    },
    {
      title: 'Painting 2',
      text_body: "I thought today we would make a happy little stream that's just running through the woods here."
    },
    {
      title: 'Painting 3',
      text_body: "See. We take the corner of the brush and let it play back-and-forth. No pressure. Just relax and watch it happen."
    },
    {
      title: 'Painting 4',
      text_body: "Just go back and put one little more happy tree in there. Without washing the brush, I'm gonna go right into some Van Dyke Brown."
    },
    {
      title: 'Painting 5',
      text_body: "Trees get lonely too, so we'll give him a little friend. If what you're doing doesn't make you happy - you're doing the wrong thing."
    },
    {
      title: 'Painting 6',
      text_body: "Son of a gun. We're not trying to teach you a thing to copy. We're just here to teach you a technique, then let you loose into the world."
    }
  ]

  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey,
      fts: true
    })

    await database.ready()
    
    const collection = await database.collection('BobRoss')

    for(const data of corpus) {
      await collection.insert({ title: data.title, text_body: data.text_body })
    }
 
    await collection.ftsIndex(['text_body', 'title'])

    const q1 = await collection.search("happy tree")

    t.equals(q1.length, 3)

    const q2 = await collection.search("happy tree", { limit: 2 })

    t.equals(q2.length, 2)

    const q3 = await collection.search("noresults")

    t.equals(q3.length, 0)
  } catch (err) {
    t.error(err)
  }
})

// Currently not a supported function of Hyperbeedeebee

// test('Database - Delete from hyperbee', async t => {
//   t.plan(1)
  
//   const keyPair = DHT.keyPair()
//   const encryptionKey = Buffer.alloc(32, 'hello world')

//   try {
//     const database = new Database(ram, {
//       keyPair,
//       encryptionKey
//     })

//     await database.ready()
    
//     const collection = await database.collection('foobar')
//     await collection.insert({ hello: 'bar' })
//     await collection.del('foo')

//     // const item = await collection.get('foo')

//     t.equals(item, null)
//   } catch (err) {
//     t.error(err)
//   }
// })