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
  t.plan(3)
  
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

    await collection.insert({ foo: "bar" })
    await collection.insert({ foo: "biz" })
    await collection.insert({ foo: "baz" })

    const docs = await collection.find()

    t.equals(docs[0].foo, 'bar')

    let doc = await collection.findOne({
      foo: 'biz'
    })

    t.equals(doc.foo, 'biz')

    await collection.update({_id: docs[0]._id }, { hello: "baz" })

    doc = await collection.findOne({
      foo: 'baz'
    })

    t.equals(doc.foo, 'baz')
  } catch (err) {
    t.error(err)
  }
})

test('Database - Full text search', async t => {
  t.plan(3)

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
    },
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
    const inserted = []
    
    for(const data of corpus) {
      const doc = await collection.insert({ title: data.title, text_body: data.text_body })
      inserted.push(doc)
      collection.ftsIndex(['text_body', 'title'], [doc])
    }
 
    setTimeout(async() => {
      await collection.delete({ title: 'Painting 2' })   

      const q1 = await collection.search("happy tree")

      t.equals(q1.length, 5)

      const q2 = await collection.search("happy tree", { limit: 1 })

      t.equals(q2.length, 1)

      const q3 = await collection.search("noresults")

      t.equals(q3.length, 0)
    })
  } catch (err) {
    t.error(err)
  }
})

test('Database - Remove item from hyperbee', async t => {
  t.plan(1)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey
    })

    await database.ready()
    
    const collection = await database.collection('foobar')
    const doc = await collection.insert({ foo: 'bar' })
    await collection.delete({ _id: doc._id })

    const item = await collection.find({ _id: doc._id })

    t.equals(0, item.length)
  } catch (err) {
    t.error(err)
  }
})