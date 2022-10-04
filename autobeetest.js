(async() => {
const RAM = require('random-access-memory')
const Hypercore = require('hypercore')
const Autobase = require('autobase')
const HyperbeeDeeBee = require('hyperbeedeebee')
const Autodeebee = require('./lib/autodeebee')
const { DB } = HyperbeeDeeBee

function getBee () {
  const firstUser = new Hypercore(RAM)
  const firstOutput = new Hypercore(RAM)
  const inputs = [firstUser]

  const base1 = new Autobase({
    inputs,
    localOutput: firstOutput,
    localInput: firstUser
  })
  return new Autodeebee(base1)
}


const db = new DB(getBee())
  try {
    const collection = db.collection('example')


    const doc = await collection.insert({ example: 'Hello World!' })

    console.log('doc', doc)
    const otherDoc = await collection.findOne({ _id: doc._id })
    console.log('otherDoc', otherDoc)

  } catch(err) {
    console.log(err)
  }
})()