(async () =>{
  const Hypercore = require('hypercore')
  const Autobase = require('autobase')
  const Autobee = require('./lib/autobee')

  const firstUser = new Hypercore('./core1')
  const firstOutput = new Hypercore('./core2')

  const inputs = [firstUser]

  const base = new Autobase({
    inputs,
    localInput: firstUser,
    localOutput: firstOutput
  })

  await base.ready()

  await base.close()

  await base.ready()

  await base.append(Buffer.from('foo')) // This breaks

  await base.close()

  await base.ready()

  // const val = await autobee.get(Buffer.from('hello'))
  // console.log(val)

})()