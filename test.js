(async () => {

  const Drive = require('./')

  const drive1 = new Drive(__dirname + '/drive1', null, {
    checkNetworkStatus: true,
    fullTextSearch: true,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive1.ready()

  drive1.on('network-updated', async data => {

    if(data.drive) {
      try {
        await drive1.close()
        
        console.log('CLOSED')

        await drive1.ready()

        console.log('READY AGAIN')
      } catch(err) {
        console.log('ERRRR', err)
      }
    }
  })
  
})()