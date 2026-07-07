require('dotenv').config()
const express = require('express')
const cors = require('cors')
const helmet = require('helmet')
const morgan = require('morgan')

const { seedUsers } = require('./services/userStore')
const { ORGS } = require('./services/fabricConfig')
const { buildGatewayForOrg, closeAll } = require('./services/fabricGateway')
const authRoutes = require('./routes/auth')
const fabricRoutes = require('./routes/fabric')
const lotRoutes = require('./routes/lots')
const batchRoutes = require('./routes/batches')
const shipmentRoutes = require('./routes/shipments')
const recallRoutes = require('./routes/recalls')

const app = express()

app.use(helmet())
app.use(cors())
app.use(express.json())
app.use(morgan('dev'))

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'pakmedtrail-api',
    time: new Date().toISOString()
  })
})

app.use('/api/auth', authRoutes)
app.use('/api/fabric', fabricRoutes)
app.use('/api/lots', lotRoutes)
app.use('/api/batches', batchRoutes)
app.use('/api/shipments', shipmentRoutes)
app.use('/api/recalls', recallRoutes)

app.use((req, res) => {
  res.status(404).json({ error: 'not found', path: req.path })
})

app.use((err, req, res, next) => {
  console.error(err)
  res.status(500).json({ error: 'internal server error' })
})

const PORT = process.env.PORT || 4000

async function start() {
  await seedUsers()

  console.log('Initializing Fabric gateways...')
  for (const orgKey of ORGS) {
    try {
      await buildGatewayForOrg(orgKey)
    } catch (err) {
      console.error(`Failed to initialize gateway for ${orgKey}:`, err.message)
      throw err
    }
  }
  console.log('All gateways initialized.')

  const server = app.listen(PORT, () => {
    console.log(`PakMedTrail API listening on port ${PORT}`)
  })

  const shutdown = async () => {
    console.log('Shutting down...')
    await closeAll()
    server.close(() => {
      console.log('Server closed.')
      process.exit(0)
    })
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
}

start().catch(err => {
  console.error('Failed to start server', err)
  process.exit(1)
})
