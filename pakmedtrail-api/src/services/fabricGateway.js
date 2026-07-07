const fs = require('fs')
const crypto = require('crypto')
const grpc = require('@grpc/grpc-js')
const { connect, signers } = require('@hyperledger/fabric-gateway')

const { loadOrgConfig, CHANNEL } = require('./fabricConfig')

const gatewayCache = new Map()

async function buildGatewayForOrg(orgKey) {
  if (gatewayCache.has(orgKey)) {
    return gatewayCache.get(orgKey)
  }

  const config = loadOrgConfig(orgKey)

  const allTlsCertPaths = [
  process.env.FABRIC_SUPPLIER_TLS_CERT,
  process.env.FABRIC_MANUFACTURER_TLS_CERT,
  process.env.FABRIC_DISTRIBUTOR_TLS_CERT,
  process.env.FABRIC_RETAILER_TLS_CERT,
  process.env.FABRIC_DRAP_TLS_CERT,
  process.env.FABRIC_ORDERER_TLS_CERT
]

const combinedTlsBuffer = Buffer.concat(
  allTlsCertPaths.map(p => fs.readFileSync(p))
)

const tlsCredentials = grpc.credentials.createSsl(combinedTlsBuffer)

  const client = new grpc.Client(config.peerEndpoint, tlsCredentials, {
    'grpc.ssl_target_name_override': config.peerHostOverride
  })

  const userCert = fs.readFileSync(config.userCertPath)
  const identity = {
    mspId: config.mspId,
    credentials: userCert
  }

  const privateKeyPem = fs.readFileSync(config.userKeyPath)
  const privateKey = crypto.createPrivateKey(privateKeyPem)
  const signer = signers.newPrivateKeySigner(privateKey)

  const gateway = connect({
    client,
    identity,
    signer,
    evaluateOptions: () => ({ deadline: Date.now() + 5000 }),
    endorseOptions: () => ({ deadline: Date.now() + 15000 }),
    submitOptions: () => ({ deadline: Date.now() + 15000 }),
    commitStatusOptions: () => ({ deadline: Date.now() + 60000 })
  })

  const network = gateway.getNetwork(CHANNEL)

  const entry = { gateway, network, client }
  gatewayCache.set(orgKey, entry)

  console.log(`Gateway connected for ${orgKey} (mspId=${config.mspId}, peer=${config.peerEndpoint})`)

  return entry
}

function getContract(orgKey, chaincodeName) {
  const entry = gatewayCache.get(orgKey)
  if (!entry) {
    throw new Error(`Gateway not initialized for ${orgKey}. Call buildGatewayForOrg first.`)
  }
  return entry.network.getContract(chaincodeName)
}

async function closeAll() {
  for (const [orgKey, entry] of gatewayCache.entries()) {
    try {
      entry.gateway.close()
      entry.client.close()
      console.log(`Closed gateway for ${orgKey}`)
    } catch (err) {
      console.error(`Error closing gateway for ${orgKey}:`, err.message)
    }
  }
  gatewayCache.clear()
}

module.exports = { buildGatewayForOrg, getContract, closeAll }
