require('dotenv').config()

const ORGS = ['supplier', 'manufacturer', 'distributor', 'retailer', 'drap']

function loadOrgConfig(orgKey) {
  const upper = orgKey.toUpperCase()

  const config = {
    org: orgKey,
    mspId: process.env[`FABRIC_${upper}_MSP`],
    peerEndpoint: process.env[`FABRIC_${upper}_PEER_ENDPOINT`],
    peerHostOverride: process.env[`FABRIC_${upper}_PEER_HOST_OVERRIDE`],
    tlsCertPath: process.env[`FABRIC_${upper}_TLS_CERT`],
    userCertPath: process.env[`FABRIC_${upper}_USER_CERT`],
    userKeyPath: process.env[`FABRIC_${upper}_USER_KEY`]
  }

  for (const [key, value] of Object.entries(config)) {
    if (!value) {
      throw new Error(`Missing Fabric config for ${orgKey}: ${key}`)
    }
  }

  return config
}

function loadAllOrgs() {
  const orgs = {}
  for (const orgKey of ORGS) {
    orgs[orgKey] = loadOrgConfig(orgKey)
  }
  return orgs
}

const ORDERER = {
  endpoint: process.env.FABRIC_ORDERER_ENDPOINT,
  tlsCertPath: process.env.FABRIC_ORDERER_TLS_CERT,
  hostOverride: process.env.FABRIC_ORDERER_HOST_OVERRIDE
}

const CHANNEL = process.env.FABRIC_CHANNEL || 'pakmedtrail'

module.exports = { ORGS, loadOrgConfig, loadAllOrgs, ORDERER, CHANNEL }
