const { getContract } = require('./fabricGateway')
const { decodeResult } = require('../utils/fabric')
const { CHAINCODES } = require('./chaincodeConfig')

const DIST = CHAINCODES.distribution
const RETAIL = CHAINCODES.retail

// Distribution shipments (manufacturer -> distributor)

async function createDistributionShipment(orgKey, payload) {
  const contract = getContract(orgKey, DIST.name)
  const args = [
    payload.shipmentId,
    payload.batchId,
    payload.toMSP,
    String(payload.quantity),
    payload.metadataJson || '{}'
  ]
  const bytes = await contract.submitTransaction(DIST.functions.createShipmentOffer, ...args)
  return decodeResult(bytes)
}

async function acceptDistributionShipment(orgKey, shipmentId) {
  const contract = getContract(orgKey, DIST.name)
  const bytes = await contract.submitTransaction(DIST.functions.acceptShipment, shipmentId)
  return decodeResult(bytes)
}

async function markDistributionDelivered(orgKey, shipmentId) {
  const contract = getContract(orgKey, DIST.name)
  const bytes = await contract.submitTransaction(DIST.functions.markDelivered, shipmentId)
  return decodeResult(bytes)
}

async function readDistributionShipment(orgKey, shipmentId) {
  const contract = getContract(orgKey, DIST.name)
  const bytes = await contract.evaluateTransaction(DIST.functions.readShipment, shipmentId)
  return decodeResult(bytes)
}

async function getDistributionShipmentsByParty(orgKey, partyMSP) {
  const contract = getContract(orgKey, DIST.name)
  const bytes = await contract.evaluateTransaction(DIST.functions.getShipmentsByParty, partyMSP)
  return decodeResult(bytes)
}

// Retail shipments (distributor -> retailer)

async function createRetailShipment(orgKey, payload) {
  const contract = getContract(orgKey, RETAIL.name)
  const args = [
    payload.shipmentId,
    payload.batchId,
    payload.toMSP,
    String(payload.quantity),
    payload.metadataJson || '{}'
  ]
  const bytes = await contract.submitTransaction(RETAIL.functions.createRetailShipmentOffer, ...args)
  return decodeResult(bytes)
}

async function acceptRetailShipment(orgKey, shipmentId) {
  const contract = getContract(orgKey, RETAIL.name)
  const bytes = await contract.submitTransaction(RETAIL.functions.acceptRetailShipment, shipmentId)
  return decodeResult(bytes)
}

async function markRetailDelivered(orgKey, shipmentId) {
  const contract = getContract(orgKey, RETAIL.name)
  const bytes = await contract.submitTransaction(RETAIL.functions.markRetailDelivered, shipmentId)
  return decodeResult(bytes)
}

async function readRetailShipment(orgKey, shipmentId) {
  const contract = getContract(orgKey, RETAIL.name)
  const bytes = await contract.evaluateTransaction(RETAIL.functions.readShipment, shipmentId)
  return decodeResult(bytes)
}

async function getRetailShipmentsByParty(orgKey, partyMSP) {
  const contract = getContract(orgKey, RETAIL.name)
  const bytes = await contract.evaluateTransaction(RETAIL.functions.getShipmentsByParty, partyMSP)
  return decodeResult(bytes)
}

// Dispense (retail consumer-side)

async function verifyDispense(orgKey, payload) {
  const contract = getContract(orgKey, RETAIL.name)
  const args = [
    payload.dispenseId,
    payload.batchId,
    String(payload.quantity),
    payload.metadataJson || '{}'
  ]
  const bytes = await contract.submitTransaction(RETAIL.functions.verifyDispense, ...args)
  return decodeResult(bytes)
}

async function readDispense(orgKey, dispenseId) {
  const contract = getContract(orgKey, RETAIL.name)
  const bytes = await contract.evaluateTransaction(RETAIL.functions.readDispense, dispenseId)
  return decodeResult(bytes)
}

module.exports = {
  createDistributionShipment,
  acceptDistributionShipment,
  markDistributionDelivered,
  readDistributionShipment,
  getDistributionShipmentsByParty,
  createRetailShipment,
  acceptRetailShipment,
  markRetailDelivered,
  readRetailShipment,
  getRetailShipmentsByParty,
  verifyDispense,
  readDispense
}
