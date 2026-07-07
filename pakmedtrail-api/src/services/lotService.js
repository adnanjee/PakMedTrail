const { getContract } = require('./fabricGateway')
const { decodeResult } = require('../utils/fabric')
const { CHAINCODES } = require('./chaincodeConfig')

const CC = CHAINCODES.apitransfer

async function getAllLots(orgKey) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.getAllLots)
  return decodeResult(bytes)
}

async function getLot(orgKey, lotId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.readLot, lotId)
  return decodeResult(bytes)
}

async function createLot(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)
  const args = [
    payload.lotId,
    payload.name,
    payload.batchNumber || '',
    String(payload.quantity),
    payload.unit,
    payload.manufactureDate || '',
    payload.expiryDate || '',
    payload.metadataJson || '{}'
  ]
  const bytes = await contract.submitTransaction(CC.functions.createLot, ...args)
  return decodeResult(bytes)
}

async function drapApproveLot(orgKey, lotId, note) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(
    CC.functions.approveLotByDRAP,
    lotId,
    note || ''
  )
  return decodeResult(bytes)
}

async function proposeTransfer(orgKey, lotId, proposedOwnerMSP) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(
    CC.functions.proposeTransfer,
    lotId,
    proposedOwnerMSP
  )
  return decodeResult(bytes)
}

async function acceptTransfer(orgKey, lotId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(CC.functions.acceptTransfer, lotId)
  return decodeResult(bytes)
}

module.exports = {
  getAllLots,
  getLot,
  createLot,
  drapApproveLot,
  proposeTransfer,
  acceptTransfer
}
