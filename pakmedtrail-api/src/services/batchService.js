const { getContract } = require('./fabricGateway')
const { decodeResult } = require('../utils/fabric')
const { CHAINCODES } = require('./chaincodeConfig')

const CC = CHAINCODES.manufacturing

async function getAllFormulations(orgKey) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.getAllFormulations)
  return decodeResult(bytes)
}

async function readFormulation(orgKey, drugCode) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.readFormulation, drugCode)
  return decodeResult(bytes)
}

async function createFormulation(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)
  const args = [
    payload.drugCode,
    payload.unit,
    JSON.stringify(payload.requirements || [])
  ]
  const bytes = await contract.submitTransaction(CC.functions.createFormulation, ...args)
  return decodeResult(bytes)
}

async function readBatch(orgKey, batchId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.readBatch, batchId)
  return decodeResult(bytes)
}

async function getBatchesByOwner(orgKey, ownerMSP) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.getBatchesByOwner, ownerMSP)
  return decodeResult(bytes)
}

async function produceDrug(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)

  const normalizedInputs = (payload.inputs || []).map(input => ({
    lotId: input.lotId || input.lotID || input.LotID,
    ingredientName: input.ingredientName || input.name,
    amount: String(input.amount)
  }))

  const args = [
    payload.batchId,
    payload.drugCode,
    String(payload.outputQuantity),
    payload.unit,
    JSON.stringify(normalizedInputs)
  ]
  const bytes = await contract.submitTransaction(CC.functions.produceDrug, ...args)
  return decodeResult(bytes)
}

async function drapApproveBatch(orgKey, batchId, note) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(
    CC.functions.approveDrugBatchByDRAP,
    batchId,
    note || ''
  )
  return decodeResult(bytes)
}

async function proposeBatchTransfer(orgKey, batchId, proposedOwnerMSP) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(
    CC.functions.proposeBatchTransfer,
    batchId,
    proposedOwnerMSP
  )
  return decodeResult(bytes)
}

async function acceptBatchTransfer(orgKey, batchId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(CC.functions.acceptBatchTransfer, batchId)
  return decodeResult(bytes)
}

module.exports = {
  getAllFormulations,
  readFormulation,
  createFormulation,
  readBatch,
  getBatchesByOwner,
  produceDrug,
  drapApproveBatch,
  proposeBatchTransfer,
  acceptBatchTransfer
}
