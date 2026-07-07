const { getContract } = require('./fabricGateway')
const { decodeResult } = require('../utils/fabric')
const { CHAINCODES } = require('./chaincodeConfig')

const CC = CHAINCODES.recall

async function initiateRecall(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)

  let directives = payload.directives || {}

  // Accept either array-of-objects or flat map shapes.
  // If client sends array, convert to map using `action` as key.
  if (Array.isArray(directives)) {
    const map = {}
    for (const d of directives) {
      if (d && d.action) {
        map[d.action] = d.description || ''
      }
    }
    directives = map
  }

  const args = [
    payload.recallId,
    payload.title,
    payload.reason,
    payload.severity || 'HIGH',
    JSON.stringify(directives)
  ]
  const bytes = await contract.submitTransaction(CC.functions.initiateRecallByDRAP, ...args)
  return decodeResult(bytes)
}

async function addAffectedAssets(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)
  const csvAssetIds = Array.isArray(payload.assetIds)
    ? payload.assetIds.join(',')
    : String(payload.assetIds || '')

  const args = [
    payload.recallId,
    payload.assetType || 'BATCH',
    csvAssetIds
  ]
  const bytes = await contract.submitTransaction(CC.functions.addAffectedAssetsByDRAP, ...args)
  return decodeResult(bytes)
}

async function acknowledgeRecall(orgKey, recallId, note) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(CC.functions.acknowledgeRecall, recallId, note || '')
  return decodeResult(bytes)
}

async function quarantineAsset(orgKey, payload) {
  const contract = getContract(orgKey, CC.name)
  const args = [
    payload.assetType || 'BATCH',
    payload.assetId,
    payload.recallId,
    payload.reason || ''
  ]
  const bytes = await contract.submitTransaction('QuarantineAsset', ...args)
  return decodeResult(bytes)
}

async function clearQuarantine(orgKey, assetType, assetId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(
    CC.functions.clearQuarantine,
    assetType,
    assetId
  )
  return decodeResult(bytes)
}

async function closeRecall(orgKey, recallId, note) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.submitTransaction(CC.functions.closeRecallByDRAP, recallId, note || '')
  return decodeResult(bytes)
}

async function readRecall(orgKey, recallId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.readRecall, recallId)
  return decodeResult(bytes)
}

async function isAssetUnderActiveRecall(orgKey, assetType, assetId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(
    CC.functions.isAssetUnderActiveRecall,
    assetType,
    assetId
  )
  return decodeResult(bytes)
}

async function getQuarantine(orgKey, assetType, assetId) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(
    CC.functions.getQuarantine,
    assetType,
    assetId
  )
  return decodeResult(bytes)
}

async function listActiveRecalls(orgKey) {
  const contract = getContract(orgKey, CC.name)
  const bytes = await contract.evaluateTransaction(CC.functions.listActiveRecalls)
  return decodeResult(bytes)
}

module.exports = {
  initiateRecall,
  addAffectedAssets,
  acknowledgeRecall,
  quarantineAsset,
  clearQuarantine,
  closeRecall,
  readRecall,
  isAssetUnderActiveRecall,
  getQuarantine,
  listActiveRecalls
}
