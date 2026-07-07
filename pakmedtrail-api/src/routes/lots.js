const express = require('express')
const lotService = require('../services/lotService')
const { mapRoleToOrgKey } = require('../utils/fabric')
const { requireAuth, requireRole } = require('../middleware/auth')

const router = express.Router()

function getOrgKeyFromUser(req, res) {
  const orgKey = mapRoleToOrgKey(req.user.role)
  if (!orgKey) {
    res.status(400).json({ error: 'no Fabric identity mapped for this role' })
    return null
  }
  return orgKey
}

function handleFabricError(err, res) {
  console.error(err)
  res.status(500).json({
    error: 'chaincode call failed',
    message: err.message,
    details: err.details || null
  })
}

router.get('/', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const lots = await lotService.getAllLots(orgKey)
    res.json({ lots: lots || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/:lotId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const lot = await lotService.getLot(orgKey, req.params.lotId)
    res.json({ lot })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/', requireAuth, requireRole('supplier'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { lotId, name, batchNumber, quantity, unit, manufactureDate, expiryDate, metadata } = req.body

  if (!lotId || !name || quantity === undefined || !unit) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['lotId', 'name', 'quantity', 'unit'],
      optional: ['batchNumber', 'manufactureDate', 'expiryDate', 'metadata']
    })
  }

  try {
    const result = await lotService.createLot(orgKey, {
      lotId,
      name,
      batchNumber,
      quantity,
      unit,
      manufactureDate,
      expiryDate,
      metadataJson: metadata ? JSON.stringify(metadata) : '{}'
    })
    res.status(201).json({ lot: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:lotId/drap-approve', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await lotService.drapApproveLot(orgKey, req.params.lotId, req.body.note)
    res.json({ lot: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:lotId/propose-transfer', requireAuth, requireRole('supplier'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { proposedOwnerMSP } = req.body

  if (!proposedOwnerMSP) {
    return res.status(400).json({ error: 'proposedOwnerMSP required' })
  }

  try {
    const result = await lotService.proposeTransfer(
      orgKey,
      req.params.lotId,
      proposedOwnerMSP
    )
    res.json({ lot: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:lotId/accept-transfer', requireAuth, requireRole('manufacturer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await lotService.acceptTransfer(orgKey, req.params.lotId)
    res.json({ lot: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

module.exports = router
