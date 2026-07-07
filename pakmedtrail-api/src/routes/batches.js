const express = require('express')
const batchService = require('../services/batchService')
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

// Formulations

router.get('/formulations', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const formulations = await batchService.getAllFormulations(orgKey)
    res.json({ formulations: formulations || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/formulations/:drugCode', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const formulation = await batchService.readFormulation(orgKey, req.params.drugCode)
    res.json({ formulation })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/formulations', requireAuth, requireRole('manufacturer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { drugCode, unit, requirements } = req.body

  if (!drugCode || !unit) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['drugCode', 'unit'],
      optional: ['requirements']
    })
  }

  try {
    const result = await batchService.createFormulation(orgKey, {
      drugCode,
      unit,
      requirements
    })
    res.status(201).json({ formulation: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

// Batches

router.get('/', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const ownerMSP = req.query.owner || req.user.org

  try {
    const batches = await batchService.getBatchesByOwner(orgKey, ownerMSP)
    res.json({ batches: batches || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/:batchId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const batch = await batchService.readBatch(orgKey, req.params.batchId)
    res.json({ batch })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/', requireAuth, requireRole('manufacturer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { batchId, drugCode, outputQuantity, unit, inputs } = req.body

  if (!batchId || !drugCode || outputQuantity === undefined || !unit) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['batchId', 'drugCode', 'outputQuantity', 'unit'],
      optional: ['inputs']
    })
  }

  try {
    const result = await batchService.produceDrug(orgKey, {
      batchId,
      drugCode,
      outputQuantity,
      unit,
      inputs
    })
    res.status(201).json({ batch: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:batchId/drap-approve', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await batchService.drapApproveBatch(orgKey, req.params.batchId, req.body.note)
    res.json({ batch: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:batchId/propose-transfer', requireAuth, requireRole('manufacturer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { proposedOwnerMSP } = req.body
  if (!proposedOwnerMSP) {
    return res.status(400).json({ error: 'proposedOwnerMSP required' })
  }

  try {
    const result = await batchService.proposeBatchTransfer(
      orgKey,
      req.params.batchId,
      proposedOwnerMSP
    )
    res.json({ batch: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:batchId/accept-transfer', requireAuth, requireRole('distributor'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await batchService.acceptBatchTransfer(orgKey, req.params.batchId)
    res.json({ batch: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

module.exports = router
