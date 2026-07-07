const express = require('express')
const shipmentService = require('../services/shipmentService')
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

// Distribution shipments (manufacturer ships to distributor)

router.post('/distribution', requireAuth, requireRole('manufacturer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { shipmentId, batchId, toMSP, quantity, metadata } = req.body

  if (!shipmentId || !batchId || !toMSP || quantity === undefined) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['shipmentId', 'batchId', 'toMSP', 'quantity'],
      optional: ['metadata']
    })
  }

  try {
    const result = await shipmentService.createDistributionShipment(orgKey, {
      shipmentId,
      batchId,
      toMSP,
      quantity,
      metadataJson: metadata ? JSON.stringify(metadata) : '{}'
    })
    res.status(201).json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/distribution/:shipmentId/accept', requireAuth, requireRole('distributor'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await shipmentService.acceptDistributionShipment(orgKey, req.params.shipmentId)
    res.json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/distribution/:shipmentId/deliver', requireAuth, requireRole('distributor'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await shipmentService.markDistributionDelivered(orgKey, req.params.shipmentId)
    res.json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/distribution/:shipmentId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const shipment = await shipmentService.readDistributionShipment(orgKey, req.params.shipmentId)
    res.json({ shipment })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/distribution', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const partyMSP = req.query.party || req.user.org

  try {
    const shipments = await shipmentService.getDistributionShipmentsByParty(orgKey, partyMSP)
    res.json({ shipments: shipments || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

// Retail shipments (distributor ships to retailer)

router.post('/retail', requireAuth, requireRole('distributor'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { shipmentId, batchId, toMSP, quantity, metadata } = req.body

  if (!shipmentId || !batchId || !toMSP || quantity === undefined) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['shipmentId', 'batchId', 'toMSP', 'quantity'],
      optional: ['metadata']
    })
  }

  try {
    const result = await shipmentService.createRetailShipment(orgKey, {
      shipmentId,
      batchId,
      toMSP,
      quantity,
      metadataJson: metadata ? JSON.stringify(metadata) : '{}'
    })
    res.status(201).json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/retail/:shipmentId/accept', requireAuth, requireRole('retailer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await shipmentService.acceptRetailShipment(orgKey, req.params.shipmentId)
    res.json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/retail/:shipmentId/deliver', requireAuth, requireRole('retailer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await shipmentService.markRetailDelivered(orgKey, req.params.shipmentId)
    res.json({ shipment: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/retail/:shipmentId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const shipment = await shipmentService.readRetailShipment(orgKey, req.params.shipmentId)
    res.json({ shipment })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/retail', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const partyMSP = req.query.party || req.user.org

  try {
    const shipments = await shipmentService.getRetailShipmentsByParty(orgKey, partyMSP)
    res.json({ shipments: shipments || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

// Dispense (retailer to consumer)

router.post('/dispense', requireAuth, requireRole('retailer'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { dispenseId, batchId, quantity, metadata } = req.body

  if (!dispenseId || !batchId || quantity === undefined) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['dispenseId', 'batchId', 'quantity'],
      optional: ['metadata']
    })
  }

  try {
    const result = await shipmentService.verifyDispense(orgKey, {
      dispenseId,
      batchId,
      quantity,
      metadataJson: metadata ? JSON.stringify(metadata) : '{}'
    })
    res.status(201).json({ dispense: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/dispense/:dispenseId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const dispense = await shipmentService.readDispense(orgKey, req.params.dispenseId)
    res.json({ dispense })
  } catch (err) {
    handleFabricError(err, res)
  }
})

module.exports = router
