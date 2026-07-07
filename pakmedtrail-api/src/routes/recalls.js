const express = require('express')
const recallService = require('../services/recallService')
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

router.post('/', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { recallId, title, reason, severity, directives } = req.body
  if (!recallId || !title || !reason) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['recallId', 'title', 'reason'],
      optional: ['severity', 'directives']
    })
  }

  try {
    const result = await recallService.initiateRecall(orgKey, {
      recallId, title, reason, severity, directives
    })
    res.status(201).json({ recall: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:recallId/affected-assets', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { assetType, assetIds } = req.body
  if (!assetIds || (Array.isArray(assetIds) && assetIds.length === 0)) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['assetIds']
    })
  }

  try {
    const result = await recallService.addAffectedAssets(orgKey, {
      recallId: req.params.recallId,
      assetType,
      assetIds
    })
    res.status(201).json({ result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:recallId/acknowledge', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await recallService.acknowledgeRecall(orgKey, req.params.recallId, req.body.note)
    res.json({ acknowledgement: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:recallId/quarantine', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  const { assetType, assetId, reason } = req.body
  if (!assetId) {
    return res.status(400).json({
      error: 'missing required fields',
      required: ['assetId'],
      optional: ['assetType', 'reason']
    })
  }

  try {
    const result = await recallService.quarantineAsset(orgKey, {
      assetType,
      assetId,
      recallId: req.params.recallId,
      reason
    })
    res.status(201).json({ quarantine: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/:recallId/close', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await recallService.closeRecall(orgKey, req.params.recallId, req.body.note)
    res.json({ recall: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/active', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const recalls = await recallService.listActiveRecalls(orgKey)
    res.json({ recalls: recalls || [] })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/:recallId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const recall = await recallService.readRecall(orgKey, req.params.recallId)
    res.json({ recall })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/quarantine/:assetType/:assetId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const quarantine = await recallService.getQuarantine(
      orgKey,
      req.params.assetType,
      req.params.assetId
    )
    res.json({ quarantine })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.get('/active-check/:assetType/:assetId', requireAuth, async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const active = await recallService.isAssetUnderActiveRecall(
      orgKey,
      req.params.assetType,
      req.params.assetId
    )
    res.json({ assetType: req.params.assetType, assetId: req.params.assetId, active })
  } catch (err) {
    handleFabricError(err, res)
  }
})

router.post('/quarantine/:assetType/:assetId/clear', requireAuth, requireRole('drap'), async (req, res) => {
  const orgKey = getOrgKeyFromUser(req, res)
  if (!orgKey) return

  try {
    const result = await recallService.clearQuarantine(
      orgKey,
      req.params.assetType,
      req.params.assetId
    )
    res.json({ quarantine: result })
  } catch (err) {
    handleFabricError(err, res)
  }
})

module.exports = router
