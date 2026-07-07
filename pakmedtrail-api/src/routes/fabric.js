const express = require('express')
const { getContract } = require('../services/fabricGateway')
const { decodeResult, mapRoleToOrgKey } = require('../utils/fabric')
const { requireAuth } = require('../middleware/auth')

const router = express.Router()

router.get('/ping', requireAuth, async (req, res) => {
  try {
    const orgKey = mapRoleToOrgKey(req.user.role)
    if (!orgKey) {
      return res.status(400).json({ error: 'no Fabric identity mapped for this role' })
    }

    const contract = getContract(orgKey, 'apitransfer')

    const result = await contract.evaluateTransaction('GetAllLots')
    const decoded = decodeResult(result)

    res.json({
      org: orgKey,
      chaincode: 'apitransfer',
      function: 'GetAllLots',
      result: decoded
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({
      error: 'fabric query failed',
      message: err.message,
      details: err.details || null
    })
  }
})

module.exports = router
