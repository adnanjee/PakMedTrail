const express = require('express')
const bcrypt = require('bcryptjs')
const { findByUsername, addUser, listUsers } = require('../services/userStore')
const { sign } = require('../utils/jwt')
const { requireAuth, requireRole } = require('../middleware/auth')

const router = express.Router()

router.post('/login', async (req, res) => {
  const { username, password } = req.body

  if (!username || !password) {
    return res.status(400).json({ error: 'username and password required' })
  }

  const user = findByUsername(username)

  if (!user) {
    return res.status(401).json({ error: 'invalid credentials' })
  }

  const match = await bcrypt.compare(password, user.passwordHash)

  if (!match) {
    return res.status(401).json({ error: 'invalid credentials' })
  }

  const token = sign({
    username: user.username,
    org: user.org,
    role: user.role
  })

  res.json({
    token,
    user: {
      username: user.username,
      org: user.org,
      role: user.role
    }
  })
})

router.post('/register', async (req, res) => {
  const { username, password, org, role } = req.body

  if (!username || !password || !org || !role) {
    return res.status(400).json({ error: 'username, password, org, and role required' })
  }

  const validOrgs = ['supplierMSP', 'manufacturerMSP', 'distributorMSP', 'retailerMSP', 'drapMSP']
  const validRoles = ['supplier', 'manufacturer', 'distributor', 'retailer', 'drap']

  if (!validOrgs.includes(org)) {
    return res.status(400).json({ error: 'invalid org', validOrgs })
  }

  if (!validRoles.includes(role)) {
    return res.status(400).json({ error: 'invalid role', validRoles })
  }

  try {
    const user = await addUser({ username, password, org, role })
    res.status(201).json({
      username: user.username,
      org: user.org,
      role: user.role,
      createdAt: user.createdAt
    })
  } catch (err) {
    res.status(409).json({ error: err.message })
  }
})

router.get('/me', requireAuth, (req, res) => {
  res.json({
    username: req.user.username,
    org: req.user.org,
    role: req.user.role
  })
})

router.get('/users', requireAuth, requireRole('drap'), (req, res) => {
  res.json({ users: listUsers() })
})

module.exports = router
