const { verify } = require('../utils/jwt')

function requireAuth(req, res, next) {
  const header = req.headers.authorization

  if (!header || !header.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'missing or invalid authorization header' })
  }

  const token = header.slice('Bearer '.length)

  try {
    const decoded = verify(token)
    req.user = decoded
    next()
  } catch (err) {
    return res.status(401).json({ error: 'invalid or expired token' })
  }
}

function requireRole(...allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'not authenticated' })
    }

    if (!allowedRoles.includes(req.user.role)) {
      return res.status(403).json({
        error: 'forbidden',
        required: allowedRoles,
        actual: req.user.role
      })
    }

    next()
  }
}

module.exports = { requireAuth, requireRole }
