const jwt = require('jsonwebtoken')

const SECRET = process.env.JWT_SECRET
const EXPIRES_IN = '8h'

function sign(payload) {
  return jwt.sign(payload, SECRET, { expiresIn: EXPIRES_IN })
}

function verify(token) {
  return jwt.verify(token, SECRET)
}

module.exports = { sign, verify }
