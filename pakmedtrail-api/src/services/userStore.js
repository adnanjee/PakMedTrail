const bcrypt = require('bcryptjs')

const users = new Map()

async function seedUsers() {
  const seedData = [
    { username: 'supplier_admin', password: 'supplier123', org: 'supplierMSP', role: 'supplier' },
    { username: 'mfg_admin', password: 'mfg123', org: 'manufacturerMSP', role: 'manufacturer' },
    { username: 'dist_admin', password: 'dist123', org: 'distributorMSP', role: 'distributor' },
    { username: 'retail_admin', password: 'retail123', org: 'retailerMSP', role: 'retailer' },
    { username: 'drap_admin', password: 'drap123', org: 'drapMSP', role: 'drap' }
  ]

  for (const u of seedData) {
    const passwordHash = await bcrypt.hash(u.password, 10)
    users.set(u.username, {
      username: u.username,
      passwordHash,
      org: u.org,
      role: u.role,
      createdAt: new Date().toISOString()
    })
  }

  console.log(`Seeded ${users.size} users`)
}

function findByUsername(username) {
  return users.get(username) || null
}

async function addUser({ username, password, org, role }) {
  if (users.has(username)) {
    throw new Error('username already exists')
  }

  const passwordHash = await bcrypt.hash(password, 10)
  const user = {
    username,
    passwordHash,
    org,
    role,
    createdAt: new Date().toISOString()
  }

  users.set(username, user)
  return user
}

function listUsers() {
  return Array.from(users.values()).map(u => ({
    username: u.username,
    org: u.org,
    role: u.role,
    createdAt: u.createdAt
  }))
}

module.exports = { seedUsers, findByUsername, addUser, listUsers }
