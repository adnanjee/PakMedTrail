const utf8Decoder = new TextDecoder()

function decodeResult(bytes) {
  if (!bytes || bytes.length === 0) {
    return null
  }

  const text = utf8Decoder.decode(bytes)

  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function mapRoleToOrgKey(role) {
  const map = {
    supplier: 'supplier',
    manufacturer: 'manufacturer',
    distributor: 'distributor',
    retailer: 'retailer',
    drap: 'drap'
  }
  return map[role] || null
}

module.exports = { decodeResult, mapRoleToOrgKey }
