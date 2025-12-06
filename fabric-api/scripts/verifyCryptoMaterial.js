// scripts/verifyCryptoMaterial.js
const fs = require('fs');
const path = require('path');

function verifyCryptoMaterial() {
  const cryptoConfigPath = path.join(__dirname, '../crypto-config');
  
  if (!fs.existsSync(cryptoConfigPath)) {
    console.error('❌ crypto-config directory not found at:', cryptoConfigPath);
    console.log('Please ensure crypto-config is in the parent directory of your API project');
    return false;
  }

  const organizations = ['supplier', 'manufacturer', 'distributor', 'retailer', 'drap'];
  
  for (const org of organizations) {
    const userPath = path.join(
      cryptoConfigPath,
      'peerOrganizations',
      `${org}.com`,
      'users',
      `User1@${org}.com`,
      'msp'
    );
    
    const certPath = path.join(userPath, 'signcerts', 'cert.pem');
    const keyDir = path.join(userPath, 'keystore');
    
    if (!fs.existsSync(certPath)) {
      console.error(`❌ Certificate not found for ${org}: ${certPath}`);
      return false;
    }
    
    if (!fs.existsSync(keyDir)) {
      console.error(`❌ Keystore directory not found for ${org}: ${keyDir}`);
      return false;
    }
    
    const keyFiles = fs.readdirSync(keyDir);
    if (keyFiles.length === 0) {
      console.error(`❌ No private key files found for ${org} in ${keyDir}`);
      return false;
    }
    
    console.log(`✅ ${org} crypto material verified`);
  }
  
  console.log('✅ All crypto material verified successfully!');
  return true;
}

verifyCryptoMaterial();