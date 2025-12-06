const { FileSystemWallet, X509WalletMixin } = require('fabric-network');
const FabricCAServices = require('fabric-ca-client');
const fs = require('fs');
const path = require('path');

async function enrollUser(organization, userId) {
  try {
    // Create a new CA client for interacting with the CA
    const caInfo = await getCAInfo(organization);
    const caTLSCACerts = caInfo.tlsCACerts.pem;
    const ca = new FabricCAServices(caInfo.url, { trustedRoots: caTLSCACerts, verify: false }, caInfo.caName);

    // Create a new file system based wallet for managing identities
    const walletPath = path.join(process.cwd(), 'wallet');
    const wallet = new FileSystemWallet(walletPath);

    // Check to see if we've already enrolled the user
    const userExists = await wallet.exists(userId);
    if (userExists) {
      console.log(`An identity for the user "${userId}" already exists in the wallet`);
      return;
    }

    // Check to see if we've already enrolled the admin user
    const adminExists = await wallet.exists('admin');
    if (!adminExists) {
      console.log('An identity for the admin user "admin" does not exist in the wallet');
      console.log('Run the enrollAdmin.js application before retrying');
      return;
    }

    // Create a new gateway for connecting to our peer node
    const { gateway, network } = await connectToFabric('admin', organization);
    
    // Get the CA client object from the gateway for interacting with the CA
    const client = gateway.getClient();
    const caClient = client.getCertificateAuthority();
    
    const adminIdentity = gateway.getCurrentIdentity();
    
    // Register the user, enroll the user, and import the new identity into the wallet
    const secret = await caClient.register({
      enrollmentID: userId,
      role: 'client'
    }, adminIdentity);
    
    const enrollment = await caClient.enroll({
      enrollmentID: userId,
      enrollmentSecret: secret
    });
    
    const userIdentity = X509WalletMixin.createIdentity(
      getMSPID(organization),
      enrollment.certificate,
      enrollment.key.toBytes()
    );
    
    await wallet.import(userId, userIdentity);
    console.log(`Successfully registered and enrolled admin user "${userId}" and imported it into the wallet`);

    await gateway.disconnect();
  } catch (error) {
    console.error(`Failed to enroll user "${userId}": ${error}`);
  }
}

function getMSPID(organization) {
  const mspMap = {
    'supplier': 'supplierMSP',
    'manufacturer': 'manufacturerMSP',
    'distributor': 'distributorMSP',
    'retailer': 'retailerMSP',
    'drap': 'drapMSP'
  };
  return mspMap[organization.toLowerCase()];
}

async function getCAInfo(organization) {
  // This would be implemented based on your CA configuration
  // Return CA URL, CA name, and TLS certs
}

module.exports = { enrollUser };