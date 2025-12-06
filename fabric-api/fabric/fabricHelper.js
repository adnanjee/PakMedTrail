const { Wallets, Gateway } = require('fabric-network');
const fs = require('fs');
const path = require('path');

class FabricHelper {
  constructor() {
    this.connectionProfile = this.loadConnectionProfile();
    this.walletPath = path.join(process.cwd(), 'wallet');
    this.cryptoConfigPath = path.join(process.cwd(), '../crypto-config');
  }

  loadConnectionProfile() {
    try {
      const ccpPath = path.join(__dirname, '../connection-profiles/connection.json');
      if (!fs.existsSync(ccpPath)) {
        throw new Error(`Connection profile not found at: ${ccpPath}`);
      }
      const ccpJSON = fs.readFileSync(ccpPath, 'utf8');
      const ccp = JSON.parse(ccpJSON);
      console.log('Loaded connection profile successfully');
      return ccp;
    } catch (error) {
      throw new Error(`Failed to load connection profile: ${error.message}`);
    }
  }

  // Build user identity from existing crypto material
  async buildUserIdentity(organization, userName) {
    try {
      const orgDomain = this.getOrgDomain(organization);
      const mspId = this.getMSPID(organization);
      
      // Read the user certificate
      const certPath = path.join(
        this.cryptoConfigPath,
        'peerOrganizations',
        orgDomain,
        'users',
        `${userName}@${orgDomain}`,
        'msp',
        'signcerts',
        'cert.pem'
      );
      
      if (!fs.existsSync(certPath)) {
        throw new Error(`Certificate not found at: ${certPath}`);
      }
      
      // Read the user private key
      const keyDir = path.join(
        this.cryptoConfigPath,
        'peerOrganizations',
        orgDomain,
        'users',
        `${userName}@${orgDomain}`,
        'msp',
        'keystore'
      );
      
      if (!fs.existsSync(keyDir)) {
        throw new Error(`Keystore directory not found at: ${keyDir}`);
      }
      
      // Get the first file in keystore directory (the private key)
      const keyFiles = fs.readdirSync(keyDir);
      if (keyFiles.length === 0) {
        throw new Error(`No private key found for user ${userName} in ${keyDir}`);
      }
      
      const keyPath = path.join(keyDir, keyFiles[0]);
      
      // Read certificate and key files
      const certificate = fs.readFileSync(certPath, 'utf8');
      const privateKey = fs.readFileSync(keyPath, 'utf8');
      
      const identity = {
        credentials: {
          certificate,
          privateKey
        },
        mspId,
        type: 'X.509'
      };
      
      return identity;
    } catch (error) {
      throw new Error(`Failed to build identity for ${userName}@${organization}: ${error.message}`);
    }
  }

  // Initialize wallet with user identities
  async initializeWallet() {
    try {
      const wallet = await Wallets.newFileSystemWallet(this.walletPath);
      console.log(`Wallet path: ${this.walletPath}`);
      
      // Define users for each organization
      const users = [
        { org: 'supplier', user: 'User1' },
        { org: 'manufacturer', user: 'User1' },
        { org: 'distributor', user: 'User1' },
        { org: 'retailer', user: 'User1' },
        { org: 'drap', user: 'User1' }
      ];

      for (const { org, user } of users) {
        const identityLabel = `${user}@${org}`;
        
        const exists = await wallet.get(identityLabel);
        if (!exists) {
          try {
            const identity = await this.buildUserIdentity(org, user);
            await wallet.put(identityLabel, identity);
            console.log(`✅ Successfully imported identity: ${identityLabel}`);
          } catch (error) {
            console.warn(`⚠️ Could not import ${identityLabel}: ${error.message}`);
          }
        } else {
          console.log(`✅ Identity already exists: ${identityLabel}`);
        }
      }
      
      return wallet;
    } catch (error) {
      throw new Error(`Wallet initialization failed: ${error.message}`);
    }
  }

  // Connect to fabric network
  async connect(userId, organization) {
    try {
      const wallet = await Wallets.newFileSystemWallet(this.walletPath);
      
      // Check if user exists in wallet
      const userExists = await wallet.get(userId);
      if (!userExists) {
        const identities = await this.listIdentities();
        throw new Error(`User "${userId}" not found in wallet. Available identities: ${identities.join(', ')}`);
      }

      const gateway = new Gateway();
      
      const connectionOptions = {
        wallet,
        identity: userId,
        discovery: { 
          enabled: true, 
          asLocalhost: false 
        },
        eventHandlerOptions: {
          commitTimeout: 300,
          strategy: null
        }
      };

      await gateway.connect(this.connectionProfile, connectionOptions);

      const network = await gateway.getNetwork(process.env.CHANNEL_NAME || 'rawmaterialsupply');
      return { gateway, network };
    } catch (error) {
      throw new Error(`Failed to connect to Fabric network: ${error.message}`);
    }
  }

  // List available identities in wallet
  async listIdentities() {
    try {
      const wallet = await Wallets.newFileSystemWallet(this.walletPath);
      const identities = [];
      for await (const label of wallet.list()) {
        identities.push(label);
      }
      return identities;
    } catch (error) {
      console.error('Error listing identities:', error);
      return [];
    }
  }

  getOrgDomain(organization) {
    const domainMap = {
      'supplier': 'supplier.com',
      'manufacturer': 'manufacturer.com', 
      'distributor': 'distributor.com',
      'retailer': 'retailer.com',
      'drap': 'drap.com'
    };
    const domain = domainMap[organization.toLowerCase()];
    if (!domain) {
      throw new Error(`Unknown organization: ${organization}`);
    }
    return domain;
  }

  getMSPID(organization) {
    const mspMap = {
      'supplier': 'supplierMSP',
      'manufacturer': 'manufacturerMSP',
      'distributor': 'distributorMSP', 
      'retailer': 'retailerMSP',
      'drap': 'drapMSP'
    };
    const mspId = mspMap[organization.toLowerCase()];
    if (!mspId) {
      throw new Error(`Unknown organization MSP: ${organization}`);
    }
    return mspId;
  }

  async getContract(network, chaincodeName) {
    try {
      return network.getContract(chaincodeName);
    } catch (error) {
      throw new Error(`Failed to get contract ${chaincodeName}: ${error.message}`);
    }
  }

  async submitTransaction(contract, transactionName, ...args) {
    try {
      console.log(`Submitting transaction: ${transactionName} with args:`, args);
      const result = await contract.submitTransaction(transactionName, ...args);
      const resultString = result.toString();
      try {
        return JSON.parse(resultString);
      } catch (e) {
        return resultString;
      }
    } catch (error) {
      throw new Error(`Transaction ${transactionName} failed: ${error.message}`);
    }
  }

  async evaluateTransaction(contract, transactionName, ...args) {
    try {
      console.log(`Evaluating transaction: ${transactionName} with args:`, args);
      const result = await contract.evaluateTransaction(transactionName, ...args);
      const resultString = result.toString();
      try {
        return JSON.parse(resultString);
      } catch (e) {
        return resultString;
      }
    } catch (error) {
      throw new Error(`Query ${transactionName} failed: ${error.message}`);
    }
  }
}

module.exports = new FabricHelper();