const express = require('express');
const router = express.Router();
const fabricHelper = require('../fabric/fabricHelper');
const { validateLotCreate, validateTransfer } = require('../middleware/validation');

// Middleware to validate headers
const validateHeaders = (req, res, next) => {
  const { userId, organization } = req.headers;
  if (!userId || !organization) {
    return res.status(400).json({
      success: false,
      error: 'Missing required headers: userId and organization'
    });
  }
  next();
};

// Create API Lot
router.post('/lots', validateHeaders, validateLotCreate, async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { 
      lotID, 
      name, 
      batchNumber, 
      quantity, 
      unit, 
      manufactureDate, 
      expiryDate, 
      metadata 
    } = req.body;

    console.log(`Creating lot ${lotID} for organization ${organization} by user ${userId}`);

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'CreateLot', 
      lotID, name, batchNumber, quantity.toString(), unit, 
      manufactureDate, expiryDate, JSON.stringify(metadata || {})
    );

    await gateway.disconnect();
    res.status(201).json({ success: true, data: result });
  } catch (error) {
    console.error('Error creating lot:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Read API Lot
router.get('/lots/:lotID', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.evaluateTransaction(contract, 'ReadLot', lotID);
    
    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// DRAP Approval for API Lot
router.post('/lots/:lotID/drap-approve', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;
    const { note } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ApproveLotByDRAP', 
      lotID, note || ''
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Propose Transfer
router.post('/lots/:lotID/transfer', validateTransfer, async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;
    const { proposedOwnerMSP } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ProposeTransfer', 
      lotID, proposedOwnerMSP
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Accept Transfer
router.post('/lots/:lotID/transfer/accept', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'AcceptTransfer', 
      lotID
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Consume API Lot
router.post('/lots/:lotID/consume', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;
    const { amount } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'Consume', 
      lotID, amount.toString()
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get Lots by Owner
router.get('/lots/owner/:ownerMSP', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { ownerMSP } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'GetLotsByOwner', 
      ownerMSP
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get Pending DRAP Approval Lots
router.get('/lots/pending/drap-approval', async (req, res) => {
  try {
    const { userId, organization } = req.headers;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'GetLotsPendingDRAPApproval'
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;