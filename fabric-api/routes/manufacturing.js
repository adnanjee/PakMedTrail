const express = require('express');
const router = express.Router();
const fabricHelper = require('../fabric/fabricHelper');
const { validateFormulation, validateProduction } = require('../middleware/validation');

// Create Formulation
router.post('/formulations', validateFormulation, async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { drugCode, unit, requirements } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'CreateFormulation', 
      drugCode, unit, JSON.stringify(requirements)
    );

    await gateway.disconnect();
    res.status(201).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Read Formulation
router.get('/formulations/:drugCode', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { drugCode } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'ReadFormulation', 
      drugCode
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Produce Drug Batch
router.post('/batches/produce', validateProduction, async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { batchID, drugCode, outputQuantity, unit, inputs } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ProduceDrug', 
      batchID, drugCode, outputQuantity.toString(), unit, JSON.stringify(inputs)
    );

    await gateway.disconnect();
    res.status(201).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// DRAP Approval for Drug Batch
router.post('/batches/:batchID/drap-approve', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { batchID } = req.params;
    const { note } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ApproveDrugBatchByDRAP', 
      batchID, note || ''
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Propose Batch Transfer
router.post('/batches/:batchID/transfer', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { batchID } = req.params;
    const { proposedOwnerMSP } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ProposeBatchTransfer', 
      batchID, proposedOwnerMSP
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Read Drug Batch
router.get('/batches/:batchID', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { batchID } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'ReadBatch', 
      batchID
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get Batches by Owner
router.get('/batches/owner/:ownerMSP', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { ownerMSP } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'GetBatchesByOwner', 
      ownerMSP
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get Batches by Producer
router.get('/batches/producer/:producerMSP', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { producerMSP } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'GetBatchesByProducer', 
      producerMSP
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Expire All Pending Transfers
router.post('/batches/expire-pending-transfers', async (req, res) => {
  try {
    const { userId, organization } = req.headers;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    const result = await fabricHelper.submitTransaction(
      contract, 
      'ExpireAllPendingTransfers'
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;