const express = require('express');
const router = express.Router();
const fabricHelper = require('../fabric/fabricHelper');

// Rich Query for API Lots
router.post('/raw-materials', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { selector, pageSize, bookmark } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    let result;
    if (pageSize && bookmark) {
      result = await fabricHelper.evaluateTransaction(
        contract, 
        'QueryLots', 
        JSON.stringify(selector), pageSize.toString(), bookmark
      );
    } else {
      result = await fabricHelper.evaluateTransaction(
        contract, 
        'QueryLots', 
        JSON.stringify(selector), '', ''
      );
    }

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Rich Query for Drug Batches
router.post('/drug-batches', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { selector, pageSize, bookmark } = req.body;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_MANUFACTURING);
    
    let result;
    if (pageSize && bookmark) {
      result = await fabricHelper.evaluateTransaction(
        contract, 
        'QueryBatchesPaged', 
        JSON.stringify(selector), pageSize.toString(), bookmark
      );
    } else {
      result = await fabricHelper.evaluateTransaction(
        contract, 
        'QueryBatches', 
        JSON.stringify(selector)
      );
    }

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get Lot History
router.get('/raw-materials/:lotID/history', async (req, res) => {
  try {
    const { userId, organization } = req.headers;
    const { lotID } = req.params;

    const { gateway, network } = await fabricHelper.connect(userId, organization);
    const contract = await fabricHelper.getContract(network, process.env.CHAINCODE_APITRANSFER);
    
    const result = await fabricHelper.evaluateTransaction(
      contract, 
      'GetHistory', 
      lotID
    );

    await gateway.disconnect();
    res.status(200).json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;