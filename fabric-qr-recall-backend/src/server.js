const express = require('express');
const bodyParser = require('body-parser');
const morgan = require('morgan');
const cors = require('cors');
require('dotenv').config();

const { generateQrForAsset } = require('./qrService');
const { verifyQrPayload } = require('./verifyService');

const app = express();

app.use(cors());
app.use(bodyParser.json());
app.use(morgan('dev'));

// Health check
app.get('/', (req, res) => {
  res.json({
    name: 'Fabric QR Recall Backend',
    status: 'ok',
    docs: {
      generateAssetQr: 'POST /api/qr/asset',
      generateBatchQr: 'POST /api/qr/batch/:batchId',
      verify: 'GET /api/verify?payload=MQR|BATCH|...'
    }
  });
});

// Generate QR for a generic asset
app.post('/api/qr/asset', async (req, res) => {
  const { assetType, assetId } = req.body || {};
  if (!assetType || !assetId) {
    return res.status(400).json({ error: 'assetType and assetId are required' });
  }

  try {
    const result = await generateQrForAsset(assetType, assetId);
    res.json(result);
  } catch (err) {
    console.error('Error generating asset QR:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// Convenience endpoint for batches
app.post('/api/qr/batch/:batchId', async (req, res) => {
  const { batchId } = req.params;
  try {
    const result = await generateQrForAsset('BATCH', batchId);
    res.json(result);
  } catch (err) {
    console.error('Error generating batch QR:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// Verify a scanned QR payload
app.get('/api/verify', async (req, res) => {
  const { payload } = req.query;
  if (!payload) {
    return res.status(400).json({ error: 'payload query parameter is required' });
  }

  try {
    const result = await verifyQrPayload(payload);
    res.json(result);
  } catch (err) {
    console.error('Error verifying QR:', err);
    res.status(400).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`QR backend listening on http://localhost:${PORT}`);
});
