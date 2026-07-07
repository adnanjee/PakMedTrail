const QRCode = require('qrcode');
const { evaluateDomain } = require('./fabricClient');
const { parseQrPayload, buildQrPayload } = require('./verifyService');

async function generateQrDataUrl(payload) {
  return QRCode.toDataURL(payload, {
    errorCorrectionLevel: 'M',
    margin: 2,
    scale: 6
  });
}

async function generateQrForAsset(assetType, assetId) {
  // Optional: verify that the asset exists by evaluating a domain chaincode function.
  // You MUST adjust the function names here to match your chaincode.
  // Example mapping assumes functions: ReadBatch, ReadShipment, ReadRetailUnit.
  let readFn;
  switch (assetType) {
    case 'BATCH':
      readFn = 'ReadBatch';
      break;
    case 'SHIPMENT':
      readFn = 'ReadShipment';
      break;
    case 'RETAIL_UNIT':
      readFn = 'ReadRetailUnit';
      break;
    default:
      throw new Error(`Unsupported assetType for QR generation: ${assetType}`);
  }

  const assetJson = await evaluateDomain(readFn, assetId);
  if (!assetJson) {
    throw new Error(`Asset not found on ledger for ${assetType}:${assetId}`);
  }

  const asset = JSON.parse(assetJson);

  const payload = buildQrPayload(assetType, assetId);
  const qrImageDataUrl = await generateQrDataUrl(payload);

  return {
    assetType,
    assetId,
    payload,
    qrImageDataUrl,
    ledgerData: asset
  };
}

module.exports = {
  generateQrForAsset
};
