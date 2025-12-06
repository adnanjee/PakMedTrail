const { getNetworkAndContracts, evaluateRecall } = require('./fabricClient');

function parseQrPayload(payload) {
  if (!payload) {
    throw new Error('Empty QR payload');
  }
  const parts = payload.split('|');
  if (parts.length < 3 || parts[0] !== 'MQR') {
    throw new Error('Invalid QR payload format');
  }
  const assetType = parts[1];
  const assetId = parts.slice(2).join('|'); // in case assetId itself contains '|'
  return { assetType, assetId };
}

function buildQrPayload(assetType, assetId) {
  if (!assetType || !assetId) {
    throw new Error('assetType and assetId are required to build QR payload');
  }
  return `MQR|${assetType}|${assetId}`;
}

function buildVerdict(assetType, asset, underRecall, quarantine) {
  if (!asset) {
    return {
      status: 'UNKNOWN',
      message: 'Asset not found on ledger; product may be counterfeit or unregistered.'
    };
  }

  if (underRecall) {
    return {
      status: 'RECALLED',
      message: 'This asset is under an ACTIVE recall. Do not dispense or use this product.'
    };
  }

  if (quarantine && quarantine.status === 'ON') {
    return {
      status: 'QUARANTINED',
      message: 'This asset is quarantined. Follow internal quarantine procedures.'
    };
  }

  return {
    status: 'OK',
    message: 'No active recall or quarantine detected for this asset.'
  };
}

async function verifyQrPayload(payload) {
  const { assetType, assetId } = parseQrPayload(payload);
  const { gateway, domainContract, recallContract } = await getNetworkAndContracts();

  try {
    // 1. Read asset from domain chaincode.
    // Adjust function name selection according to your domain chaincodes.
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
        throw new Error(`Unsupported assetType in QR payload: ${assetType}`);
    }

    let asset = null;
    try {
      const assetBuf = await domainContract.evaluateTransaction(readFn, assetId);
      const assetStr = assetBuf.toString();
      if (assetStr) {
        asset = JSON.parse(assetStr);
      }
    } catch (e) {
      // Asset not found or error; keep asset = null.
    }

    // 2. Check recall flag.
    let underRecall = false;
    try {
      const recallFlagBuf = await recallContract.evaluateTransaction(
        'IsAssetUnderActiveRecall',
        assetType,
        assetId
      );
      underRecall = (recallFlagBuf.toString() === 'true');
    } catch (e) {
      // If recallContract throws, assume not under recall but log.
      console.error('Error calling IsAssetUnderActiveRecall:', e.message || e);
    }

    // 3. Check quarantine record.
    let quarantine = null;
    try {
      const qBuf = await recallContract.evaluateTransaction(
        'GetQuarantine',
        assetType,
        assetId
      );
      const qStr = qBuf.toString();
      if (qStr) {
        quarantine = JSON.parse(qStr);
      }
    } catch (e) {
      // no quarantine record or error; ignore
    }

    // 4. Build verdict.
    const verdict = buildVerdict(assetType, asset, underRecall, quarantine);

    return {
      payload,
      assetType,
      assetId,
      asset,
      underRecall,
      quarantine,
      verdict
    };
  } finally {
    gateway.close();
  }
}

module.exports = {
  parseQrPayload,
  buildQrPayload,
  verifyQrPayload,
  buildVerdict
};
