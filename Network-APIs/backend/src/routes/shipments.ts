import { Router } from 'express';
import { requireAuth, AuthenticatedRequest } from '../middleware/auth';
import { submitTx, evaluateTx } from '../fabric/fabricService';
import { OrgMSP } from '../config/orgConfig';

const router = Router();

const DIST_CC = process.env.FABRIC_CHAINCODE_DISTRIBUTION || 'distribution';

router.use(requireAuth);

// List shipments for a party (defaults to caller orgMSP)
router.get('/', async (req: AuthenticatedRequest, res, next) => {
  try {
    const callerMSP = req.user!.orgMSP as OrgMSP;
    const partyMSP = (req.query.partyMSP as string) || callerMSP;
    const shipments = await evaluateTx<any[]>(callerMSP, DIST_CC, 'GetShipmentsByParty', String(partyMSP));
    res.json(shipments || []);
  } catch (err) {
    next(err);
  }
});

// Read a single shipment
router.get('/:shipmentId', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const s = await evaluateTx<any>(orgMSP, DIST_CC, 'ReadShipment', shipmentId);
    if (!s) {
      return res.status(404).json({ error: 'Shipment not found' });
    }
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Create a shipment offer (manufacturer)
router.post('/', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const { shipmentId, batchId, toMSP, quantity, metadata } = req.body;
    if (!shipmentId || !batchId || !toMSP || quantity === undefined) {
      return res.status(400).json({ error: 'shipmentId, batchId, toMSP, quantity required' });
    }

    const md = JSON.stringify(metadata || {});
    const s = await submitTx<any>(
      orgMSP,
      DIST_CC,
      'CreateShipmentOffer',
      String(shipmentId),
      String(batchId),
      String(toMSP),
      String(quantity),
      md,
    );
    res.status(201).json(s);
  } catch (err) {
    next(err);
  }
});

// Accept shipment (proposed receiver)
router.post('/:shipmentId/accept', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const s = await submitTx<any>(orgMSP, DIST_CC, 'AcceptShipment', shipmentId);
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Reject shipment (proposed receiver)
router.post('/:shipmentId/reject', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const reason = req.body.reason || '';
    const s = await submitTx<any>(orgMSP, DIST_CC, 'RejectShipment', shipmentId, String(reason));
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Cancel shipment (sender)
router.post('/:shipmentId/cancel', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const reason = req.body.reason || '';
    const s = await submitTx<any>(orgMSP, DIST_CC, 'CancelShipment', shipmentId, String(reason));
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Mark delivered (receiver confirms physical arrival)
router.post('/:shipmentId/delivered', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const s = await submitTx<any>(orgMSP, DIST_CC, 'MarkDelivered', shipmentId);
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Put sensitive commercial terms (PDC)
router.put('/:shipmentId/terms', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const { priceAmt, currency, discount, incoterms, notes } = req.body;

    const hasPrice = priceAmt !== undefined && priceAmt !== null && String(priceAmt).trim() !== '';
    const hasDiscount = discount !== undefined && discount !== null && String(discount).trim() !== '';

    const priceAmtStr = hasPrice ? String(priceAmt) : '';
    const hasPriceStr = hasPrice ? 'true' : 'false';

    const discountStr = hasDiscount ? String(discount) : '';
    const hasDiscountStr = hasDiscount ? 'true' : 'false';

    const rec = await submitTx<any>(
      orgMSP,
      DIST_CC,
      'PutSensitive',
      shipmentId,
      priceAmtStr,
      String(currency || ''),
      hasPriceStr,
      discountStr,
      hasDiscountStr,
      String(incoterms || ''),
      String(notes || ''),
    );

    res.json(rec);
  } catch (err) {
    next(err);
  }
});

// Read sensitive commercial terms (PDC)
router.get('/:shipmentId/terms', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const rec = await evaluateTx<any>(orgMSP, DIST_CC, 'ReadSensitive', shipmentId);
    res.json(rec);
  } catch (err) {
    next(err);
  }
});

// Link sensitive hash into public metadata
router.post('/:shipmentId/terms/hash', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const s = await submitTx<any>(orgMSP, DIST_CC, 'LinkSensitiveHash', shipmentId);
    res.json(s);
  } catch (err) {
    next(err);
  }
});

// Quarantine shipment by recall (current owner)
router.post('/:shipmentId/quarantine', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const shipmentId = req.params.shipmentId;
    const s = await submitTx<any>(orgMSP, DIST_CC, 'QuarantineByRecall', shipmentId);
    res.json(s);
  } catch (err) {
    next(err);
  }
});

export default router;
