import { Router } from 'express';
import { requireAuth, requireRoles, AuthenticatedRequest } from '../middleware/auth';
import { submitTx, evaluateTx } from '../fabric/fabricService';
import { OrgMSP } from '../config/orgConfig';

const router = Router();

const LOTS_CC = process.env.FABRIC_CHAINCODE_LOTS || 'apitransfer';

router.use(requireAuth);

// List lots (defaults to lots owned by current org unless DRAP)
router.get('/', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const query = req.query as Record<string, string | undefined>;

    const name = query.name || '';
    const status = query.status || '';
    let ownerMSP = query.ownerMSP || '';

    if (!ownerMSP && orgMSP !== 'drapMSP') {
      ownerMSP = orgMSP;
    }

    // This assumes your API lot contract has QueryLotsBy(name, status, ownerMSP)
    const lots = await evaluateTx<any[]>(orgMSP, LOTS_CC, 'QueryLotsBy', String(name || ''), String(status || ''), String(ownerMSP || ''));
    res.json(lots || []);
  } catch (err) {
    next(err);
  }
});

// Get a single lot
router.get('/:lotId', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const lot = await evaluateTx<any>(orgMSP, LOTS_CC, 'ReadLot', lotId);
    if (!lot) {
      return res.status(404).json({ error: 'Lot not found' });
    }
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// Create lot (supplier only)
router.post(
  '/',
  requireRoles(['SUPPLIER_USER', 'SUPPLIER_ADMIN']),
  async (req: AuthenticatedRequest, res, next) => {
    try {
      const orgMSP = req.user!.orgMSP as OrgMSP;
      const { lotId, name, batchNumber, quantity, unit, manufactureDate, expiryDate, metadata } = req.body;

      if (!lotId || !name || !batchNumber || quantity === undefined || !unit || !manufactureDate || !expiryDate) {
        return res.status(400).json({ error: 'Missing required fields' });
      }

      const lot = await submitTx<any>(
        orgMSP,
        LOTS_CC,
        'CreateLot',
        String(lotId),
        String(name),
        String(batchNumber),
        String(quantity),
        String(unit),
        String(manufactureDate),
        String(expiryDate),
        JSON.stringify(metadata || {}),
      );

      res.status(201).json(lot);
    } catch (err) {
      next(err);
    }
  },
);

// Approve lot (DRAP)
router.post(
  '/:lotId/drap/approve',
  requireRoles(['DRAP_OFFICER']),
  async (req: AuthenticatedRequest, res, next) => {
    try {
      const orgMSP = req.user!.orgMSP as OrgMSP;
      const lotId = req.params.lotId;
      const note = req.body.note || '';
      const lot = await submitTx<any>(orgMSP, LOTS_CC, 'ApproveLotByDRAP', lotId, String(note));
      res.json(lot);
    } catch (err) {
      next(err);
    }
  },
);

// Reject lot (DRAP)
router.post(
  '/:lotId/drap/reject',
  requireRoles(['DRAP_OFFICER']),
  async (req: AuthenticatedRequest, res, next) => {
    try {
      const orgMSP = req.user!.orgMSP as OrgMSP;
      const lotId = req.params.lotId;
      const reason = req.body.reason || '';
      const lot = await submitTx<any>(orgMSP, LOTS_CC, 'RejectLotByDRAP', lotId, String(reason));
      res.json(lot);
    } catch (err) {
      next(err);
    }
  },
);

// Get lots pending DRAP approval
router.get(
  '/pending-drap',
  requireRoles(['DRAP_OFFICER']),
  async (req: AuthenticatedRequest, res, next) => {
    try {
      const orgMSP = req.user!.orgMSP as OrgMSP;
      const lots = await evaluateTx<any[]>(orgMSP, LOTS_CC, 'GetLotsPendingDRAPApproval');
      res.json(lots || []);
    } catch (err) {
      next(err);
    }
  },
);

// Propose transfer (owner)
router.post('/:lotId/propose-transfer', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const { proposedOwnerMSP } = req.body;
    if (!proposedOwnerMSP) {
      return res.status(400).json({ error: 'proposedOwnerMSP is required' });
    }
    const lot = await submitTx<any>(orgMSP, LOTS_CC, 'ProposeTransfer', lotId, String(proposedOwnerMSP));
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// Accept transfer (proposed owner)
router.post('/:lotId/accept-transfer', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const lot = await submitTx<any>(orgMSP, LOTS_CC, 'AcceptTransfer', lotId);
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// Reject transfer
router.post('/:lotId/reject-transfer', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const reason = req.body.reason || '';
    const lot = await submitTx<any>(orgMSP, LOTS_CC, 'RejectTransfer', lotId, String(reason));
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// Consume quantity (owner, DRAP-approved)
router.post('/:lotId/consume', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const { amount } = req.body;
    if (amount === undefined) {
      return res.status(400).json({ error: 'amount is required' });
    }
    const lot = await submitTx<any>(orgMSP, LOTS_CC, 'Consume', lotId, String(amount));
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// Destroy lot (owner)
router.post('/:lotId/destroy', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const lot = await submitTx<any>(orgMSP, LOTS_CC, 'Destroy', lotId);
    res.json(lot);
  } catch (err) {
    next(err);
  }
});

// History
router.get('/:lotId/history', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const lotId = req.params.lotId;
    const hist = await evaluateTx<any[]>(orgMSP, LOTS_CC, 'GetHistory', lotId);
    res.json(hist || []);
  } catch (err) {
    next(err);
  }
});

export default router;
