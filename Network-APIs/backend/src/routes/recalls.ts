import { Router } from 'express';
import { requireAuth, requireRoles, AuthenticatedRequest } from '../middleware/auth';
import { submitTx, evaluateTx } from '../fabric/fabricService';
import { OrgMSP } from '../config/orgConfig';

const router = Router();

const DIST_CC = process.env.FABRIC_CHAINCODE_DISTRIBUTION || 'distribution';

router.use(requireAuth);

// DRAP: create a recall notice
router.post('/', requireRoles(['DRAP_OFFICER']), async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const { recallId, batchId, reason } = req.body;
    if (!recallId || !batchId || !reason) {
      return res.status(400).json({ error: 'recallId, batchId, reason are required' });
    }

    const r = await submitTx<any>(
      orgMSP,
      DIST_CC,
      'InitiateRecallByDRAP',
      String(recallId),
      String(batchId),
      String(reason),
    );
    res.status(201).json(r);
  } catch (err) {
    next(err);
  }
});

// DRAP: close recall
router.post('/:recallId/close', requireRoles(['DRAP_OFFICER']), async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const recallId = req.params.recallId;
    const note = req.body.note || '';
    const r = await submitTx<any>(orgMSP, DIST_CC, 'CloseRecallByDRAP', recallId, String(note));
    res.json(r);
  } catch (err) {
    next(err);
  }
});

// Query recalls (optionally filter by status & batchId)
router.get('/', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const { status, batchId } = req.query as { status?: string; batchId?: string };

    const selector: any = {
      selector: {
        docType: 'ship.recall',
      },
    };

    if (status) {
      selector.selector.status = String(status);
    }
    if (batchId) {
      selector.selector.batchId = String(batchId);
    }

    const selectorJSON = JSON.stringify(selector);
    const recalls = await evaluateTx<any[]>(orgMSP, DIST_CC, 'QueryRecalls', selectorJSON);
    res.json(recalls || []);
  } catch (err) {
    next(err);
  }
});

// Recalls for a specific batch (activeOnly optional)
router.get('/batch/:batchId', async (req: AuthenticatedRequest, res, next) => {
  try {
    const orgMSP = req.user!.orgMSP as OrgMSP;
    const batchId = req.params.batchId;
    const activeOnly = String(req.query.activeOnly || 'true').toLowerCase() === 'true';

    const selector: any = {
      selector: {
        docType: 'ship.recall',
        batchId: batchId,
      },
    };
    if (activeOnly) {
      selector.selector.status = 'ACTIVE';
    }

    const selectorJSON = JSON.stringify(selector);
    const recalls = await evaluateTx<any[]>(orgMSP, DIST_CC, 'QueryRecalls', selectorJSON);
    res.json(recalls || []);
  } catch (err) {
    next(err);
  }
});

export default router;
