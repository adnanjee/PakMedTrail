import { Router } from 'express';
import jwt from 'jsonwebtoken';
import { ALL_ORG_MSPS, OrgMSP } from '../config/orgConfig';

const router = Router();

function defaultRolesForOrg(orgMSP: OrgMSP): string[] {
  switch (orgMSP) {
    case 'supplierMSP':
      return ['SUPPLIER_USER'];
    case 'manufacturerMSP':
      return ['MANUFACTURER_USER'];
    case 'distributorMSP':
      return ['DISTRIBUTOR_USER'];
    case 'retailerMSP':
      return ['RETAIL_USER'];
    case 'drapMSP':
      return ['DRAP_OFFICER'];
    default:
      return [];
  }
}

router.post('/login', (req, res) => {
  const { username, orgMSP, roles } = req.body as {
    username?: string;
    orgMSP?: OrgMSP;
    roles?: string[];
  };

  if (!username || !orgMSP) {
    return res.status(400).json({ error: 'username and orgMSP are required' });
  }

  if (!ALL_ORG_MSPS.includes(orgMSP)) {
    return res.status(400).json({ error: 'Invalid orgMSP' });
  }

  const payload = {
    sub: username,
    orgMSP,
    roles: Array.isArray(roles) && roles.length ? roles : defaultRolesForOrg(orgMSP),
  };

  const token = jwt.sign(payload, process.env.JWT_SECRET || 'changeme', {
    expiresIn: '12h',
  });

  res.json({ token, user: payload });
});

export default router;
