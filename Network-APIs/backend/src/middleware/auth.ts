import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';

export interface AuthPayload {
  sub: string;
  orgMSP: string;
  roles: string[];
}

export interface AuthenticatedRequest extends Request {
  user?: AuthPayload;
}

export function requireAuth(req: AuthenticatedRequest, res: Response, next: NextFunction) {
  const header = req.headers.authorization;
  if (!header || !header.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing Authorization header' });
  }
  const token = header.substring(7);

  try {
    const payload = jwt.verify(token, process.env.JWT_SECRET || 'changeme') as AuthPayload;
    req.user = payload;
    next();
  } catch (err) {
    console.error('JWT error', err);
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

export function requireRoles(roles: string[]) {
  return (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
    const userRoles = req.user?.roles || [];
    const ok = roles.some((r) => userRoles.includes(r));
    if (!ok) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    next();
  };
}
