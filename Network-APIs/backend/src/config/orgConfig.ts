export type OrgMSP =
  | 'supplierMSP'
  | 'manufacturerMSP'
  | 'distributorMSP'
  | 'retailerMSP'
  | 'drapMSP';

export const ALL_ORG_MSPS: OrgMSP[] = [
  'supplierMSP',
  'manufacturerMSP',
  'distributorMSP',
  'retailerMSP',
  'drapMSP',
];

const ORG_ENV_PREFIX: Record<OrgMSP, string> = {
  supplierMSP: 'SUPPLIER',
  manufacturerMSP: 'MANUFACTURER',
  distributorMSP: 'DISTRIBUTOR',
  retailerMSP: 'RETAILER',
  drapMSP: 'DRAP',
};

export interface OrgConnectionConfig {
  mspId: OrgMSP;
  peerEndpoint: string;
  tlsCertPath: string;
  certPath: string;
  keyPath: string;
}

export function getOrgConnectionConfig(mspId: OrgMSP): OrgConnectionConfig {
  const prefix = ORG_ENV_PREFIX[mspId];
  const peerEndpoint = process.env[`ORG_${prefix}_PEER_ENDPOINT`];
  const tlsCertPath = process.env[`ORG_${prefix}_TLS_CERT`];
  const certPath = process.env[`ORG_${prefix}_CERT`];
  const keyPath = process.env[`ORG_${prefix}_KEY`];

  if (!peerEndpoint || !tlsCertPath || !certPath || !keyPath) {
    throw new Error(
      `Missing Fabric config for org ${mspId}. Please set ORG_${prefix}_PEER_ENDPOINT, ORG_${prefix}_TLS_CERT, ORG_${prefix}_CERT, ORG_${prefix}_KEY in .env`,
    );
  }

  return {
    mspId,
    peerEndpoint,
    tlsCertPath,
    certPath,
    keyPath,
  };
}
