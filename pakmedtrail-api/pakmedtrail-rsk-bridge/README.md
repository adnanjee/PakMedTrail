# PakMedTrail RSK Bridge

Off-chain service that settles supply chain payments on RSK and writes the
result back to the Fabric `payment-intent` chaincode.

## What it does

1. Listens for `PaymentCreated` events from the payment chaincode.
2. Reads the payer org, payee address, amount, and token from the intent.
3. Sends the value on RSK (PST token transfer, or native RBTC if no token is set).
4. Calls `MarkPaymentSent` with the real tx hash.
5. Watches the tx until it reaches the confirmation depth, then calls `MarkPaymentConfirmed`.
6. On any send or confirm failure, calls `MarkPaymentFailed` with the reason.

It also exposes a small HTTP API so payments can be driven from Postman, which
the rest of the gateway did not expose.

## Identity model (read this first)

The chaincode only lets the payer or payee MSP call the mark functions. This
service uses Path A: it submits the mark calls **as the payer org**, reusing the
same org identities your gateway already holds. No chaincode change is needed.

The trade off: the service must hold a signing identity for every org that can
pay. If you prefer one bridge identity, add a dedicated BridgeMSP to the allow
check in `payment-intent-go`, redeploy, and point this service at that one
identity. That is Path B.

One more deployment detail. The payment chaincode endorsement policy must let
the payer org collect the endorsements it needs to submit. Confirm this on your
network before you expect submits to commit.

## Layout

```
contracts/PharmaSettlementToken.sol   PST token (minimal ERC20, London EVM target)
src/rskClient.js                      ethers wrapper: send + confirmation watch
src/bridge.js                         orchestrator: PENDING -> SENT -> CONFIRMED
src/ledger/fabricLedger.js            real Fabric Gateway calls + event listener
src/ledger/fakeLedger.js              in-memory mirror for the local test
src/server.js                         HTTP API for payments
src/index.js                          wiring and startup
scripts/generate-wallets.js           one RSK wallet per org MSP
scripts/deploy-token.js               deploy PST and distribute to org wallets
test/run-local.js                     end to end proof on a local EVM
```

## Local proof (no testnet, no Fabric)

This runs the real bridge and RSK client against a local EVM, with the Fabric
side stubbed by a fake that copies the chaincode rules.

```
npm install
npm run test:local
```

Expected tail: `ALL CHECKS PASSED`.

## Wiring to RSK testnet and your Fabric network

This part runs on your machine, since this sandbox cannot reach RSK or your peers.

1. Copy the env file and fill it in.
   ```
   cp .env.example .env
   ```

2. Make one wallet per org.
   ```
   npm run wallets
   ```
   This writes `rsk-wallets.json`. Keep it secret. Fund each address with
   testnet RBTC for gas: https://faucet.rootstock.io/

3. Set `RSK_DEPLOYER_PK` in `.env` to a funded testnet key, then deploy the token.
   ```
   npm run deploy:token
   ```
   Copy the printed address into `PST_TOKEN_ADDRESS` in `.env`. Use that same
   address as `tokenContract` when you create payment intents.

4. Point the service at your peers.
   ```
   cp fabric-orgs.example.json fabric-orgs.json
   ```
   Fill in the real peer endpoint, host alias, TLS root cert path, signcert
   path, and key path for each org that can pay. Use the same identities your
   gateway already uses.

5. Start the bridge.
   ```
   npm start
   ```

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| GET  | /health | liveness |
| POST | /api/payments | create a PENDING intent (the listener settles it) |
| GET  | /api/payments/:id | read one intent |
| GET  | /api/payments?status=SENT | list by status |
| POST | /api/payments/:id/settle | force settle now |
| POST | /api/payments/:id/cancel | cancel a PENDING or FAILED intent |

The payer org comes from the `x-org-msp` header for now. Swap that for your JWT
middleware so the org is taken from the verified token in production.

## Evidence to capture for the report

Run one real payment on testnet, then save:
- the PST token address on the testnet explorer,
- the settlement tx hash on the explorer,
- the chaincode intent showing status CONFIRMED with that tx hash.

That triple is what turns the claim into proof.

## Scope note

This is a testnet prototype, not a production deployment. The token is a minimal
ERC20 for settlement credit, not an audited contract. Treat it as such.
