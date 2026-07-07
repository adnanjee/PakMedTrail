const { ethers } = require("ethers");

const ERC20_ABI = [
  "function transfer(address to, uint256 amount) returns (bool)",
  "function balanceOf(address) view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function symbol() view returns (string)",
];

// Thin wrapper over ethers for the RSK side: send value, then watch the tx
// until it reaches the required confirmation depth. The confirmation watch is
// the lightweight SPV style check the report describes: we trust block depth on
// the RSK node rather than running a full node in every org.
class RskClient {
  constructor({ rpcUrl, chainId, confirmations, minGasPriceGwei, logger }) {
    const network = chainId ? { chainId, name: "rsk" } : undefined;
    this.provider = new ethers.JsonRpcProvider(rpcUrl, network);
    this.confirmations = confirmations || 1;
    this.minGasPriceGwei = minGasPriceGwei || "0.06";
    this.log = logger || console;
  }

  wallet(privateKey) {
    return new ethers.Wallet(privateKey, this.provider);
  }

  async gasPrice() {
    const fd = await this.provider.getFeeData();
    if (fd.gasPrice && fd.gasPrice > 0n) return fd.gasPrice;
    return ethers.parseUnits(String(this.minGasPriceGwei), "gwei");
  }

  // native RBTC transfer
  async sendNative({ privateKey, to, amount }) {
    const w = this.wallet(privateKey);
    const tx = await w.sendTransaction({
      to,
      value: ethers.parseEther(amount),
      gasPrice: await this.gasPrice(),
    });
    return tx.hash;
  }

  // ERC20 transfer (the PST settlement token path)
  async sendToken({ privateKey, tokenContract, to, amount, decimals }) {
    const w = this.wallet(privateKey);
    const token = new ethers.Contract(tokenContract, ERC20_ABI, w);
    const dec = decimals != null && decimals !== "" ? Number(decimals) : Number(await token.decimals());
    const value = ethers.parseUnits(String(amount), dec);
    const tx = await token.transfer(to, value, { gasPrice: await this.gasPrice() });
    return tx.hash;
  }

  async getReceipt(hash) {
    return this.provider.getTransactionReceipt(hash);
  }

  async balanceOfToken(tokenContract, address) {
    const token = new ethers.Contract(tokenContract, ERC20_ABI, this.provider);
    return token.balanceOf(address);
  }

  // Poll until the tx is at least `confirmations` deep. Throws on revert or timeout.
  async waitForConfirmations(hash, opts = {}) {
    const needed = opts.confirmations || this.confirmations;
    const timeoutMs = opts.timeoutMs || 120000;
    const pollMs = opts.pollMs || 3000;
    const start = Date.now();

    for (;;) {
      const r = await this.getReceipt(hash);
      if (r) {
        if (r.status === 0) throw new Error(`tx ${hash} reverted on chain`);
        const head = await this.provider.getBlockNumber();
        const depth = head - r.blockNumber + 1;
        if (depth >= needed) return { receipt: r, confirmations: depth };
      }
      if (Date.now() - start > timeoutMs) {
        throw new Error(`timeout waiting for ${needed} confirmations of ${hash}`);
      }
      await new Promise((res) => setTimeout(res, pollMs));
    }
  }
}

module.exports = { RskClient, ERC20_ABI };
