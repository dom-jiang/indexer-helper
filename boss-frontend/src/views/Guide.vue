<template>
  <div class="guide-page">
    <h2>API Usage Guide</h2>

    <el-card class="guide-section">
      <template #header><strong>1. Getting Started</strong></template>
      <div class="guide-content">
        <p>To use the Swap API, you need a <strong>JWT Token</strong> for authentication. Follow these steps:</p>
        <ol>
          <li>Go to <el-link type="primary" @click="$router.push('/dashboard')">Dashboard</el-link> and click <strong>+ New Key</strong> to create an API key.</li>
          <li>A JWT Token (valid for 30 days) will be automatically generated and displayed.</li>
          <li>Copy and securely store the JWT Token — it is your credential for API access.</li>
          <li>When your token expires, go to the key's Detail page and click <strong>Generate JWT Token</strong> to get a new one.</li>
        </ol>
        <el-alert type="info" :closable="false" style="margin-top: 12px">
          <strong>Important:</strong> If you click "Reset Secret", all previously generated JWT tokens for that key will be invalidated immediately.
        </el-alert>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>2. Authentication</strong></template>
      <div class="guide-content">
        <p>All Swap API requests require a JWT Token in the <code>Authorization</code> header:</p>
        <div class="code-block">
          <pre>Authorization: Bearer &lt;your_jwt_token&gt;</pre>
        </div>
        <p>If the token is missing, invalid, or expired, the API will return a <code>401</code> error.</p>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>3. API Endpoints</strong></template>
      <div class="guide-content">
        <el-table :data="endpoints" border size="small" style="width: 100%">
          <el-table-column prop="method" label="Method" width="80" />
          <el-table-column prop="path" label="Endpoint" min-width="240">
            <template #default="{ row }"><code>{{ row.path }}</code></template>
          </el-table-column>
          <el-table-column prop="group" label="Group" width="80" />
          <el-table-column prop="description" label="Description" min-width="280" />
        </el-table>
        <el-alert type="warning" :closable="false" style="margin-top: 12px">
          <strong>Note:</strong> The <code>quote</code> and <code>swap</code> endpoints are unified — they handle both same-chain and cross-chain swaps automatically based on <code>fromChain</code> and <code>toChain</code>.
        </el-alert>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>4. Rate Limits</strong></template>
      <div class="guide-content">
        <p>API requests are rate-limited per key on two dimensions:</p>
        <el-table :data="rateLimits" border size="small" style="width: 100%; max-width: 500px;">
          <el-table-column prop="group" label="Endpoint Group" width="150" />
          <el-table-column prop="perMinute" label="Per Minute" />
          <el-table-column prop="perMonth" label="Per Month" />
        </el-table>
        <p style="margin-top: 12px; color: #909399; font-size: 13px;">
          When a rate limit is exceeded, the API returns a <code>429</code> status code. Contact the admin if you need higher limits.
        </p>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>5. Request / Response Examples</strong></template>
      <div class="guide-content">
        <h4>Same-chain Quote (fromChain == toChain)</h4>
        <div class="code-block">
          <pre>curl -X POST {{ baseUrl }}/api/swap/quote \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "fromChain": "56",
    "toChain": "56",
    "tokenIn": "0x55d398326f99059fF775485246999027B3197955",
    "tokenOut": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    "amountIn": "1000000000000000000",
    "slippage": 0.5,
    "sender": "0xYourAddress"
  }'</pre>
        </div>

        <h4 style="margin-top: 20px;">Cross-chain Quote (fromChain != toChain)</h4>
        <div class="code-block">
          <pre>curl -X POST {{ baseUrl }}/api/swap/quote \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "fromChain": "1",
    "toChain": "42161",
    "tokenIn": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "tokenOut": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "amountIn": "1000000",
    "slippage": 0.5,
    "sender": "0xYourAddress",
    "recipient": "0xYourArbitrumAddress"
  }'</pre>
        </div>

        <h4 style="margin-top: 20px;">Build Swap Transaction</h4>
        <div class="code-block">
          <pre>curl -X POST {{ baseUrl }}/api/swap/swap \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "fromChain": "56",
    "toChain": "56",
    "tokenIn": "0x55d398326f99059fF775485246999027B3197955",
    "tokenOut": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    "amountIn": "1000000000000000000",
    "slippage": 0.5,
    "sender": "0xYourAddress",
    "router": "okx"
  }'</pre>
        </div>

        <h4 style="margin-top: 20px;">JavaScript (fetch)</h4>
        <div class="code-block">
          <pre>const JWT_TOKEN = "YOUR_JWT_TOKEN";

// Step 1: Get quote
const quoteRes = await fetch("{{ baseUrl }}/api/swap/quote", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${JWT_TOKEN}`
  },
  body: JSON.stringify({
    fromChain: "56",
    toChain: "56",
    tokenIn: "0x55d398326f99059fF775485246999027B3197955",
    tokenOut: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    amountIn: "1000000000000000000",
    slippage: 0.5,
    sender: "0xYourAddress"
  })
});
const quote = await quoteRes.json();
const router = quote.data.bestQuote.router;

// Step 2: Build swap tx (includes approve info if needed)
const swapRes = await fetch("{{ baseUrl }}/api/swap/swap", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${JWT_TOKEN}`
  },
  body: JSON.stringify({
    fromChain: "56",
    toChain: "56",
    tokenIn: "0x55d398326f99059fF775485246999027B3197955",
    tokenOut: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    amountIn: "1000000000000000000",
    slippage: 0.5,
    sender: "0xYourAddress",
    router: router,
    market: quote.data.bestQuote.market
  })
});
const swapData = await swapRes.json();

// Step 3: If approve tx is returned, sign and send it first
if (swapData.data.approve) {
  // Sign and send approve tx via wallet
}
// Then sign and send the swap tx
console.log(swapData.data.tx);</pre>
        </div>

        <h4 style="margin-top: 20px;">Python (requests)</h4>
        <div class="code-block">
          <pre>import requests

JWT_TOKEN = "YOUR_JWT_TOKEN"
BASE_URL = "{{ baseUrl }}"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {JWT_TOKEN}"
}

# Step 1: Quote
quote_resp = requests.post(f"{BASE_URL}/api/swap/quote", headers=HEADERS, json={
    "fromChain": "56",
    "toChain": "56",
    "tokenIn": "0x55d398326f99059fF775485246999027B3197955",
    "tokenOut": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    "amountIn": "1000000000000000000",
    "slippage": 0.5,
    "sender": "0xYourAddress"
})
quote = quote_resp.json()
router = quote["data"]["bestQuote"]["router"]

# Step 2: Swap (build tx)
swap_resp = requests.post(f"{BASE_URL}/api/swap/swap", headers=HEADERS, json={
    "fromChain": "56",
    "toChain": "56",
    "tokenIn": "0x55d398326f99059fF775485246999027B3197955",
    "tokenOut": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    "amountIn": "1000000000000000000",
    "slippage": 0.5,
    "sender": "0xYourAddress",
    "router": router,
    "market": quote["data"]["bestQuote"].get("market", "")
})
print(swap_resp.json())</pre>
        </div>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>5.1 Response Shape of /api/swap/swap</strong></template>
      <div class="guide-content">
        <p>The <code>data</code> field of <code>/api/swap/swap</code> uses the <strong>same outer shape</strong> for both same-chain and cross-chain, and the <code>data.tx</code> field is <strong>consistent per source chain type</strong>:</p>
        <div class="code-block">
          <pre>{
  "code": 0,
  "msg": "success",
  "data": {
    "isCrossChain": false | true,
    "chainType": "evm" | "solana" | "aptos",      // source chain type
    "router": "okx | bitget | jupiter | panora | omnibridge | nearintents",
    "fromChain": "56",
    "toChain":   "42161",
    "tokenIn":  { "address", "symbol", "decimals" },
    "tokenOut": { "address", "symbol", "decimals" },
    "amountIn": "1000000",
    "estimatedOut":  "999898",
    "minAmountOut":  "994898",
    "tx":      <source-chain-specific, see below>,
    "approve": null | { "tx": {...}, "spender": "0x..." },    // EVM same-chain only
    "deposit": null | {                                        // only for cross-chain
      "depositAddress": "0x...",
      "depositMemo":    "",
      "depositChain":   "56",
      "orderId":        "...",
      "estimatedOut":   "...",
      "minAmountOut":   "...",
      "timeEstimate":   35
    }
  }
}</pre>
        </div>

        <h4 style="margin-top: 16px;">EVM — <code>data.tx</code> shape (same for same-chain & cross-chain)</h4>
        <div class="code-block">
          <pre>{
  "to":       "0x...",            // DEX router (same-chain) or token / deposit address (cross-chain)
  "data":     "0x...",            // calldata (empty "0x" for native transfer)
  "value":    "0x0" | "0x<hex>",  // native amount in hex
  "gasLimit": "0x...",
  "chainId":  56
}</pre>
        </div>

        <h4 style="margin-top: 16px;">Aptos — <code>data.tx</code> shape (Move entry function)</h4>
        <div class="code-block">
          <pre>{
  "function":       "0x1::aptos_account::transfer_coins",   // or Panora's swap function
  "type_arguments": ["<CoinType>"],
  "arguments":      ["<recipient>", "<amount>"]
}</pre>
        </div>

        <h4 style="margin-top: 16px;">Solana — <code>data.tx</code> shape</h4>
        <p>Both same-chain and cross-chain return the same top-level keys. Dispatch on <code>tx.format</code>:</p>
        <div class="code-block">
          <pre>// Same-chain (Jupiter / OKX) — pre-built base64 transaction
{ "transaction": "&lt;base64&gt;", "format": "base64" }

// Cross-chain — descriptor, frontend builds SPL/SOL transfer with @solana/web3.js
// SOL (native):
{ "transaction": "", "format": "sol_transfer",
  "depositAddress": "...", "amount": "...", "decimals": 9, "depositMemo": "" }
// SPL token:
{ "transaction": "", "format": "spl_transfer",
  "depositAddress": "...", "mint": "...",
  "amount": "...", "decimals": 6, "depositMemo": "" }</pre>
        </div>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>5.2 Report Signed Tx & Query History</strong></template>
      <div class="guide-content">
        <p>After the user signs and broadcasts a swap, report the source-chain tx hash to the backend so it can be stored and (for cross-chain) polled for settlement status.</p>

        <h4>POST /api/swap/report</h4>
        <div class="code-block">
          <pre>curl -X POST {{ baseUrl }}/api/swap/report \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "sender":          "0xYourAddress",
    "from_hash":       "0xsourceChainTxHash",
    "from_token":      "0x...",
    "to_token":        "0x...",
    "deposit_address": "0x...",            // "" for same-chain
    "from_chain":      "56",
    "to_chain":        "42161",
    "amount_in":       "1000000",
    "estimated_out":   "999898",
    "router":          "nearintents",
    "multi_addr":      "optional",
    "swap_id":         "optional",
    "extra":           { "any": "frontend context" }
  }'</pre>
        </div>
        <p><strong>Required:</strong> <code>sender</code>, <code>from_hash</code>, <code>from_token</code>, <code>to_token</code>.
        The call is <strong>idempotent on <code>from_hash</code></strong>: re-reporting the same hash returns the existing record id.
        <code>deposit_address</code> is required for cross-chain (drives backend polling).</p>

        <h4 style="margin-top: 16px;">GET /api/swap/history</h4>
        <div class="code-block">
          <pre>curl -G {{ baseUrl }}/api/swap/history \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  --data-urlencode "sender=0xYourAddress" \
  --data-urlencode "pageNumber=1" \
  --data-urlencode "pageSize=20"</pre>
        </div>
        <p>Returns newest-first paginated records for the given <code>sender</code>. For cross-chain rows, a background job
        updates <code>status</code>, <code>to_hash</code>, and <code>actual_out</code> over time until a terminal state
        (<code>SUCCESS</code> / <code>FAILED</code> / <code>REFUNDED</code> / <code>EXPIRED</code>) is reached.</p>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>6. Typical Workflow</strong></template>
      <div class="guide-content">
        <div class="workflow-steps">
          <div class="step">
            <div class="step-number">1</div>
            <div class="step-body">
              <strong>Get Quote</strong>
              <p>Call <code>POST /api/swap/quote</code> with token pair, amount, and chain IDs. The response tells you which router has the best price and whether it's a same-chain or cross-chain swap.</p>
            </div>
          </div>
          <div class="step">
            <div class="step-number">2</div>
            <div class="step-body">
              <strong>Build Swap</strong>
              <p>Call <code>POST /api/swap/swap</code> with the router from the quote. The response includes the transaction data to sign, and <code>approve</code> data if token approval is needed (EVM same-chain only).</p>
            </div>
          </div>
          <div class="step">
            <div class="step-number">3</div>
            <div class="step-body">
              <strong>Approve (if needed)</strong>
              <p>If the swap response includes an <code>approve</code> field with a tx, sign and send the approval transaction first. For Solana, Aptos, and cross-chain swaps, this step is not needed.</p>
            </div>
          </div>
          <div class="step">
            <div class="step-number">4</div>
            <div class="step-body">
              <strong>Sign & Send</strong>
              <p><strong>Same-chain:</strong> Sign the <code>tx</code> data with your wallet and broadcast it.<br/>
              <strong>Cross-chain:</strong> Send funds to the <code>depositAddress</code> returned in the tx data. Optionally include the <code>depositMemo</code> if provided.</p>
            </div>
          </div>
          <div class="step">
            <div class="step-number">5</div>
            <div class="step-body">
              <strong>Report the Signed Tx</strong>
              <p>After your source-chain tx is broadcast, call <code>POST /api/swap/report</code> with <code>sender</code>, <code>from_hash</code>, <code>from_token</code>, <code>to_token</code>, and (for cross-chain) <code>deposit_address</code>. The backend persists the record and, if cross-chain, starts polling for settlement status.</p>
            </div>
          </div>
          <div class="step">
            <div class="step-number">6</div>
            <div class="step-body">
              <strong>Track History / Status</strong>
              <p>Use <code>GET /api/swap/history?sender=&lt;addr&gt;</code> to list the user's swaps (newest first). Cross-chain status fields — <code>status</code>, <code>to_hash</code>, <code>actual_out</code> — are refreshed by the backend until a terminal state is reached. You can still call <code>GET /api/swap/order-status</code> for an ad-hoc check against the provider.</p>
            </div>
          </div>
        </div>
      </div>
    </el-card>

    <el-card class="guide-section">
      <template #header><strong>7. Parameter Reference</strong></template>
      <div class="guide-content">
        <h4>POST /api/swap/quote & POST /api/swap/swap</h4>
        <el-table :data="paramDocs" border size="small" style="width: 100%">
          <el-table-column prop="param" label="Parameter" width="130">
            <template #default="{ row }"><code>{{ row.param }}</code></template>
          </el-table-column>
          <el-table-column prop="type" label="Type" width="80" />
          <el-table-column prop="required" label="Required" width="80" />
          <el-table-column prop="description" label="Description" min-width="300" />
        </el-table>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const baseUrl = computed(() => window.location.origin)

const endpoints = [
  { method: 'POST', path: '/api/swap/quote', group: 'Quote', description: 'Unified quote — same-chain & cross-chain, returns best price from multiple providers' },
  { method: 'POST', path: '/api/swap/swap', group: 'Build', description: 'Unified swap — build transaction data (per-source-chain unified shape, includes approve for EVM same-chain)' },
  { method: 'GET', path: '/api/swap/order-status', group: 'Quote', description: 'Cross-chain order status — query by orderId and router' },
  { method: 'POST', path: '/api/swap/report', group: 'Build', description: 'Report a user-signed swap tx so backend persists history and polls cross-chain status' },
  { method: 'GET', path: '/api/swap/history', group: 'Quote', description: "Query a sender's swap history (paginated, newest first)" },
]

const rateLimits = [
  { group: 'Quote', perMinute: '60 requests', perMonth: '300,000 requests' },
  { group: 'Build', perMinute: '30 requests', perMonth: '300,000 requests' },
]

const paramDocs = [
  { param: 'fromChain', type: 'string', required: 'Yes', description: 'Source chain ID (e.g. "1" for Ethereum, "56" for BSC, "42161" for Arbitrum, "solana" for Solana)' },
  { param: 'toChain', type: 'string', required: 'Yes', description: 'Destination chain ID. Same as fromChain for same-chain swaps.' },
  { param: 'tokenIn', type: 'string', required: 'Yes', description: 'Source token contract address on fromChain' },
  { param: 'tokenOut', type: 'string', required: 'Yes', description: 'Destination token contract address on toChain' },
  { param: 'amountIn', type: 'string', required: 'Yes', description: 'Amount in smallest units (e.g. wei for EVM, lamports for Solana)' },
  { param: 'slippage', type: 'number', required: 'No', description: 'Slippage tolerance as percentage (e.g. 0.5 = 0.5%). Default: 0.5' },
  { param: 'sender', type: 'string', required: 'Yes', description: 'Sender wallet address on source chain' },
  { param: 'recipient', type: 'string', required: 'No', description: 'Recipient address on destination chain. Defaults to sender.' },
  { param: 'router', type: 'string', required: 'swap only', description: 'Router from quote response (e.g. "okx", "bitget", "omnibridge", "nearintents")' },
  { param: 'market', type: 'string', required: 'No', description: 'Market parameter from Bitget quote (only needed for Bitget router)' },
]
</script>

<style scoped>
.guide-page h2 {
  margin-bottom: 20px;
  color: #303133;
}
.guide-section {
  margin-bottom: 20px;
}
.guide-content {
  font-size: 14px;
  line-height: 1.8;
  color: #606266;
}
.guide-content p {
  margin: 8px 0;
}
.guide-content ol {
  padding-left: 20px;
}
.guide-content ol li {
  margin: 6px 0;
}
.guide-content code {
  background: #f5f7fa;
  padding: 2px 6px;
  border-radius: 3px;
  font-family: 'Courier New', monospace;
  font-size: 13px;
  color: #c7254e;
}
.code-block {
  background: #1e1e2e;
  border-radius: 8px;
  padding: 16px 20px;
  overflow-x: auto;
  margin: 8px 0;
}
.code-block pre {
  color: #cdd6f4;
  font-family: 'Courier New', Consolas, monospace;
  font-size: 13px;
  line-height: 1.6;
  margin: 0;
  white-space: pre;
}
.workflow-steps {
  display: flex;
  flex-direction: column;
  gap: 16px;
  margin-top: 8px;
}
.step {
  display: flex;
  align-items: flex-start;
  gap: 14px;
}
.step-number {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: linear-gradient(135deg, #667eea, #764ba2);
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 700;
  font-size: 14px;
  flex-shrink: 0;
}
.step-body {
  flex: 1;
}
.step-body strong {
  display: block;
  margin-bottom: 2px;
  color: #303133;
}
.step-body p {
  margin: 0;
  font-size: 13px;
  color: #909399;
}
</style>
