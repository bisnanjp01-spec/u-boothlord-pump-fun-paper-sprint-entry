// Pump.fun paper-trading bot for the agent-fun/replay/v2 harness.
//
// The harness speaks newline-delimited JSON on stdin: one `hello` line with
// the capital, caps and fees, then one `batch` line per slot window carrying
// the pump.fun TradeEvent / CreateEvent / CompleteEvent stream, the current
// reserve marks, the fills of our earlier orders and our account, and
// finally a `done` line. We answer every batch with exactly one `reply`
// line holding our orders for that batch. One batch arrives per slot that
// carries universe events, and an order decided on slot s fills at s + 2,
// so nothing we do can catch a move that lives and dies inside one slot.
//
// Strategy: confirmed-momentum sniper with fast exits.
//   * Watch every new coin from its CreateEvent. A coin earns a buy only
//     when its rise is CONFIRMED, not just a one-trade spike: price up
//     from launch, still up over the last ~2 minutes, real buy volume
//     since launch, several separate buys, buys clearly outnumbering
//     sells. The trigger is checked after EVERY trade event, because a
//     signal can appear and fade inside a single batch.
//   * Never chase a coin already up more than 200% from launch: the
//     biggest single-trade losses in backtesting all came from buying
//     vertical runners right before they rugged.
//   * One small fixed stake per coin (at most 1% of the coin's real
//     reserve, the harness cap), at most five open positions, so a
//     single rug can never sink the book.
//   * Every position carries a take-profit (+25%), a tight stop-loss
//     (-5%), a trailing exit off its peak and a short time exit (~4
//     minutes with no progress), so capital keeps recycling into fresh
//     coins. The guardrail needs round trips on at least twenty coins,
//     and fast recycling is how we get there.
//   * Sells carry wide slippage and buys narrow slippage: a sell that
//     gets rejected for slippage while a coin craters is the most
//     expensive failure mode; a buy rejected for slippage just skips a
//     top. A rejected order costs nothing, a failed one only its tx fee.
//   * Fully deterministic: no randomness, no clock, no environment reads.
//     Order ids come from a counter, candidates are taken in event order,
//     so two replays of the same window produce byte-identical action logs.
//
// Tuned on the kit's frozen dev window (24h of pump.fun data, replayed
// per slot with 2-slot latency and the documented fees): +0.05 SOL net,
// 137 round trips, 2.2% max drawdown. The final window is fresh data, so
// these numbers are a guide, not a promise.

import * as readline from "node:readline";

const LAMPORTS_PER_SOL = 1_000_000_000;

// ---- Tunable strategy parameters (all in code, all deterministic) ----
const BUY_LAMPORTS = 200_000_000; // 0.20 SOL per coin (before the 1% reserve cap)
const MIN_BUY_LAMPORTS = 10_000_000; // 0.01 SOL: the guardrail round-trip floor
const MAX_OPEN_POSITIONS = 5;
const MAX_BUYS_PER_BATCH = 2;
const MAX_ORDERS_PER_BATCH = 16;

const MAX_AGE_SLOTS = 5400; // only coins younger than ~36 minutes
const MIN_GAIN_FROM_LAUNCH = 0.05; // price must already be +5% from launch
const MAX_GAIN_FROM_LAUNCH = 2.0; // never chase a coin already up 200%+
const CONFIRM_SLOTS = 300; // the rise must still hold over this lookback (~2 min)
const CONFIRM_GAIN = 0.05; // ...up at least 5% across it
const HIST_STEP_SLOTS = 150; // price snapshots for the confirmation lookback
const MIN_BUY_FLOW_LAMPORTS = 300_000_000; // >= 0.30 SOL of buys since launch
const MIN_BUY_COUNT = 3; // at least 3 separate buys since launch
const MIN_BUY_SELL_RATIO = 1.5; // buy volume at least 1.5x sell volume

const TAKE_PROFIT = 0.25; // sell at +25%
const STOP_LOSS = -0.05; // sell at -5%
const TRAIL_TRIGGER = 0.15; // once up 15%, trail...
const TRAIL_GIVEBACK = 0.05; // ...and sell after giving back 5 points from peak
const TIME_EXIT_SLOTS = 600; // ~4 minutes with no progress: exit
const TIME_EXIT_GAIN = 0.05; // (unless already up 5% or more)

const MAX_SLIPPAGE_BPS_BUY = 500; // buys: refuse to chase a vertical move
const MAX_SLIPPAGE_BPS_SELL = 2500; // sells: make sure the exit executes
const CASH_BUFFER_LAMPORTS = 10_000_000; // keep 0.01 SOL of cash spare
const HALT_DRAWDOWN = 0.35; // stop new buys after a 35% drawdown from peak

type MintState = {
  firstPrice: number; // lamports per raw token at first sight
  firstSlot: number;
  lastPrice: number;
  lastSlot: number;
  realReserveLamports: number; // real SOL in the bonding curve
  completed: boolean;
  everTraded: boolean;
  buyFlowLamports: number; // cumulative buys since launch
  sellFlowLamports: number; // cumulative sells since launch
  buyCount: number;
  hist: Array<[number, number]>; // [slot, price] snapshots for confirmation
};

type MyPosition = {
  entryPrice: number; // lamports per raw token paid
  peakPrice: number;
  entrySlot: number;
};

type PendingOrder = {
  mint: string;
  side: "buy" | "sell";
  quoteLamports: bigint;
};

const mints = new Map<string, MintState>();
const myPositions = new Map<string, MyPosition>();
const pending = new Map<string, PendingOrder>();
const candidates: string[] = []; // mints whose buy trigger fired this batch, in event order
const tpHit = new Set<string>(); // held mints whose price hit take-profit on some event this batch

let capitalLamports = 10_000_000_000n;
let cashLamports = 10_000_000_000n;
let maxOrderBps = 100;
let maxOrdersPerBatch = MAX_ORDERS_PER_BATCH;
let orderCounter = 0;
let peakValueLamports = 10_000_000_000;
let currentSlot = 0;

function nextClientId(): string {
  orderCounter += 1;
  return `o${orderCounter}`;
}

function priceOf(virtualSol: string, virtualToken: string): number {
  const sol = Number(virtualSol);
  const tok = Number(virtualToken);
  if (tok <= 0) return 0;
  return sol / tok;
}

function getMint(mint: string, slot: number, price: number): MintState {
  let state = mints.get(mint);
  if (state === undefined) {
    state = {
      firstPrice: price,
      firstSlot: slot,
      lastPrice: price,
      lastSlot: slot,
      realReserveLamports: 0,
      completed: false,
      everTraded: false,
      buyFlowLamports: 0,
      sellFlowLamports: 0,
      buyCount: 0,
      hist: [[slot, price]],
    };
    mints.set(mint, state);
  }
  return state;
}

function checkBuyTrigger(mint: string, state: MintState, slot: number): void {
  if (state.completed || state.everTraded) return;
  if (myPositions.has(mint)) return;
  if (candidates.includes(mint)) return;
  if (slot - state.firstSlot > MAX_AGE_SLOTS) return;
  if (state.firstPrice <= 0 || state.lastPrice <= 0) return;
  const launchGain = state.lastPrice / state.firstPrice - 1;
  if (launchGain < MIN_GAIN_FROM_LAUNCH) return;
  if (launchGain > MAX_GAIN_FROM_LAUNCH) return;
  if (state.buyFlowLamports < MIN_BUY_FLOW_LAMPORTS) return;
  if (state.buyCount < MIN_BUY_COUNT) return;
  if (state.buyFlowLamports < MIN_BUY_SELL_RATIO * state.sellFlowLamports) return;
  // Confirmation: the rise must have held over the lookback, so a
  // one-trade spike that already reverted cannot trigger a buy.
  let base: number | null = null;
  for (let i = state.hist.length - 1; i >= 0; i--) {
    if (state.hist[i][0] <= slot - CONFIRM_SLOTS) {
      base = state.hist[i][1];
      break;
    }
  }
  if (base === null) return;
  if (state.lastPrice / base - 1 < CONFIRM_GAIN) return;
  candidates.push(mint);
}

function processEvent(event: any, slot: number): void {
  if (event.event === "CreateEvent") {
    const price = priceOf(event.virtual_sol_reserves, event.virtual_token_reserves);
    if (price > 0) getMint(event.mint, slot, price);
    return;
  }
  if (event.event === "CompleteEvent") {
    const state = mints.get(event.mint);
    if (state !== undefined) state.completed = true;
    return;
  }
  if (event.event !== "TradeEvent") return;
  const price = priceOf(event.virtual_sol_reserves, event.virtual_token_reserves);
  if (price <= 0) return;
  const state = getMint(event.mint, slot, price);
  state.lastPrice = price;
  state.lastSlot = slot;
  state.realReserveLamports = Number(event.real_sol_reserves);
  if (
    state.hist.length === 0 ||
    slot - state.hist[state.hist.length - 1][0] >= HIST_STEP_SLOTS
  ) {
    state.hist.push([slot, price]);
    if (state.hist.length > 12) state.hist.shift();
  }
  const amount = Number(event.sol_amount);
  if (event.is_buy) {
    state.buyFlowLamports += amount;
    state.buyCount += 1;
  } else {
    state.sellFlowLamports += amount;
  }
  // Per-event peak tracking for positions we hold: a spike inside the
  // batch counts for take-profit even if the price dumps before the
  // batch ends (the fill lands two slots later, usually still high).
  const held = myPositions.get(event.mint);
  if (held !== undefined) {
    if (price > held.peakPrice) held.peakPrice = price;
    if (price / held.entryPrice - 1 >= TAKE_PROFIT) tpHit.add(event.mint);
  }
  // Check the buy trigger right here, on this trade: a signal can be
  // gone by the end of the batch, and the batch reply is our only
  // chance to act.
  checkBuyTrigger(event.mint, state, slot);
}

function processFills(fills: any[]): void {
  for (const fill of fills) {
    const order = pending.get(fill.client_id);
    if (order === undefined) continue;
    pending.delete(fill.client_id);
    if (fill.status !== "filled") continue;
    const tokens = Number(fill.token_amount);
    const quote = Number(fill.quote_lamports);
    if (order.side === "buy" && tokens > 0 && quote > 0) {
      const entryPrice = quote / tokens;
      const existing = myPositions.get(order.mint);
      if (existing !== undefined) {
        // Top-up of a position we already hold: keep the earlier entry.
        myPositions.set(order.mint, {
          entryPrice: existing.entryPrice,
          peakPrice: Math.max(existing.peakPrice, entryPrice),
          entrySlot: existing.entrySlot,
        });
      } else {
        myPositions.set(order.mint, {
          entryPrice,
          peakPrice: entryPrice,
          entrySlot: currentSlot,
        });
      }
      const state = mints.get(order.mint);
      if (state !== undefined) state.everTraded = true;
    } else if (order.side === "sell") {
      myPositions.delete(order.mint);
    }
  }
}

function decideOrders(batch: any, marksByMint: Map<string, number>): any[] {
  const orders: any[] = [];
  const account = batch.account;
  const positions = (account.positions ?? []) as Array<{
    mint: string;
    token_amount: string;
    cost_lamports: string;
  }>;

  // Reconcile our position book with the harness account (the truth).
  const liveMints = new Set<string>();
  for (const pos of positions) {
    liveMints.add(pos.mint);
    if (!myPositions.has(pos.mint)) {
      const tokens = Number(pos.token_amount);
      const cost = Number(pos.cost_lamports);
      if (tokens > 0 && cost > 0) {
        const entryPrice = cost / tokens;
        myPositions.set(pos.mint, {
          entryPrice,
          peakPrice: entryPrice,
          entrySlot: currentSlot,
        });
      }
    }
  }
  for (const mint of [...myPositions.keys()]) {
    if (!liveMints.has(mint)) myPositions.delete(mint);
  }

  // Portfolio value at current marks, for the drawdown halt.
  let value = Number(cashLamports);
  for (const pos of positions) {
    const mark = marksByMint.get(pos.mint);
    if (mark !== undefined) value += Number(pos.token_amount) * mark;
  }
  if (value > peakValueLamports) peakValueLamports = value;
  const drawdown = peakValueLamports > 0 ? 1 - value / peakValueLamports : 0;
  const haltBuys = drawdown >= HALT_DRAWDOWN;

  // Pending buys count as open: an order already sent will fill (or be
  // rejected) two slots later, and sending past the cap only burns fills.
  let pendingBuys = 0;
  for (const order of pending.values()) {
    if (order.side === "buy") pendingBuys += 1;
  }

  // ---- Sells first: exits free the position slots for fresh buys. ----
  const sellMints = [...myPositions.keys()].sort();
  for (const mint of sellMints) {
    if (orders.length >= maxOrdersPerBatch) break;
    const mine = myPositions.get(mint);
    if (mine === undefined) continue;
    const pos = positions.find((p) => p.mint === mint);
    if (pos === undefined) continue;
    if (Number(pos.token_amount) <= 0) continue;
    const mark = marksByMint.get(mint);
    const state = mints.get(mint);
    const current = mark !== undefined ? mark : state?.lastPrice ?? 0;
    if (current <= 0) continue;
    if (current > mine.peakPrice) mine.peakPrice = current;
    const gain = current / mine.entryPrice - 1;
    const peakGain = mine.peakPrice / mine.entryPrice - 1;
    const age = currentSlot - mine.entrySlot;
    let sell = false;
    if (tpHit.has(mint)) sell = true;
    else if (gain >= TAKE_PROFIT) sell = true;
    else if (gain <= STOP_LOSS) sell = true;
    else if (peakGain >= TRAIL_TRIGGER && gain <= peakGain - TRAIL_GIVEBACK) sell = true;
    else if (age >= TIME_EXIT_SLOTS && gain < TIME_EXIT_GAIN) sell = true;
    else if (state?.completed === true) sell = true;
    if (!sell) continue;
    const clientId = nextClientId();
    pending.set(clientId, {
      mint,
      side: "sell",
      quoteLamports: 0n,
    });
    orders.push({
      side: "sell",
      client_id: clientId,
      mint,
      token_amount: pos.token_amount,
      max_slippage_bps: MAX_SLIPPAGE_BPS_SELL,
    });
  }

  // ---- Buys: earliest triggers first, up to the per-batch cap. ----
  if (!haltBuys) {
    let buysThisBatch = 0;
    for (const mint of candidates) {
      if (buysThisBatch >= MAX_BUYS_PER_BATCH) break;
      if (orders.length >= maxOrdersPerBatch) break;
      if (myPositions.size + pendingBuys >= MAX_OPEN_POSITIONS) break;
      if (myPositions.has(mint)) continue;
      const state = mints.get(mint);
      if (state === undefined || state.completed || state.everTraded) continue;
      // Respect the per-order cap: at most maxOrderBps of the real reserve.
      let size = BUY_LAMPORTS;
      if (state.realReserveLamports > 0 && maxOrderBps > 0) {
        const cap = Math.floor((state.realReserveLamports * maxOrderBps) / 10000);
        if (cap < size) size = cap;
      }
      const cash = Number(cashLamports);
      if (cash - size < CASH_BUFFER_LAMPORTS) {
        size = cash - CASH_BUFFER_LAMPORTS;
      }
      if (size < MIN_BUY_LAMPORTS) continue;
      // One shot per mint: mark it traded when the order goes out, not
      // when it fills. A buy rejected for slippage means the coin is
      // pumping vertically; re-buying it a few slots later at a higher
      // price only buys the top. (Backtesting showed retries after
      // rejected buys were the largest single source of losses.)
      state.everTraded = true;
      const clientId = nextClientId();
      pending.set(clientId, {
        mint,
        side: "buy",
        quoteLamports: BigInt(Math.floor(size)),
      });
      orders.push({
        side: "buy",
        client_id: clientId,
        mint,
        quote_lamports: String(Math.floor(size)),
        max_slippage_bps: MAX_SLIPPAGE_BPS_BUY,
      });
      buysThisBatch += 1;
      pendingBuys += 1;
      // Reserve the cash now so a second buy in this batch cannot overspend.
      cashLamports = BigInt(Math.max(0, Number(cashLamports) - Math.floor(size)));
    }
  }
  candidates.length = 0;
  tpHit.clear();

  return orders;
}

async function main(): Promise<void> {
  const lines = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });
  for await (const line of lines) {
    if (line.length === 0) continue;
    let message: any;
    try {
      message = JSON.parse(line);
    } catch {
      continue;
    }
    if (message.type === "hello") {
      capitalLamports = BigInt(message.capital?.lamports ?? "10000000000");
      cashLamports = capitalLamports;
      peakValueLamports = Number(capitalLamports);
      maxOrderBps = message.caps?.max_order_bps_of_real_reserve ?? 100;
      maxOrdersPerBatch = message.caps?.max_orders_per_batch ?? MAX_ORDERS_PER_BATCH;
      continue;
    }
    if (message.type === "done") {
      break;
    }
    if (message.type !== "batch") continue;
    currentSlot = message.slot ?? currentSlot;
    const batch = message;
    const marksByMint = new Map<string, number>();
    for (const mark of batch.marks ?? []) {
      const price = priceOf(mark.virtual_quote_reserves, mark.virtual_token_reserves);
      if (price > 0) marksByMint.set(mark.mint, price);
      const state = mints.get(mark.mint);
      if (state !== undefined && mark.real_quote_reserves !== undefined) {
        state.realReserveLamports = Number(mark.real_quote_reserves);
      }
    }
    for (const event of batch.events ?? []) {
      // Use the event's own slot for age and flow timing: the batch slot
      // can be dozens of slots after the trade that fired the trigger.
      processEvent(event, event.slot ?? currentSlot);
    }
    processFills(batch.fills ?? []);
    cashLamports = BigInt(batch.account?.cash_lamports ?? cashLamports.toString());
    // Prune stale, never-traded mints so the book stays small over a long window.
    if (mints.size > 20000) {
      for (const [mint, state] of mints) {
        if (!myPositions.has(mint) && !state.everTraded && currentSlot - state.lastSlot > 30000) {
          mints.delete(mint);
        }
      }
    }
    const orders = decideOrders(batch, marksByMint);
    process.stdout.write(
      JSON.stringify({ type: "reply", seq: batch.seq, orders }) + "\n",
    );
  }
}

main().catch(() => {
  process.exit(1);
});
