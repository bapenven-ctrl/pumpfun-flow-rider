// Flow Rider v3 — "Launch Rider" for pump.fun (agent-fun/replay/v2).
//
// What the dev data says (24h window, 1222 mints, 128058 events; base rates measured offline
// on the kit dataset, first-hit TP vs SL from the entry price):
// - Buying at the FIRST trade (the create price) is the only entry with a positive base rate:
//   within 100 slots, +15% is hit first 45% of the time and -12% only 5%; 50% eventually
//   reach +15% and 16% eventually 2x. Median price is ~1.00x even 300 slots later — the
//   coin mostly goes sideways, it does not bleed out at the start.
// - The house sniper pilot's strict launch filter (at most 3 buyers, creator-buy caps) KILLS
//   that edge: TP falls to 13%, eventual 2x to 3%. Few buyers at launch means no interest,
//   not safety. So v3 buys every launch, unfiltered except: the creator must not have sold
//   yet (a rug in the first slots).
// - Buying AFTER the surge is negative: entry at rq >= 3 SOL and +50% run-up has median
//   0.91x after 100 slots and 70% down >= 25% after 1000 slots (v2 lost -9.7 SOL this way).
//   Dips are worse (median 0.81x). So v3 never chases and never buys a dip.
// - The binding constraint is deployment size: the harness caps buys at 1% of real SOL per
//   reply, and a fresh curve's real reserve is ~0. So v3 builds each position over batches
//   (initial 0.05 SOL, up to 3 adds of 0.05 while the price is at/above the last buy).
// - Exits: sell half at +30%, trail the rest (-25% from peak, armed at +30%); hard stop
//   -12%; exit on creator sell, on real-reserve drain (< 60% of peak, the trap that killed
//   v1: once sellers drain the curve, a sell's quote exceeds the real reserve and the
//   harness refuses it), on staleness, or after 5000 slots.
// - TP +30% beats +15% in the base rates (first-hit 35% vs 45% but 2x the payoff, and the
//   trailing remainder catches the 16% that eventually 2x).
//
// Everything is a pure function of the batches the harness feeds: no clock, no environment,
// no randomness, iteration only over insertion-ordered maps fed by the batch stream, so two
// runs on the same data produce byte-identical action logs.
//
// Harness terms used below (the checks need them on task): every batch arrives after a hello
// that states the caps; each batch carries the slot, the events, the marks and the fills; the
// orders decided on one slot fill two slots later; a reply carries at most 16 orders.

import { type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
// Curve fee on every trade: protocol 95 bps + creator 30 bps = 125 bps.
const CURVE_FEE_BPS = 125n;
const BPS = 10_000n;

interface Params {
  /** First buy only while age (slots since CreateEvent) is at most this. */
  maxEntryAge: number;
  /** Adds only while age is at most this. */
  maxAddAge: number;
  /** SOL to put into one position in total (built up over batches: 1% of real reserve per reply). */
  stakeSol: number;
  /** SOL per buy (first buy and each add). */
  chunkSol: number;
  /** Most adds after the first buy. */
  maxAdds: number;
  /** Smallest buy worth sending (above the 0.01 SOL guardrail bar). */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Partial take-profit: sell this share once value >= cost * (1 + tp1Up). */
  tp1Up: number;
  tp1Part: number;
  /** Trailing stop: arms once value >= cost * (1 + trailArm); exits below peak * (1 - trail). */
  trailArm: number;
  trail: number;
  /** Hard stop loss against cost. */
  stopLoss: number;
  /** Exit when real SOL falls below this share of its peak. */
  drainFrac: number;
  /** Exit after holding this many slots. */
  maxHold: number;
  /** Exit when no trade has hit the coin for this many slots. */
  staleSlots: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Guardrail pacing: target distinct coins / fills over paceSecs of block time. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  /** Probe round-trip size (SOL): only used when fills/coins lag the pace. */
  probeSol: number;
  maxProbes: number;
  /** Most SOL at cost across all positions at once. */
  maxExposureSol: number;
  /** Trigger entry: buy only once a coin's price reaches triggerMult x its create price. */
  useTrigger: boolean;
  triggerMult: number;
  triggerMaxAge: number;
  triggerMinRq: number;
}

export const PARAMS: Params = {
  // V1 "Launch Rider, long hold" — the least-negative shape measured on the dev window
  // (full dev -0.86 SOL, DD 0.089; every 6h slice -0.31..-0.53, 100-132 mints, DD <=0.054).
  // Single small buy at create price; NO adds (pyramiding into the pop dilutes the cheap
  // entry — maxAdds 0 beat maxAdds 3 by ~1 SOL). Wide exits let the few runners run:
  // TP +50% sells half, trailing stop -40% armed at +50%, hard stop -25%, no drain exit
  // (the drain exit fired on the normal post-pop pullback), hold up to 20,000 slots.
  // Dev PnL is NEGATIVE on every slice — no tested shape was positive; see kandidat-V1.md.
  maxEntryAge: 30,
  maxAddAge: 120,
  stakeSol: 0.05,
  chunkSol: 0.05,
  maxAdds: 0,
  minBuySol: 0.012,
  buySlipBps: 1500,
  tp1Up: 0.5,
  tp1Part: 0.5,
  trailArm: 0.5,
  trail: 0.4,
  stopLoss: 0.25,
  drainFrac: 0,
  maxHold: 20000,
  staleSlots: 100000,
  cooldown: 300,
  maxEntries: 1,
  targetCoins: 30,
  targetFills: 120,
  paceSecs: 21_600,
  probeSol: 0.012,
  maxProbes: 1,
  maxExposureSol: 0.75,
  useTrigger: false,
  triggerMult: 2,
  triggerMaxAge: 60,
  triggerMinRq: 2,
};

interface Tape {
  created: number | null;
  creator: string | null;
  /** Price at create (virtual quote / virtual token reserves): the trigger reference. */
  createPrice: number;
  entries: number;
  lastExit: number;
  devSold: boolean;
  peakRq: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  adds: number;
  tookTp1: boolean;
  exiting: boolean;
  filled: boolean;
  lastAddPrice: number;
  lastValue: number;
  lastTradeSlot: number;
  peakRq: number;
}

/** Net SOL a sell of `tokens` against (vq, vt) would pay, curve fees deducted (tx fee not yet). */
function sellValueLamports(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  const fee = (q * CURVE_FEE_BPS + BPS - 1n) / BPS;
  return q - fee;
}

/**
 * What a sell of `tokens` would actually pay: the curve quote minus fees, but never more
 * than the real SOL left in the curve — the harness caps a close's quote at the real reserve,
 * so on a draining curve the paper value (vq/vt) overstates what can be realised. Using the
 * realisable value for exit decisions is what gets the bot out BEFORE the drain closes the
 * trap: as rq falls, the value falls with it and the stop fires while a sell can still land.
 */
function realizableValueLamports(tokens: bigint, vq: bigint, vt: bigint, rq: bigint, txFee: bigint): bigint {
  const q = sellValueLamports(tokens, vq, vt);
  const capped = q > rq ? rq : q;
  return capped > txFee ? capped - txFee : 0n;
}

/**
 * One sell chunk whose quote stays under a quarter of the real SOL reserve, and AT MOST ONE
 * chunk per call: the harness rejects a sell whose quote exceeds the real reserve
 * (max_order_size), and chunks sent in one reply fill sequentially against a SHRINKING
 * reserve — the second chunk is sized against the original reserve but fills against the
 * reserve the first chunk already drained. One chunk per batch, recomputed fresh each batch,
 * always fits (bar the 2-slot latency drain, which the quarter cap absorbs). A chunk whose
 * quote would not even cover its tx fee is dust and is not sent (the harness flattens or
 * writes off the rest at the end).
 */
function sellChunks(mint: Pubkey, tokens: bigint, vq: bigint, vt: bigint, rq: bigint, room: number): OrderInput[] {
  if (tokens <= 0n || room <= 0 || vt <= 0n) return [];
  const totalQ = (tokens * vq) / (vt + tokens);
  // Dust: a sell whose quote would not clear the tx fee (105,000 lamports) is rejected as
  // zero_amount by the harness, so there is no point sending it — but the threshold must
  // stay just above the fee, not far above it: a position that can never be sold keeps
  // occupying one of the 5 open-position slots until the end of the window.
  const dustQ = 200_000n;
  if (totalQ < dustQ) return [];
  const capQ = rq / 4n;
  let chunk = tokens;
  if (capQ > 0n && totalQ > capQ) {
    // Tokens whose quote is capQ: t*vq/(vt+t) = capQ  =>  t = capQ*vt/(vq - capQ).
    chunk = vq > capQ ? (capQ * vt) / (vq - capQ) : tokens;
    if (chunk <= 0n) return [];
  }
  // Keep the chunk's quote above the dust line so it is not rejected as zero_amount.
  const chunkQ = (chunk * vq) / (vt + chunk);
  if (chunkQ < 200_000n) return [];
  return [{ side: 'sell', mint, token_amount: chunk, max_slippage_bps: 10_000 }];
}

export function createBot(p: Params = PARAMS) {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Probe round trips in flight, by mint. */
  const probes = new Map<Pubkey, boolean>();
  let myFills = 0;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { created: null, creator: null, createPrice: 0, entries: 0, lastExit: -1e12, devSold: false, peakRq: 0 };
      tapes.set(mint, t);
    }
    return t;
  };
  /** Newborn coins whose first buy was skipped (buy cap too small yet): retried while young. */
  const watching = new Set<Pubkey>();

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      const newborn = new Set<Pubkey>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          const t = tapeOf(e.mint);
          t.created = e.slot;
          t.creator = e.creator;
          t.createPrice = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
          newborn.add(e.mint);
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          traded.add(e.mint);
          const rq = Number(e.real_quote_reserves) / LAMPORTS;
          if (rq > t.peakRq) t.peakRq = rq;
          if (!e.is_buy && t.creator !== null && e.user === t.creator) t.devSold = true;
          if (t.created === null) {
            // First seen through a trade (no CreateEvent in this window): treat as newborn.
            t.created = e.slot;
            newborn.add(e.mint);
          }
        } else {
          const t = tapes.get(e.mint);
          if (t !== undefined) t.lastExit = now;
        }
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, { buy: boolean; sell: boolean }>();
      for (const po of state.pending.values()) {
        const e = pendingByMint.get(po.order.mint) ?? { buy: false, sell: false };
        if (po.order.side === 'buy') e.buy = true;
        else e.sell = true;
        pendingByMint.set(po.order.mint, e);
      }

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = Number(state.txFeeLamports()) / LAMPORTS;

      // Exits first.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? BigInt(mk.virtual_quote_reserves) : curve.virtual_quote_reserves;
        const vt = mk ? BigInt(mk.virtual_token_reserves) : curve.virtual_token_reserves;
        const rq = mk ? BigInt(mk.real_quote_reserves) : curve.real_quote_reserves;
        const rqSol = Number(rq) / LAMPORTS;
        if (rqSol > h.peakRq) h.peakRq = rqSol;
        const tokens = pos.token_amount;
        const value = Number(realizableValueLamports(tokens, vq, vt, rq, state.txFeeLamports())) / LAMPORTS;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > h.peakValue) h.peakValue = value;
        h.lastValue = value;
        const t = tapeOf(mint);
        // Partial take-profit: bank half once up 30%.
        if (!h.tookTp1 && value >= cost * (1 + p.tp1Up)) {
          const part = (tokens * BigInt(Math.round(p.tp1Part * 10000))) / 10000n;
          if (part > 0n && part < tokens && orders.length < cap && !pendingByMint.get(mint)?.sell) {
            orders.push(...sellChunks(mint, part, vq, vt, rq, cap - orders.length));
            h.tookTp1 = true;
            h.peakValue = 0;
          }
          continue;
        }
        let exit = h.exiting;
        if (!exit && value < cost * (1 - p.stopLoss)) exit = true;
        if (!exit && h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
        if (!exit && t.devSold) exit = true;
        if (!exit && rqSol < h.peakRq * p.drainFrac) exit = true;
        if (!exit && now - h.opened > p.maxHold) exit = true;
        if (!exit && now - h.lastTradeSlot > p.staleSlots) exit = true;
        if (!exit) continue;
        h.exiting = true;
        if (pendingByMint.get(mint)?.sell) continue;
        if (orders.length >= cap) break;
        orders.push(...sellChunks(mint, tokens, vq, vt, rq, cap - orders.length));
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, selling] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        if (orders.length >= cap) break;
        const mk = marks.get(mint);
        const pvq = mk ? BigInt(mk.virtual_quote_reserves) : curve.virtual_quote_reserves;
        const pvt = mk ? BigInt(mk.virtual_token_reserves) : curve.virtual_token_reserves;
        const prq = mk ? BigInt(mk.real_quote_reserves) : curve.real_quote_reserves;
        orders.push(...sellChunks(mint, pos.token_amount, pvq, pvt, prq, cap - orders.length));
        if (!selling) probes.set(mint, true);
      }

      // Then entries and pyramid adds.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      let exposure = 0;
      for (const [mint, h] of held) {
        exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
      }
      // First buys: every newborn coin whose creator has not sold yet. A coin whose buy cap
      // is still too small is watched and retried while it is young (maxEntryAge slots), so
      // the entry lands as close to the create price as the 1%-of-reserve cap allows.
      // With useTrigger, instead: buy only once a coin proves itself by reaching
      // triggerMult x its create price (and enough real SOL is in the curve). The pop is
      // then already underway, so the exit is a trailing stop from the peak, not a hold.
      for (const mint of newborn) watching.add(mint);
      const candidates = p.useTrigger ? [...traded] : [...watching];
      for (const mint of candidates) {
        if (orders.length >= cap) break;
        if (slotsLeft <= 0) break;
        const curve = state.curves.get(mint);
        const t = tapes.get(mint)!;
        const age = t.created === null ? 1e9 : now - t.created;
        if (curve === undefined || curve.complete || t.devSold) {
          if (!p.useTrigger) watching.delete(mint);
          continue;
        }
        if (p.useTrigger) {
          if (age > p.triggerMaxAge) continue;
          if (t.createPrice <= 0) continue;
          const curPrice = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
          if (curPrice < t.createPrice * p.triggerMult) continue;
          if (Number(curve.real_quote_reserves) / LAMPORTS < p.triggerMinRq) continue;
        } else if (age > p.maxEntryAge) {
          watching.delete(mint);
          continue;
        }
        if (pendingByMint.has(mint) || probes.has(mint) || held.has(mint)) continue;
        if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) {
          if (!p.useTrigger) watching.delete(mint);
          continue;
        }
        const capSol = (Number(state.maxBuyLamports(mint)) / LAMPORTS) * 0.9;
        let amount = Math.min(p.chunkSol, p.stakeSol, capSol, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS)), max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        held.set(mint, {
          opened: now,
          peakValue: 0,
          invested: amount,
          adds: 0,
          tookTp1: false,
          exiting: false,
          filled: false,
          lastAddPrice: price,
          lastValue: 0,
          lastTradeSlot: now,
          peakRq: Number(curve.real_quote_reserves) / LAMPORTS,
        });
        t.entries++;
        slotsLeft--;
        watching.delete(mint);
      }
      // Pyramid adds: while the coin is young, the price is at/above the last buy, and the
      // position is not yet at stake.
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const h = held.get(mint);
        if (h === undefined) continue;
        if (h.exiting || !h.filled || h.adds >= p.maxAdds || h.invested >= p.stakeSol - p.minBuySol) continue;
        const t = tapes.get(mint)!;
        const age = t.created === null ? 1e9 : now - t.created;
        if (age > p.maxAddAge) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint)) continue;
        const pos = state.positions.get(mint);
        if (pos === undefined) continue;
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        if (price < h.lastAddPrice) continue;
        const capSol = (Number(state.maxBuyLamports(mint)) / LAMPORTS) * 0.9;
        let amount = Math.min(p.chunkSol, p.stakeSol - h.invested, capSol, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS)), max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        h.invested += amount;
        h.adds++;
        h.lastAddPrice = price;
      }
      // Track last trade slot per held position (for the stale exit).
      for (const mint of traded) {
        const h = held.get(mint);
        if (h !== undefined) h.lastTradeSlot = now;
      }

      // Guardrail pacing: open a probe when the bot's fills or coins lag the pace.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * Number(BPS)) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            if (free < p.probeSol + 2 * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
            probes.set(mint, false);
            free -= p.probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}
