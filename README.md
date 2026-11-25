# Pump.fun Flow Rider

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bapenven-ctrl/pumpfun-flow-rider/rank.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bapenven-ctrl/pumpfun-flow-rider/reward.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

Entry for the [Pump.fun Paper Sprint: Any Bot Can Win](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) bounty on [make.fun](https://make.fun): a paper-trading bot for pump.fun bonding curves, starting from 10 SOL, scored on Net PnL.

## Strategy — "Launch Rider, long hold"

Buy every newborn coin at (or as close as the 1%-of-real-reserve buy cap allows to) its
create price, one small 0.05 SOL buy per coin, no pyramiding — adding into the pop
diluted the cheap entry and cost ~1 SOL on the dev window. Then let the few runners
run: take half off at +50%, a trailing stop of −40% armed at +50%, a hard stop of
−25%, and hold up to 20,000 slots. No drain exit: the real-reserve drain that follows
the initial pop is normal, and exiting on it cut positions before their second leg.

Two mechanics the strategy is built around, both measured on the dev window:

- **Sells must fit the real reserve.** A sell whose quote exceeds the real SOL left in
  the curve is rejected (`max_order_size`), and chunks sent in one reply fill
  sequentially against a shrinking reserve — so the bot sends at most one sell chunk
  per batch, capped at a quarter of the real reserve, and judges exits on the
  *realisable* value (curve quote capped at the real reserve, minus fees), not the
  paper value.
- **Slots are the scarce resource.** A position that can never be sold (dust) still
  counts against the 5-open-position cap until the window ends, so the sell dust line
  sits just above the tx fee (0.0002 SOL) instead of far above it.

## Backtest (dev window, kit replay)

Local replay of the frozen 24h dev window (1222 mints):

| | |
|---|---|
| Net PnL | −0.859 SOL |
| Drawdown | 0.089 (guardrail max 0.5) |
| Distinct mints | 179 (guardrail min 20) |
| Win rate | 16.8% |
| Rejected orders | 8 of ~1,400 fills |

Per 6h slice (final-window proxies): −0.31…−0.53 SOL, 100–132 mints, drawdown
≤ 0.054. The dev PnL is negative on every slice — no tested shape was positive
(see `strategy.ts` header and the handover note in the build repo for why: the
1% buy cap plus 2-slot latency mean every attainable entry lands after the
initial pop, and the price then mean-reverts toward its create price). The
candidate is handed over with these numbers stated plainly.

## Files

- `main.ts` — thin runner: `runBot(createBot())`.
- `strategy.ts` — the strategy and its parameters (`PARAMS`).
- `vendor/makefun-bot.ts` — the kit SDK, copied in as the SDK itself instructs.

## Run

Requires Node 22. The bot speaks the `agent-fun/replay/v2` protocol on stdin/stdout
(`hello`, `batch`, `orders`); it makes no network calls and reads no environment.
