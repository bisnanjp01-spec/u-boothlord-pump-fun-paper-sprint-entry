# Pump.fun Paper Sprint Bot

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bisnanjp01-spec/u-boothlord-pump-fun-paper-sprint-entry/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bisnanjp01-spec/u-boothlord-pump-fun-paper-sprint-entry/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A paper-trading bot for the [Pump.fun Paper Sprint](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
on [make.fun](https://make.fun): it paper-trades pump.fun on a 6-hour replay
of real market data, and the highest net PnL wins. No real money, paper only.

## How it works

`main.ts` (Node 22, no dependencies) speaks the `agent-fun/replay/v2`
protocol: newline-delimited JSON on stdin (`hello`, then one `batch` per
slot, then `done`), one `reply` per batch on stdout.

Strategy — a confirmed-momentum sniper with fast exits:

- Watch every new coin from its `CreateEvent`. A coin earns a buy only
  when its rise is confirmed, not just a one-trade spike: price up from
  launch, still up over the last ~2 minutes, real buy volume since
  launch, several separate buys, buys clearly outnumbering sells.
- Never chase a coin already up more than 200% from launch.
- One small fixed stake per coin (at most 1% of the coin's real reserve,
  the harness cap), at most five open positions.
- Every position carries a take-profit (+30%), a tight stop-loss (-5%),
  a trailing exit off its peak, and a short time exit (~4 minutes with
  no progress), so capital keeps recycling into fresh coins.

Fully deterministic: no randomness, no clock, no environment reads, so
two replays of the same window produce byte-identical action logs.

## Parameters (tuned on the dev window)

| Parameter | Value | Why |
| --- | --- | --- |
| Buy size | 0.20 SOL, capped at 1% of real reserve | Harness cap; keeps one rug small |
| Entry | +5% from launch, still +5% over the last ~2 min, ≥0.30 SOL buys, ≥3 buys, buys ≥1.5× sells | Confirmation filters one-trade spikes |
| Never chase | skip coins already up >200% from launch | The worst single-trade losses came from vertical runners |
| Take-profit | +30% (checked on every trade event, not just batch end) | Banks the spikes that persist ≥2 slots; +30% beat +25%/+40%/+60% on the dev window |
| Stop-loss | −5% | Cuts losers before the dump finishes |
| Trailing exit | after +15%, sell on a 5-point give-back | Lets runners run, exits on the first pullback |
| Time exit | ~4 min with <+5% | Recycles capital into fresh coins |
| Slippage | buys 5%, sells 25% | A rejected sell during a crater is the most expensive failure |

## Local result

Tuned on the kit's frozen dev window (24h of pump.fun data, replayed per
slot with 2-slot latency and the documented fees, graded with the kit's
own grade-local on the real harness): **+0.0365 SOL net PnL**, 138 round
trips, 2.2% max drawdown. The final window is fresh data, so treat that
as a guide, not a promise.

## License

MIT. See [LICENSE](LICENSE).
