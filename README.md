# Chainwatch Service

Monitors Ethereum Mainnet and compatible L2 chains (Arbitrum) over native JSON-RPC,
detects transactions involving a set of 500,000 known addresses, and publishes the
matches to Kafka with at-least-once delivery.

Built to survive being killed at any moment: it runs on spot instances, so an
interruption must cost duplicates, never a missed transaction.

## Quick start

```sh
make up          # single-node Redpanda
make topics      # tx-events (6 partitions) + compacted tx-checkpoints
make dataset     # generate 500k addresses + active-wallet seeds
cp .env.example .env
make run
```

Chain selection is config, not code:

```sh
CHAIN=ethereum  RPC_URL=https://ethereum-rpc.publicnode.com make run
CHAIN=arbitrum  RPC_URL=https://arb1.arbitrum.io/rpc        make run
```

## Event contract

Published to `tx-events`, keyed by `userId`.

```json
{
  "userId": "42",
  "from": "0x…",
  "to": "0x…",
  "amount": "1000000000000000000",
  "hash": "0x…",
  "blockNumber": 25854432
}
```

`amount` is wei as a decimal **string**. A `uint256` does not survive a JSON number:
10 ETH is 1e19, past the 2^53 where `float64` starts silently rounding.

When both sides of a transaction are known users, two events are published — one per
`userId` — so that per-user partitioning stays correct.

## How it works

_Filled in with the architecture diagram and measured numbers once the pipeline lands._

## Design decisions

_Filled in as each one is made. See the git history for the reasoning per commit._

## Out of scope

Deliberate exclusions, each with the argument for why it matters in production and why
it is not here. _Written up at the end._

## Testing

```sh
make test-race
```

## Development

```
cmd/chainwatch/     composition root
internal/           the pipeline, one package per stage
testdata/           dataset generator and real block fixtures
```
