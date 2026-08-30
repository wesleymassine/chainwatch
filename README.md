# chainwatch

Watches Ethereum Mainnet and compatible L2 chains for transactions involving a
set of 500,000 known addresses, and publishes each match to Kafka.

It is built to be killed at any moment. The service is meant to run on spot
instances, so an interruption has to cost duplicate events, never a missed one.

## Quick start

```sh
make up          # single-node Redpanda
make dataset     # generate the 500k address dataset
make run         # creates its own topics, then starts
```

The defaults work with nothing else set. Every setting is an environment
variable, listed with its reasoning in [.env.example](.env.example).

Point it at another chain and it reconfigures itself:

```sh
RPC_URL=https://arb1.arbitrum.io/rpc make run
```

```sh
curl localhost:9090/metrics
```

## What it publishes

To `tx-events`, keyed by `userId`:

```json
{
  "userId": 42,
  "from": "0x28c6c06298d514db089934071355e5743bf21d60",
  "to": "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "amount": "211395000000000000",
  "hash": "0xc5229d88f1cf0a7b9505715ddb7c98566affc2c8cd25037f23b8b0db2ca643a7",
  "blockNumber": 25860269
}
```

## How it works

```
head ──► fetch (N workers, batched) ──► match ──► sequencer ──► publish ──► checkpoint
                                                     │
                                          holds blocks until they
                                          are contiguous again
```

Each stage is one package: [ethrpc](internal/ethrpc/) reads the chain,
[matcher](internal/matcher/) decides which transactions belong to a user,
[pipeline](internal/pipeline/) runs the whole thing concurrently and in order,
[publisher](internal/publisher/) writes to Kafka, and
[checkpoint](internal/checkpoint/) remembers where it got to.

---

# Design decisions

Every one of these starts with something the blockchain does, not with something
the code does.

## The node already knows who sent the transaction

**The chain:** a transaction does not carry the sender's address. It carries a
signature, and the sender is recovered from it.

**The consequence:** `go-ethereum`'s `ethclient` gives you a transaction without
a `from` field, so getting it means recovering an ECDSA key — 50 to 100
microseconds each. Over an hour of Arbitrum blocks that is about ten minutes of
CPU spent computing something the node already knew.

**What we do:** read the field the node sends. JSON-RPC returns `from` on every
transaction, so [`Tx`](internal/ethrpc/types.go#L32) reads it and never touches
the signature. Ten minutes becomes fifteen seconds.

Once the typed decoding is gone, `go-ethereum` has nothing left to offer: it
would pull 168 modules to provide an HTTP wrapper and a 20-byte array. So
[`ethrpc`](internal/ethrpc/client.go) is `net/http` and `encoding/json`, and the
only dependency in the project is the Kafka client.

## Chains keep inventing new transaction types

**The chain:** Arbitrum emits a system transaction of type `0x6a` in **every
single block** — 300 out of 300 in a sample. Mainnet now carries `0x3` blob and
`0x4` set-code transactions in ordinary ones.

**The consequence:** a decoder built around a fixed set of types rejects what it
does not recognise, and the usual reaction is to skip the whole block. On
Arbitrum that means skipping every block. An L1→L2 deposit landing in a user's
wallet is exactly the event a neobank cannot afford to miss.

**What we do:** [`Tx`](internal/ethrpc/types.go#L32) never reads the type field.
It takes the four things it needs and ignores everything else, so a chain can
introduce a type tomorrow without breaking us.

## Amounts do not fit in a JSON number

**The chain:** values are `uint256` in wei. Ten ether is 10,000,000,000,000,000,000.

**The consequence:** that is past 2^53, where any float64-backed JSON parser
starts rounding. The value arrives subtly wrong with nothing to signal it, which
is the worst thing that can happen to a ledger.

**What we do:** [`Event.Amount`](internal/matcher/matcher.go#L22) is a decimal
string, and `*big.Int` internally.

## Blockchains change their mind about recent history

**The chain:** two miners can produce a block at nearly the same moment. The
network settles on one and **discards** the other, taking its transactions with
it.

**The consequence:** if we already published those events, we told the bank a
user received a deposit that stopped existing. Nothing corrects that on its own,
and nothing logs it.

**What we do:** two defences.

Stay behind. `ConfirmDepth` keeps us two blocks back from the head on mainnet,
where a block with two others on top is effectively settled. Arbitrum has a
single sequencer deciding order, nothing competes, so it runs at zero and pays no
latency for a risk it does not have —
see [profiles](internal/config/config.go#L31).

Check the links. Every block carries the fingerprint of the one before it: that
is what makes it a chain. [`publish`](internal/pipeline/pipeline.go#L306) checks
the whole batch links to what we last published **before** sending any of it. If
it does not, the chain moved, so we publish nothing, go back sixteen blocks and
read again.

We do not hunt for the exact fork point, because we are allowed to repeat.
Duplicates are permitted, so going back far enough is as correct as going back
precisely — and it saved a whole data structure.

## The chain can move while the service is down

**The consequence:** a reorg during a restart would otherwise be invisible. We
would resume at the next number and never notice that the block before it is no
longer the one we published.

**What we do:** the checkpoint stores the block's **hash** alongside its number,
and [`resume`](internal/pipeline/pipeline.go#L183) restores it. The first block
fetched after a restart is checked against the last one published before it.

## Public endpoints rate limit, and giving up loses blocks

**The chain:** free RPC providers answer HTTP 429 under load. Arbitrum's does it
within a second of eight workers asking for hundred-block batches.

**The consequence:** the brief says not to *slow down* for rate limits. It does
not say to *give up* on them — and a 429 that ends the request drops the blocks
it was carrying.

**What we do:** [`post`](internal/ethrpc/transport.go#L88) retries a 429 for as
long as the caller's context lives, and it does not spend the failure budget,
which stays reserved for 5xx and transport errors. Nothing is throttled: the
steady state is never slowed and no concurrency is given up. The retries are
logged so the rate limiting stays visible rather than hidden.

Without this the service died on Arbitrum after 21 rate limits, having published
nothing — while every offline test stayed green.

## A batch response can come back short, and say nothing

**The chain:** the mainnet endpoint caps its response at about 24 MB. Ask for a
hundred blocks and forty come back — HTTP 200, valid JSON, no error anywhere.

**The consequence:** trusting the count silently drops sixty blocks. That is the
exact failure the at-least-once requirement exists to prevent, and it leaves no
trace.

**What we do:** [`Blocks`](internal/ethrpc/client.go#L104) correlates responses
by JSON-RPC id, works out which blocks did not come back, and asks again. A block
counts as fetched only when its result is in hand.

The same cap sometimes refuses outright instead of truncating, which no retry can
fix, so that error names the setting to lower rather than reporting a bare
protocol code.

## Fetching concurrently breaks the order the chain has

**The chain:** blocks have an order, and it is the only thing that makes a
checkpoint meaningful.

**The consequence:** eight workers return whatever finishes first. Publishing in
arrival order would put block 340 into Kafka before 320, and a checkpoint written
after 340 would claim work that never happened.

**What we do:** workers fetch contiguous ranges rather than single blocks, so
what arrives out of order is whole chunks. The
[sequencer](internal/pipeline/sequencer.go#L31) holds a chunk until every block
before it has been released. It does no I/O, takes no lock and starts no
goroutine — ordering is the property most likely to break under concurrency, so
it lives somewhere it can be tested without any.

## A checkpoint may only move after the broker has the events

**The consequence:** this is the entire at-least-once guarantee, and it only
reads correctly in one direction. Save first and an interruption in between
leaves the checkpoint claiming work that was never published — those transactions
are never looked at again.

**What we do:** [`publish`](internal/pipeline/pipeline.go#L306) matches,
publishes, waits for the broker's acknowledgement, and only then saves. That
save gets a context that survives shutdown, because abandoning it would
republish the batch on the next start: a duplicate we chose to create rather than
one we could not avoid.

## Local disk does not survive a spot instance

**The consequence:** the machine and its disk go away together. A checkpoint
written to a file is gone with the instance that wrote it.

**What we do:** the checkpoint lives in a **compacted Kafka topic**, keyed by
chain id — [`checkpoint/kafka.go`](internal/checkpoint/kafka.go). Compaction
keeps only the latest record per key, so it stays one record per chain however
long the service runs. Kafka is already a dependency and already durable, so this
adds nothing to deploy and does not violate the brief's "don't add databases".

Deciding there is *no* checkpoint is the dangerous direction: it sends the
service to the chain head and skips everything in between. So
[`Load`](internal/checkpoint/kafka.go#L64) establishes where the log ends before
reading, and proves a topic empty rather than inferring it from a read that has
not returned anything yet.

## Topics must exist with the right configuration

**The consequence:** a topic created implicitly by a producer comes back with
server defaults — retention instead of compaction. Applied to the checkpoint
topic, retention is free to delete the one record the service depends on.

**What we do:** [`EnsureTopics`](internal/publisher/topics.go#L34) creates them
explicitly at startup with the configuration they need, and does nothing if they
already exist. Starting the service is the only thing anyone has to run, and the
configuration lives in the code rather than in someone's shell history.

## Chains differ, and configuration can lie

**The chain:** mainnet produces a block every 12–13 seconds at around 620 KB
each. Arbitrum produces up to four a second, roughly a hundred times smaller.

**The consequence:** no single setting serves both. But a `CHAIN=ethereum`
setting could disagree with the endpoint it is pointed at, and resuming mainnet's
block numbers against another chain is far worse than any tuning mistake.

**What we do:** the service asks the chain for its id and picks the measured
profile — [`config.Load`](internal/config/config.go#L49). The chain cannot be
wrong about its own identity. An unmeasured chain gets mainnet's cautious
settings and a warning.

## One user's events must stay in order

**The consequence:** partitioning is what makes Kafka fast, and it is also what
breaks order if the key is wrong.

**What we do:** the record key is the `userId`, so one user's events land on one
partition — [`Publish`](internal/publisher/kafka.go#L42). The idempotent producer
keeps a partition's writes in sequence even with several requests in flight, and
`acks=all` is what makes idempotency legal at all. Order across different users
is not guaranteed and should not be: that is what allows parallel consumption.

Proved against a real broker: 200 events for one user, on a six-partition topic,
all on one partition and in exact order.

## What a consumer has to do

At-least-once is only half a contract. The other half is what the reader is
expected to handle, and it is not obvious from the payload alone.

**Deduplicate on `hash` + `userId`.** Not on `hash` alone: one transaction
between two watched users produces two events, one per user, and both are
correct. The pair is the natural idempotency key — a restart, a reorg rewind or
a retried batch all republish the same pair.

**Order is per user, not global.** Events for one `userId` land on one partition
and arrive in the order they happened. Events for different users are on
different partitions and have no order between them. That is deliberate: global
ordering would mean one partition, and one partition would mean no parallel
consumption.

**Nothing is ever retracted.** If a block is published and then discarded by a
reorg, its events stay in the topic. Rewinding republishes the replacement
chain, so a transaction that survives the reorg is deduplicated by the pair
above — but a transaction that existed only on the discarded fork remains as an
event for something that no longer happened.

`ConfirmDepth` is what makes this rare rather than routine: a block two deep on
mainnet is effectively settled. A system that cannot tolerate it at all would
wait for finality instead of a fixed depth, or publish retractions — both are
larger designs than at-least-once asks for, and the brief asks for
at-least-once.

---

# Numbers

All measured, none estimated. Method and raw output in the commit history.

## What the service does

An in-process node serving mainnet-shaped blocks — 300 transactions each —
against the real 500,000-address dataset. This is the brief's *assume
unrestricted usage of the RPC node*.

| workers | blocks/s | transactions/s |
|---:|---:|---:|
| 1 | 417 | 125,080 |
| 4 | 1,440 | 432,039 |
| **8** | **2,199** | **659,747** |
| 16 | 2,288 | 686,375 |

Eight workers on eight cores give 5.3x the throughput of one. Sixteen adds four
percent, which is where the machine runs out, not the design.

Mainnet produces 0.08 blocks per second and Arbitrum one to four. There are three
to four orders of magnitude of headroom.

## What the public endpoints do

The same code against free providers:

| | 4 workers | 8 | 16 | 32 |
|---|---:|---:|---:|---:|
| mainnet | 48 | 69 | **76** | 74 |

Adding workers stops helping and then starts hurting. **The provider is roughly
thirty times slower than the service reading from it.** That is what "the ceiling
is external" means as a number rather than an assertion.

Arbitrum's free endpoint is tighter still: it serves about one hundred-block
request every 15–20 seconds, and concurrency makes it strictly worse — 429s scale
linearly with workers while completed work falls to zero.

**Real time is unaffected.** At the head there is nothing to batch: each poll
finds one to four new blocks, the requests are small, and every one is served.
Measured on Arbitrum with the full profile: 4.0 blocks/s, **zero rate limits**.
Only a deep catch-up asks for hundred-block batches, and that ceiling belongs to
the provider.

## The parts

| | |
|---|---|
| Load 500k addresses | 135 ms, 31.9 MB resident, ~10 ns per lookup, zero allocations |
| Match a block | ~18 ns per transaction, zero allocations when nothing matches |
| Generate the dataset | 500k rows in 0.7 s, 23 allocations total |

---

# Testing

```sh
make test-race        # everything, offline and deterministic
make test-live        # against real nodes and a real broker, both chains
go test -short ./...  # skips the throughput measurement, ~5s
```

The offline suite talks to an `httptest` server, and the block fixtures are
**real blocks captured from mainnet and Arbitrum** — between them they cover
every transaction type both chains currently produce, plus a contract creation.
Invented JSON would prove nothing here; the whole point is surviving what the
chains actually emit.

Two tests carry more weight than the rest:

**No gaps.** 500 blocks end to end, every one checked individually in the output.
A hole is the failure this service exists to prevent, and it is invisible unless
something counts.

**Survives a restart.** The pipeline is killed mid-run and a new one starts
against the same checkpoint with no configured start block. Every block is still
accounted for afterwards.

Verified separately with a real `SIGKILL` against mainnet, reading the events
back out of Kafka rather than trusting the logs: 7,261 events across the expected
301-block range, with no block missing. Two blocks produced no events at all —
checked against the chain, one holds nine transactions that touch none of the
watched addresses and the other is empty, so publishing nothing there is correct.

Coverage where it matters: matcher and config 100%, pipeline 95%, addresses 97%.

---

# Out of scope

Each of these matters in production and was left out deliberately.

**ERC-20 transfers.** A token transfer moves no native value, so it appears here
as an event with `amount: "0"` — the wallet interacted with a contract. Catching
the token movement means `eth_getLogs` with a `Transfer` topic filter, which is a
second read path and a second event shape. The brief asks for transactions
involving the addresses, and that is what this publishes.

**Internal transactions.** Value moved by a contract during execution does not
appear as a transaction at all. Finding it needs `debug_traceBlock`, which most
public endpoints do not expose.

**Receipt verification.** A transfer *to* an externally owned account cannot
revert once included — validity is checked at inclusion and the gas cost is
fixed. Since the watched addresses are user wallets, deposits are never false
positives without fetching a single receipt. Only the outbound case, a user
calling a contract that reverts, would need `eth_getBlockReceipts`.

**WebSocket subscriptions.** Measured: 250 ms polling covers Arbitrum's 1–4
blocks per second with headroom. `eth_subscribe` would add a second code path,
reconnection logic and a dependency on endpoint support, for no measured gain.

**Adaptive concurrency.** The service could discover a provider's capacity and
match it, instead of being tuned per chain. The brief says to assume an
unrestricted node, which makes this a problem it explicitly asks us not to solve.

**Multi-endpoint failover, finality tracking beyond a fixed depth, and a schema
registry.** All reasonable next steps; none of them are the core problem.

---

# Reading the history

This was delivered as a ZIP with `.git` included, so the commit history travels
with it:

```sh
git log --oneline
```

It is worth the minute. The order the decisions were made in explains more than
the final state does — the walking skeleton before the concurrency, the
checkpoint before the reorg handling — and several `fix:` commits mark the points
where a measurement contradicted an assumption I had been confident about.

The same repository is on GitHub, private. Ask and I will grant access:

**https://github.com/wesleymassine/chainwatch**
