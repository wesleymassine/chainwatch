# chainwatch

Watches Ethereum Mainnet and compatible L2 chains for transactions involving a
set of 500,000 known addresses, and publishes each match to Kafka.

It is built to be killed at any moment. It runs on spot instances, so being
interrupted has to cost duplicate events, never a missing one.

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

See what it is doing, in one screen — and see
[Verify it yourself](#verify-it-yourself) for a short tour of every claim in this
file:

```sh
make verify
```

```
── dataset ─────────────────────────────────────────
   500000 addresses in testdata/addresses.csv

── service ─────────────────────────────────────────
   {
     "watching": {
       "chain": "ethereum",
       "chainId": 1,
       "addresses": 500000
     },
     "progress": {
       "blocksProcessed": 3,
       "transactionsScanned": 821,
       "eventsPublished": 86,
       "reorgsHandled": 0,
       "lastBlock": 25869470,
       "chainHead": 25869472,
       "blocksBehind": 2
     }
   }

── kafka ───────────────────────────────────────────
   86 events across 6 partitions
   16 distinct users have events
```

The two groups answer different questions. `watching` is what the service was
told to do and never changes while it runs; the address set is built once at
startup and only read after that, which is also why it needs no lock. `progress`
is what it has done since it started.

`eventsPublished` and the Kafka count agree, which is the point: what the service
reports having published is what is actually in the topic. `blocksBehind` is the
number to watch. Here it is exactly `ConfirmDepth`, so the service is as close to
the head as it is allowed to be.

Only the seeded wallets ever appear. The other 499,980 addresses are random, so
they correctly never match anything on a live chain.

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

**The consequence:** `go-ethereum`'s `ethclient` hands you a transaction with no
`from` field. Getting it back means recovering an ECDSA key, which takes 50 to 100
microseconds each time. Over an hour of Arbitrum blocks that is about ten minutes
of CPU, spent working out something the node already knew.

**What we do:** read the field the node sends. JSON-RPC returns `from` on every
transaction, so [`Tx`](internal/ethrpc/types.go#L32) reads it and never touches
the signature. Ten minutes becomes fifteen seconds.

Once the typed decoding is gone, `go-ethereum` has nothing left to offer. It
would pull in 168 modules to give us an HTTP wrapper and a 20-byte array. So
[`ethrpc`](internal/ethrpc/client.go) uses `net/http` and `encoding/json`, and the
only dependency in the project is the Kafka client.

## Chains keep inventing new transaction types

**The chain:** Arbitrum emits a system transaction of type `0x6a` in **every
single block** — 300 out of 300 in a sample. Mainnet now carries `0x3` blob and
`0x4` set-code transactions in ordinary ones.

**The consequence:** a decoder built around a fixed set of types rejects what it
does not know. The usual reaction is to skip the whole block, and on Arbitrum that
means skipping every block. An L1→L2 deposit landing in a user's wallet is exactly
the event a neobank cannot afford to miss.

**What we do:** [`Tx`](internal/ethrpc/types.go#L32) never reads the type field.
It takes the four things it needs and ignores everything else, so a chain can
introduce a type tomorrow without breaking us.

## Amounts do not fit in a JSON number

**The chain:** values are `uint256` in wei. Ten ether is 10,000,000,000,000,000,000.

**The consequence:** that is past 2^53, where a float64-backed JSON parser starts
rounding. The value arrives slightly wrong and nothing warns you. For a ledger,
that is the worst kind of bug.

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

Check the links. Every block carries the fingerprint of the one before it, and
that is what makes it a chain. [`publish`](internal/pipeline/pipeline.go#L306)
checks that the whole batch links to what we last published, **before** sending any
of it. If it does not, the chain has moved. We publish nothing, go back sixteen
blocks and read again.

We do not look for the exact fork point, because we are allowed to repeat.
Duplicates are permitted, so going back far enough is as correct as going back
precisely. It also saved us a whole data structure.

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
long as the caller's context lives. It does not spend the failure budget, which is
reserved for 5xx and transport errors. Nothing is throttled: the steady state is
never slowed down and no concurrency is given up. The retries are logged, so the
rate limiting stays visible instead of hidden.

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

Sometimes the same cap refuses outright instead of truncating. No retry can fix
that, so the error names the setting to lower instead of showing a bare protocol
code.

## Fetching concurrently breaks the order the chain has

**The chain:** blocks have an order, and it is the only thing that makes a
checkpoint meaningful.

**The consequence:** eight workers return whatever finishes first. Publishing in
arrival order would put block 340 into Kafka before 320, and a checkpoint written
after 340 would claim work that never happened.

**What we do:** workers fetch contiguous ranges instead of single blocks, so what
arrives out of order is whole chunks. The
[sequencer](internal/pipeline/sequencer.go#L31) holds a chunk until every block
before it has been released. It does no I/O, takes no lock and starts no
goroutine. Ordering is what breaks most easily under concurrency, so it lives
where it can be tested without any.

## A checkpoint may only move after the broker has the events

**The consequence:** this is the entire at-least-once guarantee, and it only
reads correctly in one direction. Save first and an interruption in between
leaves the checkpoint claiming work that was never published — those transactions
are never looked at again.

**What we do:** [`publish`](internal/pipeline/pipeline.go#L306) matches,
publishes, waits for the broker's acknowledgement, and only then saves. That save
gets a context that survives shutdown. Giving up there would republish the whole
batch on the next start, and that is a duplicate we chose to create rather than
one we could not avoid.

## Local disk does not survive a spot instance

**The consequence:** the machine and its disk go away together. A checkpoint
written to a file is gone with the instance that wrote it.

**What we do:** the checkpoint lives in a **compacted Kafka topic**, keyed by
chain id — [`checkpoint/kafka.go`](internal/checkpoint/kafka.go). Compaction
keeps only the latest record per key, so it stays one record per chain however
long the service runs. Kafka is already a dependency and already durable, so this
adds nothing to deploy and does not violate the brief's "don't add databases".

Deciding there is *no* checkpoint is the dangerous direction. It sends the service
to the chain head and skips everything in between. So
[`Load`](internal/checkpoint/kafka.go#L64) finds out where the log ends before it
reads. An empty topic is proved empty, not guessed at from a read that has not
returned anything yet.

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

**Deduplicate on `hash` + `userId`.** Not on `hash` alone. One transaction between
two watched users produces two events, one per user, and both are correct. The
pair is the natural idempotency key. A restart, a reorg rewind and a retried batch
all republish the same pair.

**Order is per user, not global.** Events for one `userId` land on one partition
and arrive in the order they happened. Events for different users are on
different partitions and have no order between them. That is deliberate: global
ordering would mean one partition, and one partition would mean no parallel
consumption.

**Nothing is ever retracted.** If a block is published and then discarded by a
reorg, its events stay in the topic. Rewinding republishes the new chain, so a
transaction that survives the reorg is deduplicated by the pair above. One that
existed only on the discarded fork stays behind, as an event for something that no
longer happened.

`ConfirmDepth` is what makes this rare instead of routine: a block two deep on
mainnet is effectively settled. A system that could not tolerate it at all would
wait for finality instead of a fixed depth, or publish retractions. Both are
bigger designs than at-least-once needs, and at-least-once is what the brief
asks for.

## What each decision cost

Every decision above bought something. None of them was free, and a reader should
be able to challenge any of them without having to work out the price first.

| Decision | What it cost |
|---|---|
| Staying `ConfirmDepth` blocks behind the head | About 25 seconds of latency on mainnet. Arbitrum pays nothing — it runs at zero. |
| Rewinding a fixed 16 blocks instead of locating the fork | Up to 16 blocks re-emitted per reorg, and one data structure never written. |
| One acknowledgement per chunk rather than per block | The duplicate window on restart is the batch size: up to 20 blocks on mainnet, 100 on Arbitrum. |
| Writing the JSON-RPC client instead of using go-ethereum | 446 lines that are now ours to maintain, against 168 modules that were not. |
| Keeping the checkpoint in Kafka rather than on disk | 82 lines of metadata and offset plumbing, purely to know when to stop reading. |
| Eight workers rather than sixteen | Ten percent of the throughput ceiling, for 100 MB less resident memory. |
| Polling instead of `eth_subscribe` | Up to one poll interval of latency: 3s on mainnet, 250ms on Arbitrum. |
| The service creating its own topics | It needs `CreateTopics` on the broker, which a locked-down cluster may not grant. |
| `amount` as a decimal string | Every consumer has to parse it. A JSON number would not survive the values. |
| At-least-once without retractions | A deeply reorged block leaves events for transactions that no longer happened. |

Two of these are worth questioning. The confirmation delay is what we pay to avoid
publishing work the chain then discards; on a chain that settles differently it
should be a different number. The duplicate window is what we pay for an
acknowledgement barrier wide enough not to cost throughput. Narrowing it to one
block is a single line of code, and it is measurably slower for a guarantee the
brief does not ask for.

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

Arbitrum's free endpoint is tighter still. It serves about one hundred-block
request every 15–20 seconds, and adding workers makes it worse: the 429s grow with
every worker while the work completed falls to zero.

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

# Verify it yourself

Every claim in this file can be checked in a few minutes, on your machine, with
the numbers coming from your run rather than from mine.

## 1. It watches 500,000 addresses and publishes their transactions

```sh
make up && make dataset && make run
```

In another terminal:

```sh
make verify
```

Look for `"addresses": 500000` under `watching`, and `eventsPublished` under
`progress` matching the Kafka count on the line below it. Those two numbers come
from different places — the service and the broker — and they agree.

`blocksBehind` should sit at `2`, which is exactly `ConfirmDepth`: as close to the
head as the service is allowed to be.

Only the seeded wallets produce events. The other 499,980 addresses are random,
so they correctly never match anything on a live chain.

## 2. The same binary runs on an L2 and configures itself

```sh
RPC_URL=https://arb1.arbitrum.io/rpc make run
```

The startup line will read `chain=arbitrum poll=250ms batch=100 confirmDepth=0`.
None of that was passed in. The service asked the chain for its id, got 42161 and
picked the measured profile. `confirmDepth=0` is the one that comes from how the
chain works rather than how fast it is: Arbitrum has a single sequencer, so there
is no competing block to wait out.

Watch the block ranges in the log. The batch is configured at 100 and the real
ones are one to three blocks, because at the head there is nothing to batch.

## 3. It survives being killed without warning

While it is running, note the last block in the log, then:

```sh
pkill -9 chainwatch
make run
```

It will log `resuming from checkpoint` and start at that block plus one. Not at
the chain head, and not at `START_BLOCK`. Kill it as rudely as you like: the
checkpoint only ever moves after Kafka has acknowledged the events before it.

## 4. Rate limiting slows it down but never stops it

Leave it stopped for a minute, then start it again. It resumes a few thousand
blocks behind, which puts it into catch-up and asks for hundred-block batches.
A public endpoint will refuse some of those:

```
WARN rate limited, retrying attempt=5 backoff=1.534793811s
WARN rate limited, retrying attempt=6 backoff=1.209745772s
```

It keeps going. The backoff grows and every value is different, so eight workers
never retry in lockstep. Nothing is throttled to avoid the limit, and nothing is
dropped because of it.

## 5. The tests

```sh
make test-race    # everything, offline and deterministic
make test-live    # against real nodes and a real broker, both chains
```

The throughput measurement prints what the pipeline does when the node is not the
constraint:

```sh
go test -run TestThroughput -v ./internal/pipeline/
```

---

# Testing

```sh
make test-race        # everything, offline and deterministic
make test-live        # against real nodes and a real broker, both chains
go test -short ./...  # skips the throughput measurement, ~5s
```

The offline suite talks to an `httptest` server, and the block fixtures are **real
blocks captured from mainnet and Arbitrum**. Between them they cover every
transaction type both chains currently produce, plus a contract creation. Invented
JSON would prove nothing here. The whole point is surviving what the chains
actually emit.

Two tests carry more weight than the rest:

**No gaps.** 500 blocks end to end, every one checked individually in the output.
A hole is the failure this service exists to prevent, and it is invisible unless
something counts.

**Survives a restart.** The pipeline is killed mid-run and a new one starts
against the same checkpoint with no configured start block. Every block is still
accounted for afterwards.

Verified again with a real `SIGKILL` against mainnet. The events were read back
out of Kafka instead of trusting the logs: 7,261 events across the expected
301-block range, with no block missing. Two blocks produced no events at all. Both
were checked against the chain: one holds nine transactions that touch none of the
watched addresses, and the other is empty. Publishing nothing there is correct.

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
the final state does: the walking skeleton before the concurrency, the checkpoint
before the reorg handling. Several `fix:` commits mark the points where a
measurement contradicted an assumption I had been confident about.

The same repository is on GitHub, private. Ask and I will grant access:

**https://github.com/wesleymassine/chainwatch**
