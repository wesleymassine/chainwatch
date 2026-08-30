GO      ?= go
BINARY  ?= bin/chainwatch
COMPOSE ?= docker compose

KAFKA_BROKERS ?= localhost:9092
METRICS_ADDR  ?= localhost:9090
ADDRESSES     ?= testdata/addresses.csv
ETH_RPC       ?= https://ethereum-rpc.publicnode.com
ARB_RPC       ?= https://arb1.arbitrum.io/rpc

.PHONY: build run test test-race test-live verify vet fmt fmt-check tidy up down dataset dist clean

build:
	$(GO) build -o $(BINARY) ./cmd/chainwatch

run: build
	./$(BINARY)

test:
	$(GO) test ./...

# The concurrency requirements make -race the test target that actually matters.
test-race:
	$(GO) test -race -count=1 ./...

# One screen showing what the service has actually done: how many addresses it
# watches, how much chain it has read, and how many events reached Kafka.
verify:
	@echo "── dataset ─────────────────────────────────────────"
	@printf "   %s addresses in %s\n\n" "$$(( $$(wc -l < $(ADDRESSES)) - 1 ))" "$(ADDRESSES)"
	@echo "── service ─────────────────────────────────────────"
	@out=$$(curl -sf $(METRICS_ADDR)/metrics 2>/dev/null); \
	if [ -n "$$out" ]; then echo "$$out" | sed 's/^/   /'; \
	else echo "   not running (start it with make run)"; fi
	@echo
	@echo "── kafka ───────────────────────────────────────────"
	@$(COMPOSE) exec -T redpanda rpk topic describe tx-events -p 2>/dev/null \
		| awk 'NR>1 && NF>=6 {p++; n+=$$6} END {printf "   %d events across %d partitions\n", n, p}'
	@n=$$($(COMPOSE) exec -T redpanda rpk topic describe tx-events -p 2>/dev/null \
		| awk 'NR>1 && NF>=6 {n+=$$6} END {print n+0}'); \
	if [ "$$n" -gt 0 ]; then \
		$(COMPOSE) exec -T redpanda rpk topic consume tx-events -o start -n $$n -f '%v\n' 2>/dev/null > /tmp/chainwatch-events.json; \
		printf "   %s distinct users have events\n" "$$(grep -o '"userId":[0-9]*' /tmp/chainwatch-events.json | sort -u | wc -l | tr -d ' ')"; \
		echo "   (the seeded wallets; the rest of the dataset is random and never appears on chain)"; \
		echo; \
		echo "── sample event ────────────────────────────────────"; \
		head -1 /tmp/chainwatch-events.json | sed 's/^/   /'; \
	fi

# Everything above runs offline. This one talks to real nodes and a real broker,
# and it covers BOTH chains: the service once died on Arbitrum while every
# offline test stayed green, because nothing exercised the L2 end to end.
test-live:
	CHAINWATCH_KAFKA_BROKERS=$(KAFKA_BROKERS) $(GO) test -race -count=1 ./internal/publisher/... ./internal/checkpoint/...
	CHAINWATCH_LIVE_RPC=$(ETH_RPC) $(GO) test -race -count=1 -run TestLive ./internal/ethrpc/...
	CHAINWATCH_LIVE_RPC=$(ARB_RPC) $(GO) test -race -count=1 -run TestLive ./internal/ethrpc/...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

# What CI enforces: fails instead of rewriting.
fmt-check:
	@test -z "$$(gofmt -l ./cmd ./internal)" || { gofmt -l ./cmd ./internal; exit 1; }

tidy:
	$(GO) mod tidy

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

dataset:
	$(GO) run ./cmd/gendataset -n 500000 -o testdata/addresses.csv

# The submission bundle keeps .git: the commit history is part of what is delivered.
dist: clean
	mkdir -p dist
	git ls-files -z | xargs -0 tar -cf dist/tree.tar
	tar -rf dist/tree.tar .git
	mkdir -p dist/chainwatch && tar -xf dist/tree.tar -C dist/chainwatch && rm dist/tree.tar
	cd dist && zip -qr ../chainwatch_submission.zip chainwatch
	@echo "built chainwatch_submission.zip - rename to {first}_{last}_go_interview.zip"

clean:
	rm -rf bin dist chainwatch_submission.zip
