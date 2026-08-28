GO      ?= go
BINARY  ?= bin/chainwatch
COMPOSE ?= docker compose

.PHONY: build run test test-race vet fmt tidy up down topics dataset dist clean

build:
	$(GO) build -o $(BINARY) ./cmd/chainwatch

run: build
	./$(BINARY)

test:
	$(GO) test ./...

# The concurrency requirements make -race the test target that actually matters.
test-race:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

# tx-events is partitioned by userId; the checkpoint topic is compacted so only the
# latest block per chain survives.
topics:
	$(COMPOSE) exec redpanda rpk topic create tx-events -p 6
	$(COMPOSE) exec redpanda rpk topic create tx-checkpoints -p 1 -c cleanup.policy=compact

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
