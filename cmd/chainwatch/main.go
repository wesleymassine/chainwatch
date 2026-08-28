// Command chainwatch monitors Ethereum and compatible L2 chains for transactions
// involving a known set of addresses, and publishes the matches to Kafka.
package main

import (
	"log/slog"
	"os"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	log.Info("chainwatch: scaffold only, nothing wired up yet")
}
