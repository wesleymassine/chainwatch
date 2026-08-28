// Command gendataset writes the address dataset the service monitors: one CSV
// row per user, mapping a userId to an Ethereum address.
//
// The brief supplies 500,000 addresses but not the file itself, so we generate
// it. Most rows are random 20-byte addresses; the first rows are the verified
// active wallets in seeds.go, which is what makes a live demo produce output.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
)

func main() {
	n := flag.Int("n", 500_000, "number of addresses to generate")
	out := flag.String("o", "testdata/addresses.csv", "output file")
	seed := flag.Uint64("seed", 1, "PRNG seed, for a reproducible dataset")
	flag.Parse()

	if *n < len(seeds) {
		log.Fatalf("-n must be at least %d, the number of seed addresses", len(seeds))
	}
	if err := generate(*out, *n, *seed); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %d addresses to %s (%d seeded, %d random)\n",
		*n, *out, len(seeds), *n-len(seeds))
}

func generate(path string, n int, seed uint64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, "userId,address")
	for i, addr := range seeds {
		fmt.Fprintf(w, "%d,%s\n", i+1, addr)
	}

	// A fixed seed keeps the dataset reproducible, so a benchmark run today is
	// comparable with one from last week.
	rng := rand.New(rand.NewPCG(seed, seed))
	var addr [20]byte
	line := make([]byte, 0, 64)
	for i := len(seeds); i < n; i++ {
		binary.LittleEndian.PutUint64(addr[0:8], rng.Uint64())
		binary.LittleEndian.PutUint64(addr[8:16], rng.Uint64())
		binary.LittleEndian.PutUint32(addr[16:20], rng.Uint32())

		line = append(line[:0], fmt.Sprintf("%d,0x", i+1)...)
		line = hex.AppendEncode(line, addr[:])
		line = append(line, '\n')
		if _, err := w.Write(line); err != nil {
			return err
		}
	}
	return w.Flush()
}
