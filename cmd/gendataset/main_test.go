package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "addresses.csv")
	const n = 1000
	if err := writeFile(path, n, 1); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Scan()
	if got := sc.Text(); got != "userId,address" {
		t.Fatalf("header = %q, want %q", got, "userId,address")
	}

	rows := 0
	for sc.Scan() {
		id, addr, ok := strings.Cut(sc.Text(), ",")
		if !ok {
			t.Fatalf("row %d is not userId,address: %q", rows+1, sc.Text())
		}
		if len(addr) != 42 || !strings.HasPrefix(addr, "0x") {
			t.Fatalf("row %s: malformed address %q", id, addr)
		}
		rows++
		// The seeds come first so they are easy to find in a 500k-row file.
		if rows <= len(seeds) && addr != seeds[rows-1] {
			t.Fatalf("row %d = %s, want seed %s", rows, addr, seeds[rows-1])
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != n {
		t.Fatalf("wrote %d rows, want %d", rows, n)
	}
}

func TestGenerateIsReproducible(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.csv"), filepath.Join(dir, "b.csv")
	for _, p := range []string{a, b} {
		if err := writeFile(p, 5000, 42); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("same seed produced a different dataset")
	}
}

func BenchmarkGenerate(b *testing.B) {
	const n = 500_000
	b.SetBytes(int64(n))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := generate(io.Discard, n, 1); err != nil {
			b.Fatal(err)
		}
	}
}
