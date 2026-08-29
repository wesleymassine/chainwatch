package addresses

import (
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseAddress(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  string
		isErr bool
	}{
		{name: "with prefix", in: "0x28c6c06298d514db089934071355e5743bf21d60", want: "0x28c6c06298d514db089934071355e5743bf21d60"},
		{name: "without prefix", in: "28c6c06298d514db089934071355e5743bf21d60", want: "0x28c6c06298d514db089934071355e5743bf21d60"},
		{name: "uppercase is normalised", in: "0x28C6C06298D514DB089934071355E5743BF21D60", want: "0x28c6c06298d514db089934071355e5743bf21d60"},
		{name: "too short", in: "0x28c6", isErr: true},
		{name: "too long", in: "0x28c6c06298d514db089934071355e5743bf21d6000", isErr: true},
		{name: "not hex", in: "0xzzc6c06298d514db089934071355e5743bf21d60", isErr: true},
		{name: "empty", in: "", isErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAddress([]byte(tt.in))
			if tt.isErr {
				if err == nil {
					t.Fatalf("ParseAddress(%q) = %s, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAddress(%q): %v", tt.in, err)
			}
			if got.String() != tt.want {
				t.Errorf("ParseAddress(%q) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	const a1 = "0x28c6c06298d514db089934071355e5743bf21d60"
	const a2 = "0x05ff6964d21e5dae3b1010d5ae0465b3c450f381"

	tests := []struct {
		name    string
		in      string
		want    int
		wantErr string
	}{
		{name: "with header", in: "userId,address\n1," + a1 + "\n2," + a2 + "\n", want: 2},
		{name: "without header", in: "1," + a1 + "\n2," + a2 + "\n", want: 2},
		{name: "blank lines are skipped", in: "1," + a1 + "\n\n\n2," + a2 + "\n", want: 2},
		{name: "header after a blank line", in: "\n\nuserId,address\n1," + a1 + "\n", want: 1},
		{name: "no trailing newline", in: "1," + a1, want: 1},
		{name: "missing comma", in: "1" + a1, wantErr: "line 1"},
		{name: "userId is not a number", in: "abc," + a1, wantErr: "bad userId"},
		{name: "malformed address", in: "1,0xnope", wantErr: "line 1"},
		{name: "duplicate address", in: "1," + a1 + "\n2," + a1, wantErr: "already mapped to user 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set, err := Load(strings.NewReader(tt.in))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load(): %v", err)
			}
			if set.Len() != tt.want {
				t.Fatalf("Len() = %d, want %d", set.Len(), tt.want)
			}
		})
	}
}

func TestLookup(t *testing.T) {
	const watched = "0x28c6c06298d514db089934071355e5743bf21d60"
	set, err := Load(strings.NewReader("userId,address\n7," + watched + "\n"))
	if err != nil {
		t.Fatal(err)
	}

	addr, _ := ParseAddress([]byte(watched))
	if id, ok := set.Lookup(addr); !ok || id != 7 {
		t.Errorf("Lookup(watched) = %d, %v; want 7, true", id, ok)
	}

	other, _ := ParseAddress([]byte("0x0000000000000000000000000000000000000001"))
	if id, ok := set.Lookup(other); ok {
		t.Errorf("Lookup(unwatched) = %d, true; want 0, false", id)
	}
}

// TestLoadFullDataset runs against the real generated file when it exists, and
// reports what 500k addresses actually cost. Estimates about memory are worth
// very little; this prints the number.
func TestLoadFullDataset(t *testing.T) {
	const path = "../../testdata/addresses.csv"
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("run `make dataset` first: %v", err)
	}
	defer f.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()
	set, err := Load(f)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	heap := after.HeapAlloc - before.HeapAlloc
	t.Logf("loaded %d addresses in %v, %.1f MB resident (%.0f bytes/address)",
		set.Len(), elapsed.Round(time.Millisecond),
		float64(heap)/(1<<20), float64(heap)/float64(set.Len()))
}

func BenchmarkLookup(b *testing.B) {
	f, err := os.Open("../../testdata/addresses.csv")
	if err != nil {
		b.Skipf("run `make dataset` first: %v", err)
	}
	defer f.Close()
	set, err := Load(f)
	if err != nil {
		b.Fatal(err)
	}

	// A hit and a miss: the matcher does far more misses than hits, since most
	// transactions in a block belong to nobody we watch.
	hit, _ := ParseAddress([]byte("0x28c6c06298d514db089934071355e5743bf21d60"))
	miss, _ := ParseAddress([]byte("0x0000000000000000000000000000000000000001"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Lookup(hit)
		set.Lookup(miss)
	}
}
