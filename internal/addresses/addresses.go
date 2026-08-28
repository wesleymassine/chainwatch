// Package addresses loads the watched address set: the mapping from an Ethereum
// address to the user who owns it.
package addresses

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
)

// Address is a 20-byte Ethereum address.
//
// It is an array rather than a slice so it can be a map key and be copied and
// compared without allocating. The matcher looks one up for every from and every
// to of every transaction, so this is the type on the hot path.
type Address [20]byte

func (a Address) String() string { return "0x" + hex.EncodeToString(a[:]) }

// ParseAddress decodes a hex address, with or without the 0x prefix.
func ParseAddress(b []byte) (Address, error) {
	var a Address
	if len(b) >= 2 && b[0] == '0' && (b[1] == 'x' || b[1] == 'X') {
		b = b[2:]
	}
	if len(b) != 2*len(a) {
		return a, fmt.Errorf("address has %d hex digits, want %d", len(b), 2*len(a))
	}
	if _, err := hex.Decode(a[:], b); err != nil {
		return a, fmt.Errorf("address %q: %w", b, err)
	}
	return a, nil
}

// Set is the watched address set.
//
// It is built once at startup and never written to again, which is why it needs
// no lock: every worker in the pipeline reads the same map concurrently.
type Set struct {
	m map[Address]uint64
}

// Load reads "userId,address" rows, one per line, with an optional header.
func Load(r io.Reader) (*Set, error) {
	s := &Set{m: make(map[Address]uint64)}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 || (n == 1 && bytes.HasPrefix(line, []byte("userId"))) {
			continue
		}
		i := bytes.IndexByte(line, ',')
		if i < 0 {
			return nil, fmt.Errorf("line %d: want userId,address, got %q", n, line)
		}
		id, err := strconv.ParseUint(string(line[:i]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: bad userId: %w", n, err)
		}
		addr, err := ParseAddress(line[i+1:])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		// The brief promises 500,000 unique addresses. A duplicate means the
		// dataset is wrong, and quietly keeping one of the two userIds would
		// send another user's deposits to the wrong account.
		if prev, ok := s.m[addr]; ok {
			return nil, fmt.Errorf("line %d: %s is already mapped to user %d", n, addr, prev)
		}
		s.m[addr] = id
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// Lookup returns the user who owns addr.
func (s *Set) Lookup(addr Address) (uint64, bool) {
	id, ok := s.m[addr]
	return id, ok
}

// Len returns how many addresses are being watched.
func (s *Set) Len() int { return len(s.m) }
