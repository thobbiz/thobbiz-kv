package main

import (
	"fmt"
	"sync"
	"testing"

	models "github.com/thobbiz/thobbixDB/definitions"
)

func openTestStore(b *testing.B) *models.KVStore {
	b.Helper()
	kvStore, err := models.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	return kvStore
}

func generatePairs(n int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Appendf(keys[i], "key-%d", i)
		values[i] = fmt.Appendf(values[i], "value-%d", i)
	}
	return keys, values
}

// ── Sequential Writes
func BenchmarkPut(b *testing.B) {
	store := openTestStore(b)
	defer store.Close()

	keys, values := generatePairs(b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSyncMapPut(b *testing.B) {
	var m sync.Map
	keys, values := generatePairs(b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Store(string(keys[i]), values[i])
	}
}

// ── Sequential Reads ─────────────────────────────────────────────────────────

func BenchmarkGet(b *testing.B) {
	store := openTestStore(b)
	defer store.Close()

	// pre-populate
	n := 10_000
	keys, values := generatePairs(n)
	for i := range n {
		if err := store.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := store.Get(keys[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSyncMapGet(b *testing.B) {
	var m sync.Map
	n := 10_000
	keys, values := generatePairs(n)
	for i := range n {
		m.Store(string(keys[i]), values[i])
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		m.Load(string(keys[i%n]))
	}
}

// ── Concurrent Reads ─────────────────────────────────────────────────────────

func BenchmarkConcurrentGet(b *testing.B) {
	store := openTestStore(b)
	defer store.Close()

	n := 10_000
	keys, values := generatePairs(n)
	for i := range n {
		if err := store.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := store.Get(keys[i%n]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkSyncMapConcurrentGet(b *testing.B) {
	var m sync.Map
	n := 10_000
	keys, values := generatePairs(n)
	for i := range n {
		m.Store(string(keys[i]), values[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Load(string(keys[i%n]))
			i++
		}
	})
}
