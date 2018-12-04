package emap

import (
	"hash/fnv"
	"sync"
)

type TTLMapPool struct {
	buckets  []*TTLMap
	counter  int64
	poolSize int
	sync.Mutex
}

func NewTTLMapPool(poolSize int, triggerExpireCount int) (*TTLMapPool, error) {
	m := &TTLMapPool{
		poolSize: poolSize,
	}
	m.buckets = make([]*TTLMap, poolSize)
	for idx, _ := range m.buckets {
		newM, err := NewMap(triggerExpireCount)
		if err != nil {
			return m, err
		}
		m.buckets[idx] = newM
	}

	return m, nil
}

func (p *TTLMapPool) Get() {
}

func (p *TTLMapPool) Set() {
}

func (p *TTLMapPool) Len() {
}

func (p *TTLMapPool) GC() {
}

func (p *TTLMapPool) Stop() {
}

func (p *TTLMapPool) StartGC() {
}

func (p *TTLMapPool) getBucket(key string) *TTLMap {
	slot := hashToInt(key)
	return p.buckets[slot%p.poolSize]
}

func hashToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
