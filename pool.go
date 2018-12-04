package emap

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

const (
	defaultMaxFreeCount = 1
)

type TTLMapPool struct {
	buckets      []*TTLMap
	counter      int64
	poolSize     int
	maxFreeCount int
	sync.Mutex
}

func NewTTLMapPool(poolSize int, triggerExpireCount int) (*TTLMapPool, error) {
	m := &TTLMapPool{
		poolSize:     poolSize,
		maxFreeCount: defaultMaxFreeCount,
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

func (p *TTLMapPool) SetDefaultTTL(ttlSeconds int) error {
	if ttlSeconds < 1 {
		return errors.New("ttlSeconds must > 1")
	}

	// to do
	return nil
}

func (p *TTLMapPool) SetMaxFreeCount(val int) error {
	if val < 1 {
		return errors.New("maxFreeCount must > 1")
	}
	p.maxFreeCount = val
	for _, bucket := range p.buckets {
		bucket.maxFreeCount = val
	}

	return nil
}

func (p *TTLMapPool) Get(key string) (interface{}, bool) {
	bucket := p.GetBucket(key)
	return bucket.Get(key)
}

func (p *TTLMapPool) Set(key string, value interface{}, ttlSeconds int) error {
	bucket := p.GetBucket(key)
	return bucket.Set(key, value, ttlSeconds)
}

func (p *TTLMapPool) Del(key string) error {
	bucket := p.GetBucket(key)
	return bucket.Del(key)
}

func (p *TTLMapPool) Range(f func(k string, v interface{})) error {
	for _, bucket := range p.buckets {
		bucket.Range(f)
	}

	return nil
}

func (p *TTLMapPool) Len() int {
	c := 0
	for _, bucket := range p.buckets {
		c += bucket.Len()
	}
	return c
}

func (p *TTLMapPool) GC(args ...int) {
	maxFreeCount := 1
	if len(args) > 0 {
		maxFreeCount = args[0]
	}

	// block call once
	for _, bucket := range p.buckets {
		bucket.freeSpace(maxFreeCount)
	}
}

func (p *TTLMapPool) Stop() {
	for _, bucket := range p.buckets {
		go bucket.Stop()
	}
}

func (p *TTLMapPool) StartGC(d time.Duration) error {
	if d.Seconds() < 1 {
		return errors.New("startGC interval must > 1 second")
	}
	for _, bucket := range p.buckets {
		go bucket.StartActiveGC(d)
	}

	return nil
}

func (p *TTLMapPool) GetBucket(key string) *TTLMap {
	slot := hashToInt(key)
	return p.buckets[slot%p.poolSize]
}

func hashToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
