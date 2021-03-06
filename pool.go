package emap

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

const (
	defaultMaxFreeCount = 1
	defaultPoolSize     = 64
)

type Options struct {
	capacityBucket int
	maxFreeCount   int
	lruMaxSize     int

	// bucket idx
	idx int

	poolSize int
}

func NewOptions() *Options {
	return &Options{
		maxFreeCount:   defaultMaxFreeCount,
		lruMaxSize:     0,
		capacityBucket: 1000,

		poolSize: defaultPoolSize,
	}
}

type TTLMapPool struct {
	opt        Options
	buckets    []*TTLMap
	counter    int64
	gcInterval time.Duration

	sync.Mutex
}

func NewTTLMapPool(opt Options) (*TTLMapPool, error) {
	if opt.poolSize < 1 {
		return nil, errors.New("poolSize must > 1")
	}

	m := &TTLMapPool{
		gcInterval: 30 * time.Second,
	}
	m.opt = opt
	err := m.initPool()
	if err != nil {
		return m, err
	}

	return m, nil
}

func (p *TTLMapPool) initPool() error {
	p.buckets = make([]*TTLMap, p.opt.poolSize)
	for idx, _ := range p.buckets {
		opt := p.opt
		opt.idx = idx
		newM, err := NewMap(opt)
		if err != nil {
			return err
		}

		p.buckets[idx] = newM
	}

	return nil
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
	p.opt.maxFreeCount = val
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

func (p *TTLMapPool) Range(f func(k interface{}, v interface{})) error {
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

func (p *TTLMapPool) Reset() {
	p.StopGC()
	p.initPool()
	p.StartGC()
}

func (p *TTLMapPool) StopGC() {
	for _, bucket := range p.buckets {
		go bucket.Stop()
	}
}

func (p *TTLMapPool) StartGC() {
	for _, bucket := range p.buckets {
		go bucket.StartActiveGC(p.gcInterval)
	}
}

func (p *TTLMapPool) StartGCInterval(d time.Duration) error {
	if d.Seconds() < 1 {
		return errors.New("startGC interval must > 1 second")
	}

	// cover default value 30s
	p.gcInterval = d
	for _, bucket := range p.buckets {
		go bucket.StartActiveGC(d)
	}

	return nil
}

func (p *TTLMapPool) GetBucket(key string) *TTLMap {
	slot := hashToInt(key)
	return p.buckets[slot%p.opt.poolSize]
}

func hashToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
