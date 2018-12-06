package emap

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type TTLMap struct {
	running bool

	// store map default cap size, and thold of trigger freespace.
	capacity     int
	maxFreeCount int
	// if size > 0; open lru feature
	lruMaxSize int

	// save meta data
	store       map[interface{}]*mapElement
	lruCache    *LRU
	expiryTimes *MinHeap

	// lock
	mutex *sync.RWMutex

	// onExpire callback will be called when element is expired
	onExpire Callback
}

type mapElement struct {
	key    interface{}
	value  interface{}
	heapEl *Element
}

type TTLMapOption func(m *TTLMap) error

// NewMap thread safe
func NewMap(option Options, opts ...TTLMapOption) (*TTLMap, error) {
	if option.capacityBucket <= 0 {
		return nil, errors.New("Capacity should be > 0")
	}

	m := &TTLMap{
		running:      true,
		capacity:     option.capacityBucket,
		maxFreeCount: defaultMaxFreeCount,
		store:        make(map[interface{}]*mapElement, option.capacityBucket),
		expiryTimes:  NewMinHeap(),
		mutex:        new(sync.RWMutex),
	}

	if option.lruMaxSize > 0 {
		bucketLruSize := option.lruMaxSize / option.poolSize
		cache, err := newLRU(bucketLruSize, nil)
		if err != nil {
			return m, err
		}

		m.lruCache = cache
	}

	for _, o := range opts {
		if err := o(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *TTLMap) StartActiveGC(d time.Duration) error {
	for m.running {
		time.Sleep(d)
		m.mutex.RLock()
		if len(m.store) >= m.capacity {
			m.freeSpace(m.maxFreeCount)
		}
		m.mutex.RUnlock()
	}
	return nil
}

func (m *TTLMap) Stop() {
	m.running = false
}

func (m *TTLMap) Set(key string, value interface{}, ttlSeconds int) error {
	expiryTime, err := m.toEpochSeconds(ttlSeconds)
	if err != nil {
		return err
	}
	if m.mutex != nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()
	}
	return m.set(key, value, expiryTime)
}

func (m *TTLMap) Get(key interface{}) (interface{}, bool) {
	value, mapEl, expired := m.lockNGet(key)
	if mapEl == nil {
		return nil, false
	}
	if expired {
		m.lockNDel(mapEl)
		return nil, false
	}
	return value, true
}

func (m *TTLMap) Del(key interface{}) error {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}

	mapEl, ok := m.store[key]
	if !ok {
		return errors.New("not found")
	}

	m.del(mapEl)
	return nil
}

func (m *TTLMap) Len() int {
	// don't need lock, hmap struct contains count field.
	return len(m.store)
}

func (m *TTLMap) Range(f func(k interface{}, v interface{})) {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}

	for k, v := range m.store {
		f(k, v)
	}
}

func (m *TTLMap) Increment(key interface{}, value int, ttlSeconds int) (int, error) {
	expiryTime, err := m.toEpochSeconds(ttlSeconds)
	if err != nil {
		return 0, err
	}

	if m.mutex != nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()
	}

	mapEl, expired := m.get(key)
	if mapEl == nil || expired {
		m.set(key, value, expiryTime)
		return value, nil
	}

	currentValue, ok := mapEl.value.(int)
	if !ok {
		return 0, fmt.Errorf("Expected existing value to be integer, got %T", mapEl.value)
	}

	currentValue += value
	m.set(key, currentValue, expiryTime)
	return currentValue, nil
}

// GetInt get value and transport interface{} to int. return int value, exists_bool, type error
func (m *TTLMap) GetInt(key interface{}) (int, bool, error) {
	valueI, exists := m.Get(key)
	if !exists {
		return 0, false, nil
	}
	value, ok := valueI.(int)
	if !ok {
		return 0, false, fmt.Errorf("Expected existing value to be integer, got %T", valueI)
	}
	return value, true, nil
}

func (m *TTLMap) GetString(key interface{}) (string, bool, error) {
	valueI, exists := m.Get(key)
	if !exists {
		return "", false, nil
	}
	value, ok := valueI.(string)
	if !ok {
		return "", false, fmt.Errorf("Expected existing value to be string, got %T", valueI)
	}
	return value, true, nil
}

func (m *TTLMap) set(key interface{}, value interface{}, expiryTime int) error {
	if mapEl, ok := m.store[key]; ok {
		mapEl.value = value
		m.expiryTimes.UpdateEl(mapEl.heapEl, expiryTime)
		return nil
	}

	// gc
	if len(m.store) >= m.capacity {
		m.freeSpace(m.maxFreeCount)
	}

	// expire heap
	heapEl := &Element{
		Priority: expiryTime,
	}
	mapEl := &mapElement{
		key:    key,
		value:  value,
		heapEl: heapEl,
	}
	heapEl.Value = mapEl
	m.store[key] = mapEl
	m.expiryTimes.PushEl(heapEl)

	// lru cache
	if m.lruCache != nil {
		ent, isFull := m.lruCache.add(key, mapEl)
		if isFull {
			m.del(ent.value.(*mapElement))
		}
	}
	return nil
}

func (m *TTLMap) lockNGet(key interface{}) (value interface{}, mapEl *mapElement, expired bool) {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}

	mapEl, expired = m.get(key)
	value = nil
	if mapEl != nil {
		value = mapEl.value
	}
	return value, mapEl, expired
}

func (m *TTLMap) get(key interface{}) (*mapElement, bool) {
	mapEl, ok := m.store[key]
	if !ok {
		return nil, false
	}

	now := int(time.Now().Unix())
	expired := mapEl.heapEl.Priority <= now

	// lru cache
	if m.lruCache != nil {
		m.lruCache.get(key)
	}

	return mapEl, expired
}

func (m *TTLMap) lockNDel(mapEl *mapElement) {
	if m.mutex != nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		// Map element could have been updated. Now that we have a lock
		// retrieve it again and check if it is still expired.
		var ok bool
		if mapEl, ok = m.store[mapEl.key]; !ok {
			return
		}

		now := int(time.Now().Unix())
		if mapEl.heapEl.Priority > now {
			return
		}
	}
	m.del(mapEl)
}

func (m *TTLMap) del(mapEl *mapElement) {
	if m.onExpire != nil {
		m.onExpire(mapEl.key, mapEl.value)
	}

	delete(m.store, mapEl.key)
	m.expiryTimes.RemoveEl(mapEl.heapEl)

	// lru cache
	if m.lruCache != nil {
		m.lruCache.remove(mapEl.key)
	}
}

func (m *TTLMap) delStoreOnly(mapEl *mapElement) {
	if m.onExpire != nil {
		m.onExpire(mapEl.key, mapEl.value)
	}

	delete(m.store, mapEl.key)

	// lru cache
	if m.lruCache != nil {
		m.lruCache.remove(mapEl.key)
	}
}

func (m *TTLMap) freeSpace(count int) {
	removed := m.removeExpired(count)
	if removed >= count {
		return
	}
}

func (m *TTLMap) removeExpired(iterations int) int {
	removed := 0
	now := int(time.Now().Unix())
	for i := 0; i < iterations; i += 1 {
		if len(m.store) == 0 {
			break
		}
		heapEl := m.expiryTimes.PeekEl()
		if heapEl.Priority > now {
			break
		}
		m.expiryTimes.PopEl()
		mapEl := heapEl.Value.(*mapElement)
		m.delStoreOnly(mapEl)
		removed += 1
	}
	return removed
}

// removeLastUsed force remove item, contains un expire item.
func (m *TTLMap) removeLastUsed(iterations int) {
	for i := 0; i < iterations; i += 1 {
		if len(m.store) == 0 {
			return
		}
		heapEl := m.expiryTimes.PopEl()
		mapEl := heapEl.Value.(*mapElement)
		m.delStoreOnly(mapEl)
	}
}

func (m *TTLMap) toEpochSeconds(ttlSeconds int) (int, error) {
	if ttlSeconds <= 0 {
		return 0, fmt.Errorf("ttlSeconds should be >= 0, got %d", ttlSeconds)
	}
	return int(time.Now().Add(time.Second * time.Duration(ttlSeconds)).Unix()), nil
}

type Callback func(key interface{}, el interface{})

// CallOnExpire will call this callback on expiration of store
func CallOnExpire(cb Callback) TTLMapOption {
	return func(m *TTLMap) error {
		m.onExpire = cb
		return nil
	}
}
