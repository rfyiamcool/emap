package emap

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type TTLMapOption func(m *TTLMap) error

type TTLMap struct {
	running      bool
	capacity     int
	maxFreeCount int
	elements     map[string]*mapElement
	expiryTimes  *MinHeap
	mutex        *sync.RWMutex

	// onExpire callback will be called when element is expired
	onExpire Callback
}

type mapElement struct {
	key    string
	value  interface{}
	heapEl *Element
}

// NewMap thread safe
func NewMap(capacity int, opts ...TTLMapOption) (*TTLMap, error) {
	if capacity <= 0 {
		return nil, errors.New("Capacity should be > 0")
	}

	m := &TTLMap{
		running:      true,
		capacity:     capacity,
		maxFreeCount: defaultMaxFreeCount,
		elements:     make(map[string]*mapElement),
		expiryTimes:  NewMinHeap(),
		mutex:        new(sync.RWMutex),
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
		if len(m.elements) >= m.capacity {
			m.freeSpace(m.maxFreeCount)
		}
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

func (m *TTLMap) Len() int {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}
	return len(m.elements)
}

func (m *TTLMap) Get(key string) (interface{}, bool) {
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

func (m *TTLMap) Del(key string) error {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}

	mapEl, ok := m.elements[key]
	if !ok {
		return errors.New("not found")
	}

	m.del(mapEl)
	return nil
}

func (m *TTLMap) Range(f func(k string, v interface{})) {
	if m.mutex != nil {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
	}

	for k, v := range m.elements {
		f(k, v)
	}
}

func (m *TTLMap) Increment(key string, value int, ttlSeconds int) (int, error) {
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
func (m *TTLMap) GetInt(key string) (int, bool, error) {
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

func (m *TTLMap) GetString(key string) (string, bool, error) {
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

func (m *TTLMap) set(key string, value interface{}, expiryTime int) error {
	if mapEl, ok := m.elements[key]; ok {
		mapEl.value = value
		m.expiryTimes.UpdateEl(mapEl.heapEl, expiryTime)
		return nil
	}

	if len(m.elements) >= m.capacity {
		m.freeSpace(m.maxFreeCount)
	}
	heapEl := &Element{
		Priority: expiryTime,
	}
	mapEl := &mapElement{
		key:    key,
		value:  value,
		heapEl: heapEl,
	}
	heapEl.Value = mapEl
	m.elements[key] = mapEl
	m.expiryTimes.PushEl(heapEl)
	return nil
}

func (m *TTLMap) lockNGet(key string) (value interface{}, mapEl *mapElement, expired bool) {
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

func (m *TTLMap) get(key string) (*mapElement, bool) {
	mapEl, ok := m.elements[key]
	if !ok {
		return nil, false
	}

	now := int(time.Now().Unix())
	expired := mapEl.heapEl.Priority <= now
	return mapEl, expired
}

func (m *TTLMap) lockNDel(mapEl *mapElement) {
	if m.mutex != nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		// Map element could have been updated. Now that we have a lock
		// retrieve it again and check if it is still expired.
		var ok bool
		if mapEl, ok = m.elements[mapEl.key]; !ok {
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

	delete(m.elements, mapEl.key)
	m.expiryTimes.RemoveEl(mapEl.heapEl)
}

func (m *TTLMap) freeSpace(count int) {
	removed := m.removeExpired(count)
	if removed >= count {
		return
	}

	m.removeLastUsed(count - removed)
}

func (m *TTLMap) removeExpired(iterations int) int {
	removed := 0
	now := int(time.Now().Unix())
	for i := 0; i < iterations; i += 1 {
		if len(m.elements) == 0 {
			break
		}
		heapEl := m.expiryTimes.PeekEl()
		if heapEl.Priority > now {
			break
		}
		m.expiryTimes.PopEl()
		mapEl := heapEl.Value.(*mapElement)
		delete(m.elements, mapEl.key)
		removed += 1
	}
	return removed
}

func (m *TTLMap) removeLastUsed(iterations int) {
	for i := 0; i < iterations; i += 1 {
		if len(m.elements) == 0 {
			return
		}
		heapEl := m.expiryTimes.PopEl()
		mapEl := heapEl.Value.(*mapElement)
		delete(m.elements, mapEl.key)
	}
}

func (m *TTLMap) toEpochSeconds(ttlSeconds int) (int, error) {
	if ttlSeconds <= 0 {
		return 0, fmt.Errorf("ttlSeconds should be >= 0, got %d", ttlSeconds)
	}
	return int(time.Now().Add(time.Second * time.Duration(ttlSeconds)).Unix()), nil
}

type Callback func(key string, el interface{})

// CallOnExpire will call this callback on expiration of elements
func CallOnExpire(cb Callback) TTLMapOption {
	return func(m *TTLMap) error {
		m.onExpire = cb
		return nil
	}
}
