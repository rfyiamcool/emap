package emap

import (
	"container/list"
	"errors"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(key interface{}, value interface{})

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	size      int
	evictList *list.List
	items     map[interface{}]*list.Element
	onEvict   EvictCallback
}

type entry struct {
	key   interface{}
	value interface{}
}

// NewLRU constructs an LRU of the given size
func NewLRU(size int, onEvict EvictCallback) (*LRU, error) {
	if size <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	c := &LRU{
		size:      size,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvict:   onEvict,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *LRU) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value.(*entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// add set kv, return self, when cache is full, del old entry and return
func (c *LRU) add(key, value interface{}) (ent *entry, isFull bool) {
	// Check for existing item
	if item, ok := c.items[key]; ok {
		c.evictList.MoveToFront(item)
		item.Value.(*entry).value = value
		return item.Value.(*entry), false
	}

	// Add new item
	ent = &entry{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	isFull = c.evictList.Len() > c.size
	if isFull {
		ent, _ = c.removeOldest()
	}
	return ent, isFull
}

// Get looks up a key's value from the cache.
func (c *LRU) get(key interface{}) (value interface{}, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
func (c *LRU) contains(key interface{}) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU) remove(key interface{}) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRU) removeOldest() (ent *entry, ok bool) {
	item := c.evictList.Back()
	if item != nil {
		c.removeElement(item)
		ent = item.Value.(*entry)
		return ent, true
	}
	return nil, false
}

// GetOldest returns the oldest entry
func (c *LRU) getOldest() (key interface{}, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRU) keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

func (c *LRU) len() int {
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *LRU) removeOldestDirect() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
