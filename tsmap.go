package tsmap

import (
	"sync"

	"github.com/dolthub/maphash"
)

type Shard[k comparable, v any] struct {
	lock        sync.RWMutex
	InternalMap map[k]v
}

func (s *Shard[k, v]) Lock() {
	s.lock.Lock()
}

func (s *Shard[k, v]) Unlock() {
	s.lock.Unlock()
}

func (s *Shard[k, v]) RLock() {
	s.lock.RLock()
}

func (s *Shard[k, v]) RUnlock() {
	s.lock.RUnlock()
}

type Map[k comparable, v any] struct {
	m      []*Shard[k, v]
	shards int
	Hash   func(k) uint64
}

func (p *Map[k, v]) Load(key k) (value v, ok bool) {
	return p.Get(key)
}

func (p *Map[k, v]) Store(key k, value v) {
	p.Set(key, value)
}

var GoLimit = 70

func (p *Map[k, v]) Free() {
	if p.shards > GoLimit {
		p.goFree()
	} else {
		p.free()
	}
}

func (p *Map[k, v]) free() {
	for _, m := range p.m {
		m.Lock()
		m.InternalMap = make(map[k]v)
		m.Unlock()
	}
}

func (p *Map[k, v]) goFree() {
	wg := sync.WaitGroup{}
	wg.Add(p.shards)
	for _, m := range p.m {
		m := m
		go func() {
			m.Lock()
			m.InternalMap = make(map[k]v)
			m.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
}

func (p *Map[k, v]) Keys() []k {
	if p.shards < GoLimit {
		return p.keys()
	}
	return p.goKeys()
}

func (p *Map[k, v]) keys() []k {
	keys := make([]k, 0, len(p.m))
	for _, s := range p.m {
		s.RLock()
		for key := range s.InternalMap {
			keys = append(keys, key)
		}
		s.RUnlock()
	}
	return keys
}

func (p *Map[k, v]) goKeys() []k {
	mu := new(sync.Mutex)
	length := 0
	for i := range p.m {
		length += len(p.m[i].InternalMap)
	}
	keys := make([]k, 0, length)
	wg := new(sync.WaitGroup)
	wg.Add(p.shards)
	for _, m := range p.m {
		m := m
		go func() {
			m.RLock()
			defer wg.Done()
			keysi := make([]k, 0, len(m.InternalMap))
			for k := range m.InternalMap {
				keysi = append(keysi, k)
			}
			mu.Lock()
			keys = append(keys, keysi...)
			mu.Unlock()
			m.RUnlock()
		}()
	}
	wg.Wait()
	return keys
}

func (p *Map[k, v]) Set(key k, value v) {
	shard := p.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.InternalMap[key] = value
}

func (p *Map[k, v]) Get(key k) (value v, ok bool) {
	shard := p.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	value, ok = shard.InternalMap[key]
	return
}

// GetOrInit return the value itself(pointer)
// if already exists, return value and false
// if not, use init func and store the new value, return value and true
func (p *Map[k, v]) GetOrInit(key k, init func() v) (actual v, initialized bool) {
	shard := p.getShard(key)
	shard.RLock()
	value, ok := shard.InternalMap[key]
	shard.RUnlock()
	if ok {
		// load
		return value, true
	}

	shard.Lock()
	actual, ok = shard.InternalMap[key]
	if !ok {
		// init
		actual = init()
		shard.InternalMap[key] = actual
	}
	shard.Unlock()
	return
}

func (p *Map[k, v]) getShard(key k) *Shard[k, v] {
	i := p.Hash(key) & uint64(p.shards-1)
	return p.m[i]
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (p *Map[k, v]) LoadOrStore(key k, value v) (actual v, loaded bool) {
	value, initialized := p.GetOrInit(key, func() v {
		return value
	})
	if initialized {
		return value, false
	}
	return value, true
}

func NewTSMap[k comparable, v any](len int) *Map[k, v] {
	hasher := maphash.NewHasher[k]()
	m := &Map[k, v]{
		m:      make([]*Shard[k, v], len),
		shards: len,
		Hash:   hasher.Hash,
	}

	wg := sync.WaitGroup{}
	wg.Add(len)
	for i := 0; i < len; i++ {
		go func(i int) {
			m.m[i] = &Shard[k, v]{InternalMap: make(map[k]v)}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return m
}
