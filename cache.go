package tsmap

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Infinity = -1
	Expired  = 0
)

type CacheItem[v any] struct {
	Value      v
	Expiration time.Duration
}

func (i *CacheItem[v]) applyOptions(options ...ItemOptions[v]) {
	for _, option := range options {
		option(i)
	}
}

func (i *CacheItem[v]) NeverExpire() {
	i.Expiration = Infinity
}

func (i *CacheItem[v]) Delay(duration time.Duration) {
	i.Expiration += duration
}

func (i *CacheItem[v]) ExpireIt() {
	i.Expiration = Expired
}

const (
	// shutdown the goroutine
	shutdown = -1
	// stop the goroutine temporarily
	stop = 0
	// running start/restart the goroutine
	running = 1
)

type ScannerOptions[k comparable, v any] func(scanner *Scanner[k, v])

type Scanner[k comparable, v any] struct {
	toScan          *KVCache[k, v]
	Interval        time.Duration
	status          atomic.Int32
	stopStartSignal chan struct{}
	parentContext   context.Context
	canceler        context.CancelFunc
}

func (s *Scanner[k, v]) signal(c chan<- struct{}) {
	c <- struct{}{}
}

func (s *Scanner[k, v]) control() {
	go func() {
		for {
			select {
			case <-s.parentContext.Done():
				return
			default:
				switch s.status.Load() {
				case shutdown:
					s.canceler()
					return
				case stop:
					s.signal(s.stopStartSignal)
				loop:
					for {
						switch s.status.Load() {
						case shutdown:
							s.canceler()
							return
						case stop:
						case running:
							s.signal(s.stopStartSignal)
							break loop
						}
					}
				}
			}
			// time.Sleep(time.Nanosecond)
		}
	}()
}

func (s *Scanner[k, v]) Go() {
	s.status.Store(running)
	ticker := time.NewTicker(s.Interval)
	s.control()
	var block sync.Mutex
	go func(ctx context.Context) {
		for {
			ctx2, canceler := context.WithCancel(context.Background()) //nolint:govet
			select {
			case <-ticker.C:
				block.Lock() // if the last scan is not finished, the current scan will be blocked
				go func(ctx2 context.Context) {
					s.toScan.internal.IterRemoveIfWithCanceler(func(k k, v *CacheItem[v]) bool {
						return s.toScan.checkExpire(v)
					}, ctx2)
					block.Unlock()
				}(ctx2)
			case <-ctx.Done():
				canceler()
				return
			case <-s.stopStartSignal:
				canceler()
				select {
				case <-s.stopStartSignal: // wait for the second stopStartSignal signal
				case <-ctx.Done():
					return
				}
			}
		}
	}(s.parentContext)
}

func (s *Scanner[k, v]) Stop() {
	s.status.Store(stop)
}

func (s *Scanner[k, v]) Shutdown() {
	s.status.Store(shutdown)
}

func (s *Scanner[k, v]) Running() {
	s.status.Store(running)
}

func (s *Scanner[k, v]) applyOptions(options ...ScannerOptions[k, v]) {
	for _, option := range options {
		option(s)
	}
}

var DefaultScanInterval = 50 * time.Millisecond

func NewScanner[k comparable, v any](toScan *KVCache[k, v]) *Scanner[k, v] {
	c, canceler := context.WithCancel(context.Background())
	s := &Scanner[k, v]{
		toScan:          toScan,
		Interval:        DefaultScanInterval,
		status:          atomic.Int32{},
		stopStartSignal: make(chan struct{}),
		parentContext:   c,
		canceler:        canceler,
	}

	return s
}

type KVCache[k comparable, v any] struct {
	removeWhenGettingAnExpiredItem bool
	createdAt                      time.Time
	internal                       *Map[k, *CacheItem[v]]
	scanner                        *Scanner[k, v]
	scannerOnce                    sync.Once
}

// AllWithExpired return all items, including expired items
func (c *KVCache[k, v]) AllWithExpired() []Pair[k, *CacheItem[v]] {
	return c.internal.Pairs()
}

// All return all valid items, without expired items
func (c *KVCache[k, v]) All() []Pair[k, *CacheItem[v]] {
	p := c.internal.Pairs()
	res := make([]Pair[k, *CacheItem[v]], 0, len(p))
	for i := range p {
		if !c.checkExpire(p[i].Value()) {
			res = append(res, p[i])
		}
	}
	return res
}

func (c *KVCache[k, v]) LenWithExpired() int {
	return c.internal.Len()
}

func (c *KVCache[k, v]) Len() int {
	l := 0
	i := c.internal.Iter()
	for i.Valid() {
		current := i.Next()
		if !c.checkExpire(current.Value()) {
			l++
		}
	}
	return l
}

var DefaultShards = 200

func NewKVCache[k comparable, v any]() *KVCache[k, v] {
	c := &KVCache[k, v]{
		createdAt: time.Now(),
		internal:  NewTSMap[k, *CacheItem[v]](DefaultShards),
	}
	return c
}

func NewKVCacheWithShard[k comparable, v any](shard int) *KVCache[k, v] {
	c := &KVCache[k, v]{
		createdAt: time.Now(),
		internal:  NewTSMap[k, *CacheItem[v]](shard),
	}
	return c
}

func NewKVCacheWithOption[k comparable, v any](options ...CacheOptions[k, v]) *KVCache[k, v] {
	c := &KVCache[k, v]{
		createdAt: time.Now(),
		internal:  NewTSMap[k, *CacheItem[v]](DefaultShards),
	}
	c.applyOptions(options...)
	return c
}

// Clone return a new KVCache with the same options and items
// the scanner will not be cloned
func (c *KVCache[k, v]) Clone() *KVCache[k, v] {
	return &KVCache[k, v]{
		removeWhenGettingAnExpiredItem: c.removeWhenGettingAnExpiredItem,
		createdAt:                      c.createdAt,
		internal:                       c.internal.Clone(),
		scanner:                        nil,
		scannerOnce:                    sync.Once{},
	}
}

type (
	CacheOptions[k comparable, v any] func(cache *KVCache[k, v])
	ItemOptions[v any]                func(cache *CacheItem[v])
)

// checkExpireWithNow if expired, return true, else return false
func (c *KVCache[k, v]) checkExpireWithNow(value *CacheItem[v], now time.Time) bool {
	switch value.Expiration {
	case Expired:
		return true
	case Infinity:
		return false
	default:
		return now.After(c.createdAt.Add(value.Expiration))
	}
}

// checkExpire if expired, return true, else return false
func (c *KVCache[k, v]) checkExpire(value *CacheItem[v]) bool {
	return c.checkExpireWithNow(value, time.Now())
}

func (c *KVCache[k, v]) CreatedAt() time.Time {
	return c.createdAt
}

func (c *KVCache[k, v]) SetCreatedTime(createdAt time.Time) {
	c.createdAt = createdAt
}

func (c *KVCache[k, v]) RemoveWhenGettingAnExpiredItem() {
	c.removeWhenGettingAnExpiredItem = true
}

func (c *KVCache[k, v]) applyOptions(options ...CacheOptions[k, v]) {
	for _, option := range options {
		option(c)
	}
}

func (c *KVCache[k, v]) Set(key k, value v, options ...ItemOptions[v]) {
	c.Insert(key, value, options...)
}

func (c *KVCache[k, v]) Insert(key k, value v, options ...ItemOptions[v]) {
	cacheItem := &CacheItem[v]{
		Value:      value,
		Expiration: Infinity,
	}
	cacheItem.applyOptions(options...)
	c.internal.Set(key, cacheItem)
}

func (c *KVCache[k, v]) SetWithExpiration(key k, value v, expiration time.Duration, options ...ItemOptions[v]) {
	c.InsertWithExpiration(key, value, expiration, options...)
}

func (c *KVCache[k, v]) InsertWithExpiration(key k, value v, expiration time.Duration, options ...ItemOptions[v]) {
	cacheItem := &CacheItem[v]{
		Value:      value,
		Expiration: expiration,
	}
	cacheItem.applyOptions(options...)
	c.internal.Set(key, cacheItem)
}

// Get return the cache item and a bool value to indicate whether the item is found.
func (c *KVCache[k, v]) Get(key k) (value *CacheItem[v], ok bool) {
	get, ok := c.internal.Get(key)
	if ok {
		if c.checkExpire(get) {
			if c.removeWhenGettingAnExpiredItem {
				c.internal.Remove(key)
			}
			return nil, false
		}
		return get, true
	}
	return nil, false
}

// GetValue return the value and a bool value to indicate whether the item is found.
// if the item is expired or not exist, the value will be the zero value of the type.
func (c *KVCache[k, v]) GetValue(key k) (value v, ok bool) {
	get, ok := c.internal.Get(key)
	if ok {
		return get.Value, ok
	}
	var zero v
	return zero, false
}

func (c *KVCache[k, v]) Delete(key k) {
	c.internal.Remove(key)
}

func (c *KVCache[k, v]) SetExpired(key k) {
	item, ok := c.Get(key)
	if ok {
		item.ExpireIt()
	}
}

func (c *KVCache[k, v]) DeleteExpired() {
	now := time.Now()
	c.internal.IterRemoveIf(func(key k, value *CacheItem[v]) bool {
		return c.checkExpireWithNow(value, now)
	})
}

func (c *KVCache[k, v]) DeleteExpiredAccurate() {
	c.internal.IterRemoveIf(func(key k, value *CacheItem[v]) bool {
		return c.checkExpireWithNow(value, time.Now())
	})
}

func WithExpiration[v any](expiration time.Duration) ItemOptions[v] {
	return func(cache *CacheItem[v]) {
		cache.Expiration = expiration
	}
}

// StartScanning start the goroutine
func (c *KVCache[k, v]) StartScanning(options ...ScannerOptions[k, v]) {
	c.scannerOnce.Do(func() {
		c.scanner = NewScanner(c)
		runtime.SetFinalizer(c, func(c *KVCache[k, v]) {
			c.ShutdownScanning()
		})
	})
	c.scanner.applyOptions(options...)
	c.scanner.Go()
}

func (c *KVCache[k, v]) InitScanning(options ...ScannerOptions[k, v]) {
	c.scannerOnce.Do(func() {
		c.scanner = NewScanner(c)
		runtime.SetFinalizer(c, func(c *KVCache[k, v]) {
			c.ShutdownScanning()
		})
	})
	c.scanner.applyOptions(options...)
}

// StopScanning stop the goroutine
func (c *KVCache[k, v]) StopScanning() {
	c.scanner.Stop()
}

// ShutdownScanning shutdown the goroutine
func (c *KVCache[k, v]) ShutdownScanning() {
	c.scanner.Shutdown()
}

func (c *KVCache[k, v]) SetScanInterval(interval time.Duration) {
	c.scanner.Interval = interval
}

func (c *KVCache[k, v]) RestartScanning() {
	c.scanner.Running()
}
