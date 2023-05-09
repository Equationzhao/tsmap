package tsmap

import "time"

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

type KVCache[k comparable, v any] struct {
	removeWhenGettingAnExpiredItem bool
	createdAt                      time.Time
	internal                       *Map[k, *CacheItem[v]]
}

var DefaultShards = 200

func NewKVCache[k comparable, v any]() *KVCache[k, v] {
	c := &KVCache[k, v]{
		createdAt: time.Now(),
	}
	return c
}

func NewKVCacheWithOption[k comparable, v any](options ...CacheOptions[k, v]) *KVCache[k, v] {
	c := &KVCache[k, v]{
		createdAt: time.Now(),
	}
	c.applyOptions(options...)
	return c
}

func (c *KVCache[k, v]) Clone() *KVCache[k, v] {
	return &KVCache[k, v]{
		removeWhenGettingAnExpiredItem: c.removeWhenGettingAnExpiredItem,
		createdAt:                      c.createdAt,
		internal:                       c.internal.Clone(),
	}
}

type CacheOptions[k comparable, v any] func(cache *KVCache[k, v])
type ItemOptions[v any] func(cache *CacheItem[v])

func (c *KVCache[k, v]) checkExpireWithNow(value CacheItem[v], now time.Time) bool {
	switch value.Expiration {
	case Expired:
		return true
	case Infinity:
		return false
	default:
		return now.After(c.createdAt.Add(value.Expiration))
	}
}

func (c *KVCache[k, v]) checkExpire(value CacheItem[v]) bool {
	return c.checkExpireWithNow(value, time.Now())
}

func (c *KVCache[k, v]) CreatedAt() time.Time {
	return c.createdAt
}

func (c *KVCache[k, v]) SetCreatedTime(createdAt time.Time) {
	c.createdAt = createdAt
}

func RemoveWhenGettingAnExpiredItem[k comparable, v any](cache *KVCache[k, v]) {
	cache.removeWhenGettingAnExpiredItem = true
}

func (c *KVCache[k, v]) applyOptions(options ...CacheOptions[k, v]) {
	for _, option := range options {
		option(c)
	}
}

func (c *KVCache[k, v]) Insert(key k, value v, options ...ItemOptions[v]) {
	cacheItem := &CacheItem[v]{
		Value:      value,
		Expiration: Infinity,
	}
	cacheItem.applyOptions(options...)
	c.internal.Set(key, cacheItem)
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
		if c.checkExpire(*get) {
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
		return c.checkExpireWithNow(*value, now)
	})
}

func (c *KVCache[k, v]) DeleteExpiredAccurate() {
	c.internal.IterRemoveIf(func(key k, value *CacheItem[v]) bool {
		return c.checkExpireWithNow(*value, time.Now())
	})
}

func WithExpiration[v any](expiration time.Duration) ItemOptions[v] {
	return func(cache *CacheItem[v]) {
		cache.Expiration = expiration
	}
}
