package tsmap

import "github.com/Equationzhao/tsmap/pair"

type Pair[k comparable, v any] struct {
	internal pair.Pair[k, v]
}

func MakePair[k comparable, v any](key k, value v) Pair[k, v] {
	return Pair[k, v]{
		internal: pair.MakePair(key, value),
	}
}

func (kvp *Pair[k, v]) Key() k {
	return kvp.internal.GetFirst()
}

func (kvp *Pair[k, v]) Value() v {
	return kvp.internal.GetSecond()
}
