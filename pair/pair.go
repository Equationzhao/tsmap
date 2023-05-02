package pair

import "fmt"

// Pair is a struct that contains two variables ptr
type Pair[T, U any] struct {
	First  *T
	Second *U
}

func (p *Pair[T, U]) GetFirst() T {
	return *p.First
}

func (p *Pair[T, U]) GetSecond() U {
	return *p.Second
}

// String return a string
// format: "Pair(%v, %v)", *p.First, *p.Second
func (p Pair[T, U]) String() string {
	return fmt.Sprintf("Pair(%v, %v)", *p.First, *p.Second)
}

// Clone return a new Pair deep Copy
func (p *Pair[T, U]) Clone() Pair[T, U] {
	f := *p.First
	s := *p.Second
	return Pair[T, U]{
		First:  &f,
		Second: &s,
	}
}

// NewPair copy the `first` and `second`
// return a new Pair Ptr
func NewPair[T, U any](first T, second U) *Pair[T, U] {
	f := first
	s := second
	return &Pair[T, U]{
		First:  &f,
		Second: &s,
	}
}

// MakePair return a new Pair
// receive two value
func MakePair[T, U any](first T, second U) Pair[T, U] {
	return Pair[T, U]{
		First:  &first,
		Second: &second,
	}
}

// EmplacePair return a new Pair
// receive two pointers
// any change apply to *first and *second will affect this pair
func EmplacePair[T, U any](first *T, second *U) Pair[T, U] {
	return Pair[T, U]{
		First:  first,
		Second: second,
	}
}

// Set the pair
// Copy the `first` and `second` to the pair
func (p *Pair[T, U]) Set(first T, second U) {
	*p.First = first
	*p.Second = second
}

// Move the pointer to the pair
func (p *Pair[T, U]) Move(first *T, second *U) {
	p.First = first
	p.Second = second
}

// ExchangePairs exchange the pair
func ExchangePairs[T, U any](a, b *Pair[T, U]) {
	*a, *b = *b, *a
}

// Clear the pair
// set internal ptr to nil
// should not access until next set, or it will panic
func (p *Pair[T, U]) Clear() {
	p.First = nil
	p.Second = nil
}
