package taskgraph

import (
	"errors"
)

var (
	// ErrIsAbsent is returned from Key[T].Get() when the Key has been bound as absent without
	// providing a more specific error.
	ErrIsAbsent = errors.New("is absent")

	// ErrIsPending is returned from Key[T].Get() when the Key has not been bound.
	ErrIsPending = errors.New("is pending")

	// ErrWrongType is returned from Key[T].Get() when the Binder contains a Binding for the Key's ID,
	// but that Binding contains a value which is not of type T. This can only happen if 2 Keys are
	// created with the same ID but different types.
	ErrWrongType = errors.New("wrong type")
)

// ReadOnlyKey represents a key which can be read from a binder, but not bound itself.
type ReadOnlyKey[T any] interface {
	// ID returns the type-parameter-less ID which identifies the key.
	ID() ID

	// Location returns the file and line where this key was defined.
	Location() string

	// Get retrieves the value for this key from the binder.
	Get(Binder) (T, error)
}

// A Key identifies an input and/or output to a task or graph, which can be bound to a value.
type Key[T any] interface {
	ReadOnlyKey[T]

	// Bind this key to the given value.
	Bind(val T) Binding

	// Bind this key as absent (see the comment on the Absent BindStatus). This is equivalent to
	// calling BindError(ErrIsAbsent)
	BindAbsent() Binding

	// Bind this key as absent with a specific error (see the comment on the Absent BindStatus).
	BindError(err error) Binding
}

type key[T any] struct {
	id       ID
	location string
}

func (k *key[T]) ID() ID {
	return k.id
}

func (k *key[T]) Location() string {
	return k.location
}

func (k *key[T]) Bind(val T) Binding {
	return bind(k.id, val)
}

func (k *key[T]) BindAbsent() Binding {
	return bindAbsent(k.id)
}

func (k *key[T]) BindError(err error) Binding {
	return bindAbsentWithError(k.id, err)
}

func (k *key[T]) Get(b Binder) (T, error) {
	var empty T
	binding := b.Get(k.id)
	switch binding.Status() {
	case Absent:
		return empty, wrapStackErrorf("cannot get key %q: %w", k.id, binding.Error())
	case Pending:
		return empty, wrapStackErrorf("cannot get key %q: %w", k.id, ErrIsPending)
	case Present:
		typed, ok := binding.Value().(T)
		if !ok {
			var want T
			return empty, wrapStackErrorf(
				"cannot get key %q: %w (got %T, want %T)",
				k.id,
				ErrWrongType,
				binding.Value(),
				want,
			)
		}
		return typed, nil
	default:
		return empty, wrapStackErrorf("unknown binding status %v", binding.Status())
	}
}

// NewKey creates a new Key. This should typically be called at the top level of a package as a var.
func NewKey[T any](id string) Key[T] {
	return &key[T]{
		id:       newID("", id),
		location: getLocation(2),
	}
}

// NewNamespacedKey creates a new namespaced Key. This should typically be called at the top level
// of a package as a var.
func NewNamespacedKey[T any](namespace, id string) Key[T] {
	return &key[T]{
		id:       newID(namespace, id),
		location: getLocation(2),
	}
}

type presenceKey[T any] struct {
	ReadOnlyKey[T]
	location string
}

func (k *presenceKey[T]) Location() string {
	return k.location
}

func (k *presenceKey[T]) Get(b Binder) (bool, error) {
	return b.Get(k.ID()).Status() == Present, nil
}

// Presence returns a ReadOnlyKey key which returns whether the underlying key is present in the binder.
func Presence[T any](key ReadOnlyKey[T]) ReadOnlyKey[bool] {
	return &presenceKey[T]{
		ReadOnlyKey: key,
		location:    getLocation(2),
	}
}

type mappedKey[In, Out any] struct {
	ReadOnlyKey[In]
	fn       func(i In) Out
	location string
}

func (k *mappedKey[In, Out]) Location() string {
	return k.location
}

func (k *mappedKey[In, Out]) Get(b Binder) (Out, error) {
	val, err := k.ReadOnlyKey.Get(b)
	if err != nil {
		var o Out
		return o, err
	}
	return k.fn(val), nil
}

// Mapped returns a ReadOnlyKey which applies the given mapping function when Get() is called.
// This is primarily intended for generating keys to use with Conditional().
func Mapped[In, Out any](key ReadOnlyKey[In], fn func(In) Out) ReadOnlyKey[Out] {
	return &mappedKey[In, Out]{
		ReadOnlyKey: key,
		fn:          fn,
		location:    getLocation(2),
	}
}

// Not negates a boolean key.
func Not(key ReadOnlyKey[bool]) ReadOnlyKey[bool] {
	return Mapped(key, func(val bool) bool { return !val })
}

type optionalKey[T any] struct {
	ReadOnlyKey[T]
	location string
}

func (k *optionalKey[T]) Location() string {
	return k.location
}

// Get must return an error to fulfil the ReadOnlyKey interface, but the error will always be nil.
func (k *optionalKey[T]) Get(b Binder) (Maybe[T], error) {
	return WrapMaybe(k.ReadOnlyKey.Get(b)), nil
}

// Optional returns a key that will wrap any error from the base key in a Maybe, such that Get will
// never return an error. This is intended for use with reflection, where a task's parameters may be
// absent.
func Optional[T any](base ReadOnlyKey[T]) ReadOnlyKey[Maybe[T]] {
	return &optionalKey[T]{
		ReadOnlyKey: base,
		location:    getLocation(2),
	}
}
