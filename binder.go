package taskgraph

import (
	"errors"
	"fmt"
	"sync"

	set "github.com/deckarep/golang-set/v2"
)

var (
	// ErrDuplicateBinding is returned when Binder.Store is called with a binding whose ID has already
	// been stored (which implies that the graph being executed contains multiple tasks producing
	// bindings for the same Key).
	ErrDuplicateBinding = errors.New("duplicate binding")
)

// BindStatus represents the tristate of a Binding.
type BindStatus int

const (
	// Pending represents where the key is unbound (i.e. that no task has yet provided a binding for
	// the key, and no input binding was provided).
	Pending BindStatus = iota

	// Absent represents where the key is explicitly unbound (i.e. that the task which provides it was
	// unable to provide a value). The binding will contain an error, which is ErrIsAbsent by default
	// but can be another error to propagate information between tasks. This allows for errors which
	// do not terminate the execution of a graph.
	Absent

	// Present represents where the key is bound to a valid value (i.e. the task was able to provide a
	// value, or the value was bound as an input).
	Present
)

func (bs BindStatus) String() string {
	return map[BindStatus]string{
		Pending: "PENDING",
		Absent:  "ABSENT",
		Present: "PRESENT",
	}[bs]
}

// A Binding is a tristate wrapper around a key ID and an optional value or error. See the
// documentation for BindStatus for details of the 3 states. Bindings are produced by calling the
// Bind, BindAbsent, or BindError methods on a Key.
type Binding interface {
	// ID returns the ID of the key which is bound by this Binding.
	ID() ID

	// Status returns the status of this binding.
	Status() BindStatus

	// Value returns the value bound to the key. This should only be called if Status() returns Present.
	Value() any

	// Value returns the error bound to the key. This should only be called if Status() returns Absent.
	Error() error
}

type binding struct {
	id     ID
	status BindStatus
	value  any
	err    error
}

func (b *binding) ID() ID {
	return b.id
}

func (b *binding) Status() BindStatus {
	return b.status
}

func (b *binding) Value() any {
	return b.value
}

func (b *binding) Error() error {
	return b.err
}

func (b *binding) String() string {
	if b.status == Present {
		return fmt.Sprintf("%s(%s -> %v)", b.status, b.id, b.value)
	}
	return fmt.Sprintf("%s(%s)", b.status, b.id)
}

// bind a value to a key ID.
func bind(id ID, value any) Binding {
	return &binding{
		id:     id,
		status: Present,
		value:  value,
	}
}

// bindAbsent produces an absent binding for the given Key ID.
func bindAbsent(id ID) Binding {
	return bindAbsentWithError(id, ErrIsAbsent)
}

// bindAbsent produces an absent binding for the given Key ID.
func bindAbsentWithError(id ID, err error) Binding {
	return &binding{
		id:     id,
		status: Absent,
		err:    err,
	}
}

// bindPending produces a pending binding; this is only ever used when calling Get on a Binder.
func bindPending(id ID) Binding {
	return &binding{
		id:     id,
		status: Pending,
	}
}

// A Binder is the state store for tasks in a graph.
type Binder interface {
	// Store adds bindings to the binder that can be retrieved with Get().
	Store(...Binding) error

	// Returns whether the given IDs have all been bound (as Present or Absent).
	Has(...ID) bool

	// Get a previously stored binding. If no binding with the given ID has yet been stored, a binding with Status() = Pending is generated.
	Get(ID) Binding

	// GetAll returns all stored bindings. This is typically used only for tests.
	GetAll() []Binding
}

type binder struct {
	// Protects against concurrent access to the map
	sync.RWMutex

	bindings map[ID]Binding
}

func (b *binder) Store(bs ...Binding) error {
	b.Lock()
	defer b.Unlock()

	for _, binding := range bs {
		if _, ok := b.bindings[binding.ID()]; ok {
			return wrapStackErrorf("%w: %q", ErrDuplicateBinding, binding.ID())
		}
		b.bindings[binding.ID()] = binding
	}

	return nil
}

func (b *binder) Has(ids ...ID) bool {
	b.RLock()
	defer b.RUnlock()

	for _, id := range ids {
		if _, ok := b.bindings[id]; !ok {
			return false
		}
	}
	return true
}

func (b *binder) Get(id ID) Binding {
	b.RLock()
	defer b.RUnlock()

	if binding, ok := b.bindings[id]; ok {
		return binding
	}
	return bindPending(id)
}

func (b *binder) GetAll() []Binding {
	b.RLock()
	defer b.RUnlock()

	res := make([]Binding, 0, len(b.bindings))
	for _, binding := range b.bindings {
		res = append(res, binding)
	}
	return res
}

// NewBinder returns a new binder.
func NewBinder() Binder {
	return &binder{
		bindings: map[ID]Binding{},
	}
}

// overlayBinder implements Binder to provide an overlay over an existing binder, such that newly
// stored keys are added to the overlay only, but bindings can still be read from the base. This
// still does not allow duplicate bindings (attempting to Store() a binding already present in the
// base will return an error)
type overlayBinder struct {
	base, overlay Binder
}

func (ob *overlayBinder) Store(bindings ...Binding) error {
	for _, b := range bindings {
		if ob.base.Has(b.ID()) {
			return wrapStackErrorf("%w: %q", ErrDuplicateBinding, b.ID())
		}
	}
	return ob.overlay.Store(bindings...)
}

func (ob *overlayBinder) Has(ids ...ID) bool {
	for _, id := range ids {
		if !ob.overlay.Has(id) && !ob.base.Has(id) {
			return false
		}
	}
	return true
}

func (ob *overlayBinder) Get(id ID) Binding {
	if b := ob.overlay.Get(id); b.Status() != Pending {
		return b
	}
	return ob.base.Get(id)
}

func (ob *overlayBinder) GetAll() []Binding {
	return append(ob.base.GetAll(), ob.overlay.GetAll()...)
}

// NewOverlayBinder creates a new overlay binder.
func NewOverlayBinder(base, overlay Binder) Binder {
	return &overlayBinder{
		base:    base,
		overlay: overlay,
	}
}

// graphTaskBinder implements Binder to run a Graph as a task. Any bindings for keys that should be
// exposed are immediately added to the Binder of the parent graph, so that dependent tasks outside
// this graph do not have to wait for every task in this graph to complete.
type graphTaskBinder struct {
	internal, external Binder
	exposeKeys         set.Set[ID]
}

func (gtb *graphTaskBinder) Store(bindings ...Binding) error {
	for _, binding := range bindings {
		if gtb.exposeKeys.Contains(binding.ID()) {
			if err := gtb.external.Store(binding); err != nil {
				return err
			}
		} else {
			if err := gtb.internal.Store(binding); err != nil {
				return err
			}
		}
	}
	return nil
}

func (gtb *graphTaskBinder) Has(ids ...ID) bool {
	for _, id := range ids {
		if !gtb.internal.Has(id) && !gtb.external.Has(id) {
			return false
		}
	}
	return true
}

func (gtb *graphTaskBinder) Get(id ID) Binding {
	if ib := gtb.internal.Get(id); ib.Status() != Pending {
		return ib
	}
	return gtb.external.Get(id)
}

func (gtb *graphTaskBinder) GetAll() []Binding {
	return append(gtb.internal.GetAll(), gtb.external.GetAll()...)
}

// TestOnlyNewGraphTaskBinder creates a new graph task binder. This is exported for testing, and
// should not be called in production code.
func TestOnlyNewGraphTaskBinder(internal, external Binder, exposeKeys set.Set[ID]) Binder {
	return &graphTaskBinder{
		internal:   internal,
		external:   external,
		exposeKeys: exposeKeys,
	}
}
