package taskgraph_test

import (
	"errors"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	tg "github.com/thought-machine/taskgraph"
	tgt "github.com/thought-machine/taskgraph/taskgraphtest"
)

func TestBindersBindingsAndKeys(t *testing.T) {
	key1 := tg.NewKey[string]("key1")
	key2 := tg.NewKey[int]("key2")
	key3 := tg.NewKey[bool]("key3")

	key1Present := tg.Presence(key1)
	key2Present := tg.Presence(key2)
	key3Present := tg.Presence(key3)

	mappedKey1 := tg.Mapped[string, int](key1, func(s string) int { return len(s) })

	b := tg.NewBinder()

	tgt.ExpectPending[string](t, b, key1)
	tgt.ExpectPending[int](t, b, key2)
	tgt.ExpectPending[bool](t, b, key3)
	tgt.ExpectPending(t, b, key1Present)
	tgt.ExpectPending(t, b, key2Present)
	tgt.ExpectPending(t, b, key3Present)
	tgt.ExpectPending(t, b, mappedKey1)

	if err := b.Store(
		key1.Bind("foo"),
		key2.Bind(123),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := key3.Get(b); !errors.Is(err, tg.ErrIsPending) {
		t.Errorf("Expected key3.Get(b) to return error %v; got %v", tg.ErrIsPending, err)
	}

	if err := b.Store(key2.Bind(456)); !errors.Is(err, tg.ErrDuplicateBinding) {
		t.Errorf(
			"Expected b.Store(key2.Bind(456)) to return error %v; got %v",
			tg.ErrDuplicateBinding,
			err,
		)
	}

	tgt.DiffPresent[string](t, b, key1, "foo")
	tgt.ExpectPresent[int](t, b, key2, 123)
	tgt.ExpectPending[bool](t, b, key3)
	tgt.ExpectPresent(t, b, key1Present, true)
	tgt.ExpectPresent(t, b, key2Present, true)
	tgt.ExpectPending(t, b, key3Present)
	tgt.ExpectPresent(t, b, mappedKey1, 3)

	if err := b.Store(
		key3.BindAbsent(),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := key3.Get(b); !errors.Is(err, tg.ErrIsAbsent) {
		t.Errorf("Expected key3.Get(b) to return error %v; got %v", tg.ErrIsPending, err)
	}

	tgt.ExpectAbsent[bool](t, b, key3)
	tgt.ExpectPresent(t, b, key3Present, false)

	tgt.ExpectBindings(t, b, []tgt.BindingMatcher{
		tgt.MatchDiff(key1.Bind("foo")),
		tgt.Match(key2.Bind(123)),
		tgt.Match(key3.BindAbsent()),
	})

	tgt.ExpectExactBindings(t, b, []tgt.BindingMatcher{
		tgt.MatchDiff(key1.Bind("foo")),
		tgt.Match(key2.Bind(123)),
		tgt.Match(key3.BindAbsent()),
	})
}

// Asserts that keys created using the same call to NewKey are treated as equivalent even though
// they are not the same object in memory.
func TestKeyIdentity(t *testing.T) {
	b := tg.NewBinder()

	keyA := tg.NewKey[string]("foo")
	keyB := tg.NewKey[string]("foo")

	if err := b.Store(keyA.Bind("bar")); err != nil {
		t.Fatal(err)
	}

	tgt.ExpectPresent(t, b, keyB, "bar")
}

func TestOptionalKey(t *testing.T) {
	key1 := tg.NewKey[int]("key1")
	key2 := tg.NewKey[int]("key2")
	key3 := tg.NewKey[int]("key3")

	optional1 := tg.Optional(key1)
	optional2 := tg.Optional(key2)
	optional3 := tg.Optional(key3)

	sentinelError := errors.New("sentinel")

	b := tg.NewBinder()

	if err := b.Store(
		key1.Bind(123),
		key2.BindAbsent(),
		key3.BindError(sentinelError),
	); err != nil {
		t.Fatal(err)
	}

	opts := []cmp.Option{
		tgt.TransformMaybe[int](),
		cmpopts.EquateErrors(),
	}

	tgt.ExpectPresent(t, b, optional1, tg.MaybeOf[int](123), opts...)
	tgt.ExpectPresent(t, b, optional2, tg.MaybeErr[int](tg.ErrIsAbsent), opts...)
	tgt.ExpectPresent(t, b, optional3, tg.MaybeErr[int](sentinelError), opts...)
}

func TestOverlayBinder(t *testing.T) {
	base := tg.NewBinder()
	overlay := tg.NewBinder()
	ob := tg.NewOverlayBinder(base, overlay)

	key1 := tg.NewKey[int]("key1")
	key2 := tg.NewKey[int]("key2")

	if err := base.Store(key1.Bind(123)); err != nil {
		t.Fatal(err)
	}

	tgt.ExpectPresent[int](t, ob, key1, 123)
	tgt.ExpectPending[int](t, ob, key2)

	if err := ob.Store(key1.Bind(123)); !errors.Is(err, tg.ErrDuplicateBinding) {
		t.Errorf(
			"Expected ob.Store(key1.Bind(123)) to return error %v; got %v",
			tg.ErrDuplicateBinding,
			err,
		)
	}

	if err := ob.Store(key2.Bind(456)); err != nil {
		t.Fatal(err)
	}

	tgt.ExpectExactBindings(t, ob, []tgt.BindingMatcher{
		tgt.Match(key1.Bind(123)),
		tgt.Match(key2.Bind(456)),
	})

	tgt.ExpectExactBindings(t, base, []tgt.BindingMatcher{
		tgt.Match(key1.Bind(123)),
	})

	tgt.ExpectExactBindings(t, overlay, []tgt.BindingMatcher{
		tgt.Match(key2.Bind(456)),
	})
}

func TestGraphTaskBinder(t *testing.T) {
	key1 := tg.NewKey[int]("key1")
	key2 := tg.NewKey[int]("key2")

	internal := tg.NewBinder()
	external := tg.NewBinder()
	gtb := tg.TestOnlyNewGraphTaskBinder(internal, external, set.NewSet[tg.ID](key1.ID()))

	if err := gtb.Store(
		key1.Bind(123),
		key2.Bind(456),
	); err != nil {
		t.Fatal(err)
	}

	tgt.ExpectExactBindings(t, gtb, []tgt.BindingMatcher{
		tgt.Match(key1.Bind(123)),
		tgt.Match(key2.Bind(456)),
	})

	tgt.ExpectExactBindings(t, internal, []tgt.BindingMatcher{
		tgt.Match(key2.Bind(456)),
	})

	tgt.ExpectExactBindings(t, external, []tgt.BindingMatcher{
		tgt.Match(key1.Bind(123)),
	})

	if err := gtb.Store(key1.Bind(456)); !errors.Is(err, tg.ErrDuplicateBinding) {
		t.Errorf(
			"Expected gtb.Store(key1.Bind(456)) to return error %v; got %v",
			tg.ErrDuplicateBinding,
			err,
		)
	}

	if err := gtb.Store(key2.Bind(123)); !errors.Is(err, tg.ErrDuplicateBinding) {
		t.Errorf(
			"Expected gtb.Store(key2.Bind(123)) to return error %v; got %v",
			tg.ErrDuplicateBinding,
			err,
		)
	}
}
