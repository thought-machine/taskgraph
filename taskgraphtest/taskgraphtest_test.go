package taskgraphtest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	tg "github.com/thought-machine/taskgraph"
	tgt "github.com/thought-machine/taskgraph/taskgraphtest"
)

func TestOverrides(t *testing.T) {
	keyIn, keyA, keyB := tg.NewKey[int]("in"), tg.NewKey[int]("a"), tg.NewKey[int]("b")
	var runsA atomic.Int32
	graph := tgt.Must[tg.Graph](t)(tg.New("overrides", tg.WithTasks(
		tg.SimpleTask1[int, int]("A", keyA, func(_ context.Context, in int) (int, error) {
			runsA.Add(1)
			return in * 2, nil
		}, keyIn),
		tg.SimpleTask1[int, int]("B", keyB, func(_ context.Context, a int) (int, error) {
			return a + 1, nil
		}, keyA),
	)))

	tgt.Suite{
		Graph:     graph,
		Overrides: []tg.Binding{keyA.Bind(10)},
		Tests: []tgt.Test{
			{
				Description:  "a suite's overrides apply to its tests",
				Inputs:       []tg.Binding{keyIn.Bind(1)},
				WantBindings: []tgt.BindingMatcher{tgt.Match(keyB.Bind(11))},
			},
			{
				Description:  "a test's override replaces the suite's for the same key",
				Inputs:       []tg.Binding{keyIn.Bind(1)},
				Overrides:    []tg.Binding{keyA.Bind(20)},
				WantBindings: []tgt.BindingMatcher{tgt.Match(keyB.Bind(21))},
			},
			{
				Description: "overrides are part of the result",
				Inputs:      []tg.Binding{keyIn.Bind(1)},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(keyA.Bind(10)),
					tgt.Match(keyB.Bind(11)),
				},
			},
			{
				Description: "excess bindings includes them",
				Inputs:      []tg.Binding{keyIn.Bind(1)},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(keyA.Bind(10)),
					tgt.Match(keyB.Bind(11)),
				},
				CheckExcessBindings: true,
			},
			{
				Description:  "an absent override makes the task fail",
				Inputs:       []tg.Binding{keyIn.Bind(1)},
				Overrides:    []tg.Binding{keyA.BindAbsent()},
				WantBindings: []tgt.BindingMatcher{tgt.Match(keyA.BindAbsent())},
				WantError:    tg.ErrIsAbsent,
			},
		},
	}.Run(t)

	if runsA.Load() != 0 {
		t.Errorf("A ran %d times; want it replaced in every test", runsA.Load())
	}

	// A test which isn't part of a suite can override too.
	tgt.Test{
		Graph:        graph,
		Inputs:       []tg.Binding{keyIn.Bind(1)},
		Overrides:    []tg.Binding{keyA.Bind(5)},
		WantBindings: []tgt.BindingMatcher{tgt.Match(keyB.Bind(6))},
	}.Run(t)
}

// An operator which has a graph to create and one to delete is given one set of overrides.
func TestOverrider(t *testing.T) {
	keyIn, keyA, keyB := tg.NewKey[int]("in"), tg.NewKey[int]("a"), tg.NewKey[int]("b")
	keyD, keyUnused := tg.NewKey[int]("d"), tg.NewKey[int]("unused")
	create := tgt.Must[tg.Graph](t)(tg.New("create", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"A",
			keyA,
			func(_ context.Context, in int) (int, error) { return in * 2, nil },
			keyIn,
		),
		tg.SimpleTask1[int, int](
			"B",
			keyB,
			func(_ context.Context, a int) (int, error) { return a + 1, nil },
			keyA,
		),
	)))
	del := tgt.Must[tg.Graph](t)(tg.New("delete", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"D",
			keyD,
			func(_ context.Context, in int) (int, error) { return in * 3, nil },
			keyIn,
		),
	)))

	overrider := tg.NewOverrider(keyA.Bind(10), keyD.Bind(20))
	if err := overrider.Check(create, del); err != nil {
		t.Fatalf("Check(): %v", err)
	}

	// Each graph takes the overrides which are for it, and ignores the others.
	result, err := overrider.Run(context.Background(), create, keyIn.Bind(1))
	if err != nil {
		t.Fatalf("running the graph to create: %v", err)
	}
	if got, err := keyB.Get(result); err != nil || got != 11 {
		t.Errorf("b = %d, %v; want 11, from the override of a", got, err)
	}
	result, err = overrider.Run(context.Background(), del, keyIn.Bind(1))
	if err != nil {
		t.Fatalf("running the graph to delete: %v", err)
	}
	if got, err := keyD.Get(result); err != nil || got != 20 {
		t.Errorf("d = %d, %v; want 20, from the override", got, err)
	}

	// An override for neither graph is found by Check, though each Run leaves it out.
	stale := tg.NewOverrider(keyA.Bind(10), keyUnused.Bind(1))
	if err := stale.Check(create, del); !errors.Is(err, tg.ErrUnknownOverride) {
		t.Errorf("Check() with a stale override: got %v; want ErrUnknownOverride", err)
	}
	if _, err := stale.Run(context.Background(), create, keyIn.Bind(1)); err != nil {
		t.Errorf("Run() with an override for another graph: %v", err)
	}
}

// fakeGraph is a Graph which taskgraph.New didn't make.
type fakeGraph struct{ tg.Graph }

func TestOverridesNeedAGraphMadeByTaskgraph(t *testing.T) {
	if _, err := tg.RunWithOverrides(context.Background(), fakeGraph{}, nil, nil); err == nil {
		t.Error(
			"RunWithOverrides() on a Graph which taskgraph.New didn't make: got nil; want an error",
		)
	}
}

// A Test which has no overrides runs the Graph with Graph.Run, so it still works for a Graph which
// taskgraph.New didn't make.
func TestTestWithoutOverridesUsesGraphRun(t *testing.T) {
	key := tg.NewKey[int]("a")
	inner := tgt.Must[tg.Graph](t)(tg.New("inner", tg.WithTasks(
		tg.SimpleTask("A", key, func(context.Context, tg.Binder) (int, error) { return 1, nil }),
	)))

	tgt.Test{
		Graph:        fakeGraph{inner},
		WantBindings: []tgt.BindingMatcher{tgt.Match(key.Bind(1))},
	}.Run(t)
}

func TestOverriderCheck(t *testing.T) {
	keyIn, keyA := tg.NewKey[int]("in"), tg.NewKey[int]("a")
	keyM1, keyM2 := tg.NewKey[int]("m1"), tg.NewKey[int]("m2")
	create := tgt.Must[tg.Graph](t)(tg.New("create", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"A",
			keyA,
			func(_ context.Context, in int) (int, error) { return in, nil },
			keyIn,
		),
	)))
	// A task which provides two keys, as one which has had a key added to it does.
	del := tgt.Must[tg.Graph](t)(tg.New("delete", tg.WithTasks(
		tg.NewTask("M", func(context.Context, tg.Binder) ([]tg.Binding, error) {
			return []tg.Binding{keyM1.Bind(1), keyM2.Bind(2)}, nil
		}, nil, []tg.ID{keyM1.ID(), keyM2.ID()}),
	)))

	for _, tc := range []struct {
		description string
		overrider   *tg.Overrider
		graphs      []tg.Graph
		wantError   error
		// wantSomeError is for errors which have no sentinel.
		wantSomeError bool
	}{
		{
			description: "every key of a task, in the graph which has it",
			overrider:   tg.NewOverrider(keyA.Bind(1), keyM1.Bind(1), keyM2.Bind(2)),
			graphs:      []tg.Graph{create, del},
		},
		{
			description: "only some of the keys of a task, in a graph which no test might run",
			overrider:   tg.NewOverrider(keyA.Bind(1), keyM1.Bind(1)),
			graphs:      []tg.Graph{create, del},
			wantError:   tg.ErrPartialOverride,
		},
		{
			description: "the same key twice",
			overrider:   tg.NewOverrider(keyA.Bind(1), keyA.Bind(2)),
			graphs:      []tg.Graph{create, del},
			wantError:   tg.ErrDuplicateBinding,
		},
		{
			// Which is found before the key is found to be one which no graph has.
			description: "the same key twice, which no graph provides",
			overrider:   tg.NewOverrider(tg.NewKey[int]("stale").Bind(1), tg.NewKey[int]("stale").Bind(2)),
			graphs:      []tg.Graph{create, del},
			wantError:   tg.ErrDuplicateBinding,
		},
		{
			description: "a key which no graph provides",
			overrider:   tg.NewOverrider(keyA.Bind(1), tg.NewKey[int]("stale").Bind(1)),
			graphs:      []tg.Graph{create, del},
			wantError:   tg.ErrUnknownOverride,
		},
		{
			description:   "a graph which taskgraph.New didn't make",
			overrider:     tg.NewOverrider(keyA.Bind(1)),
			graphs:        []tg.Graph{create, fakeGraph{}},
			wantSomeError: true,
		},
		{
			description:   "a graph which taskgraph.New didn't make, with nothing to override",
			overrider:     tg.NewOverrider(),
			graphs:        []tg.Graph{fakeGraph{}},
			wantSomeError: true,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			err := tc.overrider.Check(tc.graphs...)
			switch {
			case tc.wantSomeError:
				if err == nil {
					t.Fatal("Check(): got nil; want an error")
				}
				// It is the graph which is wrong, not an override which is for no graph.
				if errors.Is(err, tg.ErrUnknownOverride) {
					t.Errorf("Check(): got %v; want an error about the graph", err)
				}
			case !errors.Is(err, tc.wantError):
				t.Errorf("Check(): got %v; want %v", err, tc.wantError)
			}
		})
	}
}
