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

	overrider := tgt.NewOverrider(keyA.Bind(10), keyD.Bind(20))
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
	stale := tgt.NewOverrider(keyA.Bind(10), keyUnused.Bind(1))
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
	if _, err := tgt.RunWithOverrides(context.Background(), fakeGraph{}, nil, nil); err == nil {
		t.Error(
			"RunWithOverrides() on a Graph which taskgraph.New didn't make: got nil; want an error",
		)
	}
}
