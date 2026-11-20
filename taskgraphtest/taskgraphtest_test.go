package taskgraphtest_test

import (
	"context"
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
