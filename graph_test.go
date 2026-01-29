package taskgraph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	tg "github.com/thought-machine/taskgraph"
	tgt "github.com/thought-machine/taskgraph/taskgraphtest"
)

const wantGraphviz = `digraph G {
  A [label="A"];
  A_input_in [label="Input - in", shape=diamond];
  B [label="B"];
  C [label="C"];
  D [label="D"];
  E [label="E"];
  F [label="F"];
  G [label="G"];
  out [label="out"];
  out_output_out [label="Output", shape=diamond];

  A -> B [label="a"];
  A -> C [label="a"];
  A_input_in -> A;
  B -> D [label="b"];
  C -> E [label="c"];
  D -> F [label="d1"];
  D -> G [label="d2"];
  E -> F [label="e1"];
  E -> G [label="e2"];
  F -> out [label="f"];
  G -> out [label="g"];
  out -> out_output_out [label="out"];
}
`

// Tests all of the mechanisms for creating tasks
func TestGraph(t *testing.T) {
	keyIn := tg.NewKey[int]("in")
	keyA := tg.NewKey[int]("a")
	keyB := tg.NewKey[int]("b")
	keyC := tg.NewKey[int]("c")
	keyD1 := tg.NewKey[int]("d1")
	keyD2 := tg.NewKey[int]("d2")
	keyE1 := tg.NewKey[int]("e1")
	keyE2 := tg.NewKey[int]("e2")
	keyF := tg.NewKey[int]("f")
	keyG := tg.NewKey[int]("g")
	keyOut := tg.NewKey[int]("out")

	g := tgt.Must[tg.Graph](t)(tg.New("test_graph", tg.WithTasks(
		tg.SimpleTask1[int, int]("A", keyA, func(_ context.Context, arg int) (int, error) {
			return arg * 2, nil
		}, keyIn),
		tg.SimpleTask1[int, int]("B", keyB, func(_ context.Context, arg int) (int, error) {
			return arg * 3, nil
		}, keyA),
		tg.SimpleTask1[int, int]("C", keyC, func(_ context.Context, arg int) (int, error) {
			return arg * 5, nil
		}, keyA),
		tgt.Must[tg.Task](t)(tg.ReflectMulti{
			Name: "D",
			Fn: func(b int) ([]tg.Binding, error) {
				return []tg.Binding{
					keyD1.Bind(b),
					keyD2.Bind(b),
				}, nil
			},
			Provides: []tg.ID{keyD1.ID(), keyD2.ID()},
			Depends:  []any{keyB},
		}.Locate().Build()),
		tgt.Must[tg.Task](t)(tg.ReflectMulti{
			Name: "E",
			Fn: func(c int) ([]tg.Binding, error) {
				return []tg.Binding{
					keyE1.Bind(c),
					keyE2.Bind(c),
				}, nil
			},
			Provides: []tg.ID{keyE1.ID(), keyE2.ID()},
			Depends:  []any{keyC},
		}.Locate().Build()),
		tg.SimpleTask2[int, int]("F", keyF, func(_ context.Context, d, e int) (int, error) {
			return d + e, nil
		}, keyD1, keyE1),
		tg.SimpleTask2[int, int]("G", keyG, func(_ context.Context, d, e int) (int, error) {
			return d * e, nil
		}, keyD2, keyE2),
		tg.SimpleTask2[int, int]("out", keyOut, func(_ context.Context, f, g int) (int, error) {
			return f + g, nil
		}, keyF, keyG),
	)))

	t.Run("Graphviz", func(t *testing.T) {
		got := g.Graphviz(true)

		if diff := cmp.Diff(wantGraphviz, got); diff != "" {
			t.Errorf("Unexpected diff in Graphviz output:\n%s", diff)
		}
	})

	t.Run("Run", func(t *testing.T) {
		tgt.Test{
			Graph: g,
			Inputs: []tg.Binding{
				keyIn.Bind(1),
			},
			WantBindings: []tgt.BindingMatcher{
				tgt.Match(keyOut.Bind(76)),
			},
		}.Run(t)
	})

	t.Run("Run, missing inputs", func(t *testing.T) {
		tgt.Test{
			Graph:     g,
			WantError: tg.ErrMissingInputs,
		}.Run(t)
	})

	t.Run("AsTask", func(t *testing.T) {
		tgt.Test{
			Task: tgt.Must[tg.Task](t)(g.AsTask(keyOut.ID())),
			Inputs: []tg.Binding{
				keyIn.Bind(1),
			},
			WantBindings: []tgt.BindingMatcher{
				tgt.Match(keyOut.Bind(76)),
			},
			CheckExcessBindings: true,
		}.Run(t)
	})
}

func TestGraphErrors(t *testing.T) {
	key1 := tg.NewKey[string]("key1")
	key2 := tg.NewKey[string]("key2")

	t.Run("ErrDuplicateTaskNames", func(t *testing.T) {
		if _, err := tg.New("test_graph", tg.WithTasks(
			tg.NewTask("task", tgt.DummyTaskFunc(), nil, nil),
			tg.NewTask("task", tgt.DummyTaskFunc(), nil, nil),
		)); !errors.Is(err, tg.ErrDuplicateTaskNames) {
			t.Errorf("expected error %v; got %v", tg.ErrDuplicateTaskNames, err)
		}
	})

	t.Run("ErrDuplicateProvidedKeys", func(t *testing.T) {
		if _, err := tg.New("test_graph", tg.WithTasks(
			tg.NewTask("task1", tgt.DummyTaskFunc(), nil, []tg.ID{key1.ID()}),
			tg.NewTask("task2", tgt.DummyTaskFunc(), nil, []tg.ID{key1.ID()}),
		)); !errors.Is(err, tg.ErrDuplicateProvidedKeys) {
			t.Errorf("expected error %v; got %v", tg.ErrDuplicateProvidedKeys, err)
		}
	})

	t.Run("ErrGraphCycle", func(t *testing.T) {
		if _, err := tg.New("test_graph", tg.WithTasks(
			tg.NewTask("task1", tgt.DummyTaskFunc(), []tg.ID{key1.ID()}, []tg.ID{key2.ID()}),
			tg.NewTask("task2", tgt.DummyTaskFunc(), []tg.ID{key2.ID()}, []tg.ID{key1.ID()}),
		)); !errors.Is(err, tg.ErrGraphCycle) {
			t.Errorf("expected error %v; got %v", tg.ErrGraphCycle, err)
		}
	})

	t.Run("ErrTooManyTasks", func(t *testing.T) {
		var tasks []tg.TaskSet
		for i := 0; i <= 1000; i++ {
			tasks = append(tasks, tg.NewTask("task", tgt.DummyTaskFunc(), nil, nil))
		}
		if _, err := tg.New(
			"test_graph",
			tg.WithTasks(tasks...),
		); !errors.Is(
			err,
			tg.ErrTooManyTasks,
		) {
			t.Errorf("expected error %v; got %v", tg.ErrTooManyTasks, err)
		}
	})

	t.Run("ErrExposedKeyNotProvided", func(t *testing.T) {
		if _, err := tgt.Must[tg.Graph](t)(tg.New("test_graph", tg.WithTasks(
			tg.NewTask("task", tgt.DummyTaskFunc(), nil, nil),
		))).AsTask(key1.ID()); !errors.Is(err, tg.ErrExposedKeyNotProvided) {
			t.Errorf("expected error %v; got %v", tg.ErrExposedKeyNotProvided, err)
		}
	})
}
