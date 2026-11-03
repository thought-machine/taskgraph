package taskgraph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	tg "github.com/thought-machine/taskgraph"
	tgt "github.com/thought-machine/taskgraph/taskgraphtest"
)

// overrideFixture is a small graph, in -> A -> B, whose tasks count how often they run:
//
//	A: a = in * 2
//	B: b = a + 1
type overrideFixture struct {
	keyIn, keyA, keyB tg.Key[int]
	runsA, runsB      atomic.Int32
	graph             tg.Graph
}

func newOverrideFixture(t *testing.T) *overrideFixture {
	t.Helper()
	f := &overrideFixture{
		keyIn: tg.NewKey[int]("in"),
		keyA:  tg.NewKey[int]("a"),
		keyB:  tg.NewKey[int]("b"),
	}
	f.graph = tgt.Must[tg.Graph](t)(tg.New("override_graph", tg.WithTasks(
		tg.SimpleTask1[int, int]("A", f.keyA, func(_ context.Context, in int) (int, error) {
			f.runsA.Add(1)
			return in * 2, nil
		}, f.keyIn),
		tg.SimpleTask1[int, int]("B", f.keyB, func(_ context.Context, a int) (int, error) {
			f.runsB.Add(1)
			return a + 1, nil
		}, f.keyA),
	)))
	return f
}

func (f *overrideFixture) run(t *testing.T, inputs ...tg.Binding) tg.Binder {
	t.Helper()
	result, err := runWith(testContext(t), f.graph, inputs...)
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	return result
}

// testContext returns a context which ends soon, so that a graph which wrongly waits for something
// which will never happen fails the test, rather than hanging it.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// override marks an input as an override, for runWith and checkWith to separate from the inputs.
type override struct{ tg.Binding }

func ov(b tg.Binding) tg.Binding { return override{b} }

func splitOverrides(inputs []tg.Binding) (ordinary, overrides []tg.Binding) {
	for _, input := range inputs {
		if o, ok := input.(override); ok {
			overrides = append(overrides, o.Binding)
		} else {
			ordinary = append(ordinary, input)
		}
	}
	return ordinary, overrides
}

func runWith(ctx context.Context, g tg.Graph, inputs ...tg.Binding) (tg.Binder, error) {
	ordinary, overrides := splitOverrides(inputs)
	return tgt.RunWithOverrides(ctx, g, ordinary, overrides)
}

func checkWith(g tg.Graph, inputs ...tg.Binding) error {
	ordinary, overrides := splitOverrides(inputs)
	return tgt.CheckWithOverrides(g, ordinary, overrides)
}

func mustGet[T any](t *testing.T, key tg.ReadOnlyKey[T], b tg.Binder) T {
	t.Helper()
	got, err := key.Get(b)
	if err != nil {
		t.Fatalf("getting %s: %v", key.ID(), err)
	}
	return got
}

func TestOverride_WithoutOverrideTasksRun(t *testing.T) {
	f := newOverrideFixture(t)
	result := f.run(t, f.keyIn.Bind(1))

	if got, want := mustGet(t, f.keyB, result), 3; got != want {
		t.Errorf("b = %d; want %d", got, want)
	}
	if f.runsA.Load() != 1 || f.runsB.Load() != 1 {
		t.Errorf("tasks ran A %d times, B %d times; want once each", f.runsA.Load(), f.runsB.Load())
	}
}

func TestOverride_ReplacesTheTaskWhichProvidesTheKey(t *testing.T) {
	f := newOverrideFixture(t)
	result := f.run(t, f.keyIn.Bind(1), ov(f.keyA.Bind(100)))

	if f.runsA.Load() != 0 {
		t.Errorf("A ran %d times; want it replaced", f.runsA.Load())
	}
	if got, want := mustGet(t, f.keyB, result), 101; got != want {
		t.Errorf("b = %d; want %d, from the override", got, want)
	}
	// The override takes the place of the result the task would have produced.
	if got, want := mustGet(t, f.keyA, result), 100; got != want {
		t.Errorf("a in the result = %d; want the override, %d", got, want)
	}
}

func TestOverride_GraphCanBeRunAgainWithDifferentOverrides(t *testing.T) {
	f := newOverrideFixture(t)

	if got, want := mustGet(
		t,
		f.keyB,
		f.run(t, f.keyIn.Bind(1), ov(f.keyA.Bind(10))),
	), 11; got != want {
		t.Errorf("first run: b = %d; want %d", got, want)
	}
	if got, want := mustGet(t, f.keyB, f.run(t, f.keyIn.Bind(1))), 3; got != want {
		t.Errorf("second run, without an override: b = %d; want %d", got, want)
	}
	if got, want := mustGet(
		t,
		f.keyB,
		f.run(t, f.keyIn.Bind(1), ov(f.keyA.Bind(20))),
	), 21; got != want {
		t.Errorf("third run: b = %d; want %d", got, want)
	}
	if f.runsA.Load() != 1 {
		t.Errorf(
			"A ran %d times over the three runs; want once, in the one without an override",
			f.runsA.Load(),
		)
	}
}

func TestOverride_AbsentOverrideFailsTheTask(t *testing.T) {
	errBoom := errors.New("boom")
	keyIn, keyA, keyB := tg.NewKey[int]("in"), tg.NewKey[int]("a"), tg.NewKey[int]("b")
	var runsA atomic.Int32
	g := tgt.Must[tg.Graph](t)(tg.New("absent_override", tg.WithTasks(
		tg.SimpleTask1[int, int]("A", keyA, func(_ context.Context, in int) (int, error) {
			runsA.Add(1)
			return in, nil
		}, keyIn),
		// Takes the absence of a in its stride, as a task which can cope with a missing dependency does.
		tg.NewTask("B", func(_ context.Context, b tg.Binder) ([]tg.Binding, error) {
			if _, err := keyA.Get(b); err != nil {
				return []tg.Binding{keyB.Bind(-1)}, nil
			}
			return []tg.Binding{keyB.Bind(1)}, nil
		}, []tg.ID{keyA.ID()}, []tg.ID{keyB.ID()}),
	)))

	result, err := runWith(testContext(t), g, keyIn.Bind(1), ov(keyA.BindError(errBoom)))
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if runsA.Load() != 0 {
		t.Errorf("A ran %d times; want it replaced by the failure", runsA.Load())
	}
	if got := mustGet(t, keyB, result); got != -1 {
		t.Errorf("b = %d; want -1, as B saw a absent", got)
	}
	tgt.ExpectAbsentError(t, result, keyA, errBoom)
}

func TestOverride_ConditionalTaskIsReplacedWhateverItsCondition(t *testing.T) {
	for _, flag := range []bool{true, false} {
		keyFlag, keyT, keyOut := tg.NewKey[bool]("flag"), tg.NewKey[int]("t"), tg.NewKey[int]("out")
		var runsT atomic.Int32
		g := tgt.Must[tg.Graph](t)(tg.New("conditional_override", tg.WithTasks(
			tg.Conditional{
				Wrapped: tg.SimpleTask("T", keyT, func(context.Context, tg.Binder) (int, error) {
					runsT.Add(1)
					return 1, nil
				}),
				Condition: tg.ConditionAnd{keyFlag},
			}.Locate(),
			tg.SimpleTask1[int, int]("Out", keyOut, func(_ context.Context, v int) (int, error) {
				return v * 10, nil
			}, keyT),
		)))

		result, err := runWith(testContext(t), g, keyFlag.Bind(flag), ov(keyT.Bind(7)))
		if err != nil {
			t.Fatalf("flag=%v: Run() failed: %v", flag, err)
		}
		if runsT.Load() != 0 {
			t.Errorf("flag=%v: T ran %d times; want it replaced", flag, runsT.Load())
		}
		if got, want := mustGet(t, keyOut, result), 70; got != want {
			t.Errorf("flag=%v: out = %d; want %d", flag, got, want)
		}
	}
}

func TestOverride_TasksWhichFedAReplacedTaskStillRunAndDoNotBlock(t *testing.T) {
	// This documents a limitation: the graph can't know which results its caller wants, so it can't
	// tell that nothing needs what only the replaced task consumed.
	f := newOverrideFixture(t)
	keyU, runsU := tg.NewKey[int]("u"), atomic.Int32{}
	g := tgt.Must[tg.Graph](t)(tg.New("upstream_still_runs", tg.WithTasks(
		tg.SimpleTask1[int, int]("U", keyU, func(_ context.Context, in int) (int, error) {
			runsU.Add(1)
			return in, nil
		}, f.keyIn),
		tg.SimpleTask1[int, int]("A", f.keyA, func(_ context.Context, u int) (int, error) {
			return u * 2, nil
		}, keyU),
	)))

	if _, err := runWith(testContext(t), g, f.keyIn.Bind(1), ov(f.keyA.Bind(5))); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if runsU.Load() != 1 {
		t.Errorf("U ran %d times; want once", runsU.Load())
	}
}

func TestOverride_InputsOnlyAReplacedTaskNeededAreNotRequired(t *testing.T) {
	keyOnlyForA, keyA, keyOut := tg.NewKey[int](
		"only_for_a",
	), tg.NewKey[int](
		"a",
	), tg.NewKey[int](
		"out",
	)
	g := tgt.Must[tg.Graph](t)(tg.New("needed_inputs", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"A",
			keyA,
			func(_ context.Context, v int) (int, error) { return v, nil },
			keyOnlyForA,
		),
		tg.SimpleTask1[int, int](
			"Out",
			keyOut,
			func(_ context.Context, a int) (int, error) { return a + 1, nil },
			keyA,
		),
	)))

	if _, err := runWith(testContext(t), g); !errors.Is(err, tg.ErrMissingInputs) {
		t.Errorf("without the override: got %v; want ErrMissingInputs", err)
	}
	result, err := runWith(testContext(t), g, ov(keyA.Bind(4)))
	if err != nil {
		t.Fatalf("with the override: Run() failed: %v", err)
	}
	if got, want := mustGet(t, keyOut, result), 5; got != want {
		t.Errorf("out = %d; want %d", got, want)
	}
}

func TestOverride_TasksDependingOnReplacedAndOrdinaryKeysDoNotWaitForeverOrDeadlock(t *testing.T) {
	keyIn, keyA, keyP, keyBoth, keyOnlyA := tg.NewKey[int](
		"in",
	), tg.NewKey[int](
		"a",
	), tg.NewKey[int](
		"p",
	),
		tg.NewKey[int](
			"both",
		), tg.NewKey[int](
			"only_a",
		)
	g := tgt.Must[tg.Graph](t)(tg.New("mixed_dependents", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"A",
			keyA,
			func(_ context.Context, v int) (int, error) { return v, nil },
			keyIn,
		),
		tg.SimpleTask1[int, int]("P", keyP, func(_ context.Context, v int) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return v * 100, nil
		}, keyIn),
		tg.SimpleTask2[int, int, int](
			"Both",
			keyBoth,
			func(_ context.Context, a, p int) (int, error) {
				return a + p, nil
			},
			keyA,
			keyP,
		),
		tg.SimpleTask1[int, int](
			"OnlyA",
			keyOnlyA,
			func(_ context.Context, a int) (int, error) { return a, nil },
			keyA,
		),
	)))

	result, err := runWith(testContext(t), g, keyIn.Bind(1), ov(keyA.Bind(7)))
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if got, want := mustGet(t, keyBoth, result), 107; got != want {
		t.Errorf("both = %d; want %d", got, want)
	}
	if got, want := mustGet(t, keyOnlyA, result), 7; got != want {
		t.Errorf("only_a = %d; want %d", got, want)
	}
}

func TestOverride_Errors(t *testing.T) {
	keyIn, keyA, keyM1, keyM2, keyZ := tg.NewKey[int](
		"in",
	), tg.NewKey[int](
		"a",
	), tg.NewKey[int](
		"m1",
	),
		tg.NewKey[int](
			"m2",
		), tg.NewKey[int](
			"z",
		)
	var runsM atomic.Int32
	g := tgt.Must[tg.Graph](t)(tg.New("override_errors", tg.WithTasks(
		tg.SimpleTask1[int, int](
			"A",
			keyA,
			func(_ context.Context, v int) (int, error) { return v, nil },
			keyIn,
		),
		tg.NewTask("M", func(_ context.Context, _ tg.Binder) ([]tg.Binding, error) {
			runsM.Add(1)
			return []tg.Binding{keyM1.Bind(1), keyM2.Bind(2)}, nil
		}, nil, []tg.ID{keyM1.ID(), keyM2.ID()}),
	)))

	for _, tc := range []struct {
		description string
		inputs      []tg.Binding
		wantError   error
		// Whether the problem is found before anything runs, which is true of everything but a plain
		// input for a key which a task provides, which is only found when that task stores its result.
		foundBeforeRunning bool
	}{
		{
			description:        "a key which no task provides",
			inputs:             []tg.Binding{keyIn.Bind(1), ov(keyZ.Bind(1))},
			wantError:          tg.ErrUnknownOverride,
			foundBeforeRunning: true,
		},
		{
			description: "an input, not an override, for a key which a task provides",
			inputs:      []tg.Binding{keyIn.Bind(1), keyA.Bind(5)},
			wantError:   tg.ErrDuplicateBinding,
		},
		{
			description:        "the same key overridden twice",
			inputs:             []tg.Binding{keyIn.Bind(1), ov(keyA.Bind(1)), ov(keyA.Bind(2))},
			wantError:          tg.ErrDuplicateBinding,
			foundBeforeRunning: true,
		},
		{
			description:        "a key which is also an ordinary input",
			inputs:             []tg.Binding{keyIn.Bind(1), keyA.Bind(1), ov(keyA.Bind(2))},
			wantError:          tg.ErrDuplicateBinding,
			foundBeforeRunning: true,
		},
		{
			description:        "only some of the keys which a task provides",
			inputs:             []tg.Binding{keyIn.Bind(1), ov(keyM1.Bind(1))},
			wantError:          tg.ErrPartialOverride,
			foundBeforeRunning: true,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			runsM.Store(0)
			if _, err := runWith(testContext(t), g, tc.inputs...); !errors.Is(err, tc.wantError) {
				t.Errorf("Run(): got %v; want %v", err, tc.wantError)
			}
			if tc.foundBeforeRunning {
				if runsM.Load() != 0 {
					t.Errorf(
						"M ran %d times; want nothing to run, as this is rejected first",
						runsM.Load(),
					)
				}
				if err := checkWith(g, tc.inputs...); !errors.Is(err, tc.wantError) {
					t.Errorf("Check(): got %v; want %v", err, tc.wantError)
				}
			}
		})
	}

	t.Run("all of the keys which a task provides", func(t *testing.T) {
		runsM.Store(0)
		inputs := []tg.Binding{keyIn.Bind(1), ov(keyM1.Bind(10)), ov(keyM2.Bind(20))}
		if err := checkWith(g, inputs...); err != nil {
			t.Fatalf("Check(): %v", err)
		}
		result, err := runWith(testContext(t), g, inputs...)
		if err != nil {
			t.Fatalf("Run(): %v", err)
		}
		if runsM.Load() != 0 {
			t.Errorf("M ran %d times; want it replaced", runsM.Load())
		}
		if got := mustGet(t, keyM2, result); got != 20 {
			t.Errorf("m2 = %d; want 20", got)
		}
	})
}

func TestOverride_GraphRunAsATaskIsReplacedAsAWhole(t *testing.T) {
	keyIn, keyX, keyY := tg.NewKey[int]("in"), tg.NewKey[int]("x"), tg.NewKey[int]("y")
	var runsInner atomic.Int32
	inner := tgt.Must[tg.Graph](t)(tg.New("inner", tg.WithTasks(
		tg.NewTask("XY", func(_ context.Context, b tg.Binder) ([]tg.Binding, error) {
			runsInner.Add(1)
			in := mustGet(t, keyIn, b)
			return []tg.Binding{keyX.Bind(in), keyY.Bind(in * 2)}, nil
		}, []tg.ID{keyIn.ID()}, []tg.ID{keyX.ID(), keyY.ID()}),
	)))
	asTask := tgt.Must[tg.Task](t)(inner.AsTask(keyX.ID(), keyY.ID()))
	outer := tgt.Must[tg.Graph](t)(tg.New("outer", tg.WithTasks(asTask)))

	if _, err := runWith(
		testContext(t),
		outer,
		keyIn.Bind(1),
		ov(keyX.Bind(9)),
	); !errors.Is(
		err,
		tg.ErrPartialOverride,
	) {
		t.Errorf(
			"overriding one of the keys the task exposes: got %v; want ErrPartialOverride",
			err,
		)
	}
	result, err := runWith(testContext(t), outer, keyIn.Bind(1), ov(keyX.Bind(9)), ov(keyY.Bind(8)))
	if err != nil {
		t.Fatalf("overriding both: Run() failed: %v", err)
	}
	if runsInner.Load() != 0 {
		t.Errorf("the inner graph ran %d times; want it replaced", runsInner.Load())
	}
	if got := mustGet(t, keyY, result); got != 8 {
		t.Errorf("y = %d; want 8", got)
	}
}
