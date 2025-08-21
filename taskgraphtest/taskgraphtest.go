// Package taskgraphtest provides utilities for testing Graphs and Tasks built using the taskgraph
// package.
package taskgraphtest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/go-cmp/cmp"

	tg "github.com/thought-machine/taskgraph"
)

// TransformMaybe returns a cmp.Option which allows comparison of Maybe[T]. When using this option,
// the cmpopts.EquateErrors() option should also be passed.
func TransformMaybe[T any]() cmp.Option {
	var val T
	return cmp.Transformer(fmt.Sprintf("%T", val), func(m tg.Maybe[T]) struct {
		Val T
		Err error
	} {
		val, err := m.Get()
		return struct {
			Val T
			Err error
		}{val, err}
	})
}

// ExpectPresent asserts that b contains key bound to want.
func ExpectPresent[T any](t *testing.T, b tg.Binder, key tg.ReadOnlyKey[T], want T, opts ...cmp.Option) {
	t.Helper()

	// Use key.Get so that this works with virtual keys like tg.Presence().
	got, err := key.Get(b)
	if err != nil {
		t.Error(err)
		return
	}
	if !cmp.Equal(got, want, opts...) {
		t.Errorf("difference in bound value: got %v; want %v", got, want)
	}
}

// DiffPresent asserts that b contains key bound to want, and produces a diff if the value is not
// correct.
func DiffPresent[T any](t *testing.T, b tg.Binder, key tg.ReadOnlyKey[T], want T, opts ...cmp.Option) {
	t.Helper()

	// Use key.Get so that this works with virtual keys like tg.Presence().
	got, err := key.Get(b)
	if err != nil {
		t.Error(err)
		return
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("difference in binding value (-want, +got):\n%s", diff)
	}
}

// ExpectAbsent asserts that b contains key bound as absent.
func ExpectAbsent[T any](t *testing.T, b tg.Binder, key tg.ReadOnlyKey[T]) {
	t.Helper()

	got := b.Get(key.ID())
	if got.Status() != tg.Absent {
		t.Errorf("difference in binding status: got %s; want %s", got.Status(), tg.Absent)
	}
}

// ExpectAbsentError asserts that b contains key bound as absent with the given error.
func ExpectAbsentError[T any](t *testing.T, b tg.Binder, key tg.ReadOnlyKey[T], wantErr error) {
	t.Helper()

	got := b.Get(key.ID())
	if got.Status() != tg.Absent {
		t.Errorf("difference in binding status: got %s; want %s", got.Status(), tg.Absent)
	} else if gotErr := got.Error(); !errors.Is(gotErr, wantErr) {
		t.Errorf("difference in absent binding error: got %v; want %v", gotErr, wantErr)
	}
}

// ExpectPending asserts that b does not contain a binding for key (i.e. it is pending).
func ExpectPending[T any](t *testing.T, b tg.Binder, key tg.ReadOnlyKey[T]) {
	t.Helper()

	got := b.Get(key.ID())
	if got.Status() != tg.Pending {
		t.Errorf("difference in binding status: got %s; want %s", got.Status(), tg.Pending)
	}
}

// A BindingMatcher is used to compare a binding produced by a graph. A BindingMatcher should
// return an error if the bindings do not match, and nil otherwise.
type BindingMatcher interface {
	// ID should return the id of the binding to be checked.
	ID() tg.ID

	// Match should return an error if the binding does not match, and nil otherwise.
	Match(got tg.Binding) error
}

type bindingMatcher struct {
	id      tg.ID
	matchFn func(got tg.Binding) error
}

func (bm bindingMatcher) ID() tg.ID {
	return bm.id
}

func (bm bindingMatcher) Match(got tg.Binding) error {
	return bm.matchFn(got)
}

// Match creates a BindingMatcher which checks the value of a single binding using cmp.Equal with
// the given options.
func Match(want tg.Binding, opts ...cmp.Option) BindingMatcher {
	return MatchFunc(want, func(got, want any) error {
		if !cmp.Equal(got, want, opts...) {
			return fmt.Errorf("difference in binding value: got %v; want %v", got, want)
		}
		return nil
	})
}

// MatchDiff creates a BindingMatcher which checks the value of a single binding using cmp.Diff with
// the given options, returning the diff (if any).
func MatchDiff(want tg.Binding, opts ...cmp.Option) BindingMatcher {
	return MatchFunc(want, func(got, want any) error {
		if diff := cmp.Diff(want, got, opts...); diff != "" {
			return fmt.Errorf("difference in binding value (-want, +got):\n%s", diff)
		}
		return nil
	})
}

// MatchFunc creates a BindingMatcher which compares present bindings using the given function.
func MatchFunc[T any](want tg.Binding, compareFn func(got, want T) error) BindingMatcher {
	return bindingMatcher{want.ID(), func(got tg.Binding) error {
		if got.Status() != want.Status() {
			err := fmt.Errorf("difference in binding %s status: got %s; want %s", want.ID(), got.Status(), want.Status())
			if got.Status() == tg.Absent {
				err = fmt.Errorf("%w - absent binding error: %w", err, got.Error())
			}
			return err
		}
		switch got.Status() {
		case tg.Present:
			gotVal, ok := got.Value().(T)
			if !ok {
				var x T
				return fmt.Errorf("got value was %T; want %T", gotVal, x)
			}
			wantVal, ok := want.Value().(T)
			if !ok {
				var x T
				return fmt.Errorf("want value was %T; want %T", wantVal, x)
			}
			if err := compareFn(gotVal, wantVal); err != nil {
				return err
			}
		case tg.Absent:
			if !errors.Is(got.Error(), want.Error()) {
				return fmt.Errorf("difference in binding error: got %v; want %v", got.Error(), want.Error())
			}
		}
		return nil
	}}
}

// ExpectBindings checks if the binder contains bindings which match the given matchers.
func ExpectBindings(t *testing.T, b tg.Binder, want []BindingMatcher) {
	t.Helper()

	expectBindings(t, b, want, false)
}

// ExpectExactBindings checks if the binder contains bindings which match the given matchers, and no
// other bindings. Note that this will not play nicely with virtual bindings such as those generated
// by tg.Presence().
func ExpectExactBindings(t *testing.T, b tg.Binder, want []BindingMatcher) {
	t.Helper()

	expectBindings(t, b, want, true)
}

func expectBindings(t *testing.T, b tg.Binder, want []BindingMatcher, exact bool) {
	t.Helper()

	for _, matcher := range want {
		if err := matcher.Match(b.Get(matcher.ID())); err != nil {
			t.Error(err)
		}
	}

	if exact {
		allBindings := b.GetAll()
		// The individual WantBindings will report any missing bindings, so we're only checking for
		// excess rather than inequality.
		if len(allBindings) > len(want) {
			wantIDs := set.NewSet[tg.ID]()
			for _, wb := range want {
				wantIDs.Add(wb.ID())
			}
			var excess []string
			for _, b := range allBindings {
				if !wantIDs.Contains(b.ID()) {
					excess = append(excess, b.ID().String())
				}
			}
			sort.Strings(excess)
			t.Errorf("Got excess bindings for IDs: %s", strings.Join(excess, ", "))
		}
	}
}

// Test defines the expected behaviour of a Task or Graph, validating expected bindings for given
// inputs.
type Test struct {
	// Description is passed to testing.T.Run when this test is part of a suite; it may be omited if
	// the test is used in isolation.
	Description string

	// Graph defines the Graph to be tested. Should not be set in conjunction with Task. The Graph or
	// Task to be tested is preferentially read from the Test, falling back to the Suite (if used).
	Graph tg.Graph

	// Task provides a convenient approach for testing individual tasks; the given Task is turned into
	// a single-node Graph. Should not be set in conjunction with Graph. The Graph or Task to be
	// tested is preferentially read from the Test, falling back to the Suite (if used).
	//
	// For compatibility with helpers which accept and return TaskSets, this field is a TaskSet, but
	// the set is expected to contain a single element.
	Task tg.TaskSet

	// Inputs defines the inputs passed to the graph for the test.
	Inputs []tg.Binding

	// WantError defines the expected error returned from the Graph's execution (note that the test
	// will always fail if Graph setup/configuration fails). Compared using errors.Is.
	WantError error

	// WantBindings defines the expected set of bindings to be available once the graph/task is run.
	// Note that unless CheckExcessBindings is set, this field need not contain every single binding
	// produced by a graph (and in general it is advisable that it doesn't to avoid fragile tests).
	WantBindings []BindingMatcher

	// WantPendingBindings defines a set of IDs which should not have been bound by the graph. This
	// is typically only used for testing the framework itself (asserting that IDs are unbound when
	// errors occur)
	WantPendingBindings []tg.ID

	// CheckExcessBindings, if true, causes the graph result to be checked for the presence of
	// bindings not in WantBindings. This option should be used sparingly, as it can lead to fragile
	// tests. Note that this will not play nicely with virtual bindings such as those generated
	// by tg.Presence().
	CheckExcessBindings bool

	// WantInputBindings defines matchers on input bindings (i.e. those in Inputs) which are checked
	// after the graph is run. This can be used to check for side effects on inputs.
	WantInputBindings []BindingMatcher

	// How long the task/graph should be allowed to run. If unset, defaults to 10 seconds
	Timeout time.Duration
}

// Run the test.
func (test Test) Run(t *testing.T) {
	if test.Graph != nil && test.Task != nil {
		t.Fatal("Invalid Test: exactly one of Graph and Task may be set")
	}
	if test.Graph == nil && test.Task == nil {
		t.Fatal("Invalid Test: no Graph or Task set")
	}

	if test.Task != nil {
		var err error
		test.Graph, err = tg.New("test_graph", tg.WithTasks(test.Task))
		if err != nil {
			t.Fatalf("Failed to create Graph from Task: %v", err)
		}
	}

	if test.Timeout <= 0 {
		test.Timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), test.Timeout)
	defer cancel()

	result, err := test.Graph.Run(ctx, test.Inputs...)

	if !errors.Is(err, test.WantError) {
		t.Fatalf("Difference in error from Graph.Run(): got %v; want %v", err, test.WantError)
	}
	if err != nil {
		return
	}

	expectBindings(t, result, test.WantBindings, test.CheckExcessBindings)

	for _, id := range test.WantPendingBindings {
		if got := result.Get(id); got.Status() != tg.Pending {
			t.Errorf("Expected ID %s to be pending; got %s", id, got)
		}
	}

	inputsBinder := tg.NewBinder()
	if err := inputsBinder.Store(test.Inputs...); err != nil {
		t.Fatalf("Failed to create inputs binder: %v", err)
	}
	expectBindings(t, inputsBinder, test.WantInputBindings, false)
}

// Suite defines a collection of tests which can be run against a common task or graph, as a
// convenience over defining your own table-driven tests. Each Test in the Suite is run as a subtest
// using testing.T.Run.
type Suite struct {
	// Graph defines the Graph to be tested. Should not be set in conjunction with Task. The Graph or
	// Task to be tested is preferentially read from each Test, falling back to the Suite (if used).
	Graph tg.Graph

	// Task provides a convenient approach for testing individual tasks; the given Task is turned into
	// a single-node Graph. Should not be set in conjunction with Graph. The Graph or Task to be
	// tested is preferentially read from each Test, falling back to the Suite (if used).
	//
	// For compatibility with helpers which accept and return TaskSets, this field is a TaskSet, but
	// the set is expected to contain a single element.
	Task tg.TaskSet

	// Tests defines the individual tests to be run.
	Tests []Test

	// How long the task/graph should be allowed to run in each test. If unset, defaults to 10 seconds
	Timeout time.Duration
}

// Run all tests in the Suite.
func (s Suite) Run(t *testing.T) {
	if s.Graph != nil && s.Task != nil {
		t.Fatal("Invalid Suite: exactly one of Graph and Task may be set")
	}

	if s.Task != nil {
		var err error
		s.Graph, err = tg.New("test_graph", tg.WithTasks(s.Task))
		if err != nil {
			t.Fatalf("Failed to create Graph from Task: %v", err)
		}
	}

	for _, test := range s.Tests {
		if test.Graph == nil && test.Task == nil {
			test.Graph = s.Graph
		}
		if test.Timeout <= 0 {
			test.Timeout = s.Timeout
		}
		t.Run(test.Description, test.Run)
	}
}

// DummyTaskFunc returns a function suitable for passing to taskgraph.NewTask which returns the
// given bindings.
func DummyTaskFunc(bindings ...tg.Binding) func(context.Context, tg.Binder) ([]tg.Binding, error) {
	return func(_ context.Context, _ tg.Binder) ([]tg.Binding, error) {
		return bindings, nil
	}
}

// ErrorTaskFunc returns a function suitable for passing to taskgraph.NewTask which returns the
// given error.
func ErrorTaskFunc(err error) func(context.Context, tg.Binder) ([]tg.Binding, error) {
	return func(_ context.Context, _ tg.Binder) ([]tg.Binding, error) {
		return nil, err
	}
}

// Must produces a function which wraps around another function returning a value and error, calls
// t.Fatal if the error is non-nil, and otherwise returns the value. This is intended to be used for
// constructing Tasks and Graphs within tests:
//
//	func TestFoo(t *testing.T) (
//	  myTask  = taskgraphtest.Must(t)(taskgraph.Reflect(...))
//	  myGraph = taskgraphtest.Must(t)(taskgraph.New(myTask))
//	)
func Must[T any](t *testing.T) func(val T, err error) T {
	return func(val T, err error) T {
		if err != nil {
			t.Fatal(err)
		}
		return val
	}
}
