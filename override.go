package taskgraph

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	set "github.com/deckarep/golang-set/v2"
)

var (
	// ErrUnknownOverride is returned by Graph.RunWithOverrides if an override is
	// given for a key which no task in the graph provides. This is almost always a stale override,
	// left over from a task which has since been renamed or removed.
	ErrUnknownOverride = errors.New("override of a key which no task provides")

	// ErrPartialOverride is returned by Graph.RunWithOverrides if overrides cover
	// some, but not all, of the keys which a task provides. The task would still have to run, with
	// all of its effects, only to have some of its results discarded, which is very unlikely to be
	// what was intended. Override all of a task's keys, or none of them.
	ErrPartialOverride = errors.New("override of only some of the keys which a task provides")
)

// RunWithOverrides is Graph.RunWithOverrides.
func (g *graph) RunWithOverrides(
	ctx context.Context,
	inputs, overrides []Binding,
) (Binder, error) {
	replaced, err := g.withOverrides(inputs, overrides)
	if err != nil {
		return nil, err
	}
	return replaced.Run(ctx, inputs...)
}

// Provides is Graph.Provides.
func (g *graph) Provides(key ID) bool { return g.allProvided.Contains(key) }

// withOverrides returns a graph like g, but with each task whose keys are all overridden replaced by a
// task which provides the overriding bindings. The graph is g itself if there are no overrides.
//
// A replacement has no dependencies, so the tasks which fed the task it replaces have nothing to
// signal, and inputs which only the replaced task needed are not required. Those tasks still run, as
// the graph can't know that its caller doesn't want their results.
//
// An override is an error if it is for a key which is also an input or is overridden twice, as that
// is a mistake in which one is meant; if no task provides its key, which is nearly always a task
// which has been renamed or removed; or if it is for only some of the keys which a task provides,
// as the task would then have to run for the others and its effects could not be ruled out.
func (g *graph) withOverrides(inputs, overrides []Binding) (Graph, error) {
	if len(overrides) == 0 {
		return g, nil
	}

	given := set.NewSet[ID]()
	for _, input := range inputs {
		given.Add(input.ID())
	}
	overriddenBy := map[ID]Binding{}
	for _, o := range overrides {
		if given.Contains(o.ID()) {
			return nil, wrapStackErrorf(
				"override of a key which is also an input: %w: %q", ErrDuplicateBinding, o.ID())
		}
		if _, twice := overriddenBy[o.ID()]; twice {
			return nil, wrapStackErrorf("key overridden twice: %w: %q", ErrDuplicateBinding, o.ID())
		}
		overriddenBy[o.ID()] = o
	}

	var unknown []string
	for id := range overriddenBy {
		if !g.allProvided.Contains(id) {
			unknown = append(unknown, id.String())
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, wrapStackErrorf("%w: %s", ErrUnknownOverride, strings.Join(unknown, ", "))
	}

	var tasks []TaskSet
	var partial []string
	for _, t := range g.tasks {
		var covered []Binding
		var uncovered []string
		for _, id := range t.Provides() {
			if o, ok := overriddenBy[id]; ok {
				covered = append(covered, o)
			} else {
				uncovered = append(uncovered, id.String())
			}
		}
		switch {
		case len(covered) == 0:
			tasks = append(tasks, t)
		case len(uncovered) == 0:
			tasks = append(tasks, replacement(t, covered))
		default:
			partial = append(partial, fmt.Sprintf(
				"task %s also provides [%s], which are not overridden",
				t.Name(), strings.Join(uncovered, ", ")))
		}
	}
	if len(partial) > 0 {
		sort.Strings(partial)
		return nil, wrapStackErrorf("%w: %s", ErrPartialOverride, strings.Join(partial, "; "))
	}

	return New(g.name, WithTasks(tasks...), WithTracer(g.tracer), WithLogger(g.logger))
}

// replacement is a task in place of t which provides the overrides for all of the keys of t.
func replacement(t Task, overrides []Binding) Task {
	return NewTask(
		t.Name()+" (overridden)",
		func(context.Context, Binder) ([]Binding, error) { return overrides, nil },
		nil,
		t.Provides(),
	)
}
