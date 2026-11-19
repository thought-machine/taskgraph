package taskgraph

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrUnknownOverride is returned by RunWithOverrides and CheckWithOverrides if an override is
	// given for a key which no task in the graph provides. This is almost always a stale override,
	// left over from a task which has since been renamed or removed.
	ErrUnknownOverride = errors.New("override of a key which no task provides")

	// ErrPartialOverride is returned by RunWithOverrides and CheckWithOverrides if overrides cover
	// some, but not all, of the keys which a task provides. The task would still have to run, with
	// all of its effects, only to have some of its results discarded, which is very unlikely to be
	// what was intended. Override all of a task's keys, or none of them.
	ErrPartialOverride = errors.New("override of only some of the keys which a task provides")
)

// RunWithOverrides runs the graph, as Graph.Run does with the inputs, with the tasks which provide
// the keys of the overrides replaced by them. It is the way to substitute what the tasks of a graph
// would otherwise create for themselves, such as clients of other services, in a test:
//
//	taskgraph.RunWithOverrides(ctx, graph, inputs, []taskgraph.Binding{keyClient.Bind(fakeClient)})
//
// The task providing the key is not run. If the task provides other keys as well, they must all be
// overridden too (ErrPartialOverride), so that the real task's effects can never happen unnoticed.
// Tasks which have no other purpose than to produce the inputs of a task which is not run are still
// run, as are tasks which depend on the overridden key; they see the override as if the task which
// it replaces had produced it. The overrides are included in the Binder returned, as the replaced
// tasks' results would have been, and inputs which only a replaced task needed are not required.
//
// An override may be absent, which replaces the task's output with an absent, errored binding, to see
// how the tasks which depend on it cope:
//
//	keyClient.BindError(errUnreachable)
//
// Overriding a key which no task provides is an error (ErrUnknownOverride), as is overriding the same
// key twice, or overriding a key which is also given as an input. An input for a key which a task
// provides is still an error (ErrDuplicateBinding), so a key can only ever be replaced on purpose.
//
// Overrides apply to the graph's own tasks. A graph run as a task with Graph.AsTask is replaced as a
// whole, by overriding every key it exposes, but its tasks can't be overridden individually.
//
// This is a function, not a method of Graph, so that the Graph interface, which other code may
// implement, is not changed. It only works on graphs made by New.
func RunWithOverrides(
	ctx context.Context,
	g Graph,
	inputs, overrides []Binding,
) (Binder, error) {
	graph, err := asGraph(g)
	if err != nil {
		return nil, err
	}
	replaced, err := graph.withOverrides(inputs, overrides)
	if err != nil {
		return nil, err
	}
	return replaced.Run(ctx, inputs...)
}

// CheckWithOverrides is Graph.Check, with the overrides RunWithOverrides would be given: it finds
// unknown, duplicated and partial overrides, and which inputs are still needed, without running
// anything.
func CheckWithOverrides(g Graph, inputs, overrides []Binding) error {
	graph, err := asGraph(g)
	if err != nil {
		return err
	}
	replaced, err := graph.withOverrides(inputs, overrides)
	if err != nil {
		return err
	}
	return replaced.Check(inputs...)
}

// An Overrider runs graphs with one set of overrides. It is for something which runs several graphs,
// such as an operator with a graph to create and one to delete, and which can only be given one set:
// each graph is run with those of the overrides which are for keys it provides, and Check makes sure
// that every override is for some graph. Run does not report an override which is for none of the
// graphs, as it can't tell that from one for another graph, so Check is how to find a stale one.
type Overrider struct {
	overrides []Binding
}

// NewOverrider returns an Overrider with the overrides.
func NewOverrider(overrides ...Binding) *Overrider {
	return &Overrider{overrides: overrides}
}

// Run runs the graph with the inputs, and those of the Overrider's overrides which are for keys the
// graph provides. Its signature is that of a function for running a graph, so it can be handed to
// code which runs graphs.
func (o *Overrider) Run(ctx context.Context, g Graph, inputs ...Binding) (Binder, error) {
	graph, err := asGraph(g)
	if err != nil {
		return nil, err
	}
	var own []Binding
	for _, override := range o.overrides {
		if graph.allProvided.Contains(override.ID()) {
			own = append(own, override)
		}
	}
	return RunWithOverrides(ctx, g, inputs, own)
}

// Check returns an error if any of the overrides is for a key which none of the graphs provide, which
// is nearly always one left behind by a task which has been renamed or removed.
func (o *Overrider) Check(graphs ...Graph) error {
	var unknown []string
	for _, override := range o.overrides {
		provided := false
		for _, g := range graphs {
			if graph, err := asGraph(g); err == nil && graph.allProvided.Contains(override.ID()) {
				provided = true
				break
			}
		}
		if !provided {
			unknown = append(unknown, override.ID().String())
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("%w: %s", ErrUnknownOverride, strings.Join(unknown, ", "))
	}
	return nil
}

func asGraph(g Graph) (*graph, error) {
	graph, ok := g.(*graph)
	if !ok {
		return nil, fmt.Errorf(
			"overrides can only be applied to a Graph made by taskgraph.New, not %T",
			g,
		)
	}
	return graph, nil
}
