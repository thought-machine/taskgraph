package taskgraphtest

import (
	"context"
	"fmt"
	"sort"
	"strings"

	tg "github.com/thought-machine/taskgraph"
	"github.com/thought-machine/taskgraph/internal/overridehook"
)

func toAny(bindings []tg.Binding) []any {
	out := make([]any, 0, len(bindings))
	for _, b := range bindings {
		out = append(out, b)
	}
	return out
}

// RunWithOverrides runs the graph, as Graph.Run does with the inputs, with the tasks which provide
// the keys of the overrides replaced by them. It is the way to substitute what the tasks of a graph
// would otherwise create for themselves, such as clients of other services, in a test:
//
//	taskgraphtest.RunWithOverrides(ctx, graph, inputs, []tg.Binding{keyClient.Bind(fakeClient)})
//
// The task providing the key is not run. If the task provides other keys as well, they must all be
// overridden too (tg.ErrPartialOverride), so that the real task's effects can never happen
// unnoticed. Tasks which have no other purpose than to produce the inputs of a task which is not run
// are still run, as are tasks which depend on the overridden key; they see the override as if the
// task which it replaces had produced it. The overrides are included in the Binder returned, as the
// replaced tasks' results would have been, and inputs which only a replaced task needed are not
// required.
//
// An override may be absent, which replaces the task with a failure:
//
//	keyClient.BindError(errUnreachable)
//
// Overriding a key which no task provides is an error (tg.ErrUnknownOverride), as is overriding the
// same key twice, or overriding a key which is also given as an input. An input for a key which a task
// provides is still an error (tg.ErrDuplicateBinding), so a key can only ever be replaced on purpose.
//
// Overrides apply to the graph's own tasks. A graph run as a task with Graph.AsTask is replaced as a
// whole, by overriding every key it exposes, but its tasks can't be overridden individually.
func RunWithOverrides(
	ctx context.Context,
	g tg.Graph,
	inputs, overrides []tg.Binding,
) (tg.Binder, error) {
	return runWithOverrides(ctx, g, inputs, overrides, false)
}

func runWithOverrides(
	ctx context.Context,
	g tg.Graph,
	inputs, overrides []tg.Binding,
	ignoreUnprovided bool,
) (tg.Binder, error) {
	result, err := overridehook.RunWithOverrides(
		g,
		ctx,
		toAny(inputs),
		toAny(overrides),
		ignoreUnprovided,
	)
	if err != nil {
		return nil, err
	}
	return result.(tg.Binder), nil
}

// CheckWithOverrides is Graph.Check, with the overrides RunWithOverrides would be given: it finds
// unknown, duplicated and partial overrides, and which inputs are still needed, without running
// anything.
func CheckWithOverrides(g tg.Graph, inputs, overrides []tg.Binding) error {
	return overridehook.CheckWithOverrides(g, toAny(inputs), toAny(overrides))
}

// An Overrider runs graphs with one set of overrides. It is for something which runs several graphs,
// such as an operator with a graph to create and one to delete, and which can only be given one set:
// each graph is run with those of the overrides which are for keys it provides, and Check makes sure
// that every override is for some graph.
type Overrider struct {
	overrides []tg.Binding
}

// NewOverrider returns an Overrider with the overrides.
func NewOverrider(overrides ...tg.Binding) *Overrider {
	return &Overrider{overrides: overrides}
}

// Run runs the graph with the inputs, and those of the Overrider's overrides which are for keys the
// graph provides. Its signature is that of a function for running a graph, so it can be handed to
// code which runs graphs.
func (o *Overrider) Run(ctx context.Context, g tg.Graph, inputs ...tg.Binding) (tg.Binder, error) {
	return runWithOverrides(ctx, g, inputs, o.overrides, true)
}

// Check returns an error if any of the overrides is for a key which none of the graphs provide, which
// is nearly always one left behind by a task which has been renamed or removed.
func (o *Overrider) Check(graphs ...tg.Graph) error {
	var unknown []string
	for _, override := range o.overrides {
		provided := false
		for _, g := range graphs {
			if overridehook.Provides(g, override.ID()) {
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
		return fmt.Errorf("%w: %s", tg.ErrUnknownOverride, strings.Join(unknown, ", "))
	}
	return nil
}
