package taskgraph

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	set "github.com/deckarep/golang-set/v2"
	"github.com/thought-machine/taskgraph/internal/overridehook"
)

var (
	// ErrUnknownOverride is returned by taskgraphtest's RunWithOverrides and CheckWithOverrides if an
	// override is given for a key which no task in the graph provides. This is almost always a stale
	// override, left over from a task which has since been renamed or removed.
	ErrUnknownOverride = errors.New("override of a key which no task provides")

	// ErrPartialOverride is returned by taskgraphtest's RunWithOverrides and CheckWithOverrides if
	// overrides cover some, but not all, of the keys which a task provides. The task would still have to run, with all of its
	// effects, only to have some of its results discarded, which is very unlikely to be what was
	// intended. Override all of a task's keys, or none of them.
	ErrPartialOverride = errors.New("override of only some of the keys which a task provides")
)

func init() {
	overridehook.RunWithOverrides = func(g any, ctx context.Context, inputs, overrides []any, ignoreUnprovided bool) (any, error) {
		graph, err := asGraph(g)
		if err != nil {
			return nil, err
		}
		return graph.runWithOverrides(
			ctx,
			toBindings(inputs),
			toBindings(overrides),
			ignoreUnprovided,
		)
	}
	overridehook.CheckWithOverrides = func(g any, inputs, overrides []any) error {
		graph, err := asGraph(g)
		if err != nil {
			return err
		}
		_, err = graph.buildInputs(toBindings(inputs), toBindings(overrides), false)
		return err
	}
	overridehook.Provides = func(g any, id any) bool {
		graph, err := asGraph(g)
		if err != nil {
			return false
		}
		return graph.allProvided.Contains(id.(ID))
	}
}

func asGraph(g any) (*graph, error) {
	graph, ok := g.(*graph)
	if !ok {
		return nil, fmt.Errorf(
			"overrides can only be applied to a Graph made by taskgraph.New, not %T",
			g,
		)
	}
	return graph, nil
}

func toBindings(in []any) []Binding {
	out := make([]Binding, 0, len(in))
	for _, b := range in {
		out = append(out, b.(Binding))
	}
	return out
}

// runInputs is everything a graph is given to run with, once checked against the graph.
type runInputs struct {
	// inputs holds the ordinary inputs.
	inputs Binder
	// overrides holds the overriding bindings, unwrapped.
	overrides []Binding
	// skipped holds the nodes of the tasks which the overrides replace.
	skipped map[*graphNode]struct{}
}

// overriddenKeys returns the IDs of the overridden keys, sorted, for reporting.
func (ri *runInputs) overriddenKeys() []string {
	var ids []string
	for _, o := range ri.overrides {
		ids = append(ids, o.ID().String())
	}
	sort.Strings(ids)
	return ids
}

// skippedTaskNames returns the names of the tasks which are not run, sorted, for reporting.
func (ri *runInputs) skippedTaskNames() []string {
	var names []string
	for node := range ri.skipped {
		names = append(names, node.task.Name())
	}
	sort.Strings(names)
	return names
}

// buildInputs checks the given inputs and overrides against the graph, working out which tasks, if
// any, the overrides replace. If ignoreUnprovided is set, overrides for keys which no task in the
// graph provides are left out, instead of being an error.
func (g *graph) buildInputs(
	inputs, overrides []Binding,
	ignoreUnprovided bool,
) (*runInputs, error) {
	b := NewBinder()
	if err := b.Store(inputs...); err != nil {
		return nil, wrapStackErrorf("duplicate input: %w", err)
	}

	result := &runInputs{inputs: b, overrides: overrides, skipped: map[*graphNode]struct{}{}}
	if ignoreUnprovided {
		result.overrides = nil
		for _, override := range overrides {
			if g.allProvided.Contains(override.ID()) {
				result.overrides = append(result.overrides, override)
			}
		}
	}
	if len(result.overrides) > 0 {
		if err := g.checkOverrides(b, result); err != nil {
			return nil, err
		}
	}

	// The dependencies which have to be given as inputs are those of the tasks which will run, which
	// are fewer if some are replaced.
	required := g.allDependencies.Difference(g.allProvided)
	if len(result.skipped) > 0 {
		required = set.NewSet[ID]()
		for _, node := range g.nodes {
			if _, skipped := result.skipped[node]; !skipped {
				required.Append(node.task.Depends()...)
			}
		}
		required = required.Difference(g.allProvided)
	}
	var missingInputs []string
	for requiredInput := range required.Iter() {
		if !b.Has(requiredInput) {
			missingInputs = append(missingInputs, requiredInput.String())
		}
	}
	if len(missingInputs) > 0 {
		sort.Strings(missingInputs)
		return nil, wrapStackErrorf("%w: %s", ErrMissingInputs, strings.Join(missingInputs, ", "))
	}

	return result, nil
}

// checkOverrides checks the overrides, filling in which nodes they replace.
func (g *graph) checkOverrides(ordinary Binder, result *runInputs) error {
	overridden := set.NewSet[ID]()
	for _, o := range result.overrides {
		if ordinary.Has(o.ID()) || overridden.Contains(o.ID()) {
			return wrapStackErrorf("duplicate input: %w: %q", ErrDuplicateBinding, o.ID())
		}
		overridden.Add(o.ID())
	}

	var unknown []string
	for id := range overridden.Difference(g.allProvided).Iter() {
		unknown = append(unknown, id.String())
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return wrapStackErrorf("%w: %s", ErrUnknownOverride, strings.Join(unknown, ", "))
	}

	var partial []string
	for _, node := range g.nodes {
		provides := node.task.Provides()
		var covered, uncovered []string
		for _, id := range provides {
			if overridden.Contains(id) {
				covered = append(covered, id.String())
			} else {
				uncovered = append(uncovered, id.String())
			}
		}
		switch {
		case len(covered) == 0:
		case len(uncovered) == 0:
			result.skipped[node] = struct{}{}
		default:
			partial = append(partial, fmt.Sprintf(
				"task %s provides [%s, %s], of which only [%s] are overridden",
				node.task.Name(),
				strings.Join(covered, ", "),
				strings.Join(uncovered, ", "),
				strings.Join(covered, ", "),
			))
		}
	}
	if len(partial) > 0 {
		sort.Strings(partial)
		return wrapStackErrorf("%w: %s", ErrPartialOverride, strings.Join(partial, "; "))
	}
	return nil
}
