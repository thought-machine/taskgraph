// Package overridehook lets taskgraphtest reach the part of the graph engine which replaces tasks.
//
// Replacing tasks is meant for tests, so it is not part of the taskgraph package's API. Go only lets
// packages within this module import an internal package, so this is how taskgraphtest, and nothing
// outside the module, can get at it. The taskgraph package sets the variables when it is
// initialised.
//
// The functions take the arguments typed as any, as this package can't import taskgraph, which
// imports it; taskgraphtest knows the types and converts.
package overridehook

import "context"

var (
	// RunWithOverrides runs a graph made by taskgraph.New, as Graph.Run does, with some of its tasks
	// replaced by the override bindings, and returns a taskgraph.Binder. If ignoreUnprovided is set,
	// overrides for keys which the graph doesn't provide are left out, rather than being an error.
	RunWithOverrides func(g any, ctx context.Context, inputs, overrides []any, ignoreUnprovided bool) (any, error)

	// CheckWithOverrides is Graph.Check, with overrides.
	CheckWithOverrides func(g any, inputs, overrides []any) error

	// Provides reports whether some task in a graph made by taskgraph.New provides the key (a
	// taskgraph.ID).
	Provides func(g any, id any) bool
)
