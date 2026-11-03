# Taskgraph

Package taskgraph provides declarative modelling and orchestration of workflows which look like
directed acyclic graphs.

Taskgraph was designed to be used for building operators which reconcile Kubernetes custom
resources (custom resource reconciliation is often a multi-step process, where each step has
dependencies on other steps). However, it is not in any way tied to Kubernetes, and can be used for
other workflows as well.

Tasks form the nodes in the graph, and the dependencies between them form the edges between the
tasks. The dependencies are modeled as typed `Key`s; the tasks declare which keys they depend on
and which Keys they provide values for. There is thus an edge in the graph between the task which
produces a key and each task which depends on the key. It is an error for multiple tasks to
provide the same key.

When executing a graph, keys are bound to values; tasks which depend on a key can then read the
bound value and use it to perform its own work. It is an error for a task to not bind a key which
the task declares that it provides.

Execution of a graph starts with the "source" tasks whose dependencies are already fulfilled
(either because they do not depend on any keys, or because the dependencies have been provided as
inputs to the graph). As tasks complete and produce bindings for their provided keys, the
dependent tasks are triggered (with checks to wait for all dependencies to be available). As a
result, execution of the graph is performed as a parallelised breadth first search of the graph.

This package contains some creative APIs which work around limitations in Golang generics; chief
of these is the `ID` type used in place of slices of keys because Golang slices are invariant.

## Overriding tasks in tests

A graph's tasks often create things for themselves: clients of other services, say. That makes a
whole graph hard to test, as there is no way to give it a fake client. `taskgraphtest` can replace
the task which provides a key:

```go
taskgraphtest.RunWithOverrides(ctx, graph, inputs, []tg.Binding{keyClient.Bind(fakeClient)})
```

The task providing `keyClient` is not run, and the tasks which depend on it see the fake. This makes
a graph's dependencies a matter of which keys its tasks provide: anything which a test may want to
replace should be a key of its own.

Replacing tasks is only available from `taskgraphtest`. `Graph.Run` has no way to do it, and the
engine's support is in an `internal` package, which nothing outside this module can import. Code which
runs a graph and needs to be tested can take a function for running graphs, and be handed
`Overrider.Run` by its tests.

* An input for a key which a task provides is still an error, so a key is only ever replaced on
  purpose. The same goes for overriding one key twice, or overriding a key which is also an input.
* If a task provides several keys they must all be overridden, or none (`ErrPartialOverride`).
  Otherwise the task would still have to run, with all of its effects, with some of its results
  thrown away.
* Overriding a key which no task provides is an error (`ErrUnknownOverride`), which catches
  overrides left behind by a task which has since been renamed or removed. `CheckWithOverrides`
  finds these without running the graph.
* The override may be absent: `keyClient.BindError(err)` replaces the task with a failure, to see how
  the tasks which depend on it cope.
* The overrides are included in the `Binder` returned, as the results of the tasks they replace would
  have been.
* Inputs which only a replaced task needed are not required.
* Tasks which only produced inputs for a replaced task still run, as the graph can't know that its
  caller doesn't want their results.
* Overrides apply to the graph they are given. A graph run as a task with `AsTask` is replaced as a
  whole, by overriding every key it exposes, but its tasks can't be overridden individually.
* An `Overrider` holds one set of overrides for something which runs several graphs, such as an
  operator with a graph to create and a graph to delete. Each graph is run with the overrides for the
  keys it provides, and `Overrider.Check` makes sure that every override is for some graph.

`taskgraphtest.Test` and `taskgraphtest.Suite` take `Overrides`, those of a `Test` replacing those
of its `Suite` for the same key.

## Limitations and sharp edges

* Taskgraph has no deadlock detection. In theory the fact that the graph is acyclic should prevent
  deadlock, but in practice there may be other potential causes for deadlock that have not been
  considered.
* Taskgraph runs every task in its own goroutine, with no limitation on how many tasks can be
  running at the same time.
* Taskgraph is not easy to debug and understand the execution. While there is a small amount of
  logging and tracing, there is no way to inspect the data being passed through the graph.
