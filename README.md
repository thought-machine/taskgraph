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

## Limitations and sharp edges

* Taskgraph has no deadlock detection. In theory the fact that the graph is acyclic should prevent
  deadlock, but in practice there may be other potential causes for deadlock that have not been
  considered.
* Taskgraph runs every task in its own goroutine, with no limitation on how many tasks can be
  running at the same time.
* Taskgraph is not easy to debug and understand the execution. While there is a small amount of
  logging and tracing, there is no way to inspect the data being passed through the graph.
