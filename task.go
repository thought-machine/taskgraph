package taskgraph

import (
	"context"

	set "github.com/deckarep/golang-set/v2"
)

// A TaskSet defines a nestable collection of tasks. A Task fulfils the TaskSet interface, acting as
// a singleton set.
type TaskSet interface {
	Tasks() []Task
}

type taskset []TaskSet

func (ts taskset) Tasks() []Task {
	var result []Task
	for _, el := range ts {
		result = append(result, el.Tasks()...)
	}
	return result
}

// NewTaskSet creates a new TaskSet from Tasks or TaskSets (or a combination of both).
func NewTaskSet(els ...TaskSet) TaskSet {
	return taskset(els)
}

// A Task represents a small unit of work within the graph (tasks form the nodes of the graph).
type Task interface {
	// A task can be considered to be a singleton set.
	TaskSet

	// Name returns the name of the task given when the task was created.
	Name() string
	// Depends returns the IDs of the keys on which this task depends (i.e. the keys which must be
	// provided as a graph input or by another task before this task can be executed)
	Depends() []ID
	// Provides returns the IDs of the keys for which this task provides bindings (i.e. the list of
	// Bindings returned by Execute must exactly match this list aside from ordering).
	Provides() []ID
	// Execute performs the unit of work for this task, consuming its dependencies from the given
	// Binder, and returning Bindings for each key the task has declared that it provides. Any error
	// returned from Execute() will terminate the processing of the entire graph.
	Execute(context.Context, Binder) ([]Binding, error)
	// Location returns the file and line where this task was defined.
	Location() string
}

type task struct {
	name     string
	depends  []ID
	provides []ID
	fn       func(context.Context, Binder) ([]Binding, error)
	location string
}

func (t *task) Tasks() []Task {
	return []Task{t}
}

func (t *task) Name() string {
	return t.name
}

func (t *task) Depends() []ID {
	return t.depends
}

func (t *task) Provides() []ID {
	return t.provides
}

func (t *task) Execute(ctx context.Context, b Binder) ([]Binding, error) {
	return t.fn(ctx, b)
}

func (t *task) Location() string {
	return t.location
}

// NewTask builds a task with any number of inputs and outputs.
func NewTask(name string, fn func(context.Context, Binder) ([]Binding, error), depends, provides []ID) Task {
	return &task{
		name:     name,
		depends:  depends,
		provides: provides,
		fn:       fn,
		location: getLocation(),
	}
}

// NoOutputTask builds a task which may consume inputs but produces no output bindings.
func NoOutputTask(name string, fn func(ctx context.Context, b Binder) error, depends ...ID) Task {
	return &task{
		name:    name,
		depends: depends,
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			return nil, fn(ctx, b)
		},
		location: getLocation(),
	}
}

// SimpleTask builds a task which produces a single output binding.
func SimpleTask[T any](name string, key Key[T], fn func(ctx context.Context, b Binder) (T, error), depends ...ID) Task {
	return &task{
		name:     name,
		depends:  depends,
		provides: []ID{key.ID()},
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			val, err := fn(ctx, b)
			if err != nil {
				return nil, err
			}
			return []Binding{key.Bind(val)}, nil
		},
		location: getLocation(),
	}
}

// SimpleTask1 builds a task from a function taking a single argument and returning a single value plus an error.
func SimpleTask1[A1, Res any](name string, resKey Key[Res], fn func(ctx context.Context, arg1 A1) (Res, error), depKey1 ReadOnlyKey[A1]) Task {
	return &task{
		name:     name,
		depends:  []ID{depKey1.ID()},
		provides: []ID{resKey.ID()},
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			arg1, err := depKey1.Get(b)
			if err != nil {
				return nil, err
			}
			res, err := fn(ctx, arg1)
			if err != nil {
				return nil, err
			}
			return []Binding{resKey.Bind(res)}, nil
		},
		location: getLocation(),
	}
}

// SimpleTask2 builds a task from a function taking two arguments and returning a single value plus an error.
func SimpleTask2[A1, A2, Res any](name string, resKey Key[Res], fn func(ctx context.Context, arg1 A1, arg2 A2) (Res, error), depKey1 ReadOnlyKey[A1], depKey2 ReadOnlyKey[A2]) Task {
	return &task{
		name:     name,
		depends:  []ID{depKey1.ID(), depKey2.ID()},
		provides: []ID{resKey.ID()},
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			arg1, err := depKey1.Get(b)
			if err != nil {
				return nil, err
			}
			arg2, err := depKey2.Get(b)
			if err != nil {
				return nil, err
			}
			res, err := fn(ctx, arg1, arg2)
			if err != nil {
				return nil, err
			}
			return []Binding{resKey.Bind(res)}, nil
		},
		location: getLocation(),
	}
}

// Condition defines a condition for a Conditional task.
type Condition interface {
	// Evaluate should return whether the conditional task should be executed.
	Evaluate(ctx context.Context, b Binder) (bool, error)
	// Deps should return the IDs of the keys used by the Evaluate function.
	Deps() []ID
}

// ConditionAnd evaluates to true if and only if all of the keys it contains are bound to true.
type ConditionAnd []ReadOnlyKey[bool]

// Evaluate is Condition.Evaluate.
func (ca ConditionAnd) Evaluate(_ context.Context, b Binder) (bool, error) {
	for _, k := range ca {
		v, err := k.Get(b)
		if err != nil {
			return false, err
		}
		if !v {
			return false, nil
		}
	}
	return true, nil
}

// Deps is Condition.Deps
func (ca ConditionAnd) Deps() []ID {
	var deps []ID
	for _, k := range ca {
		deps = append(deps, k.ID())
	}
	return deps
}

// ConditionOr evaluates to true if any of the keys it contains are bound to true.
type ConditionOr []ReadOnlyKey[bool]

// Evaluate is Condition.Evaluate.
func (co ConditionOr) Evaluate(_ context.Context, b Binder) (bool, error) {
	for _, k := range co {
		v, err := k.Get(b)
		if err != nil {
			return false, err
		}
		if v {
			return true, nil
		}
	}
	return false, nil
}

// Deps is Condition.Deps
func (co ConditionOr) Deps() []ID {
	var deps []ID
	for _, k := range co {
		deps = append(deps, k.ID())
	}
	return deps
}

// Conditional wraps tasks such that they are only run if given Condition evaluates to true. If it
// evaluates to false, the bindings in DefaultBindings are used, with any missing keys provided by
// the wrapped tasks bound as absent.
//
// Note that the tasks will not run until all of the wrapped task's dependencies and all of the
// condition's dependencies have been bound.
//
// To run tasks if keys of any type have been bound to some value (i.e. not bound as absent), use
// Presence() to wrap the key. To check for specific values, use Mapped() to wrap the key.
type Conditional struct {
	NamePrefix      string
	Wrapped         TaskSet
	Condition       Condition
	DefaultBindings []Binding
	location        string
}

// Locate annotates the Conditional with its location in the source code, to make error messages
// easier to understand. Calling it is required.
func (c Conditional) Locate() Conditional {
	c.location = getLocation()
	return c
}

// Tasks satisfies TaskSet.Tasks.
func (c Conditional) Tasks() []Task {
	defaultBindingsMap := map[ID]Binding{}
	for _, b := range c.DefaultBindings {
		defaultBindingsMap[b.ID()] = b
	}
	var res []Task
	for _, t := range c.Wrapped.Tasks() {
		// t is captured by the fn closure below
		t := t
		allDeps := set.NewSet[ID](t.Depends()...)
		allDeps.Append(c.Condition.Deps()...)
		res = append(res, &task{
			name:     c.NamePrefix + t.Name(),
			depends:  allDeps.ToSlice(),
			provides: t.Provides(),
			fn: func(ctx context.Context, b Binder) ([]Binding, error) {
				shouldExecute, err := c.Condition.Evaluate(ctx, b)
				if err != nil {
					return nil, err
				}
				if shouldExecute {
					return t.Execute(ctx, b)
				}
				var res []Binding
				for _, id := range t.Provides() {
					if b, ok := defaultBindingsMap[id]; ok {
						res = append(res, b)
					} else {
						res = append(res, bindAbsent(id))
					}
				}
				return res, nil
			},
			location: c.location,
		})
	}
	return res
}

// AllBound returns a task which binds the result key to true without reading its dependencies.
//
// This is intended to be used with conditional tasks to wait for multiple tasks to be completed.
func AllBound(name string, result Key[bool], deps ...ID) Task {
	return &task{
		name:     name,
		depends:  deps,
		provides: []ID{result.ID()},
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			return []Binding{result.Bind(true)}, nil
		},
		location: getLocation(),
	}
}
