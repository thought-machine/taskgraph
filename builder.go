package taskgraph

import (
	"errors"
	"fmt"
)

// TaskBuilder helps construct taskgraph Tasks with a fluent API.
type TaskBuilder[T any] struct {
	name            string
	resultKey       Key[T]
	depends         []any
	fn              any
	condition       Condition
	defaultVal      T
	defaultSet      bool
	defaultBindings []Binding
}

// NewTaskBuilder creates a new builder for a task that produces a result of type T.
func NewTaskBuilder[T any](name string, key Key[T]) *TaskBuilder[T] {
	return &TaskBuilder[T]{
		name:      name,
		resultKey: key,
	}
}

// DependsOn adds dependencies to the task.
func (b *TaskBuilder[T]) DependsOn(deps ...any) *TaskBuilder[T] {
	b.depends = append(b.depends, deps...)
	return b
}

// Run sets the function to execute. The function signature must match the dependencies.
func (b *TaskBuilder[T]) Run(fn any) *TaskBuilder[T] {
	b.fn = fn
	return b
}

// RunIf sets a condition for the task execution.
func (b *TaskBuilder[T]) RunIf(cond Condition) *TaskBuilder[T] {
	b.condition = cond
	return b
}

// RunIfAll sets a ConditionAnd (logical AND) for the task execution using the provided keys.
func (b *TaskBuilder[T]) RunIfAll(keys ...ReadOnlyKey[bool]) *TaskBuilder[T] {
	b.condition = ConditionAnd(keys)
	return b
}

// RunIfAny sets a ConditionOr (logical OR) for the task execution using the provided keys.
func (b *TaskBuilder[T]) RunIfAny(keys ...ReadOnlyKey[bool]) *TaskBuilder[T] {
	b.condition = ConditionOr(keys)
	return b
}

// Default sets the default value for the result key if the condition is false.
func (b *TaskBuilder[T]) Default(val T) *TaskBuilder[T] {
	b.defaultVal = val
	b.defaultSet = true
	return b
}

// WithDefaultBindings adds arbitrary default bindings if the condition is false.
func (b *TaskBuilder[T]) WithDefaultBindings(bindings ...Binding) *TaskBuilder[T] {
	b.defaultBindings = append(b.defaultBindings, bindings...)
	return b
}

// Build constructs and returns the Task.
func (b *TaskBuilder[T]) Build() (TaskSet, error) {
	reflect := Reflect[T]{
		Name:      b.name,
		ResultKey: b.resultKey,
		Depends:   b.depends,
		Fn:        b.fn,
	}
	reflect.location = getLocation(2)

	// Eagerly build to validate and get the underlying task
	task, err := reflect.Build()
	if err != nil {
		return nil, err
	}
	var ts TaskSet = task

	if b.condition != nil {
		conditional := Conditional{
			Wrapped:   ts,
			Condition: b.condition,
		}

		if b.defaultSet {
			conditional.DefaultBindings = append(
				conditional.DefaultBindings,
				b.resultKey.Bind(b.defaultVal),
			)
		}
		conditional.DefaultBindings = append(conditional.DefaultBindings, b.defaultBindings...)

		conditional.location = getLocation(2)
		ts = conditional
	}

	return ts, nil
}

// Tasks satisfies the TaskSet interface to avoid the need to call Build(). It is equivalent to
// calling Must(Build()).
func (b *TaskBuilder[T]) Tasks() []Task {
	return Must(b.Build()).Tasks()
}

// MultiTaskBuilder helps construct taskgraph Tasks that provide multiple outputs or perform side effects.
type MultiTaskBuilder struct {
	name            string
	depends         []any
	fn              any
	provides        []ID
	condition       Condition
	defaultBindings []Binding
	errors          []error
}

// NewMultiTaskBuilder creates a new builder for a multi-output or side-effect task.
func NewMultiTaskBuilder(name string) *MultiTaskBuilder {
	return &MultiTaskBuilder{
		name: name,
	}
}

// DependsOn adds dependencies to the task.
func (b *MultiTaskBuilder) DependsOn(deps ...any) *MultiTaskBuilder {
	b.depends = append(b.depends, deps...)
	return b
}

// Provides declares the keys that this task provides.
func (b *MultiTaskBuilder) Provides(keys ...any) *MultiTaskBuilder {
	for _, k := range keys {
		rk, err := newReflectKey(k)
		if err != nil {
			b.errors = append(b.errors, fmt.Errorf("invalid key passed to Provides: %w", err))
			continue
		}
		id, err := rk.ID()
		if err != nil {
			b.errors = append(b.errors, fmt.Errorf("invalid key ID in Provides: %w", err))
			continue
		}
		b.provides = append(b.provides, id)
	}
	return b
}

// Run sets the function to execute. The function signature must match the dependencies.
// Fn must return []Binding or ([]Binding, error).
func (b *MultiTaskBuilder) Run(fn any) *MultiTaskBuilder {
	b.fn = fn
	return b
}

// RunIf sets a condition for the task execution.
func (b *MultiTaskBuilder) RunIf(cond Condition) *MultiTaskBuilder {
	b.condition = cond
	return b
}

// RunIfAll sets a ConditionAnd (logical AND) for the task execution using the provided keys.
func (b *MultiTaskBuilder) RunIfAll(keys ...ReadOnlyKey[bool]) *MultiTaskBuilder {
	b.condition = ConditionAnd(keys)
	return b
}

// RunIfAny sets a ConditionOr (logical OR) for the task execution using the provided keys.
func (b *MultiTaskBuilder) RunIfAny(keys ...ReadOnlyKey[bool]) *MultiTaskBuilder {
	b.condition = ConditionOr(keys)
	return b
}

// WithDefaultBindings adds arbitrary default bindings if the condition is false.
func (b *MultiTaskBuilder) WithDefaultBindings(bindings ...Binding) *MultiTaskBuilder {
	b.defaultBindings = append(b.defaultBindings, bindings...)
	return b
}

// Build constructs and returns the Task.
func (b *MultiTaskBuilder) Build() (TaskSet, error) {
	if len(b.errors) > 0 {
		return nil, errors.Join(b.errors...)
	}

	reflect := ReflectMulti{
		Name:     b.name,
		Depends:  b.depends,
		Fn:       b.fn,
		Provides: b.provides,
	}
	reflect.location = getLocation(2)
	var task TaskSet = reflect

	if b.condition != nil {
		conditional := Conditional{
			Wrapped:         task,
			Condition:       b.condition,
			DefaultBindings: b.defaultBindings,
		}
		conditional.location = getLocation(2)
		task = conditional
	}

	return task, nil
}

// Tasks satisfies the TaskSet interface to avoid the need to call Build(). It is equivalent to
// calling Must(Build()).
func (b *MultiTaskBuilder) Tasks() []Task {
	return Must(b.Build()).Tasks()
}
