package taskgraph

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
func (b *TaskBuilder[T]) Build() TaskSet {
	reflect := Reflect[T]{
		Name:      b.name,
		ResultKey: b.resultKey,
		Depends:   b.depends,
		Fn:        b.fn,
	}
	reflect.location = getLocation(2)
	var task TaskSet = reflect

	if b.condition != nil {
		conditional := Conditional{
			Wrapped:   task,
			Condition: b.condition,
		}

		if b.defaultSet {
			conditional.DefaultBindings = append(conditional.DefaultBindings, b.resultKey.Bind(b.defaultVal))
		}
		conditional.DefaultBindings = append(conditional.DefaultBindings, b.defaultBindings...)

		conditional.location = getLocation(2)
		task = conditional
	}

	return task
}

// EffectTaskBuilder helps construct taskgraph Tasks that perform side effects (no result key).
type EffectTaskBuilder struct {
	name            string
	depends         []any
	fn              any
	condition       Condition
	defaultBindings []Binding
}

// NewEffectTaskBuilder creates a new builder for a side-effect task.
func NewEffectTaskBuilder(name string) *EffectTaskBuilder {
	return &EffectTaskBuilder{
		name: name,
	}
}

// DependsOn adds dependencies to the task.
func (b *EffectTaskBuilder) DependsOn(deps ...any) *EffectTaskBuilder {
	b.depends = append(b.depends, deps...)
	return b
}

// Run sets the function to execute. The function signature must match the dependencies.
// Fn must return []Binding or ([]Binding, error).
func (b *EffectTaskBuilder) Run(fn any) *EffectTaskBuilder {
	b.fn = fn
	return b
}

// RunIf sets a condition for the task execution.
func (b *EffectTaskBuilder) RunIf(cond Condition) *EffectTaskBuilder {
	b.condition = cond
	return b
}

// WithDefaultBindings adds arbitrary default bindings if the condition is false.
func (b *EffectTaskBuilder) WithDefaultBindings(bindings ...Binding) *EffectTaskBuilder {
	b.defaultBindings = append(b.defaultBindings, bindings...)
	return b
}

// Build constructs and returns the Task.
func (b *EffectTaskBuilder) Build() TaskSet {
	reflect := ReflectMulti{
		Name:     b.name,
		Depends:  b.depends,
		Fn:       b.fn,
		Provides: nil,
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

	return task
}
