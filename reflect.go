package taskgraph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
)

// The roundabout mechanism of creating a nil pointer and dereference its type is the standard way
// to get reflect.Types for interfaces.
var (
	contextType     = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType       = reflect.TypeOf((*error)(nil)).Elem()
	binderType      = reflect.TypeOf((*Binder)(nil)).Elem()
	bindingListType = reflect.TypeOf([]Binding{})
	idType          = reflect.TypeOf(ID{})
)

// reflectKey is a reflection-based wrapper around a Key[T] for some unknown T, generated from a
// variable typed as any. This is a workaround for Golang reflection not having support for type
// variables yet.
type reflectKey struct {
	idFn, getFn reflect.Value
	valueType   reflect.Type
}

// ID is equivalent to Key[T].ID().
func (rk *reflectKey) ID() (ID, error) {
	outs := rk.idFn.Call(nil)
	id, ok := outs[0].Interface().(ID)
	if !ok {
		return ID{}, errors.New("ID method did not return ID")
	}
	return id, nil
}

// Get is equivalent to Key[T].Get().
func (rk *reflectKey) Get(b Binder) (reflect.Value, error) {
	outs := rk.getFn.Call([]reflect.Value{reflect.ValueOf(b)})
	if !outs[1].IsNil() {
		err, ok := outs[1].Interface().(error)
		if !ok {
			return reflect.Value{}, wrapStackErrorf("could not convert output 1 to error; got %T", outs[1].Interface())
		}
		return reflect.Value{}, err
	}
	return outs[0], nil
}

// newReflectKey checks the types of the given key, asserting that it conforms to (most of) the
// Key[T] interface.
func newReflectKey(key any) (rk *reflectKey, err error) {
	defer func() {
		// This exists to cover for any bugs in the reflection logic itself.
		if r := recover(); r != nil {
			err = wrapStackErrorf("recovered from panic: %v\n%s", r, debug.Stack())
		}
	}()

	idFn := reflect.ValueOf(key).MethodByName("ID")
	if !idFn.IsValid() {
		return nil, errors.New("key has no ID method")
	}
	idFnType := idFn.Type()
	if idFnType.NumIn() != 0 {
		return nil, errors.New("ID method does not take zero arguments")
	}
	if idFnType.NumOut() != 1 || !idFnType.Out(0).AssignableTo(idType) {
		return nil, errors.New("ID method does not return ID")
	}

	getFn := reflect.ValueOf(key).MethodByName("Get")
	if !getFn.IsValid() {
		return nil, errors.New("key has no Get method")
	}
	getFnType := getFn.Type()
	if getFnType.NumIn() != 1 || !binderType.AssignableTo(getFnType.In(0)) {
		return nil, errors.New("Get method does not take a Binder as its only argument")
	}
	if getFnType.NumOut() != 2 || !getFnType.Out(1).Implements(errorType) {
		return nil, errors.New("Get method does not return (T, error)")
	}
	valueType := getFnType.Out(0)

	return &reflectKey{
		idFn:      idFn,
		getFn:     getFn,
		valueType: valueType,
	}, nil
}

// reflectFn is a reflection-based wrapper around a function whose arguments are retrieved from a
// set of keys. The function is expected to return (T, error) for some unknown T.
type reflectFn struct {
	fn            reflect.Value
	hasContext    bool
	keys          []*reflectKey
	depIDs        []ID
	getResultFunc func(outs []reflect.Value) (any, error)
}

// Call the function, retrieving its arguments from the binder.
func (rf *reflectFn) Call(ctx context.Context, b Binder) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This exists to cover for any bugs in the reflection logic itself; if this is seen at
			// runtime and the error message refers to reflection, it's probably a bug in this library
			// rather than (or as well as) a bug in the task implementation.
			err = wrapStackErrorf("recovered from panic: %v\n%s", r, debug.Stack())
		}
	}()

	var args []reflect.Value
	if rf.hasContext {
		args = append(args, reflect.ValueOf(ctx))
	}
	for _, k := range rf.keys {
		arg, err := k.Get(b)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	return rf.getResultFunc(rf.fn.Call(args))
}

// newReflectFn checks the types of fn and deps, asserting that the function has the right signature
// and that its arguments are consistent with deps.
//
// deps should be a list of Keys; for a list []any{Key[A], Key[B], Key[C], ...}, the function
// signature should then be either func (context.Context, A, B, C, ...) (Res, error) or
// func (A, B, C, ...) (Res, error) for any result type Res.
func newReflectFn(fn any, resultType reflect.Type, deps ...any) (rf *reflectFn, err error) {
	defer func() {
		// This exists to cover for any bugs in the reflection logic itself.
		if r := recover(); r != nil {
			err = wrapStackErrorf("recovered from panic: %v\n%s", r, debug.Stack())
		}
	}()

	fnValue := reflect.ValueOf(fn)
	if fnValue.Kind() != reflect.Func {
		return nil, wrapStackErrorf("%+v is not a function", fn)
	}
	fnType := fnValue.Type()

	var getResultFunc func(outs []reflect.Value) (any, error)
	if fnType.NumOut() == 1 {
		if !fnType.Out(0).AssignableTo(resultType) {
			return nil, wrapStackErrorf("function does not return %s", resultType)
		}
		getResultFunc = func(outs []reflect.Value) (any, error) {
			return outs[0].Interface(), nil
		}
	} else if fnType.NumOut() == 2 {
		if !fnType.Out(0).AssignableTo(resultType) || !fnType.Out(1).Implements(errorType) {
			return nil, wrapStackErrorf("function does not return (%s, error)", resultType)
		}
		getResultFunc = func(outs []reflect.Value) (any, error) {
			if !outs[1].IsNil() {
				err, ok := outs[1].Interface().(error)
				if !ok {
					return nil, wrapStackErrorf("could not convert function output 1 to error; got %T", outs[1].Interface())
				}
				return nil, err
			}
			return outs[0].Interface(), nil
		}
	} else {
		return nil, wrapStackErrorf("function should return %s or (%s, error)", resultType, resultType)
	}

	hasContext := fnType.NumIn() > 0 && fnType.In(0).Implements(contextType)
	argCount := fnType.NumIn()
	offset := 0
	if hasContext {
		argCount--
		offset++
	}
	if argCount != len(deps) {
		return nil, wrapStackErrorf("function takes %d arguments (excluding any initial context), but %d deps were provided", argCount, len(deps))
	}

	var keys []*reflectKey
	var depIDs []ID
	for i, dep := range deps {
		rk, err := newReflectKey(dep)
		if err != nil {
			return nil, wrapStackErrorf("dependency %d: %w", i, err)
		}
		if !rk.valueType.AssignableTo(fnType.In(i + offset)) {
			return nil, wrapStackErrorf("dependency %d is Key[%v]; want Key[%v]", i, rk.valueType, fnType.In(i+offset))
		}
		keys = append(keys, rk)
		id, err := rk.ID()
		if err != nil {
			return nil, wrapStackErrorf("dependency %d: %w", i, err)
		}
		depIDs = append(depIDs, id)
	}

	return &reflectFn{
		fn:            fnValue,
		hasContext:    hasContext,
		keys:          keys,
		depIDs:        depIDs,
		getResultFunc: getResultFunc,
	}, nil
}

// A Reflect uses reflection to build a Task providing a single value, avoiding the need for
// the task function to call myKey.Get(binder) and check the error for each dependency. This is a
// struct to provide poor-man's named arguments.
type Reflect[T any] struct {
	// Name of the built task
	Name string

	// The key which Fn produces a value for
	ResultKey Key[T]

	// The task function. This function should:
	//  * Optionally take a context.Context as the first argument
	//  * Take one argument for each entry in Depends whose type matches the type parameter of the Key
	//  * Return T or (T, error)
	Fn any

	// A list of Key[X], where X may be different for each key (which is why this is []any). These
	// keys are used to provide the arguments to Fn. Each key is expected to be bound as present when
	// the Task is run; it is up to the user to either wrap the built task in a Conditional() or to
	// use Optional() keys where necessary.
	Depends []any

	location string
}

// Locate annotates the Reflect with its location in the source code, to make error messages
// easier to understand. Calling it is recommended but not required if wrapped in a Conditional
func (r Reflect[T]) Locate() Reflect[T] {
	r.location = getLocation()
	return r
}

func (r Reflect[T]) errorPrefix() string {
	return fmt.Sprintf("task %q (%s)", r.Name, r.location)
}

// Build the task from the parameters in the Reflect struct. This is exposed for testing; prefer
// using Reflect[T] as a TaskSet rather than calling Build() directly.
func (r Reflect[T]) Build() (Task, error) {
	// Abuse reflectKey to get the reflect.Type for the Key
	resRk, err := newReflectKey(r.ResultKey)
	if err != nil {
		return nil, wrapStackErrorf("%s result key: %w", r.errorPrefix(), err)
	}

	rf, err := newReflectFn(r.Fn, resRk.valueType, r.Depends...)
	if err != nil {
		return nil, wrapStackErrorf("%s: %w", r.errorPrefix(), err)
	}

	return &task{
		name:     r.Name,
		depends:  rf.depIDs,
		provides: []ID{r.ResultKey.ID()},
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			res, err := rf.Call(ctx, b)
			if err != nil {
				return nil, err
			}
			typed, ok := res.(T)
			if !ok {
				return nil, wrapStackErrorf("%s: could not convert function result to T; got %T", r.errorPrefix(), res)
			}
			return []Binding{r.ResultKey.Bind(typed)}, nil
		},
		location: r.location,
	}, nil
}

// Tasks satisfies the TaskSet interface to avoid the need to call Build(). It is equivalent to
// calling Must(Build()).
func (r Reflect[T]) Tasks() []Task {
	return []Task{Must(r.Build())}
}

// A ReflectMulti uses reflection to build a Task providing multiple values, avoiding the need for
// the task function to call myKey.Get(binder) and check the error for each dependency. This is a
// struct to provide poor-man's named arguments.
type ReflectMulti struct {
	// Name of the built task
	Name string

	// The task function. This function should:
	//  * Optionally take a context.Context as the first argument
	//  * Take one argument for each entry in Depends whose type matches the type parameter of the Key
	//  * Return []Binding or ([]Binding, error)
	Fn any

	// The list of key IDs which the task provides.
	Provides []ID

	// A list of Key[X], where X may be different for each key (which is why this is []any). These
	// keys are used to provide the arguments to Fn. Each key is expected to be bound as present when
	// the Task is run; it is up to the user to either wrap the built task in a Conditional() or to
	// use Optional() keys where necessary.
	Depends []any

	location string
}

// Locate annotates the ReflectMulti with its location in the source code, to make error messages
// easier to understand. Calling it is recommended but not required if wrapped in a Conditional
func (r ReflectMulti) Locate() ReflectMulti {
	r.location = getLocation()
	return r
}

func (r ReflectMulti) errorPrefix() string {
	return fmt.Sprintf("task %q (%s)", r.Name, r.location)
}

// Build the task from the parameters in the ReflectMulti struct. This is exposed for testing;
// prefer using Reflect[T] as a TaskSet rather than calling Build() directly.
func (r ReflectMulti) Build() (Task, error) {
	rf, err := newReflectFn(r.Fn, bindingListType, r.Depends...)
	if err != nil {
		return nil, wrapStackErrorf("%s: %w", r.errorPrefix(), err)
	}

	return &task{
		name:     r.Name,
		depends:  rf.depIDs,
		provides: r.Provides,
		fn: func(ctx context.Context, b Binder) ([]Binding, error) {
			res, err := rf.Call(ctx, b)
			if err != nil {
				return nil, err
			}
			typed, ok := res.([]Binding)
			if !ok {
				return nil, wrapStackErrorf("%s: could not convert function result to []Binding; got %T", r.errorPrefix(), res)
			}
			return typed, nil
		},
		location: r.location,
	}, nil
}

// Tasks satisfies the TaskSet interface to avoid the need to call Build(). It is equivalent to
// calling Must(Build()).
func (r ReflectMulti) Tasks() []Task {
	return []Task{Must(r.Build())}
}
