package taskgraph_test

import (
	"context"
	"errors"
	"testing"

	tg "github.com/thought-machine/taskgraph"
	tgt "github.com/thought-machine/taskgraph/taskgraphtest"
)

// Tests all of the mechanisms for creating tasks
func TestTasks(t *testing.T) {
	key1 := tg.NewKey[string]("key1")
	key2 := tg.NewKey[string]("key2")
	key3 := tg.NewKey[string]("key3")
	boolKey := tg.NewKey[bool]("boolkey")
	sentinelError := errors.New("sentinel error")

	tgt.Suite{
		Tests: []tgt.Test{
			{
				Description: "NewTask",
				Task: tg.NewTask("task", func(_ context.Context, b tg.Binder) ([]tg.Binding, error) {
					val, err := key1.Get(b)
					if err != nil {
						return nil, err
					}
					return []tg.Binding{
						key2.Bind(val + val),
					}, nil
				}, []tg.ID{key1.ID()}, []tg.ID{key2.ID()}),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("barbar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "NewTask with error",
				Task: tg.NewTask("task", func(_ context.Context, _ tg.Binder) ([]tg.Binding, error) {
					return nil, sentinelError
				}, []tg.ID{key1.ID()}, []tg.ID{key2.ID()}),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "NoOutputTask",
				Task: tg.NoOutputTask("task", func(_ context.Context, b tg.Binder) error {
					_, err := key1.Get(b)
					return err
				}, key1.ID()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "NoOutputTask with error",
				Task: tg.NoOutputTask("task", func(_ context.Context, _ tg.Binder) error {
					return sentinelError
				}, key1.ID()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "SimpleTask",
				Task: tg.SimpleTask("task", key2, func(_ context.Context, b tg.Binder) (string, error) {
					return key1.Get(b)
				}, key1.ID()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("bar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "SimpleTask with error",
				Task: tg.SimpleTask("task", key2, func(_ context.Context, b tg.Binder) (string, error) {
					return "", sentinelError
				}, key1.ID()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "SimpleTask1",
				Task: tg.SimpleTask1[string, string]("task", key2, func(_ context.Context, arg1 string) (string, error) {
					return arg1 + arg1, nil
				}, key1),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("barbar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "SimpleTask1 with error",
				Task: tg.SimpleTask1[string, string]("task", key2, func(_ context.Context, arg1 string) (string, error) {
					return "", sentinelError
				}, key1),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "SimpleTask2",
				Task: tg.SimpleTask2[string, string, string]("task", key3, func(_ context.Context, arg1, arg2 string) (string, error) {
					return arg1 + arg2, nil
				}, key1, key2),
				Inputs: []tg.Binding{
					key1.Bind("foo"),
					key2.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key3.Bind("foobar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "SimpleTask2 with error",
				Task: tg.SimpleTask2[string, string, string]("task", key3, func(_ context.Context, arg1, arg2 string) (string, error) {
					return "", sentinelError
				}, key1, key2),
				Inputs: []tg.Binding{
					key1.Bind("foo"),
					key2.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "Conditional, condition met",
				Task: tg.Conditional{
					NamePrefix: "cond_",
					Wrapped: tg.SimpleTask1[string, string]("task", key2, func(_ context.Context, arg1 string) (string, error) {
						return arg1 + arg1, nil
					}, key1),
					Condition: tg.ConditionAnd{boolKey},
				}.Locate(),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
					boolKey.Bind(true),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("barbar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "Conditional, condition not met",
				Task: tg.Conditional{
					NamePrefix: "cond_",
					Wrapped: tg.SimpleTask1[string, string]("task", key2, func(_ context.Context, arg1 string) (string, error) {
						return arg1 + arg1, nil
					}, key1),
					Condition: tg.ConditionOr{boolKey},
				}.Locate(),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
					boolKey.Bind(false),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.BindAbsent()),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "Reflect",
				Task: tg.Reflect[string]{
					Name:      "task",
					ResultKey: key3,
					Fn: func(arg1, arg2 string) string {
						return arg1 + arg2
					},
					Depends: []any{key1, key2},
				}.Locate(),
				Inputs: []tg.Binding{
					key1.Bind("foo"),
					key2.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key3.Bind("foobar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "Reflect with context",
				Task: tgt.Must[tg.Task](t)(tg.Reflect[string]{
					Name:      "task",
					ResultKey: key3,
					Fn: func(_ context.Context, arg1, arg2 string) string {
						return arg1 + arg2
					},
					Depends: []any{key1, key2},
				}.Locate().Build()),
				Inputs: []tg.Binding{
					key1.Bind("foo"),
					key2.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key3.Bind("foobar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "Reflect with error",
				Task: tgt.Must[tg.Task](t)(tg.Reflect[string]{
					Name:      "task",
					ResultKey: key3,
					Fn: func(arg1, arg2 string) (string, error) {
						return "", sentinelError
					},
					Depends: []any{key1, key2},
				}.Locate().Build()),
				Inputs: []tg.Binding{
					key1.Bind("foo"),
					key2.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "ReflectMulti",
				Task: tgt.Must[tg.Task](t)(tg.ReflectMulti{
					Name: "task",
					Fn: func(arg1 string) []tg.Binding {
						return []tg.Binding{
							key2.Bind(arg1),
							key3.Bind(arg1 + arg1),
						}
					},
					Provides: []tg.ID{key2.ID(), key3.ID()},
					Depends:  []any{key1},
				}.Locate().Build()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("bar")),
					tgt.Match(key3.Bind("barbar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "ReflectMulti with context",
				Task: tgt.Must[tg.Task](t)(tg.ReflectMulti{
					Name: "task",
					Fn: func(_ context.Context, arg1 string) []tg.Binding {
						return []tg.Binding{
							key2.Bind(arg1),
							key3.Bind(arg1 + arg1),
						}
					},
					Provides: []tg.ID{key2.ID(), key3.ID()},
					Depends:  []any{key1},
				}.Locate().Build()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(key2.Bind("bar")),
					tgt.Match(key3.Bind("barbar")),
				},
				CheckExcessBindings: true,
			},
			{
				Description: "ReflectMulti with error",
				Task: tgt.Must[tg.Task](t)(tg.ReflectMulti{
					Name: "task",
					Fn: func(arg1 string) ([]tg.Binding, error) {
						return nil, sentinelError
					},
					Provides: []tg.ID{key2.ID(), key3.ID()},
					Depends:  []any{key1},
				}.Locate().Build()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
				},
				WantError: sentinelError,
			},
			{
				Description: "AllBound",
				Task:        tg.AllBound("task", boolKey, key1.ID(), key2.ID(), key3.ID()),
				Inputs: []tg.Binding{
					key1.Bind("bar"),
					key2.Bind("bar"),
					key3.Bind("bar"),
				},
				WantBindings: []tgt.BindingMatcher{
					tgt.Match(boolKey.Bind(true)),
				},
			},
		},
	}.Run(t)
}
