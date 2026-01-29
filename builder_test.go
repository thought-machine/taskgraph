package taskgraph

import (
	"testing"
)

func TestTaskBuilder_RunIfAll(t *testing.T) {
	k1 := NewKey[bool]("k1")
	k2 := NewKey[bool]("k2")
	res := NewKey[string]("res")

	task := NewTaskBuilder[string]("test", res).
		Run(func() string { return "ok" }).
		RunIfAll(k1, k2).
		Default("default").
		Build()

	// Simulate execution (simplified verification)
	tasks := task.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	// We can't easily execute it without a full graph, but we can check if it didn't panic and produced a task.
}

func TestMultiTaskBuilder_Provides(t *testing.T) {
	k1 := NewKey[string]("k1")
	k2 := NewKey[int]("k2")

	task := NewMultiTaskBuilder("multi").
		Provides(k1, k2).
		Run(func() ([]Binding, error) {
			return []Binding{k1.Bind("s"), k2.Bind(1)}, nil
		}).
		Build()

	tasks := task.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	provided := tasks[0].Provides()
	if len(provided) != 2 {
		t.Fatalf("expected 2 provided keys, got %d", len(provided))
	}
}

func TestMultiTaskBuilder_Provides_InvalidKey(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invalid key")
		}
	}()
	NewMultiTaskBuilder("fail").Provides("not a key")
}

func TestMultiTaskBuilder_RunIfAny(t *testing.T) {
	k1 := NewKey[bool]("k1")
	k2 := NewKey[bool]("k2")

	task := NewMultiTaskBuilder("multi_cond").
		RunIfAny(k1, k2).
		Run(func() []Binding { return nil }).
		Build()

	if task == nil {
		t.Fatal("expected task to be built")
	}
}
