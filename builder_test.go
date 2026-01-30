package taskgraph

import (
	"testing"
)

func TestTaskBuilder_RunIfAll(t *testing.T) {
	k1 := NewKey[bool]("k1")
	k2 := NewKey[bool]("k2")
	res := NewKey[string]("res")

	task, err := NewTaskBuilder[string]("test", res).
		Run(func() string { return "ok" }).
		RunIfAll(k1, k2).
		Default("default").
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate execution (simplified verification)
	tasks := task.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
}

func TestMultiTaskBuilder_Provides(t *testing.T) {
	k1 := NewKey[string]("k1")
	k2 := NewKey[int]("k2")

	task, err := NewMultiTaskBuilder("multi").
		Provides(k1, k2).
		Run(func() ([]Binding, error) {
			return []Binding{k1.Bind("s"), k2.Bind(1)}, nil
		}).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	_, err := NewMultiTaskBuilder("fail").Provides("not a key").Build()
	if err == nil {
		t.Errorf("expected error on invalid key")
	}
}

func TestMultiTaskBuilder_RunIfAny(t *testing.T) {
	k1 := NewKey[bool]("k1")
	k2 := NewKey[bool]("k2")

	task, err := NewMultiTaskBuilder("multi_cond").
		RunIfAny(k1, k2).
		Run(func() []Binding { return nil }).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task == nil {
		t.Fatal("expected task to be built")
	}
}
