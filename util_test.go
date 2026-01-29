package taskgraph_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	tg "github.com/thought-machine/taskgraph"
)

func TestErrorsMaybeErrors(t *testing.T) {
	maybes := []tg.MaybeStatus{}
	b := tg.NewBinder()
	for i := 0; i < 3; i++ {
		key := tg.NewKey[string](fmt.Sprintf("key%d", i))
		assert.NoError(t, b.Store(key.BindError(fmt.Errorf("an error %d", i))))
		o, err := tg.Optional(key).Get(b)
		assert.NoError(t, err)
		maybes = append(maybes, o)
	}
	err := tg.ErrorsMaybe(maybes...)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.Unwrap(err)))
}

func TestErrorsMaybeAbsent(t *testing.T) {
	maybes := []tg.MaybeStatus{}
	b := tg.NewBinder()
	for i := 0; i < 3; i++ {
		key := tg.NewKey[string](fmt.Sprintf("key%d", i))
		assert.NoError(t, b.Store(key.BindAbsent()))
		o, err := tg.Optional(key).Get(b)
		assert.NoError(t, err)
		maybes = append(maybes, o)
	}
	err := tg.ErrorsMaybe(maybes...)
	assert.NoError(t, err)
}

func TestMissingMaybe(t *testing.T) {
	maybes := map[string]tg.MaybeStatus{}
	b := tg.NewBinder()
	nElements := 3
	for i := nElements; i > 0; i-- {
		key := tg.NewKey[string](fmt.Sprintf("key%d", i))
		assert.NoError(t, b.Store(key.BindAbsent()))
		o, err := tg.Optional(key).Get(b)
		assert.NoError(t, err)
		maybes[key.ID().String()] = o
	}
	missing := tg.MissingMaybe(maybes)
	assert.Equal(t, nElements, len(missing))
	assert.ElementsMatch(t, []string{"key1", "key2", "key3"}, missing)
}
