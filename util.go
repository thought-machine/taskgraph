package taskgraph

import (
	"errors"
	"fmt"
	"runtime"
	"sort"

	stackerrors "github.com/go-errors/errors"
	"github.com/sirupsen/logrus"
)

var (
	// ErrMultipleMaybesPresent is returned from SelectSingleMaybe if multiple inputs are present.
	ErrMultipleMaybesPresent = errors.New("multiple present maybes passed to SelectSingleMaybe")
	// ErrNoMaybesPresent is returned from SelectSingleMaybe if no inputs are present.
	ErrNoMaybesPresent = errors.New("no present maybes passed to SelectSingleMaybe")
)

var log = logrus.New()

// Must wraps around a function returning a value and error, calls log.Fatal if the error is
// non-nil, and otherwise returns the value. This is intended to be used for constructing top-level
// Graph variables:
//
//	var (
//	  myGraph = taskgraph.Must(taskgraph.New(myTask))
//	)
func Must[T any](val T, err error) T {
	if err != nil {
		// Override the file in the log entry to where Must was called
		if _, file, line, ok := runtime.Caller(1); ok {
			log.WithField("file", fmt.Sprintf("%s:%d", file, line)).Fatal(err)
		}
		log.Fatal(err)
	}
	return val
}

// Maybe encapsulates a value and error. This is primarily intended to be used by Optional(key),
// which in turn is primarily intended to be used to make it easier to build tasks with reflection
// where their dependencies may be absent.
type Maybe[T any] struct {
	present bool
	val     T
	err     error
}

// Get the encapsulated value and error.
func (m Maybe[T]) Get() (T, error) {
	return m.val, m.err
}

// Error returns the encapsulated error.
func (m Maybe[T]) Error() error {
	return m.err
}

// Present returns the encapsulated present value.
func (m Maybe[T]) Present() bool {
	return m.present
}

// MaybeOf constructs a Maybe containing the given value and no error.
func MaybeOf[T any](val T) Maybe[T] {
	return Maybe[T]{present: true, val: val}
}

// MaybeErr constructs a Maybe containing the given error and no value. If the error is nil,
// ErrIsAbsent is used.
func MaybeErr[T any](err error) Maybe[T] {
	if err == nil {
		err = ErrIsAbsent
	}
	return Maybe[T]{present: false, err: err}
}

// WrapMaybe takes the output of a function returning (T, error) and encapsulates it into a Maybe[T].
// The value is presumed to be valid if err is nil.
func WrapMaybe[T any](val T, err error) Maybe[T] {
	return Maybe[T]{err == nil, val, err}
}

// SelectSingleMaybe finds the single Maybe in the given list which is present. It returns an error
// if no present Maybe is found, or if multiple present Maybes are found.
func SelectSingleMaybe[T any](maybes ...Maybe[T]) (T, error) {
	var res, zero T
	found := false
	for _, m := range maybes {
		if m.present {
			if found {
				return zero, ErrMultipleMaybesPresent
			}
			res = m.val
			found = true
		}
	}
	if !found {
		return zero, ErrNoMaybesPresent
	}
	return res, nil
}

// MaybeStatus interface includes only the uniformly-typed functions of a Maybe, which is useful for status aggregation
type MaybeStatus interface {
	Error() error
	Present() bool
}

// ErrorsMaybe detects if there are any errors in the maybes. It returns a
// single error wrapping any errors present.
func ErrorsMaybe(maybes ...MaybeStatus) error {
	var err error
	for _, m := range maybes {
		if m.Error() != nil && !errors.Is(m.Error(), ErrIsAbsent) {
			if err == nil {
				err = m.Error()
			}
			err = fmt.Errorf("%s %w", err, m.Error())
		}
	}
	return err
}

// MissingMaybe returns true if any maybes are not present.
func MissingMaybe(maybes map[string]MaybeStatus) []string {
	out := []string{}
	for name, m := range maybes {
		if !m.Present() {
			out = append(out, name)
		}
	}
	// sort so we don't see changing order from the map iteration
	sort.Strings(out)
	return out
}

func getLocation() string {
	// Skip 1 for this function, and 1 for the constructor calling this.
	if _, file, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf("%s:%d", file, line)
	}
	return "<unknown location>"
}

func wrapStackErrorf(msg string, args ...any) error {
	se := new(stackerrors.Error)
	foundStackError := false
	for _, arg := range args {
		if err, ok := arg.(error); ok {
			if errors.As(err, &se) {
				foundStackError = true
				break
			}
		}
	}

	err := fmt.Errorf(msg, args...)
	if foundStackError {
		return err
	}
	return stackerrors.Wrap(err, 1)
}
