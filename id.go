package taskgraph

import "fmt"

// ID represents a type-parameter-less identifier for a Key.
type ID struct {
	namespace string
	id        string
}

func newID(namespace, id string) ID {
	return ID{namespace: namespace, id: id}
}

func (i ID) String() string {
	if i.namespace == "" {
		return i.id
	}
	// Double underscore is unconventional, but it is safe for graphviz IDs.
	return fmt.Sprintf("%s__%s", i.namespace, i.id)
}
