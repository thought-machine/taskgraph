package taskgraph_test

import (
	"context"
	"fmt"
	"log"

	tg "github.com/thought-machine/taskgraph"
)

var (
	keyInput    = tg.NewKey[string]("input")
	keyReversed = tg.NewNamespacedKey[string]("input", "reversed")
	keyResult   = tg.NewKey[bool]("result")
)

var taskReverseInput = tg.Reflect[string]{
	Name:      "reverseInput",
	ResultKey: keyReversed,
	Fn: func(input string) (string, error) {
		var res string
		for _, v := range input {
			res = string(v) + res
		}
		return res, nil
	},
	Depends: []any{keyInput},
}.Locate()

var taskIsPalindrome = tg.Reflect[bool]{
	Name:      "isPalindrome",
	ResultKey: keyResult,
	Fn: func(input, reversed string) (bool, error) {
		return input == reversed, nil
	},
	Depends: []any{keyInput, keyReversed},
}.Locate()

var graphIsPalindrome = tg.Must(tg.New("example_graph", tg.WithTasks(taskReverseInput, taskIsPalindrome)))

func Example() {
	res, err := graphIsPalindrome.Run(context.Background(), keyInput.Bind("racecar"))
	if err != nil {
		log.Fatal(err)
	}
	val, err := keyResult.Get(res)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%t\n", val)

	// Output:
	// true
}
