package deserialization

// !!! TODO: link this with main package common.go

import (
	"fmt"
	"os"
	"runtime/debug"
)

// Error macro
// TODO: add backtrace
func fatalErr(err error) {
	if err != nil {
		fmt.Println("Generic fatal error:", err)
		debug.PrintStack()
		os.Exit(1)
	}
}
