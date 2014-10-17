package binlog

import (
	"fmt"
	"os"
	"runtime/debug"
)

func fatalErr(err error) {
	if err != nil {
		fmt.Println("Generic fatal error:", err)
		debug.PrintStack()
		os.Exit(1)
	}
}
