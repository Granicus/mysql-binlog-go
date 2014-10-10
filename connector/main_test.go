package connector

import (
	"fmt"
	"testing"
)

func TestMain(t *testing.T) {
	l, err := NewConnection(":3306", "fudd", "wabbit-season")

	if err != nil {
		t.Error(err)
	}

	fmt.Println("listener:", l)
}
