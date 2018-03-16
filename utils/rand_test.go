package utils

import (
	"fmt"
	"testing"
)

func TestRand(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(RandInt(1000))
	}
}
