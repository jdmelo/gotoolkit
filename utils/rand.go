package utils

import (
	"math/rand"
	"time"
)

func RandInt(i int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(i)
}
