package check

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testingIntSlice(random bool, size int) []int32 {
	ret := []int32{}
	for i := 0; i < size; i++ {
		v := i
		if random {
			v = rand.Intn(1000)
		}
		ret = append(ret, int32(v))
	}
	return ret
}

func TestDelAll(t *testing.T) {
	s := testingIntSlice(false, 20)
	assert.True(t, contains(s, 10))
	s = delAll(s, 10)
	assert.False(t, contains(s, 10))

	assert.NotPanics(t, func() {
		delAll(s, 50)
	})
}
