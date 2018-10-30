package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUniqueStringSlice(t *testing.T) {
	tests := []struct {
		Data   []string
		Result []string
	}{
		{
			[]string{"one", "two", "three", "three"},
			[]string{"one", "two", "three"},
		},
		{
			[]string{"one", "two", "три\u1234", "три\u1234"},
			[]string{"one", "two", "три\u1234"},
		},
		{
			[]string{"one", "two", "3 3", "3 3"},
			[]string{"one", "two", "3 3"},
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.Result, UniqueStringSlice(test.Data))
	}
}
