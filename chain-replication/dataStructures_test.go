package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Adduntil(t *testing.T) {
	seenset := NewMessageSet()
	seenset.Add(10)
	seenset.Add(6)
	seenset.AddUntil(7)
	expected := NewMessageSet()
	for i := 1; i <= 6; i++ {
		expected.Add(uint64(i))
	}
	expected.Add(10)
	require.Equal(t, expected, seenset)
}
