package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_encodingMessage(t *testing.T) {
	m1 := message{1, NormalMessage, "foo", "key", "val", false}
	m2 := message{1, DummyMessage, "", "", "", true}

	e1 := encodeMessage(m1)
	e2 := encodeMessage(m2)
	dm1 := decodeMessage(e1)
	dm2 := decodeMessage(e2)

	assert.Equal(t, m1, dm1)
	assert.Equal(t, m2, dm2)
}
