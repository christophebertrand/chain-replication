package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_encodingMessage(t *testing.T) {
	m1 := message{NormalMessage, "foo", "key", value{Val: "val"}, false}
	m2 := message{DummyMessage, "", "", value{"", 1}, true}

	e1 := encodeMessage(m1)
	e2 := encodeMessage(m2)
	dm1 := decodeMessage(e1)
	dm2 := decodeMessage(e2)

	assert.Equal(t, m1, dm1)
	assert.Equal(t, m2, dm2)
}
