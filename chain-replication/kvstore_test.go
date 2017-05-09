package main

import (
	"fmt"
	"reflect"
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

func Test_kvstore_snapshot(t *testing.T) {
	tm := map[string]string{"foo": "bar"}
	seenset := NewMessageSet()
	seenset.Add(10)
	seenset.AddUntil(5)
	st := store{Kv: tm, Seen: seenset}
	s := &kvstore{kvStore: st}

	v, _ := s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}

	data, err := s.getSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.kvStore = store{}

	if err := s.recoverFromSnapshot(data); err != nil {
		t.Fatal(err)
	}
	v, _ = s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}
	if !reflect.DeepEqual(s.kvStore, st) {
		t.Fatalf("store expected %+v, got %+v", st, s.kvStore)
	}
	fmt.Print(s.kvStore.Seen)
}
