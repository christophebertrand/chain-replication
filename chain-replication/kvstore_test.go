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

// func Test_sendMessage(t *testing.T) {
// 	proposeC := make(chan message) // kv -> raft
// 	defer close(proposeC)
// 	sendMessageC := make(chan message) //kv -> cluster
// 	defer close(sendMessageC)
//
// 	commitC := make(chan *message) // raft -> kv
// 	defer close(commitC)
// 	errorC := make(chan error)
// 	defer close(errorC)
// 	kvs := newKVStore(&snap.Snapshotter{}, proposeC, sendMessageC)
// 	go kvs.start(commitC, errorC)
// 	msg0 := message{ID: 0, MsgType: NormalMessage, RetAddr: "test1"}
// 	require.Equal(t, false, kvs.isNewMessage(msg0))
// 	kvs.Propose("", "", "test1")
// 	select {
// 	case m := <-proposeC:
// 		fmt.Println(m)
// 	case <-time.After(1 * time.Second):
// 		fmt.Println("timout")
// 	}
// 	// m := <-proposeC
// 	// require.Equal(t, msg0, m)
//
// }
