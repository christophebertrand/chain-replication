// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC     chan<- message // channel for proposing updates
	mu           sync.RWMutex
	kvStore      store // current committed key-value pairs
	snapshotter  *snap.Snapshotter
	sendMessageC chan<- message // channel for sending commited messages to httpAPI
}

type store struct {
	kv   map[string]string
	seen MessageSet
}

func newStore() store {
	return store{
		kv: make(map[string]string),
		//earliestUnseen: 0,
		seen: NewMessageSet(),
	}
}

type MessageType int32

const (
	NormalMessage MessageType = 0
	DummyMessage  MessageType = 1
)

type message struct {
	ID      uint64
	MsgType MessageType
	RetAddr string
	Key     string
	Val     string
	Replay  bool
}

func (m message) String() string {
	var repl string
	id := strconv.Itoa(int(m.ID))
	if m.Replay {
		repl = "true"
	} else {
		repl = "false"
	}
	if m.MsgType == 0 {
		return "MessageID: " + id + " key/value: " + m.Key + "/" + m.Val + " Replay " + repl
	}
	return "MessageID: " + id + " dummy"
}

func (s *kvstore) start(commitC <-chan *message, errorC <-chan error) {
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- message, sendMessageC chan<- message) *kvstore {
	s := &kvstore{
		proposeC:     proposeC,
		kvStore:      newStore(),
		snapshotter:  snapshotter,
		sendMessageC: sendMessageC,
	}
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore.kv[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *kvstore) Propagate(data []byte) {
	msg := decodeMessage(data)
	if s.isNewMessage(msg) {
		s.proposeC <- msg
	}
}

//Propose sends a new message to the raft layer
func (s *kvstore) Propose(k string, v string, retAddr string) {
	msg := message{0, NormalMessage, retAddr, k, v, false}
	s.proposeC <- msg
}

func (s *kvstore) readCommits(commitC <-chan *message, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}
		msg := *data

		if s.isNewMessage(msg) {
			if msg.MsgType == NormalMessage {
				s.mu.Lock()
				s.kvStore.kv[msg.Key] = msg.Val
				s.kvStore.seen.Add(msg.ID)
				s.mu.Unlock()
				s.sendMessageC <- msg
			}
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var st store
	if err := json.Unmarshal(snapshot, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.kvStore = st
	s.mu.Unlock()
	return nil
}

func decodeMessage(b []byte) message {
	var msg message
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&msg); err != nil {
		log.Fatalf("kvstore: could not decode message (%v)", err)
	}
	return msg
}

func encodeMessage(message message) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(message); err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func (s *kvstore) isNewMessage(message message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.kvStore.seen.Contains(message.ID)
	//if message.MsgType == DummyMessage {
	//	return true
	//}
	//oldValue := s.kvStore[message.Key]
	////check if message has already been delivered once
	//if oldValue.MessageID < message.Val.MessageID {
	//	return true
	//}
	//return false
}
