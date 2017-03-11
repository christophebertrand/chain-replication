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
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]value // current committed key-value pairs
	snapshotter *snap.Snapshotter
	successor   string
}

var messageID = 0
var id = 0

type message struct {
	RetAddr string
	Key     string
	Val     value
}

type value struct {
	Val       string
	NodeID    int
	MessageID int
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string,
	errorC <-chan error, successor string, ID int) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]value), snapshotter: snapshotter, successor: successor}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	id = ID
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore[key]
	s.mu.RUnlock()
	return v.Val, ok
}

func (s *kvstore) Propagate(data io.Reader) {
	message := decodeMessage(data)
	if s.isNewMessage(message) {
		buf := encodeMessage(message)
		s.proposeC <- string(buf.Bytes())
	}
}

//Propose sends a new message to the raft layer
func (s *kvstore) Propose(k string, v string, retAddr string) {
	messageID++
	message := message{retAddr, k, value{v, id, messageID}}
	buf := encodeMessage(message)
	s.proposeC <- string(buf.Bytes())
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
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
		message := decodeMessage(bytes.NewBufferString(*data))
		fmt.Println("new message recieved " + message.Key + " " + message.Val.Val)

		if s.isNewMessage(message) {
			s.mu.Lock()
			s.kvStore[message.Key] = message.Val
			s.mu.Unlock()
			if s.successor == "-1" {
				httpSend(message.RetAddr, bytes.NewBufferString("ok"))
			} else {
				buf := encodeMessage(message)
				httpSend(s.successor, &buf)
			}
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func httpSend(urlStr string, body io.Reader) {
	req, _ := http.NewRequest("PUT", urlStr, body)
	client := &http.Client{}
	_, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]value
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.kvStore = store
	s.mu.Unlock()
	return nil
}

func decodeMessage(r io.Reader) message {
	var message message
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&message); err != nil {
		log.Fatalf("raftexample: could not decode message (%v)", err)
	}
	return message
}

func encodeMessage(message message) bytes.Buffer {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(message); err != nil {
		log.Fatal(err)
	}
	return buf
}

func (s *kvstore) isNewMessage(message message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.kvStore[message.Key]
	//check if message has already been delivere once
	if value.NodeID != message.Val.NodeID || value.MessageID != message.Val.MessageID {
		return true
	}
	return false
}
