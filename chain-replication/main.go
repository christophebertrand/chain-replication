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
	"flag"
	"fmt"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
)

func main() {
	fmt.Print("starting new raft node")
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	clusterID := flag.Int("clusterID", 0x1000, "culuster ID")
	pred := flag.String("pred", "-1", "comma separated cluster predecessor peers")
	succ := flag.String("succ", "-1", "comma separated cluster successor peers")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	succesor := "-1"
	if *succ != "-1" {
		succesor = *pred //TODO REMOVE PRED FLAG
		succesors := strings.Split(*succ, ",")
		succesor = succesors[*id-1]
	}
	proposeC := make(chan message)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, *clusterID, strings.Split(*cluster, ","),
		*join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC, succesor, *id)
	fmt.Println("returned from kv")
	// the key-value http handler will propose updates to raft
	serveHTTPKVAPI(kvs, *kvport, confChangeC, errorC)
}
