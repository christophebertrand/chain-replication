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

	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/dedis/onet/log"
)

func main() {
	fmt.Println("starting new raft node")

	raftCluster, cluster, successors, id, clusterID, join, first := parseinput()
	port, err := strconv.ParseInt(strings.Split(cluster[id-1], ":")[2], 10, 32)
	if err != nil {
		log.Fatal("could not parse port")
	}
	proposeC := make(chan message) // kv -> raft
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange) //cluster -> raft
	defer close(confChangeC)
	sendMessageC := make(chan message) //kv -> cluster
	defer close(sendMessageC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(id, clusterID, raftCluster, join, getSnapshot, proposeC, confChangeC, first)
	kvs = newKVStore(<-snapshotterReady, proposeC, sendMessageC)
	node := newClusterNode(kvs, sendMessageC, confChangeC, successors, 10, cluster, id-1)
	// the key-value http handler will propose updates to raft
	node.serveHTTPKV(int(port), errorC)
	fmt.Println("start")
	recover := kvs.start(commitC, errorC)
	fmt.Println(recover)
	if recover {
		fmt.Println("recovering")
		node.broadcast("recover", intMessage{id - 1})
	}
	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func parseinput() (raftCluster, cluster, successors []string, id, clusterID int, join, first bool) {
	raftClusterString := flag.String("raftCluster", "http://127.0.0.1:9021", "comma separated cluster raft peers")
	clusterString := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	idP := flag.Int("id", 1, "node ID")
	clusterIDP := flag.Int("clusterID", 0x1000, "culuster ID")
	pred := flag.String("pred", "-1", "comma separated cluster predecessor peers") //TODO are preds needed?
	succ := flag.String("succ", "-1", "comma separated cluster successor peers")
	//kvport := flag.Int("port", 9121, "key-value server port")
	joinP := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()
	id = *idP
	clusterID = *clusterIDP
	join = *joinP
	raftCluster = strings.Split(*raftClusterString, ",")
	cluster = strings.Split(*clusterString, ",")
	if *succ != "-1" {
		successors = strings.Split(*succ, ",")
	}
	first = false
	if *pred == "-1" {
		first = true
	}
	return
}
