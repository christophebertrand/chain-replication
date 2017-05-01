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
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
)

type peer struct {
	active  bool
	address string
}

type clusterNode struct {
	store               *kvstore
	confChangeC         chan<- raftpb.ConfChange
	earliestUndelivered uint64
	delivered           MessageSet
	//TODO optimize replace MessageSet by []MessageSet
	toDeliver map[uint64]message

	newMessage <-chan message
	successor  string
	maxPeers   int
	ID         int
	peers      []peer
	mu         sync.RWMutex
}

func newClusterNode(kv *kvstore, newMessage <-chan message, confChangeC chan<- raftpb.ConfChange, successors []string, maxPeers int, addresses []string, ID int) *clusterNode {
	//TODO check implicit ordering of peers (match to ids of peers)
	peers := make([]peer, len(addresses))
	var successor string //TODO successor is temporary, compute seccessor based on msgID and responsible(msgID)
	if successors != nil {
		successor = successors[0]
	}
	for i, addr := range addresses {
		peers[i] = peer{true, addr}
	}
	c := clusterNode{
		store:               kv,
		confChangeC:         confChangeC,
		earliestUndelivered: uint64(1),
		delivered:           NewMessageSet(),
		toDeliver:           make(map[uint64]message),
		newMessage:          newMessage,
		successor:           successor,
		maxPeers:            maxPeers,
		ID:                  ID,
		peers:               peers,
	}
	go c.processMessages()
	return &c
}

func (n *clusterNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	key = strings.TrimPrefix(key, "/")
	switch {
	case r.Method == "PUT":
		// PUT method has:
		//-RequestURI = "key/value" and body = "returnAddr"  if it is a new message
		//-RequestURI = "" 					 and body = "message" 		if it is a PUTSucc
		split := strings.Split(key, "/")
		key = split[0]
		if key == "" { //message from pred
			buf := new(bytes.Buffer)
			buf.ReadFrom(r.Body)
			b := buf.Bytes()
			// msg := decodeMessage(b)

			n.store.Propagate(b)
		} else if key == "delivered" && len(split) == 1 { // peer has delivered some messages
			str, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Failed to read on PUT (%v)\n", err)
				http.Error(w, "Failed on PUT", http.StatusBadRequest)
				return
			}
			split = strings.Split(string(str), ",")
			msgID, err := strconv.ParseUint(split[0], 10, 64)
			if err != nil {
				log.Fatal("couldn't convert msgID")
			}
			earliestUndelivered, err := strconv.ParseUint(split[1], 10, 64)
			if err != nil {
				log.Fatal("couldn't convert earliestUndelivered")
			}
			n.messageDelivered(msgID, earliestUndelivered)
		} else { // new message from client
			value := split[1]
			retAddr, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Failed to read on PUT (%v)\n", err)
				http.Error(w, "Failed on PUT", http.StatusBadRequest)
				return
			}
			n.store.Propose(key, string(value), string(retAddr))
		}
		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, ok := n.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	//case r.Method == "POST":
	//	url, err := ioutil.ReadAll(r.Body)
	//	if err != nil {
	//		log.Printf("Failed to read on POST (%v)\n", err)
	//		http.Error(w, "Failed on POST", http.StatusBadRequest)
	//		return
	//	}
	//
	//	nodeID, err := strconv.ParseUint(key[1:], 0, 64)
	//	if err != nil {
	//		log.Printf("Failed to convert ID for conf change (%v)\n", err)
	//		http.Error(w, "Failed on POST", http.StatusBadRequest)
	//		return
	//	}
	//
	//	cc := raftpb.ConfChange{
	//		Type:    raftpb.ConfChangeAddNode,
	//		NodeID:  nodeID,
	//		Context: url,
	//	}
	//	n.confChangeC <- cc
	//
	//	// As above, optimistic that raft will apply the conf change
	//	w.WriteHeader(http.StatusNoContent)
	//case r.Method == "DELETE":
	//	nodeID, err := strconv.ParseUint(key[1:], 0, 64)
	//	if err != nil {
	//		log.Printf("Failed to convert ID for conf change (%v)\n", err)
	//		http.Error(w, "Failed on DELETE", http.StatusBadRequest)
	//		return
	//	}
	//
	//	cc := raftpb.ConfChange{
	//		Type:   raftpb.ConfChangeRemoveNode,
	//		NodeID: nodeID,
	//	}
	//	n.confChangeC <- cc
	//
	//	// As above, optimistic that raft will apply the conf change
	//	w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHTTPKV starts a key-value server with a GET/PUT API and listens.
func (n *clusterNode) serveHTTPKV(port int, errorC <-chan error) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: n,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func httpPut(urlStr string, body io.Reader) (*http.Response, error) {
	req, _ := http.NewRequest("PUT", urlStr, body)
	// time.Sleep(3 * time.Second)
	client := &http.Client{}
	return client.Do(req)
}

//sendToClient sends an acknowledgement to the client that the message has been recieved.
//it also notifies the peers that the message has been delivered
func (n *clusterNode) sendToClient(msg message) {
	// returnAddr:port/msgID_Value
	fmt.Println("I am " + strconv.Itoa(n.ID) + " and I am sending message " + msg.String() + " to the client")
	retAddr := msg.RetAddr + "/" + strconv.Itoa(int(msg.ID)) + "_" + msg.Val
	body := bytes.NewBufferString(strconv.Itoa(int(msg.ID)))
	resp, err := httpPut(retAddr, body)
	if err != nil {
		//TODO implement backoff
	} else {
		n.broadcastDelivered(msg)
		n.messageDelivered(msg.ID, n.earliestUndelivered)

	}
	resp.Body.Close()
}

//responsible computes which peer, among the active peers, is responsible to send the message
func (n *clusterNode) responsible(msg message, peerLength int) int {
	r := msg.ID % uint64(peerLength)
	return int(r)
}

//processMessages reads messages coming from the kvstore ( n.newMessage) and sends it to the next cluster if it exists,
// or send an acknowledgement to the client
func (n *clusterNode) processMessages() {
	for msg := range n.newMessage {

		if n.successor == "" {
			if !msg.Replay {
				n.mu.Lock()
				contains := n.delivered.Contains(msg.ID)
				n.mu.Unlock()
				if !contains {
					// activePeers[i] gives use the ith active peer in n.peers
					activePeers := make([]int, len(n.peers))
					for i, peer := range n.peers {
						if peer.active {
							activePeers = append(activePeers, i)
						}
					}
					responsible := activePeers[n.responsible(msg, len(activePeers))]
					n.mu.Lock()
					n.toDeliver[msg.ID] = msg
					n.mu.Unlock()
					if responsible == n.ID {
						n.sendToClient(msg)
					}
				}
			}
		} else {
			buf := encodeMessage(msg)
			go httpPut(n.successor, bytes.NewBuffer(buf))
		}
	}
}

//broadcastDelivered send a message to all peers of the node that msg has been delivered to the next entity (cluster or client)
func (n *clusterNode) broadcastDelivered(msg message) {
	n.mu.Lock()
	undeliveredS := strconv.FormatUint(n.earliestUndelivered, 10)
	n.mu.Unlock()
	messageIDS := strconv.FormatUint(msg.ID, 10)
	body := bytes.NewBufferString(undeliveredS + "/" + messageIDS)
	for _, peer := range n.peers {
		if peer.active {
			url := peer.address + "," + "delivered"
			go httpPut(url, body)
		}
	}
}

//messageDelivered removes msg from the toDeliver s and adds it to delivered with compaction
func (n *clusterNode) messageDelivered(msgID uint64, earliestUndelivered uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delivered.AddUntil(earliestUndelivered)
	n.delivered.Add(msgID)
	delete(n.toDeliver, msgID)

}
