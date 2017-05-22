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
	"io"
	"io/ioutil"
	"log"
	// "net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
)

const raftTimeOut = 5 * time.Second

type peers struct {
	p map[string]bool
	//active  bool
	//address string
}
type peer struct {
	active  bool
	address string
}

type clusterNode struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	//TODO optimize replace MessageSet by []MessageSet

	newMessage <-chan message
	successors []*peer
	maxPeers   int
	ID         int
	peers      []*peer

	chanMu       sync.RWMutex
	appliedChans map[uint64]chan bool
	timeoutChans map[uint64]chan bool

	mu        sync.RWMutex
	toDeliver map[uint64]message
	delivered MessageSet
}

func newClusterNode(kv *kvstore, newMessage <-chan message, confChangeC chan<- raftpb.ConfChange, successors []string, maxPeers int, addresses []string, ID int) *clusterNode {
	//TODO check implicit ordering of peers (match to ids of peers)
	peers := make([]*peer, len(addresses))
	s := make([]*peer, len(successors))
	for i, succ := range successors {
		s[i] = &peer{true, succ}
	}
	for i, addr := range addresses {
		peers[i] = &peer{true, addr}
	}
	c := clusterNode{
		store:       kv,
		confChangeC: confChangeC,
		//earliestUndelivered: uint64(1),
		delivered:    NewMessageSet(),
		toDeliver:    make(map[uint64]message),
		newMessage:   newMessage,
		successors:   s,
		maxPeers:     maxPeers,
		ID:           ID,
		peers:        peers,
		appliedChans: make(map[uint64]chan bool), // channels to tell the waiting
		// connection that the msg has been applied
		timeoutChans: make(map[uint64]chan bool), // channels to tell the applier
		// that the msg took to long and appliedChan has been closed
	}
	go c.processMessages()
	return &c
}

func (n *clusterNode) processClientMsg(key string, value string, body []byte, w http.ResponseWriter) {
	retAddr := body
	n.store.Propose(key, value, string(retAddr))
	//client gets repsonse if it has succeeded from tail of the cluster
	w.WriteHeader(http.StatusNoContent)
}

//processDeliveredMsg processes a msg from a peer that it has delivered a message tu succ.
func (n *clusterNode) processDeliveredMsg(body []byte, w http.ResponseWriter) {
	split := strings.Split(string(body), "/")
	msgID, err := strconv.ParseUint(split[0], 10, 64)
	if err != nil {
		log.Fatal("couldn't convert msgID")
	}
	earliestUndelivered, err := strconv.ParseUint(split[1], 10, 64)
	if err != nil {
		log.Fatal("couldn't convert earliestUndelivered")
	}
	n.messageDelivered(msgID, earliestUndelivered)
	w.WriteHeader(http.StatusNoContent)
}

// processPredMsg processes a message from the previous cluster. It passes is through raft and
// when it has been applied by raft, it responds with an http.StatusOK.
// If the message is currently processed by raft it sends back an http.StatusConflict
// If raft takes more than raftTimeOut time it sends back an http.Timeout
func (n *clusterNode) processPredMsg(split []string, body []byte, w http.ResponseWriter) {
	msg := decodeMessage(body)
	n.mu.Lock()
	del := n.delivered.Contains(msg.ID)
	_, toDel := n.toDeliver[msg.ID]
	n.mu.Unlock()
	//the message has already been applied by raft
	if del || toDel {
		w.WriteHeader(http.StatusOK)
		return
	}
	n.chanMu.Lock()
	if n.appliedChans[msg.ID] != nil {
		// already received and is processed now
		n.chanMu.Unlock()
		http.Error(w, "already recieved this message", http.StatusConflict)
		return
	}
	applied := make(chan bool)
	timeout := make(chan bool)
	n.appliedChans[msg.ID] = applied
	n.timeoutChans[msg.ID] = timeout
	n.chanMu.Unlock()
	n.store.Propagate(msg)

	select {
	case <-applied:
		w.WriteHeader(http.StatusOK)

	case <-time.After(raftTimeOut):
		http.Error(w, "timed out ", http.StatusRequestTimeout)
		go func() {
			select {
			case timeout <- true:
			case <-applied:
			}
		}()

	}
	n.chanMu.Lock()
	delete(n.appliedChans, msg.ID)
	delete(n.timeoutChans, msg.ID)
	n.chanMu.Unlock()
}

func (n *clusterNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	key = strings.TrimPrefix(key, "/")
	switch {
	case r.Method == "PUT":
		// PUT method has:
		//-RequestURI = "" 					 and body = "message" 										if it is a message from pred
		//-RequestURI = "delivered"  and body = "MsgID/earliestUndelivered" 		if it is a Delivered msg
		//-RequestURI = "key/value"  and body = "returnAddr" 									if it is a new message from client
		split := strings.Split(key, "/")
		key = split[0]
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		if key == "" { //message from pred
			n.processPredMsg(split, body, w)

		} else if key == "delivered" && len(split) == 1 { // peer has delivered some messages
			n.processDeliveredMsg(body, w)

		} else if len(split) == 2 { // new message from client
			n.processClientMsg(split[0], split[1], body, w)

		} else {
			log.Printf("wrong message format")
			http.Error(w, "wrong message format", http.StatusBadRequest)
			return
		}

	case r.Method == "GET":
		if v, ok := n.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}

	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
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

//httpPut sends a PUT http request to urlStr with body body
func httpPut(urlStr string, body io.Reader) (*http.Response, error) {
	req, _ := http.NewRequest("PUT", urlStr, body)
	client := &http.Client{}
	req.Close = true
	return client.Do(req)
}

//processMessages reads messages coming from the kvstore ( n.newMessage) and sends it to the next cluster if it exists,
// or send an acknowledgement to the client
func (n *clusterNode) processMessages() {
	//TODO hacky way to wait that the successors server are listening
	time.Sleep(1 * time.Second)
	for msg := range n.newMessage {
		//send notification to pred that msg has arrived
		go func(msg message) {
			n.mu.Lock()
			del := n.delivered.Contains(msg.ID)
			_, toDel := n.toDeliver[msg.ID]
			n.mu.Unlock()
			if !del || !toDel {
				n.chanMu.Lock()
				applied, ok1 := n.appliedChans[msg.ID]
				timeout, ok2 := n.timeoutChans[msg.ID]
				n.chanMu.Unlock()
				if ok1 && ok2 {
					select {
					case applied <- true:
					case <-timeout:
					}
				}
			}
		}(msg)
		_, resp := findResponsible(msg, n.peers)
		if n.ID == resp {
			n.mu.Lock()
			areadyDelivered := n.delivered.Contains(msg.ID)
			n.mu.Unlock()
			//check if message was already delivered
			if !areadyDelivered {
				n.mu.Lock()
				n.toDeliver[msg.ID] = msg
				n.mu.Unlock()
				//we are at the tail and need to send the message to the client
				if len(n.successors) == 0 {
					if !msg.Replay {
						go n.sendToClient(msg)
					}
					if msg.MsgType == DummyMessage {
						n.broadcastDelivered(msg)
					}
				} else {
					go n.sendToNextCluster(msg)
				}
			}
		}
	}
}

// backOffTimer sends a tick through tickChan for the amount of ticks specified by numTicks
// the timer stops ticking if it recieves a message from succes chan
func backOffTimer(tickChan chan<- int, success <-chan bool, numTicks int, initial time.Duration) {
	for i, t := 1, 1; i <= numTicks; i++ {
		select {
		case <-time.After(time.Duration(t) * initial):
			t *= 2
			tickChan <- i
		case <-success:
			i = numTicks
		}
	}
	close(tickChan)
}

//sendToClient sends an acknowledgement to the client that the message has been recieved.
//it also notifies the peers that the message has been delivered
func (n *clusterNode) sendToClient(msg message) {
	// returnAddr:port/msgID_Value
	// log.Printf("I am sending message " + msg.String() + " to the client")
	tick := make(chan int)
	success := make(chan bool)
	go backOffTimer(tick, success, 5, 50*time.Millisecond)
	retAddr := msg.RetAddr + "/" + strconv.Itoa(int(msg.ID)) + "_" + msg.Val
	for {
		body := bytes.NewBufferString(strconv.Itoa(int(msg.ID)))
		resp, err := httpPut(retAddr, body)
		if err != nil {
			_, ok := <-tick
			if !ok { // tick is close which means the backOffTimer has reach the max value
				log.Printf("could not send to client %v", err)
				//TODO can we consider the message delivered?
				n.broadcastDelivered(msg)
				break
			}
		} else {
			func() {
				success <- true
				close(success)
			}()
			n.broadcastDelivered(msg)
			resp.Body.Close()
			break
		}

	}
}

func (n *clusterNode) sendToNextCluster(msg message) {
	buf := encodeMessage(msg)
	dest, i := findResponsible(msg, n.successors)
	for {
		resp, err := httpPut(dest, bytes.NewBuffer(buf))
		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			i++
			dest = n.successors[i%len(n.successors)].address
		} else {
			if resp.StatusCode == http.StatusRequestTimeout {
				time.Sleep(1 * time.Second)
			} else {
				n.broadcastDelivered(msg)
				break
			}
		}
	}
}

func findResponsible(msg message, cluster []*peer) (string, int) {
	var activeNodesIndexes []int
	for i, node := range cluster {
		if node.active {
			activeNodesIndexes = append(activeNodesIndexes, i)
		}
	}
	if len(activeNodesIndexes) == 0 {
		return "", -1
	}
	respIndex := activeNodesIndexes[responsible(msg, len(activeNodesIndexes))]
	return cluster[respIndex].address, respIndex
}

//responsible computes which peer, among the active peers, is responsible to send the message
func responsible(msg message, peerLength int) int {
	r := msg.ID % uint64(peerLength)
	return int(r)
}

//broadcastDelivered send a message to all peers of the node that msg has been delivered to the next entity (cluster or client)
func (n *clusterNode) broadcastDelivered(msg message) {
	n.mu.Lock()
	undelivered := n.delivered.EarliestUnseen
	n.mu.Unlock()
	n.messageDelivered(msg.ID, undelivered)
	n.mu.Lock()
	undelivered = n.delivered.EarliestUnseen
	n.mu.Unlock()
	messageIDS := strconv.FormatUint(msg.ID, 10)
	undeliveredS := strconv.FormatUint(undelivered, 10)
	bodyS := messageIDS + "/" + undeliveredS
	for i, p := range n.peers {
		if i != n.ID {
			if p.active {
				path := "/delivered"
				go n.sentToPeer(p, path, bodyS)
			}
		}
	}
}

func (n *clusterNode) sentToPeer(peer *peer, path string, bodyS string) {
	tick := make(chan int)
	success := make(chan bool)
	go backOffTimer(tick, success, 5, 100*time.Millisecond)
	url := peer.address + path
	for {
		body := bytes.NewBufferString(bodyS)
		resp, err := httpPut(url, body)
		if err != nil {
			_, ok := <-tick
			if !ok { // tick is close which means the backOffTimer has reach the max value
				n.removePeer(peer)
				log.Printf("could not send to peer %v considering peer has failed %v\n", url, err)
				break
			}
		} else {
			go func() {
				<-success
				close(success)
			}()
			resp.Body.Close()
			break
		}
	}
}

func (n *clusterNode) removePeer(peer *peer) {
	// peer.active = false
	// for _, p := range n.peers {
	// 	if p.active {
	// 		path := "/peer/failed"
	// 		go n.sentToPeer(p, path, peer.address)
	// 	}
	// }
}

func (n *clusterNode) addPeer(peer *peer) {
	// peer.active = true
	// for _, p := range n.peers {
	// 	if p.active {
	// 		path := "/peer/recovered"
	// 		go n.sentToPeer(p, path, peer.address)
	// 	}
	// }
}

//messageDelivered removes msg from the toDeliver s and adds it to delivered with compaction
func (n *clusterNode) messageDelivered(msgID uint64, earliestUndelivered uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delivered.Add(msgID)
	n.delivered.AddUntil(earliestUndelivered)
	delete(n.toDeliver, msgID)
}
