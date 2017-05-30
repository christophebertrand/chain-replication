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
	// "net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/coreos/etcd/raft/raftpb"
)

const raftTimeOut = 5 * time.Second

type peerStatus []*peer

type peer struct {
	ID      int
	address string
	active  bool
}

type clusterNode struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	//TODO optimize replace MessageSet by []MessageSet

	newMessage <-chan message
	successors peerStatus
	maxPeers   int
	ID         int
	peers      peerStatus

	chanMu       sync.RWMutex
	appliedChans map[uint64]chan bool
	timeoutChans map[uint64]chan bool

	mu        sync.RWMutex
	toDeliver map[uint64]message
	delivered MessageSet
}

func newClusterNode(kv *kvstore, newMessage <-chan message, confChangeC chan<- raftpb.ConfChange, successors []string, maxPeers int, addresses []string, ID int) *clusterNode {
	//TODO check implicit ordering of peers (match to ids of peers)
	s := make([]*peer, len(successors))
	for i, succ := range successors {
		s[i] = &peer{i, succ, true}
	}
	peers := make([]*peer, len(addresses))
	for i, addr := range addresses {
		peers[i] = &peer{i, addr, true}
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
	// fmt.Println("nnew message from client ", key)
	retAddr := body
	n.store.Propose(key, value, string(retAddr))
	//client gets repsonse if it has succeeded from tail of the cluster
	w.WriteHeader(http.StatusNoContent)
}

//processDeliveredMsg processes a msg from a peer that it has delivered a message tu succ.
func (n *clusterNode) processDeliveredMsg(body []byte, w http.ResponseWriter) {
	var msg deliveredMessage
	err := json.Unmarshal(body, &msg)
	if err != nil {
		log.Fatal("couldn't convert msgID")
	}
	n.messageDelivered(msg.ID, msg.Undel)
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
	// log.Printf("message from pred " + msg.String())
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

		} else if len(split) == 2 { // new message from client
			n.processClientMsg(split[0], split[1], body, w)

		} else {
			log.Printf("wrong message format")
			http.Error(w, "wrong message format", http.StatusBadRequest)
			return
		}

	case r.Method == "GET":
		v, ts, ok := n.getHighest(key)
		if ok {
			n.broadcast("write", keyValue{key, value{v, ts}})
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}

	case r.Method == "POST":
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		if key == "read" {
			v, ts, _ := n.store.Lookup(string(body))
			b, err := json.Marshal(value{v, ts})
			if err != nil {
				log.Printf("could not marshal value %v\n", err)
			} else {
				w.Write(b)
			}
		}
		if key == "delivered" {
			n.processDeliveredMsg(body, w)
			return
		}
		if key == "write" {
			msg := decodeMessage(body)
			n.store.write(msg)
			w.WriteHeader(http.StatusOK)
		}
		if key == "recover" {
			var v intMessage
			if err := json.Unmarshal(body, &v); err != nil {
				log.Printf("wrong message format %v", err)
				http.Error(w, "wrong message format", http.StatusBadRequest)
			} else {
				n.peers[v.I].active = true
				n.mu.Lock()
				undel := n.delivered.EarliestUnseen
				n.mu.Unlock()
				msg, _ := json.Marshal(deliveredMessage{Undel: undel})
				w.Write(msg)
			}
		}

	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
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
}

//httpSend sends a http request with method to urlStr with body body
func httpSend(method string, urlStr string, body io.Reader) (*http.Response, error) {
	req, _ := http.NewRequest(method, urlStr, body)
	client := &http.Client{}
	req.Close = true
	return client.Do(req)
}

//processMessages reads messages coming from the kvstore ( n.newMessage) and sends it to the next cluster if it exists,
// or send an acknowledgement to the client
func (n *clusterNode) processMessages() {
	for msg := range n.newMessage {
		fmt.Println("new message ", msg)
		n.mu.Lock()
		fmt.Println(n.delivered)
		fmt.Println(n.toDeliver)
		n.mu.Unlock()
		//send notification to pred that msg has arrived
		// fmt.Println("message from raft has arrrived ", msg)
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
		resp := findResponsible(msg, n.peers)
		// fmt.Printf("responsible is for message %v is %v,\n the peers are %v\n I am %v\n", msg, resp.ID, n.peers, n.ID)
		n.mu.Lock()
		areadyDelivered := n.delivered.Contains(msg.ID)
		n.mu.Unlock()
		//check if message was already delivered
		if !areadyDelivered {
			n.mu.Lock()
			n.toDeliver[msg.ID] = msg
			n.mu.Unlock()
			if n.ID == resp.ID {
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
		body := bytes.NewBufferString(strconv.Itoa(int(msg.ID)) + " key " + msg.Key)
		resp, err := httpSend("PUT", retAddr, body)
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
	// log.Printf("I am sending message " + msg.String() + " to next cluster")
	buf := encodeMessage(msg)
	next := findResponsible(msg, n.successors)
	for {
		resp, err := httpSend("PUT", next.address, bytes.NewBuffer(buf))
		if err != nil {
			log.Printf("failed to send to succ")
			next.active = false
			other := findResponsible(msg, n.successors)
			if next == other {
				panic("nope")
			}
			next = other
		} else {
			resp.Body.Close()
			if resp.StatusCode == http.StatusRequestTimeout {
				time.Sleep(1 * time.Second)
			} else {
				n.broadcastDelivered(msg)
				break
			}
		}
	}
}

func findResponsible(msg message, cluster []*peer) *peer {
	var activeNodesIndexes []int
	for i, node := range cluster {
		if node.active {
			activeNodesIndexes = append(activeNodesIndexes, i)
		}
	}
	if len(activeNodesIndexes) == 0 {
		log.Printf("warning: no more active notes in cluster")
	}
	respIndex := activeNodesIndexes[responsible(msg, len(activeNodesIndexes))]
	return cluster[respIndex]
}

//responsible computes which peer, among the active peers, is responsible to send the message
func responsible(msg message, peerLength int) int {
	r := msg.ID % uint64(peerLength)
	return int(r)
}

//broadcastDelivered send a message to all peers of the node that msg has been delivered to the next entity (cluster or client)
func (n *clusterNode) broadcastDelivered(msg message) {
	fmt.Println("delivered ", msg)
	n.messageDelivered(msg.ID, 0)
	n.mu.Lock()
	undelivered := n.delivered.EarliestUnseen
	n.mu.Unlock()
	m := deliveredMessage{msg.ID, undelivered}
	n.broadcast("delivered", m)
}

func (n *clusterNode) broadcast(action string, msg interface{}) {
	action = "/" + action
	done := make(chan bool)
	for _, p := range n.peers {
		if p.ID == n.ID {
			go func() {
				done <- true
			}()
		} else if p.active {
			go func(done chan<- bool, p *peer) {
				n.sentToPeer(p, action, msg)
				done <- true
			}(done, p)
		}
	}
	for i := 0; i < (len(n.peers)); i++ {
		<-done
	}
	// for i := 0; i < (len(n.peers)+1)/2; i++ {
	// 	<-done
	// }
}

func (n *clusterNode) sentToPeer(peer *peer, path string, body interface{}) {
	tick := make(chan int)
	success := make(chan bool)
	go backOffTimer(tick, success, 5, 100*time.Millisecond)
	url := peer.address + path
	for {
		body, err := json.Marshal(body)
		if err != nil {
			log.Printf("could not marshal body %v\n", err)
			return
		}
		reader := bytes.NewReader(body)
		resp, err := httpSend("POST", url, reader)
		if err != nil {
			_, ok := <-tick
			if !ok { // tick is close which means the backOffTimer has reach the max value
				n.removePeer(peer)
				log.Printf("could not send to peer %v considering peer has failed %v\n", url, err)
				break
			}
		} else {
			n.addPeer(peer)
			go func() {
				<-success
				close(success)
			}()
			resp.Body.Close()
			break
		}
	}
}

func (n *clusterNode) getHighest(key string) (string, uint64, bool) {
	type response struct {
		v     value
		found bool
	}
	var result response
	var resps = make(chan response)
	for i, p := range n.peers {
		go func(i int, done chan<- response) {
			if i == n.ID {
				v, ts, ok := n.store.Lookup(key)
				done <- response{value{v, ts}, ok}
				return
			}
			resp, err := httpSend("POST", p.address+"/read", bytes.NewBufferString(key))
			if err != nil {

			} else {
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("Failed to read on PUT (%v)\n", err)
					return
				}
				var v value
				err = json.Unmarshal(body, &v)
				if err != nil {
					log.Printf("failed to unmarshall value %v\n", err)
				} else {
					if v.Val == "" {
						done <- response{v, false}
					} else {
						done <- response{v, true}
					}
				}
			}
		}(i, resps)
	}
	for i := 0; i < (len(n.peers)/2)+1; i++ {
		r := <-resps
		if r.found && r.v.Ts > result.v.Ts {
			result = r
		}
	}
	return result.v.Val, result.v.Ts, result.found
}

func (n *clusterNode) removePeer(inactivePeer *peer) {
	log.Printf("removing peer %v\n", inactivePeer)
	inactivePeer.active = false
}

func (n *clusterNode) addPeer(newPeer *peer) {
	newPeer.active = true
}

//messageDelivered removes msg from the toDeliver s and adds it to delivered with compaction
func (n *clusterNode) messageDelivered(msgID uint64, earliestUndelivered uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delivered.Add(msgID)
	n.delivered.AddUntil(earliestUndelivered)
	delete(n.toDeliver, msgID)
}
