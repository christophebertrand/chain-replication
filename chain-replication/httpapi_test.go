package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_exponentialBackof(t *testing.T) {

	tick := make(chan int)
	success := make(chan bool)
	go backOffTimer(tick, success, 10, 50*time.Millisecond)

	success <- true
}

func Test_responsible(t *testing.T) {
	const l = 10
	var msg []message
	for i := 0; i < l; i++ {
		m := message{ID: uint64(i + l)}
		msg = append(msg, m)
	}
	var peers []*peer
	for i := 0; i < l; i++ {
		p := peer{i, strconv.Itoa(i), true}
		peers = append(peers, &p)
	}
	var nodes [l]clusterNode
	for i := 0; i < l; i++ {
		n := clusterNode{ID: i, peers: peers}
		nodes[i] = n
	}
	for i := 0; i < l; i++ {
		x := nodes[i].isResponsible(msg[i])
		require.Equal(t, true, x)
	}
	peers[3].active = false
	exprected := []bool{false, false, false, true, true, false, false, false, false, false}
	for i := 0; i < l; i++ {
		x := nodes[4].isResponsible(msg[i])
		require.Equal(t, exprected[i], x)
	}
	peers[5].active = false
	exprected = []bool{false, false, false, true, true, false, false, false, false, false}
	for i := 0; i < l; i++ {
		x := nodes[4].isResponsible(msg[i])
		require.Equal(t, exprected[i], x)
	}
}
