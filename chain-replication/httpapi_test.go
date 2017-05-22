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
	l := 10
	var msg []message
	for i := 0; i < l; i++ {
		m := message{ID: uint64(i)}
		msg = append(msg, m)
	}
	var peers []*peer
	for i := 0; i < l; i++ {
		p := peer{true, strconv.Itoa(i)}
		peers = append(peers, &p)
	}

	for i := 0; i < l; i++ {
		_, x := findResponsible(msg[i], peers)
		require.Equal(t, i, x)
	}
	peers[3].active = false
	exprected := []int{0, 1, 2, 4, 5, 6, 7, 8, 9, 0}
	for i := 0; i < l; i++ {
		_, x := findResponsible(msg[i], peers)
		require.Equal(t, exprected[i], x)
	}
	peers[5].active = false
	exprected = []int{0, 1, 2, 4, 6, 7, 8, 9, 0, 1}
	for i := 0; i < l; i++ {
		_, x := findResponsible(msg[i], peers)
		require.Equal(t, exprected[i], x)
	}
}
