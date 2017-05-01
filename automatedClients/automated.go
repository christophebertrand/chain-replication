package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var in *bufio.Reader
var out io.Writer
var put = "PUT"
var get = "GET"

type httphandler struct {
	ok chan<- bool
	id int
}

func (h *httphandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI

	id = strings.TrimPrefix(id, "/")
	fmt.Println(id)
	// fmt.Println("recieved ok at " + strconv.Itoa(h.id))
	switch {
	case r.Method == "PUT":
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		h.ok <- true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

	}
}
func newClient(destPort, retPort int, end chan<- bool) {
	ok := make(chan bool)
	returnAddr := "http://127.0.0.1:" + strconv.Itoa(retPort)
	createListener(retPort, ok)
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(30 * time.Second)
		timeout <- true
	}()
	for {
		select {
		case <-timeout:
			fmt.Println("finished execution")
			end <- true

		case <-ok:
			time.Sleep(500 * time.Millisecond)
			destAddr := "http://127.0.0.1:" + strconv.Itoa(destPort)
			key := strconv.Itoa(rand.Int())
			value := key
			fmt.Println("send")
			sendRequest(put, key, value, destAddr, returnAddr)
		}
	}
}

func createListener(port int, ok chan<- bool) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: &httphandler{ok, port},
	}
	go func() {
		ok <- true
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}

func main() {
	numberClients := 5
	destPrefix := 11380
	wait := make(chan bool)
	for client := 1; client <= numberClients; client++ {
		fmt.Println("new client")
		retPort := client + 40005
		destPort := destPrefix + (client%3)*1000
		go newClient(destPort, retPort, wait)
	}
	<-wait
}

func sendRequest(method, key, value, destAddr, returnAddr string) {
	if method == put {
		req, _ := http.NewRequest("PUT", destAddr+"/"+key+"/"+value, bytes.NewBufferString(returnAddr))
		client := &http.Client{}
		r, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(r.Body)
		}
		fmt.Println("sending to " + destAddr)
	} else {
		resp, err := http.Get(destAddr + "/" + key)
		if err != nil {
			log.Fatal("could not GET the value")
		}
		v, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read on get (%v)\n", err)
			return
		}
		fmt.Println(key + " has value: " + string(v))

	}
}
