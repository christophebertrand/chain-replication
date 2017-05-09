package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var put = "PUT"
var get = "GET"

type httphandler struct {
	ok chan<- time.Time
	id int
}

//Listens to the responses of the kv store
func (h *httphandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI

	id = strings.TrimPrefix(id, "/")
	// fmt.Println("recieved ok at " + strconv.Itoa(h.id))
	switch {
	case r.Method == put:
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		h.ok <- time.Now()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

	}
}

func newClient(destPort, retPort int, t time.Duration, end chan<- bool) {
	ok := make(chan time.Time)
	defer close(ok)
	returnAddr := "http://127.0.0.1:" + strconv.Itoa(retPort)
	createListener(retPort, ok)
	timeout := make(chan bool, 1)
	go func(t time.Duration) { //terminate client after t second
		time.Sleep(t * time.Second)
		timeout <- true
	}(t)
	var sendTime time.Time
	totalDuration := time.Duration(0)
	var numReq int
	for {
		select {
		case <-timeout:
			fmt.Printf("average time to finish the execution is %v\n", totalDuration / time.Duration(numReq))
			end <- true
			break
		case receiveTime, open := <-ok:
			if open {
				if !receiveTime.Equal(time.Unix(0, 0)) {
					reqTime := receiveTime.Sub(sendTime)
					totalDuration = reqTime + totalDuration
					numReq++
					fmt.Printf("time to respond %v\n ", reqTime)
				}
				//time.Sleep(500 * time.Millisecond)
				destAddr := "http://127.0.0.1:" + strconv.Itoa(destPort)
				key := strconv.Itoa(rand.Int())
				value := key
				sendTime = time.Now()
				sendRequest(put, key, value, destAddr, returnAddr)
			} else {
				fmt.Println("terminiating client " + strconv.Itoa(retPort))
				end <- true
				return
			}
		}
	}
}

func createListener(port int, ok chan<- time.Time) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: &httphandler{ok, port},
	}
	go func() {
		ok <- time.Unix(0, 0) //for startup
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println(err)
			close(ok)
		}
	}()
}

func main() {
	numberClients := 50
	destPrefix := 11380
	t := time.Duration(10)
	wait := make(chan bool)
	defer close(wait)
	for client := 1; client <= numberClients; client++ {
		fmt.Printf("new client %v \n", client)
		retPort := client + 40005
		destPort := destPrefix + (client%3)*1000
		go newClient(destPort, retPort, t, wait)
	}
	for i := 1; i <= numberClients; i++ {
		<-wait
	}
}

func sendRequest(method, key, value, destAddr, returnAddr string) {
	if method == put {
		req, _ := http.NewRequest("PUT", destAddr+"/"+key+"/"+value, bytes.NewBufferString(returnAddr))
		client := &http.Client{}
		_, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
		} else {
			// fmt.Println(r.Body)
		}
		// fmt.Println("sending to " + destAddr)
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
