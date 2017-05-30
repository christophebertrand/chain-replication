package main

import (
	"bytes"
	"flag"
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

type stats struct {
	total    time.Duration
	requests int
}

func main() {
	//numberClients := 50
	numberClientsP := flag.Int("clients", 50, "Amount of clients to run")
	tmP := flag.Int("time", 10, "Time which the clients should run")
	oneP := flag.Bool("one", false, "set to true if you want to send only one req")
	flag.Parse()
	numberClients := *numberClientsP
	tm := *tmP
	one := *oneP
	if one {
		numberClients = 1
		tm = 1
	}
	destPrefix := 11380
	t := time.Duration(tm) * time.Second
	wait := make(chan stats)
	fmt.Printf("new simulation with %v clients during %v ", numberClients, t)
	defer close(wait)
	for client := 1; client <= numberClients; client++ {
		//fmt.Printf("new client %v \n", client)
		retPort := client + 40005
		destPort := destPrefix + (client%3)*1000
		go newClient(destPort, retPort, t, wait, one)
	}
	var totalDuration time.Duration
	var req int
	for i := 1; i <= numberClients; i++ {
		stat := <-wait
		req += stat.requests
		totalDuration += stat.total
	}
	fmt.Printf("the total number of request was %v during %v with an total duration %v time for all clients\n", req, t, totalDuration)
}

func newClient(destPort, retPort int, t time.Duration, end chan<- stats, oneReq bool) {
	ok := make(chan time.Time)
	defer close(ok)
	returnAddr := "http://127.0.0.1:" + strconv.Itoa(retPort)
	createListener(retPort, ok)
	timeout := make(chan bool, 1)
	go func(t time.Duration) { //terminate client after t second
		time.Sleep(t)
		timeout <- true
	}(t)
	var sendTime time.Time
	totalDuration := time.Duration(0)
	var numReq int
	for {
		select {
		case <-timeout:
			fmt.Printf("finished %v requests in  %v time \n", numReq, totalDuration)
			end <- stats{totalDuration, numReq}
			break
		case receiveTime, open := <-ok:
			if open {
				if !receiveTime.Equal(time.Unix(0, 0)) { // this is the startup value
					reqTime := receiveTime.Sub(sendTime)
					totalDuration = reqTime + totalDuration
					numReq++
					if oneReq {
						end <- stats{totalDuration, numReq}
						return
					}
					//fmt.Printf("time to respond %v\n ", reqTime)
				}
				//time.Sleep(500 * time.Millisecond)
				destAddr := "http://127.0.0.1:" + strconv.Itoa(destPort)
				key := strconv.Itoa(numReq)
				value := strconv.Itoa(rand.Int())
				sendTime = time.Now()
				fmt.Printf("%v sending  \n ", retPort)
				sendRequest(put, key, value, destAddr, returnAddr)
			} else {
				fmt.Println("terminiating client " + strconv.Itoa(retPort))
				end <- stats{totalDuration / time.Duration(numReq), numReq}
				return
			}
		}
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

//Listens to the responses of the kv store
func (h *httphandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI
	id = strings.TrimPrefix(id, "/")
	// fmt.Println("recieved ok at " + strconv.Itoa(h.id))
	switch {
	case r.Method == put:
		s, err := ioutil.ReadAll(r.Body)
		fmt.Printf("%v new message %v \n", h.id, string(s))
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
