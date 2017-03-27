package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var in *bufio.Reader
var out io.Writer
var put = "PUT"
var get = "GET"

type httphandler struct {
}

func (h *httphandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI
	id = strings.TrimPrefix(id, "/")
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		fmt.Print(string(v))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

	}
}

func main() {
	fmt.Println("new client")

	returnAddr := "http://127.0.0.1:1234"
	returnPort := 1234
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(returnPort),
		Handler: &httphandler{},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	reader := bufio.NewReader(os.Stdin)
	for {
		method, key, value, destAddr := askInput(reader)
		sendRequest(method, key, value, destAddr, returnAddr)
	}
}

func sendRequest(method, key, value, destAddr, returnAddr string) {
	if method == put {
		req, _ := http.NewRequest("PUT", destAddr+"/"+key+"/"+value, bytes.NewBufferString(returnAddr))
		client := &http.Client{}
		client.Do(req)
	} else {
		resp, err := http.Get(destAddr + "/" + key)
		if err != nil {
			log.Fatalf("could not GET the value", err)
		}
		v, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read on get (%v)\n", err)
			return
		}
		fmt.Println(key + " has value: " + string(v))

	}
}

func askInput(reader *bufio.Reader) (method, key, value, destAddr string) {
	method = Input(put, "Enter method")
	key = Inputf("my-key", "Please enter the key")
	if method == put {
		value = Inputf("value1", "Please enter the new value")
	} else {
		value = ""
	}
	destPort := Inputf("11380", "Please enter kv store port")
	destAddr = "http://127.0.0.1:" + destPort
	return
}

// Inputf takes a format string and arguments and calls
// Input.
func Inputf(def string, f string, args ...interface{}) string {
	return Input(def, fmt.Sprintf(f, args...))
}

// Input prints the arguments given with an 'input'-format and
// proposes the 'def' string as default. If the user presses
// 'enter', the 'dev' will be returned.
// In the case of an error it will Fatal.
func Input(def string, args ...interface{}) string {
	fmt.Fprint(out, args...)
	fmt.Fprintf(out, " [%s]: ", def)
	str, err := in.ReadString('\n')
	if err != nil {
		log.Fatal("Could not read input.")
	}
	str = strings.TrimSpace(str)
	if str == "" {
		return def
	}
	return str
}

func init() {
	in = bufio.NewReader(os.Stdin)
	out = os.Stdout
}
