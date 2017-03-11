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

type httphandler struct {
}

func (h *httphandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		fmt.Println(id, string(v))
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
		key, value, addr := askInput(reader)
		req, _ := http.NewRequest("PUT", addr+"/"+key+"/"+value, bytes.NewBufferString(returnAddr))
		client := &http.Client{}
		client.Do(req)
	}
}

func askInput(reader *bufio.Reader) (key, value, addr string) {
	key = Inputf("my-key", "Please enter the key")
	value = Inputf("value1", "Please enter the new value")
	port := Inputf("12380", "Please enter kv store port")
	addr = "http://127.0.0.1:" + port
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
