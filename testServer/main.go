package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	rtr := mux.NewRouter()
	rtr.HandleFunc("/{event}", Event)

	fmt.Println(http.ListenAndServe(":8000", rtr))
}

func Event(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	body, _ := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	defer r.Body.Close()
	fmt.Printf("%s - %s\n", vars["event"], string(body))
}
