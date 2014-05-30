package main

import (
	"github.com/bmizerany/pat"
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"time"
	"log"
	"os"
)

type Endpoint struct{
	store *Store
}

func StartEndpoint(port string, store *Store) *Endpoint {
	endpoint := Endpoint{store:store}
	mux := pat.New()
	mux.Post(fmt.Sprintf("/:topic"), http.HandlerFunc(endpoint.PostEvent))
	go http.ListenAndServe("127.0.0.1:"+port, mux)
	return &endpoint
}

func (e *Endpoint)PostEvent(w http.ResponseWriter, req *http.Request) {
	channel := req.URL.Query().Get(":topic")
	if channel == "" {
		w.WriteHeader(400)
		w.Write(NoChannel)
	} else {
		body, err := ioutil.ReadAll(req.Body);
		if err != nil {
			w.WriteHeader(500)
			w.Write(BodyErr)
		} else {
			saved := make(chan bool)
			event := EventIn{ event: &Event{Channel:channel, Body:body}, saved: saved }
		    e.sendEvent(&event)
			timeout := time.After(1 * time.Second)
			select {
			case ok := <-saved:
				if ok {
					w.WriteHeader(200)
				} else {
					w.WriteHeader(500)
					w.Write(SaveErr)
				}
			case <-timeout:
				w.WriteHeader(500)
				w.Write(SaveTimeout)
			}

		}
	}
}

func (e *Endpoint)sendEvent(event *EventIn) {
	defer func(){
		if r := recover(); r != nil {
			log.Println("at=recover-send-event-panic")
			//if we get here, we are shutting down, but the recover stops it, so exit
			os.Exit(2)
		}
	}()
	// the store owns the in channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
    e.send(event)
}

func (e *Endpoint)send(event *EventIn) {
	e.store.EventsInChannel() <- event
}

var NoChannel = Json(ErrJson{id:"no-channel", message:"no event channel specified"})
var BodyErr = Json(ErrJson{id:"read-error", message:"error while reading body"})
var SaveErr = Json(ErrJson{id:"save-error", message:"save event returned false"})
var SaveTimeout = Json(ErrJson{id:"save-timeout", message:"save timed out"})

func Json(err ErrJson) []byte {
	j, _ := json.Marshal(err)
	return j
}

type ErrJson struct {
	id      string
	message string
}
