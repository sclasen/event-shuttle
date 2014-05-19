package main

import "github.com/bmizerany/assert"
import _ "net/http/pprof"


import (
	"testing"
	"log"
	_ "regexp" //makes things work with go -race

	"net/http"
)

func TestSerialize(t *testing.T) {
	for i := 1; i < 100000; i++ {
		read, err := readSequence(writeSequence(int64(i)))
		assert.T(t, err == nil, err)
		assert.T(t, read == int64(i))
	}
}


func TestOpenCloseStore(t *testing.T) {
	for i := 0; i < 100; i++ {
		store, err := OpenStore("test.db")
		assert.T(t, err == nil, err)
		init := store.EventsSequence()
		acks := make(chan bool)
		for i := 0; i < 100; i++ {
			store.EventsInChannel() <- &EventIn{channel:"test", body:[]byte("BODY"), saved:acks}
			<-acks
		}
		curr := store.EventsSequence()
		assert.Equal(t, init+100, curr)
		log.Println("CLOSING")
		store.Close()

		store, err = OpenStore("test.db")
		assert.T(t, err == nil, err)
		init = store.EventsSequence()
		assert.Equal(t, init, curr)
		store.Close()
	}
}


func TestLotsOfEvents(t *testing.T) {

	go func() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}   ()
	store, err := OpenStore("test.db")
	assert.T(t, err == nil, err)
	init := store.EventsSequence()
	acks := make(chan bool)
	for i := 0; i < 100000 ; i++ {
		store.EventsInChannel() <- &EventIn{channel:"test", body:[]byte("BODY"), saved:acks}
		<-acks
	}
	curr := store.EventsSequence()
	assert.Equal(t, init+100000, curr)
	log.Println("CLOSING")
	store.Close()

	store, err = OpenStore("test.db")
	assert.T(t, err == nil, err)
	init = store.EventsSequence()
	assert.Equal(t, init, curr)
	store.Close()

}


