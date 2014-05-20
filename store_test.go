package main

import "github.com/bmizerany/assert"
import _ "net/http/pprof"


import (
	"testing"
	"log"
	_ "regexp" //makes things work with go -race

	"net/http"
	"fmt"
)


func TestSerialize(t *testing.T) {
	for i := 1; i < 100000; i++ {
		read, err := readSequence(writeSequence(int64(i)))
		assert.T(t, err == nil, err)
		assert.T(t, read == int64(i))
	}
}

func TestEncodeDecodeEvent(t *testing.T){
	event := &Event{Channel: "test123", Body: []byte("test123")}
	encoded, err := encodeEvent(event)
	assert.T(t, err == nil, err)
    decoded, err := decodeEvent(encoded)
	assert.T(t, err == nil, err)
	assert.T(t, event.Channel == decoded.Channel, fmt.Sprintf("%v+ %v+", event, decoded))
	assert.T(t, string(event.Body) == string(decoded.Body), fmt.Sprintf("%v+ %v+", event, decoded))
}


func TestOpenCloseStore(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	for i := 0; i < 100; i++ {
		store, err := OpenStore("test.db")
		assert.T(t, err == nil, err)
		init := store.EventsSequence()
		acks := make(chan bool)
		for i := 0; i < 100; i++ {
			store.EventsInChannel() <- &EventIn{ event: &Event{ Channel:"test", Body:[]byte("BODY") }, saved:acks}
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


func BenchmarkEvents(b *testing.B) {
	store, _ := OpenStore("test.db")
	acks := make(chan bool)
	b.ResetTimer()
	for i := 0; i < b.N ; i++ {
		store.EventsInChannel() <- &EventIn{ event: &Event {Channel:"test", Body:[]byte("BODY")}, saved:acks}
		<-acks
	}
	b.StopTimer()
	log.Println("CLOSING")
	store.Close()
}


