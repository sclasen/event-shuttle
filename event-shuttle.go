package main

import (
	"github.com/boltdb/bolt"
	"os"
	"fmt"
	"encoding/binary"
	"os/signal"
	"syscall"
)
//import "github.com/Shopify/sarama"

func main() {

	//brokerList := "the elb for megaphone, points at 9998, used as seed broker, can brokers not on a topic find those that are?"
    //add megaphone tcp elb to ELB.yml. can elbs be restricted to sgs?
	//memory map file to ring buffer
	//broker := sarama.NewBroker(brokerList)
	//start http server on unix socket, listen POST /:topic {event}
	//start goroutine writing the buffer
	//start gorouting reading the buffer
	//http send (event e, chan<- ack) to writer on channel
    /*
On-disk format of a message

message end offset: 8 bytes
topic          : 4 bytes
crc            : 4 bytes
payload        : n bytes
    */
	db, err := bolt.Open("events.db", os.ModeExclusive | 0700)
	if err != nil {
		panic(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	events, err := tx.CreateBucketIfNotExists([]byte("events"))
	if err != nil {
		panic(err)
	}
	pointers, err := tx.CreateBucketIfNotExists([]byte("pointers"))
	if err != nil {
		panic(err)
	}

	buffer := make([]byte, 8)
	binary.PutVarint(buffer, 1)
	err = events.Put(buffer, []byte("{}"))
	if err != nil {
		panic(err)
	}
	err = pointers.Put([]byte("max"), buffer)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	tx, err = db.Begin(true)
	if err != nil {
		panic(err)
	}
	events = tx.Bucket([]byte("events"))
	if err != nil {
		panic(err)
	}
	value := events.Get(buffer)
	fmt.Println(string(value))
	tx.Commit()

	exitChan := make(chan os.Signal)

	signal.Notify(exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <- exitChan
	fmt.Println(sig)
	db.Close()



}


type EventIn struct {
	channel string
	body []byte
	saved chan bool
}

type EventOut struct {
	channel string
	body []byte
	sequence int64
}
