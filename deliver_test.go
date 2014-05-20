package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/Shopify/sarama"
	"fmt"
	_ "net/http/pprof"
	"net/http"
	"log"
	"time"
	"net"
)

const localKafka = "localhost:9092"

func kafkaIsUp() bool {
	conn, err := net.Dial("tcp", localKafka)
	if err != nil {
		log.Printf("Kakfa does not appear to be up on %s, skipping this test", localKafka)
		return false
	} else {
		conn.Close()
		return true
	}
}

func TestKafkaConfig(t *testing.T) {
	if kafkaIsUp() {
		d, err := NewKafkaDeliver(nil, "testClientId", []string{localKafka})
		assert.Nil(t, err, fmt.Sprintf("%v+", err))
		for i := 0; i < 10000; i++ {
			err = d.producer.SendMessage("test", nil, sarama.StringEncoder("hello world"))
			assert.Nil(t, err, fmt.Sprintf("%v+", err))
		}
	}
}

func TestNewKafkaDeliver(t *testing.T) {
	if kafkaIsUp() {
		store, err := OpenStore("test.db")
		assert.Nil(t, err, err)
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
		d, err := NewKafkaDeliver(store, "testClientId", []string{localKafka})
		d.Start()
		ack := make(chan bool)
		d.store.EventsInChannel() <- &EventIn{ saved: ack, event: &Event{Channel:"test", Body:[]byte("{}"), } }
		acked := <-ack
		assert.True(t, acked, "not acked")
		time.Sleep(time.Second * 5)
		d.Stop()
		d.Store().Close()
	}
}
