package main

import (
	"os"
	"fmt"
	"os/signal"
	"syscall"
	"log"
	"flag"
	"strings"
)

func main() {

	ringmaster := flag.Bool("ringmaster", false, "use RINGMASTER_URL from env to lookup seed brokers")
	brokers := flag.String("brokers", "", "comma seperated list of ip:port to use as seed brokers")
	db := flag.String("db", "events.db", "name of the boltdb database file")

	flag.Parse()

	exitChan := make(chan os.Signal)

	signal.Notify(exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	store, err := OpenStore(*db)
	if err != nil {
		log.Panicf("unable to open events.db, exiting! %v", err)
	}

	var brokerList []string

	if *ringmaster {
		rb, err := KafkaSeedBrokers(os.Getenv("RINGMASTER_URL"), "kafka")
		if err != nil {
			log.Panicf("unable to get Kafka Seed Brokers, exiting! %v", err)
		}
		brokerList = rb
	} else {
		brokerList = strings.Split(*brokers, ",")
	}

	deliver, err := NewKafkaDeliver(store, "test", brokerList)
	if err != nil {
		log.Panicf("unable to create KafkaDeliver, exiting! %v", err)
	}

	deliver.Start()
	StartEndpoint("3887", store)

	sig := <-exitChan
	fmt.Println(sig)

	store.Close()
	deliver.Stop()

}

type EventIn struct {
	event *Event
	saved chan bool
}

type EventOut struct {
	event *Event
	sequence int64
}

type Event struct{
	Channel string
	Body    []byte
}
