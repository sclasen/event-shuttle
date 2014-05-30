package main

import (
	"os"
	"os/signal"
	"syscall"
	"log"
	"flag"
	"strings"
     _ "net/http/pprof"

	"net/http"
)

func main() {

	exhibitor := flag.Bool("exhibitor", false, "use EXHIBITOR_URL from env to lookup seed brokers")
	brokers := flag.String("brokers", "", "comma seperated list of ip:port to use as seed brokers")
	db := flag.String("db", "events.db", "name of the boltdb database file")
	port := flag.String("port", "3887", "port on which to listen for events")
	debug := flag.Bool("debug", false, "start a pprof http server on 6060")

	flag.Parse()

	if *debug {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	exitChan := make(chan os.Signal)

	signal.Notify(exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	store, err := OpenStore(*db)
	if err != nil {
		log.Panicf("unable to open events.db, exiting! %v\n", err)
	}

	var brokerList []string

	if *exhibitor {
		rb, err := KafkaSeedBrokers(os.Getenv("EXHIBITOR_URL"), "kafka")
		if err != nil {
			log.Panicf("unable to get Kafka Seed Brokers, exiting! %v\n", err)
		}
		brokerList = rb
	} else {
		brokerList = strings.Split(*brokers, ",")
	}

	deliver, err := NewKafkaDeliver(store, "test", brokerList)
	if err != nil {
		log.Panicf("unable to create KafkaDeliver, exiting! %v\n", err)
	}

	deliver.Start()
	StartEndpoint(*port, store)

	select {
	case sig := <-exitChan:
		log.Printf("go=main at=received-signal signal=%s\n", sig)
		err := store.Close()
		deliver.Stop()
		if err != nil {
			log.Printf("go=main at=store-close-error error=%s\n", err)
			os.Exit(1)
		} else {
			log.Printf("go=main at=store-closed-cleanly \n")
		}
	}

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
