package main

import (
	"github.com/sclasen/sarama"
	//"net"
	//"time"
	"log"
)

type Deliver interface {
	Store() *Store
	Start() error
	Stop() error
}

type KafkaDeliver struct{
	store *Store
	clientId string
	brokerList []string
	clientConfig *sarama.ClientConfig
	client *sarama.Client
	producerConfig *sarama.ProducerConfig
	producer *sarama.Producer
	deliverGoroutines int
	shutdownDeliver chan bool
	shutdown chan bool
}

func NewKafkaDeliver(store *Store, clientId string, brokerList []string) (*KafkaDeliver, error) {
	log.Println("go=kafka at=new-kafka-deliver")
	clientConfig := new(sarama.ClientConfig)
	producerConfig := new(sarama.ProducerConfig)

    client, err := sarama.NewClient(clientId, brokerList, clientConfig)
	if err != nil {
		return nil, err
	}
	log.Println("go=kafka at=created-client")

	producer, err := sarama.NewProducer(client, producerConfig)
	if err != nil {
		return nil, err
	}
	log.Println("go=kafka at=created-producer")

	return &KafkaDeliver{
		clientId: clientId,
		brokerList: brokerList,
		store: store,
		producer: producer,
		producerConfig: producerConfig,
		client: client,
		clientConfig: clientConfig,
		deliverGoroutines: 8,
		shutdownDeliver: make(chan bool, 8),
		shutdown: make(chan bool, 8),
	}, nil

}

func (k *KafkaDeliver) Store() *Store {
	return k.store
}

func (k *KafkaDeliver) Start() error {
	for i := 0; i < k.deliverGoroutines; i++ {
		go k.deliverEvents(i)
	}
	return nil
}

func (k *KafkaDeliver) deliverEvents(num int) {
	for{
		select {
		case <- k.shutdownDeliver:
			k.shutdown <- true
			return
		case event, ok := <-k.store.eventsOut:
			if ok {
				err := k.producer.SendMessage(event.event.Channel, nil, sarama.ByteEncoder(event.event.Body))
				if err != nil {
					log.Printf("go=deliver num=%d at=send-error error=%v", num, err)
					noAckEvent(k.store, event.sequence)
				} else {
					ackEvent(k.store, event.sequence)
				}
			}
		}
	}
}

func ackEvent(store *Store, seq int64) {
	defer func(){
		if r := recover(); r != nil {
			log.Println("at=recover-ack-panic")
		}
	}()
	// the store owns the ack channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	ack(store, seq)
}

func noAckEvent(store *Store, seq int64) {
	defer func(){
		if r := recover(); r != nil {
			log.Println("at=recover-noack-panic")
		}
	}()
	// the store owns the noAck channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	noAck(store, seq)
}

func ack(store *Store, seq int64) {
	store.eventsDelivered <- seq
}

func noAck(store *Store, seq int64) {
	store.eventsFailed <- seq
}

func (k *KafkaDeliver) Stop() error {
	for i := 0; i < k.deliverGoroutines; i++ {
		k.shutdownDeliver <- true
	}
	for i := 0; i < k.deliverGoroutines; i++ {
		<-k.shutdown
	}
	return nil
}

