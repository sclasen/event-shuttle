package main

import (
	"github.com/Shopify/sarama"
	//"net"
	"time"
	"log"
)

type Deliver interface {
	Store() *Store
	Start() error
	Stop() error
}

type KafkaDeliver struct{
	store *Store
	client *sarama.Client
	producer *sarama.Producer
	deliverGoroutines int
	shutdownDeliver chan bool
	shutdown chan bool
}

func NewKafkaDeliver(store *Store, clientId string, brokerList []string) (*KafkaDeliver, error) {
	log.Println("go=kafka at=new-kafka-deliver")
	clientConfig := &sarama.ClientConfig{
		MetadataRetries:      10,
		WaitForElection:      10 * time.Second,
		ConcurrencyPerBroker: 10,
	}
	producerConfig := &sarama.ProducerConfig{
		Partitioner:      nil,
		RequiredAcks:     sarama.WaitForLocal,
		Timeout:          int32(5000),
		Compression:      sarama.CompressionNone,
		MaxBufferedBytes: uint32(1000),
		MaxBufferTime:    uint32(1000),
	}

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
		store: store,
		producer: producer,
		client: client,
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
			log.Printf("go=deliver num=%d at=event-out channel=%s", num, event.event.Channel)
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
			log.Println("recovered panic in ack")
		}
	}()
	// the store owns the ack channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	ack(store, seq)
}

func noAckEvent(store *Store, seq int64) {
	defer func(){
		if r := recover(); r != nil {
			log.Println("recovered panic in noAck")
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

