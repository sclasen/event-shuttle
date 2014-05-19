package main

import (
	//"github.com/Shopify/sarama"
	//"net"
)

type Deliver interface {
	Store() *Store
	Start() error
	// the Deliver instance owns sending on Store().eventsDelivered and Store().eventsFailed and should close them on stop.
	Stop() error
}


type Kafka struct{
	store *Store
	//the tcpmode elb for kafka, used to bootstrap producers without ZK

}

func (k *Kafka) Store() *Store {
	return k.store
}

func (k *Kafka) Start() error {

	return nil
}

func (k *Kafka) Stop() error {
	return nil
}

