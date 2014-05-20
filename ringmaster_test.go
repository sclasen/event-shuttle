package main

import (
    "github.com/bmizerany/assert"
	"testing"
	"log"
	"os"
)

func TestRingmaster(t *testing.T) {
	url := os.Getenv("RINGMASTER_URL")
	if url == "" {
		log.Println("RINGMASTER_URL not set, skipping TestRingmaster")
		return
	} else {
		brokers, err := KafkaSeedBrokers(url, "kafka")
		assert.T(t, err == nil, err)
		log.Printf("brokers: %s",brokers)
	}
}

