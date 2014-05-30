package main

import (
    "github.com/bmizerany/assert"
	"testing"
	"log"
	"os"
)

func TestExhibitor(t *testing.T) {
	url := os.Getenv("EXHIBITOR_URL")
	if url == "" {
		log.Println("EXHIBITOR_URL not set, skipping TestExhibitor")
		return
	} else {
		brokers, err := KafkaSeedBrokers(url, "kafka")
		assert.T(t, err == nil, err)
		log.Printf("brokers: %s",brokers)
	}
}

