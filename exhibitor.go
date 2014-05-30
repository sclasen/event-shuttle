package main

import (
	"net/http"
	"crypto/tls"
	"fmt"
	"encoding/json"
	"errors"
)

func KafkaSeedBrokers(exhibitorUrl, kafkaChroot string) ([]string, error) {
	tr := http.DefaultTransport.(*http.Transport)
	tr.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	client := &http.Client{Transport: tr}
	listUrl := fmt.Sprintf("%s/exhibitor/v1/explorer/node/?key=/%s/%s", exhibitorUrl, kafkaChroot, "brokers/ids")

	res, err := client.Get(listUrl)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("non 200 from get broker list: %d %s", res.StatusCode, res.Body))
	}

	var dir []ZkDirEntry
	err = json.NewDecoder(res.Body).Decode(&dir)
	if err != nil {
		return nil, err
	}

    var addresses []string
	for _, broker := range dir {
		address, err := brokerAddress(client, exhibitorUrl, broker.Key)
		if err != nil {
			return  nil, err
		}
		fmt.Println(address)
		addresses = append(addresses, address + "/" + kafkaChroot)
	}

	return addresses, nil

}

func brokerAddress(client *http.Client, exhibitorUrl, zkPath string) (string, error) {
	seedUrl := fmt.Sprintf("%s/exhibitor/v1/explorer/node-data/?key=%s", exhibitorUrl,  zkPath)
	res2, err := client.Get(seedUrl)
	if err != nil {
		return "", err
	}
	if res2.StatusCode != 200 {
		return "", errors.New("non 200 from get broker info")
	}

	brokerNode := &ZkNodeEntry{}
	err = json.NewDecoder(res2.Body).Decode(brokerNode)
	if err != nil {
		return "", err
	}

	seedBrokerJson := brokerNode.Str
	seedBroker := &ZkBrokerEntry{}

	err = json.Unmarshal([]byte(seedBrokerJson), seedBroker)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", seedBroker.Host, seedBroker.Port), nil
}

type ZkDirEntry struct{
	Title string `json:"title"`
	Key   string `json:"key"`
}

type ZkNodeEntry struct {
	Str string `json:"str"`
}

type ZkBrokerEntry struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}
