package main

import (
	"net/http"
	"crypto/tls"
	"fmt"
	"encoding/json"
	"errors"
	"strings"
)

func KafkaSeedBrokers(ringmasterUrl, kafkaChroot string) (string, error) {
	tr := http.DefaultTransport.(*http.Transport)
	tr.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	client := &http.Client{Transport: tr}
	listUrl := fmt.Sprintf("%s/exhibitor/v1/explorer/node/?key=/%s/%s", ringmasterUrl, kafkaChroot, "brokers/ids")

	res, err := client.Get(listUrl)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", errors.New(fmt.Sprintf("non 200 from get broker list: %d %s", res.StatusCode, res.Body))
	}

	var dir []ZkDirEntry
	err = json.NewDecoder(res.Body).Decode(&dir)
	if err != nil {
		return "", err
	}

    var addresses []string
	for _, broker := range dir {
		address, err := brokerAddress(client, ringmasterUrl, broker.Key)
		if err != nil {
			return  "", err
		}
		fmt.Println(address)
		addresses = append(addresses, address + "/" + kafkaChroot)
	}

	return strings.Join(addresses, ","), nil

}

func brokerAddress(client *http.Client, ringmasterUrl, zkPath string) (string, error) {
	seedUrl := fmt.Sprintf("%s/exhibitor/v1/explorer/node-data/?key=%s", ringmasterUrl,  zkPath)
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
