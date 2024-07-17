package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func createProducer() *kafka.Producer {
	hostname, _ := os.Hostname()
	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         hostname,
		"acks":              "all",
		"message.max.bytes": 10485760,
		"compression.type":  "gzip",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	return kp
}

func ProduceMessage(wg *sync.WaitGroup, data []byte) {
	defer wg.Done()
	p := createProducer()

	defer p.Close()

	var topic string = "mytopic"

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data},
		nil,
	)

	if err != nil {
		fmt.Println("failed in config producer to send msg", err.Error())
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
					} else {
						fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}

				}
			}
		}()

	}
}
