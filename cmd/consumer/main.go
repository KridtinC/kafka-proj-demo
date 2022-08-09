package main

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/KridtinC/kafka-proj-demo/internal/config"
	"github.com/Shopify/sarama"
)

func main() {
	consumer, err := sarama.NewConsumer(config.Brokers, nil)
	if err != nil {
		panic(err)
	}

	subscribe("user-msg", consumer)
}

func subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Println("Error retrieving partitionList ", err)
		panic(err)
	}

	log.Println("total partition: ", len(partitionList))
	initialOffset := sarama.OffsetNewest

	var messageCountStart = 0
	var wg sync.WaitGroup

	for _, partition := range partitionList {
		wg.Add(1)
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			panic(err)
		}

		go func() {
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)
			go func() {
				for {
					select {
					case err := <-pc.Errors():
						log.Println(err)
					case msg := <-pc.Messages():
						messageCountStart++
						log.Println("Received messages", string(msg.Key), string(msg.Value))
					case <-signals:
						log.Println("Interrupt is detected")
						wg.Done()
					}
				}
			}()
		}()

	}

	wg.Wait()

	log.Println("Processed", messageCountStart, "messages")
}
