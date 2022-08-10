package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/KridtinC/kafka-proj-demo/internal/config"
	"github.com/Shopify/sarama"
)

func main() {

	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	cg, err := sarama.NewConsumerGroup(config.Brokers, "test-cgroup", nil)
	if err != nil {
		panic(err)
	}
	subscribeGroup("user-msg", cg)

	// consumer, err := sarama.NewConsumer(config.Brokers, nil)
	// if err != nil {
	// 	panic(err)
	// }
	// subscribe("user-msg", consumer)
}

func subscribeGroup(topic string, cg sarama.ConsumerGroup) {

	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())

	keepRunning := true
	consumptionIsPaused := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := cg.Consume(ctx, strings.Split(topic, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(cg, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err := cg.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Message claimed: value = %s, topic = %s, partition = %d", string(message.Value), message.Topic, message.Partition)
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
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
			for {
				select {
				case err := <-pc.Errors():
					log.Println(err)
				case msg := <-pc.Messages():
					messageCountStart++
					log.Println("Received messages", string(msg.Key), string(msg.Value))
				case <-signals:
					log.Println("Interrupted")
					wg.Done()
				}
			}
		}()

	}

	wg.Wait()

	log.Println("Processed", messageCountStart, "messages")
}
