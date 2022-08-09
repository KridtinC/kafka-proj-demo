package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"

	"github.com/KridtinC/kafka-proj-demo/internal/config"
	"github.com/Shopify/sarama"
)

type msg struct {
	Msg string `json:"msg"`
}

func main() {

	p, err := newProducer()
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/msg", func(w http.ResponseWriter, r *http.Request) {
		val := r.URL.Query().Get("value")
		resp, err := json.Marshal(&msg{Msg: val})
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		}

		var partitionNo = int32(rand.Intn(3))
		log.Println("partition: ", partitionNo)
		log.Println(p.SendMessage(&sarama.ProducerMessage{
			Topic:     "user-msg",
			Partition: partitionNo,
			Value:     sarama.StringEncoder(fmt.Sprint(val, ": ", partitionNo)),
		}))

		w.Write(resp)
	})
	log.Println("serve at port 8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		panic(err)
	}
}

func newProducer() (sarama.SyncProducer, error) {
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(config.Brokers, conf)

	return producer, err
}
