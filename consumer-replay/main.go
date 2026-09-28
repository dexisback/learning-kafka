package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "replay-test",

		StartOffset: kafka.FirstOffset, //if last karte toh there would be no replaying of the entire history in our anohter consumer

	})
	defer reader.Close()

	fmt.Println("nahh....reader consumer started")
	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			panic(err)
		}
		//else:
		fmt.Printf("partition = %d offset = %d value = %s \n", message.Partition, message.Offset, string(message.Value))
		err = reader.CommitMessages(context.Background(), message)
		if err != nil {
			panic(err)
		}
	}
}
