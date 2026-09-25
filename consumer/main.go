package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

//we'll have a reader

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "order-tracker",
	})

	defer reader.Close()



	fmt.Println("consumer started rahhh....")
	//groupID order-track is equivalent of Group.id = order-tracker

	//now continously read:
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil{
			panic(err)
		}

		fmt.Println("received msg: offset = %d value=%s\n", message.Offset, string(message.Value))
	}
}
