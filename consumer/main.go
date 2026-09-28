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
		message, err := reader.FetchMessage(context.Background())  //FetchMessage means give me the next message, instead of just the ReadMessage architecture we're implementing FetchMessage + CommitMessage architecture (so they can be saved so it resumes from where it ended )
		
		if err != nil {
			panic(err)
		}

		fmt.Printf("partition=%d , received msg: offset = %d value=%s\n", message.Partition, message.Offset, string(message.Value))

		err = reader.CommitMessages(context.Background(), message)  //means i've succesfully processes this message, now remember my progress

		if err != nil {panic(err)}
	}
}
