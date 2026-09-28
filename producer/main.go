package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Order struct {
	OrderID  string `json:"order_id"`
	User     string `json:"user"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

func main() {

	//create an order
	orders := []Order{{
		OrderID:  "order-1",
		User:     "Amaandagoat",
		Item:     "Bhindi",
		Quantity: 2,
	},
		{
			OrderID:  "order-2",
			User:     "Amaandaextragoat",
			Item:     "Barbatti",
			Quantity: 1,
		},
		{
			OrderID:  "order-3",
			User:     "AmaanDaGreatest",
			Item:     "Gobhi",
			Quantity: 3,
		},
		{
			OrderID:  "order-4",
			User:     "Bhakti",
			Item:     "pasta",
			Quantity: 2,
		}, //adding multiple orders
		//adding some more users, with the same name, so that we can test out newly added o.User for routing instead of 
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	//making this in a loop cuh multiple orders
	for _, o := range orders {
		data, err := json.Marshal(o)
		if err != nil {
			panic(err)
		}
		err = writer.WriteMessages(
			context.Background(),
			kafka.Message{
				Key:   []byte(o.User), //changed from []byte(o.OrderID) to o.User. since we have multiple users now
				Value: data,
			},
		)

		if err != nil {
			panic(err)
		}
		fmt.Printf("rahh.. order succesfully produced: %s\n", string(data))

	}

}
