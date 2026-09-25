package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)


type Order struct {
	OrderID    string `json:"order_id"`
	User      string    `json:"user"`
	Item    string    `json:"item"`
	Quantity   int 		`json:"quantity"`
}


func main(){

	//create an order
	order := Order{
		OrderID: "alpha-beta-gamma-123",
		User: "Amaandagoat",
		Item: "Bhindi",
		Quantity: 2,
	}


	data, err := json.Marshal(order)
	if  err!= nil{
		panic(err)
	}
	fmt.Println(string(data)) //print the thing we are producing 


	writer := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
		Topic: "orders",
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()


	//writing our order:
	err = writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key: []byte(order.OrderID),
			Value: data,
		},
	)
	if err != nil{
		panic(err)
	}

	fmt.Println("order succesfully produced")
}


