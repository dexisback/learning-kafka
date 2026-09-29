package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/event"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func main() {
	count := flag.Int("count", 1000, "number of orders to produce")
	users := flag.Int("users", 100, "number of distinct users")
	flag.Parse()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Get("KAFKA_BROKERS", "localhost:9092")),
		Topic:        config.Get("KAFKA_TOPIC", "orders"),
		Balancer:     &kafka.Hash{},         //this gives us actual batching while still flushing quickly
		BatchSize:    1,                     // or sized ≥ count
		BatchTimeout: 10 * time.Millisecond, // flush fast instead of waiting 10s
		Async:        false,                 // or keep sync but pass the whole slice. async me data was getting lost . reason -- With async writes, WriteMessages() can return before Kafka has acknowledged the messages. When your producer program exits, outstanding async writes may not all have completed.

	}

	defer writer.Close()

	items := []string{
		"laptop",
		"keyboard",
		"mouse",
		"monitor",
		"headphones",
		"phone",
		"tablet",
		"charger",
		"backpack",
		"camera",
	}

	start := time.Now()

	for i := 0; i < *count; i++ {
		userID := fmt.Sprintf("user-%d", rand.Intn(*users)) //random userID

		order := event.OrderCreated{
			EventID:   uuid.New().String(),
			OrderID:   fmt.Sprintf("order-%d", i+1),
			UserID:    userID,
			Item:      items[rand.Intn(len(items))],
			Quantity:  rand.Intn(5) + 1,
			CreatedAt: time.Now(),
		} //create order

		data, err := json.Marshal(order)
		if err != nil {
			log.Fatal(err)
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(order.UserID),
			Value: data,
		})

		if err != nil {
			log.Fatal(err)
		}

		if (i+1)%1000 == 0 {
			fmt.Printf("produced %d/%d\n", i+1, *count)
		}
	}
	fmt.Printf("produced %d orders in %v\n",
		*count,
		time.Since(start))
}
