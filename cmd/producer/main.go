package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/seed"
	"github.com/segmentio/kafka-go"
)

func main() {
	count := flag.Int("count", 1000, "number of orders to produce")
	users := flag.Int("users", 100, "number of distinct users")
	flag.Parse()

	//checks:
	if *count <= 0 {
		log.Fatal("bruh count must be greater than 0")
	}
	if *users <= 0 {
		log.Fatal("bruh users must be greater than 0")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Get("KAFKA_BROKERS", "localhost:9092")),
		Topic:        config.Get("KAFKA_TOPIC", "orders"),
		Balancer:     &kafka.Hash{},         //this gives us actual batching while still flushing quickly
		BatchSize:    1,                     // or sized ≥ count
		BatchTimeout: 10 * time.Millisecond, // flush fast instead of waiting 10s
		Async:        false,                 // or keep sync but pass the whole slice. async me data was getting lost . reason -- With async writes, WriteMessages() can return before Kafka has acknowledged the messages. When your producer program exits, outstanding async writes may not all have completed.
	}

	defer writer.Close()

	//using the generator:
	generator := seed.NewGenerator(*users)
	start := time.Now()

	published := 0
	for i := 0; i < *count; i++ {
		order := generator.Order()

		data, err := json.Marshal(order)
		if err != nil {
			log.Fatal(err)
		}

		err = writer.WriteMessages(
			context.Background(), kafka.Message{
				Key:   []byte(order.UserID),
				Value: data,
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		published++
		if published%1000 == 0 {
			fmt.Printf("producer  %d/%d\n", published, *count)
		}
	}

	duration := time.Since(start)
	thruput := float64(published) / duration.Seconds() //yes i like writing like dat

	fmt.Printf("produced %d orders in %v\n", published, duration)
	fmt.Printf("throughput : %.2f orders/sec\n", thruput)
}
