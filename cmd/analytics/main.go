package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/event"
)

func main() {
	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.Get("KAFKA_BROKERS", "localhost:9092")},
		Topic:   config.Get("KAFKA_TOPIC", "orders"),
		// Distinct group so analytics receives its own copy of every event.
		GroupID: "analytics-processor",
		// Applies ONLY to partitions with no committed offset (fresh group or
		// clean test run). Groups with committed offsets always resume there.
		StartOffset: config.StartOffset(),
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	defer reader.Close()

	var totalOrders int

	fmt.Println("analytics service started")

	for {
		message, err := reader.FetchMessage(shutdownCtx)
		if err != nil {
			if shutdownCtx.Err() != nil {
				log.Println("shutdown signal received, stopping analytics service")
				break
			}
			log.Printf("fetch failed: %v", err)
			continue
		}

		var order event.OrderCreated

		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("invalid message: %v", err)
			continue
		}

		totalOrders++

		fmt.Printf(
			"analytics: order=%s user=%s total=%d\n",
			order.OrderID,
			order.UserID,
			totalOrders,
		)

		if err := reader.CommitMessages(context.Background(), message); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
