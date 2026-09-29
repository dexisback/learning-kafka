package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dexisback/learning-kafka/internal/event"
	"github.com/segmentio/kafka-go"
)

func main() {
	// reader := kafka.NewReader(kafka.ReaderConfig{
	// 	Brokers: []string{"localhost:9092"},
	// 	Topic:   "orders",
	// 	GroupID: "order-processors",
	// })
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5432/orders")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)
	//Add: verify connection:
	if err := conn.Ping(ctx); err != nil {
		log.Fatal("connection error with postgres", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		// GroupID:     "order-processors",
		GroupID:     "performance-test",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	defer reader.Close()

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		host, _ := os.Hostname()
		workerID = fmt.Sprintf("%s-pid%d", host, os.Getpid()) // unique per process
	}

	fmt.Println("rrahhhhh....starting order worker")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			// transient errors (rebalances, broker blips) must not kill the worker
			log.Printf("fetch failed: %v", err)
			continue
		}

		var order event.OrderCreated
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("skipping undecodable message (partition=%d offset=%d): %v", message.Partition, message.Offset, err)
			// commit the poison message explicitly so it is not redelivered forever
			if commitErr := reader.CommitMessages(ctx, message); commitErr != nil {
				log.Printf("commit failed: %v", commitErr)
			}
			continue
		}
		fmt.Printf(
			"order-worker=%s 🙏 started: processing order=%s user=%s partition=%d offset=%d\n",
			workerID,
			order.OrderID,
			order.UserID,
			message.Partition,
			message.Offset,
		)

		// ADD: persist the Kafka event into PostgreSQL
		// a failed insert must NEVER be skipped: a later commit would leapfrog
		// this offset forever and the row would be lost. retry a few times,
		// then stop WITHOUT committing so the message is redelivered on restart
		inserted := false
		for attempt := 1; attempt <= 5; attempt++ {
			_, err = conn.Exec(
				ctx,
				`INSERT INTO orders
        (event_id, order_id, user_id, item, quantity, created_at)
     VALUES ($1, $2, $3, $4, $5, $6)
	  ON CONFLICT (event_id) DO NOTHING`, //for idempotency
				order.EventID,
				order.OrderID,
				order.UserID,
				order.Item,
				order.Quantity,
				order.CreatedAt,
			)
			if err == nil {
				inserted = true
				break
			}
			log.Printf("insert attempt %d/5 failed: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		if !inserted {
			log.Fatal("database unreachable, stopping without commit so this message is redelivered: ", err)
		}

		// KEEP: only commit Kafka AFTER the DB write succeeds
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("commit failed: %v", err)
			continue
		}

		fmt.Printf(
			"worker=%s completed order=%s\n",
			workerID,
			order.OrderID,
		)
	}

}
