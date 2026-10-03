package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/event"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	shutdownCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := pgx.Connect(ctx, config.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)
	//Add: verify connection:
	if err := conn.Ping(ctx); err != nil {
		log.Fatal("connection error with postgres", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.Get("KAFKA_BROKERS", "localhost:9092")},
		Topic:   config.Get("KAFKA_TOPIC", "orders"),
		// GroupID: "order-processors",
		GroupID: "orders-processor",
		// Applies ONLY to partitions with no committed offset (fresh group or
		// clean test run). Groups with committed offsets always resume there.
		StartOffset: config.StartOffset(),
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	defer reader.Close()

	workerID := config.Get("WORKER_ID", "")
	if workerID == "" {
		host, _ := os.Hostname()
		workerID = fmt.Sprintf("%s-pid%d", host, os.Getpid())
	}

	fmt.Println("rrahhhhh....starting order worker")

	for {
		message, err := reader.FetchMessage(shutdownCtx)
		if err != nil {
			if shutdownCtx.Err() != nil {
				log.Println("shutdown signal received, stopping worker")
				break
			}
			log.Printf("fetch failed: %v", err)
			continue
		}

		var order event.OrderCreated
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("skipping undecodable message (partition=%d offset=%d): %v", message.Partition, message.Offset, err)
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
