//The important change is: the API becomes the thing that creates OrderCreated events.
// The old CLI producer can stay around for load testing later or just stale legacy

package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/event"
)

type CreateOrderRequest struct {
	UserID   string `json:"user_id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

func main() {
	brokers := config.Get("KAFKA_BROKERS", "localhost:9092")
	topic := config.Get("KAFKA_TOPIC", "orders")
	addr := config.Get("API_ADDR", ":8080")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}

	defer writer.Close()

	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateOrderRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if req.UserID == "" || req.Item == "" || req.Quantity <= 0 {
			http.Error(w, "invalid order", http.StatusBadRequest)
			return
		}

		order := event.OrderCreated{
			EventID:   uuid.New().String(),
			OrderID:   uuid.New().String(),
			UserID:    req.UserID,
			Item:      req.Item,
			Quantity:  req.Quantity,
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(order)
		if err != nil {
			http.Error(w, "failed to create event", http.StatusInternalServerError)
			return
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(order.UserID),
			Value: data,
		})

		if err != nil {
			log.Printf("kafka publish failed: %v", err)
			http.Error(w, "failed to create order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)

		json.NewEncoder(w).Encode(order)
	})

	// log.Fatal(http.ListenAndServe(":8080", nil)) //replace this with graceful shutdown
	server := &http.Server{
		Addr: addr,
	}
	go func() {
		log.Printf("API listening on %s ✅", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}

	}()

	stop := make(chan os.Signal, 1) //creates a channel to receive messages from the operating system.

	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM) //signal.Notify tells Go to route specific OS signals into your stop channel.
	//syscall.SIGINT (Interrupt): Triggered when a user presses Ctrl+C in the terminal attached to the process.
	//syscall.SIGTERM (Terminate): The standard signal sent by container runtimes (like Docker via docker stop or Kubernetes) asking a process to shut down politely.
	//Note: SIGKILL or docker kill cannot be caught or handled; they terminate the app immediately).
	
	<-stop    //<-stop acts as a roadblock. The main program pauses on this exact line indefinitely until it catches a SIGINT or SIGTERM

	log.Println("shutting down API....")


	// /Once a signal arrives, the roadblock lifts. The code creates a 5-second timeout context and calls server.Shutdown(). This stops accepting new connections but gives existing connections up to 5 seconds to finish their work before the program finally exits.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error : %v", err)
	}

}
