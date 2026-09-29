//The important change is: the API becomes the thing that creates OrderCreated events.
// The old CLI producer can stay around for load testing later or just stale legacy
//
// The API now also serves read endpoints that power the dashboard:
//
//	POST /orders   publish an OrderCreated event
//	GET  /orders   last 50 orders persisted by the order-worker
//	GET /stats     row/event counters
//	GET /events    the most recent events still retained in the Kafka log
//	GET /health    kafka + postgres liveness

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/dexisback/learning-kafka/internal/config"
	"github.com/dexisback/learning-kafka/internal/event"
)

type CreateOrderRequest struct {
	UserID   string `json:"user_id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

type orderRow struct {
	EventID   string    `json:"event_id"`
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Item      string    `json:"item"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

// eventView is an OrderCreated plus the Kafka coordinates it came from.
type eventView struct {
	event.OrderCreated
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type statsResponse struct {
	OrdersInDB      int64  `json:"orders_in_db"`
	EventsPublished uint64 `json:"events_published"` // by this API process, since start
	TopicEvents     int64  `json:"topic_events"`     // events currently retained in the topic
	Partitions      int    `json:"partitions"`
}

type healthResponse struct {
	Status     string `json:"status"`
	Kafka      string `json:"kafka"`
	Postgres   string `json:"postgres"`
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
}

type app struct {
	writer    *kafka.Writer
	pool      *pgxpool.Pool
	client    *kafka.Client
	brokers   string
	topic     string
	published atomic.Uint64
}

func main() {
	brokers := config.Get("KAFKA_BROKERS", "localhost:9092")
	topic := config.Get("KAFKA_TOPIC", "orders")
	addr := config.Get("API_ADDR", ":8080")

	pool, err := pgxpool.New(context.Background(), config.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders"))
	if err != nil {
		log.Fatal("postgres connect: ", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatal("postgres ping: ", err)
	}

	a := &app{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
		pool:    pool,
		client:  &kafka.Client{Addr: kafka.TCP(brokers), Timeout: 3 * time.Second},
		brokers: brokers,
		topic:   topic,
	}
	defer a.writer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders", a.listOrders)
	mux.HandleFunc("GET /stats", a.stats)
	mux.HandleFunc("GET /events", a.recentEvents)
	mux.HandleFunc("GET /health", a.health)

	// log.Fatal(http.ListenAndServe(":8080", nil)) //replace this with graceful shutdown
	server := &http.Server{
		Addr:    addr,
		Handler: withCORS(mux),
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

	<-stop //<->stop acts as a roadblock. The main program pauses on this exact line indefinitely until it catches a SIGINT or SIGTERM

	log.Println("shutting down API....")

	// /Once a signal arrives, the roadblock lifts. The code creates a 5-second timeout context and calls server.Shutdown(). This stops accepting new connections but gives existing connections up to 5 seconds to finish their work before the program finally exits.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error : %v", err)
	}
}

// withCORS lets the static dashboard (served on another origin/port) call this API.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
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

	if err := a.writer.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(order.UserID),
		Value: data,
	}); err != nil {
		log.Printf("kafka publish failed: %v", err)
		http.Error(w, "failed to create order", http.StatusInternalServerError)
		return
	}
	a.published.Add(1)

	writeJSON(w, http.StatusCreated, order)
}

func (a *app) listOrders(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	rows, err := a.pool.Query(ctx,
		`SELECT event_id, order_id, user_id, item, quantity, created_at
		   FROM orders ORDER BY created_at DESC LIMIT 50`)
	if err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	defer rows.Close()

	orders := make([]orderRow, 0, 50)
	for rows.Next() {
		var o orderRow
		if err := rows.Scan(&o.EventID, &o.OrderID, &o.UserID, &o.Item, &o.Quantity, &o.CreatedAt); err != nil {
			http.Error(w, "database read failed", http.StatusInternalServerError)
			return
		}
		orders = append(orders, o)
	}

	writeJSON(w, http.StatusOK, orders)
}

// topicOverview reads cluster metadata for the topic and, per partition, the
// retained event count. When perPartition > 0 it also seeks near the end of
// every partition and returns the most recent events (the log is still there,
// so this works without any extra storage).
func (a *app) topicOverview(ctx context.Context, perPartition int) (events []eventView, partitions int, total int64, err error) {
	meta, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{a.topic}})
	if err != nil {
		return nil, 0, 0, err
	}

	events = []eventView{}
	for _, t := range meta.Topics {
		if t.Name != a.topic {
			continue
		}
		if t.Error != nil {
			return nil, 0, 0, t.Error
		}
		partitions = len(t.Partitions)
		for _, p := range t.Partitions {
			conn, derr := kafka.DialLeader(ctx, "tcp", a.brokers, a.topic, p.ID)
			if derr != nil {
				continue
			}
			last, lerr := conn.ReadLastOffset()
			if lerr != nil {
				conn.Close()
				continue
			}
			total += last
			if perPartition > 0 {
				events = append(events, a.readTail(conn, p.ID, last, perPartition)...)
			}
			conn.Close()
		}
	}
	if partitions == 0 {
		return nil, 0, 0, errors.New("topic not found")
	}
	sort.Slice(events, func(i, j int) bool { return events[i].CreatedAt.After(events[j].CreatedAt) })
	return events, partitions, total, nil
}

// readTail reads the last n messages of one partition (or fewer if the
// partition is young) and stops as soon as it reaches the log end, so the
// request stays fast.
func (a *app) readTail(conn *kafka.Conn, partition int, last int64, n int) []eventView {
	defer conn.Close()
	if last <= 0 {
		return nil
	}
	first, err := conn.ReadFirstOffset()
	if err != nil {
		return nil
	}
	start := last - int64(n)
	if start < first {
		start = first
	}
	if _, err := conn.Seek(start, kafka.SeekAbsolute); err != nil {
		return nil
	}
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

	out := []eventView{}
	for {
		m, err := conn.ReadMessage(1 << 20)
		if err != nil {
			break
		}
		var o event.OrderCreated
		if json.Unmarshal(m.Value, &o) != nil {
			continue
		}
		out = append(out, eventView{OrderCreated: o, Partition: partition, Offset: m.Offset})
		if m.Offset >= last-1 {
			break
		}
	}
	return out
}

func (a *app) stats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var ordersInDB int64
	if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&ordersInDB); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}

	_, partitions, total, err := a.topicOverview(ctx, 0)
	if err != nil {
		http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, http.StatusOK, statsResponse{
		OrdersInDB:      ordersInDB,
		EventsPublished: a.published.Load(),
		TopicEvents:     total,
		Partitions:      partitions,
	})
}

func (a *app) recentEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	events, _, _, err := a.topicOverview(ctx, 4)
	if err != nil {
		http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	h := healthResponse{Status: "ok", Kafka: "ok", Postgres: "ok", Topic: a.topic}

	meta, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{a.topic}})
	if err != nil {
		h.Kafka, h.Status = "unreachable", "degraded"
	} else {
		for _, t := range meta.Topics {
			if t.Name == a.topic {
				h.Partitions = len(t.Partitions)
			}
		}
	}

	if err := a.pool.Ping(ctx); err != nil {
		h.Postgres, h.Status = "unreachable", "degraded"
	}

	writeJSON(w, http.StatusOK, h)
}
