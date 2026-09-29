package event

import "time"

type OrderCreated struct {
	EventID   string    `json:"event_id"`
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Item      string    `json:"item"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}
