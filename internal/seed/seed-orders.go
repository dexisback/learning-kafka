package seed

import (
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/dexisback/learning-kafka/internal/event"
	"github.com/google/uuid"
)

func GenerateOrder() event.OrderCreated {
	return event.OrderCreated{
		EventID:   uuid.New().String(),
		OrderID:   uuid.New().String(),
		UserID:    gofakeit.UUID(),
		Item:      gofakeit.ProductName(),
		Quantity:  gofakeit.IntRange(1, 5),
		CreatedAt: time.Now(),
	}
}
