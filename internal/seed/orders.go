package seed

//producer becomes the seeder, this just creates the data . producer can now see the seed.GenerateOrder() now
import (
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"

	"github.com/dexisback/learning-kafka/internal/event"
)

// func GenerateOrder() event.OrderCreated {
// 	return event.OrderCreated{
// 		EventID:   uuid.New().String(),
// 		OrderID:   uuid.New().String(),
// 		UserID:    gofakeit.UUID(),
// 		Item:      gofakeit.ProductName(),
// 		Quantity:  gofakeit.IntRange(1, 5),
// 		CreatedAt: time.Now(),
// 	}
// }

type Generator struct {
	users []string
}

func NewGenerator(userCount int) *Generator {
	users := make([]string, userCount)
	for i := range users {
		users[i] = uuid.New().String()
	}

	return &Generator{
		users: users,
	}

}

func (g *Generator) Order() event.OrderCreated {
	return event.OrderCreated{
		EventID:   uuid.New().String(),
		OrderID:   uuid.New().String(),
		UserID:    g.users[gofakeit.IntRange(0, len(g.users)-1)],
		Item:      gofakeit.ProductName(),
		Quantity:  gofakeit.IntRange(1, 5),
		CreatedAt: time.Now(),
	}
}













//generator responsible for generating data, and the producer responsible for Kafka + CLI + benchmarking.