FROM golang:1.27 AS builder

#explain
WORKDIR /app  

COPY go.mod go.sum ./
RUN go mod download 

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /order-worker ./cmd/order-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /analytics ./cmd/analytics
RUN CGO_ENABLED=0 GOOS=linux go build -o /notifications ./cmd/notifications


FROM alpine:3.21

WORKDIR /app

COPY --from=builder /api .
COPY --from=builder /order-worker .
COPY --from=builder /analytics .
COPY --from=builder /notifications .


CMD ["./api"]

# The same image contains all four binaries; Compose decides which one runs.



