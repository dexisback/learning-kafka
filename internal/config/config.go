package config

import (
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

func Get(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

// StartOffset returns where a consumer group should begin reading a partition
// that has NO committed offset yet — a brand-new group or a clean test run.
//
//	KAFKA_START_OFFSET=first (default) -> read the topic from the beginning
//	KAFKA_START_OFFSET=last            -> start after the current end of the log
//
// This does NOT affect groups that already have committed offsets: Kafka
// always resumes those from their committed offset, so normal operation and
// failure recovery behave the same no matter what this is set to.
func StartOffset() int64 {
	switch value := strings.ToLower(os.Getenv("KAFKA_START_OFFSET")); value {
	case "", "first", "earliest":
		return kafka.FirstOffset
	case "last", "latest":
		return kafka.LastOffset
	default:
		log.Fatalf("invalid KAFKA_START_OFFSET %q: use \"first\"/\"earliest\" or \"last\"/\"latest\"", value)
		return 0 // unreachable
	}
}
