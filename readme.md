- using kafka-go because it is go native kafka client and keeps the first implementation focused on kafka concepts rather than the mechanics of librdkafka/CGO
- making this entire thing in layers:
    > 1: simple consumer producer topic single
    > 2: logs: (so partitioning/offsets/append onlylogs/retention/persistence/replay)
    > 3: consumer groups (one kafka order under load to many consumers )
    > 4: partitions: a single order will have partition 0/1/2 
    > 5: keys + orderingg . key = order_id ki jagah we'll experiment with key = user_id
    > 6: offsets + failures (we'll deliberately kill the consumer, and check where it resumes/resume behaviour)
    > 7:delivery guarantees: investigate atmost once , atleast once, exactly once delivery
    > 8: producer relibility: experiment with acks/retries/idempotent producer/batching and compression
    > 9: retention + replay
    > 10: mock backend/apis and hence test it with a real microservice (have order-service, analytics-service, notification-service as three different microservies. all of em being separate individual topic)