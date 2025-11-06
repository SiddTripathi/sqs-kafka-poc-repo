# SqsToKafka

A .NET worker microservice that polls **AWS SQS** and publishes to **Kafka/Redpanda**.  
Supports multiple topics, routing via SQS message attributes or JSON body, delete-after-ack, and an in-memory dedup safety net.

---

## Overview

**Flow:** SQS → (route resolve) → Kafka produce → on success delete from SQS → else retry (visibility extended).

- **Routing**  
  - From SQS message attributes: `KafkaTopic`, `KafkaKey`  
  - Fallback from JSON body: `kafkaTopic`, `kafkaKey`  
  - Else `Kafka:DefaultTopic`

- **Idempotency & Safety**  
  - Kafka producer `acks=all`, idempotent producer enabled  
  - Delete from SQS **only after** Kafka ack  
  - In-memory dedup cache keyed by SQS `MessageId` (skip duplicates)

---

## Configuration

`appsettings.json` (env vars override via `:` → `__`):

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Acks": "all",
    "MessageTimeoutMs": 30000,
    "DefaultTopic": "sqs.to.kafka.dev"
  },
  "Sqs": {
    "Profile": "sqs-dev",
    "Region": "ap-southeast-2",
    "QueueUrl": "https://ap-southeast-2.queue.amazonaws.com/<account-id>/sqs-to-kafka-dev",
    "WaitTimeSeconds": 10,
    "MaxMessages": 5,
    "VisibilityTimeoutSeconds": 30,
    "EmptyWaitDelayMs": 2000
  },
  "Routing": {
    "BodyField": "event",
    "Map": {
      "payment.succeeded": "routed.a",
      "order.cancelled": "routed.b"
    }
  }
}
