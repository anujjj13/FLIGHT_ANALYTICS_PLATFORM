#!/bin/bash
# Creates all Kafka topics needed for the Flight Analytics Platform.
# Run this once after `docker compose up -d` and the broker is healthy:
#   bash docker/init-topics.sh

set -e

BROKER="localhost:9092"
CONTAINER="flight-kafka"

echo "Waiting for Kafka to be ready..."
until docker exec "$CONTAINER" kafka-broker-api-versions --bootstrap-server "$BROKER" > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka is ready. Creating topics..."

create_topic () {
  TOPIC_NAME=$1
  PARTITIONS=$2
  RETENTION_MS=$3
  docker exec "$CONTAINER" kafka-topics --create \
    --if-not-exists \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS"
  echo "  -> $TOPIC_NAME (partitions=$PARTITIONS, retention_ms=$RETENTION_MS)"
}

# Main topic: raw flight state vectors from the OpenSky producer
create_topic "flight_raw" 3 604800000        # 7 days retention

# Dead-letter topic: records that failed hard validation in Silver processing
create_topic "flight_deadletter" 1 1209600000  # 14 days retention (keep longer for debugging)

echo "All topics created."
docker exec "$CONTAINER" kafka-topics --list --bootstrap-server "$BROKER"
