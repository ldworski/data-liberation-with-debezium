#!/bin/sh
set -e

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

for topic in app.public.orders app.public.order_items order_events; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR"
done

echo 'Topics created'
