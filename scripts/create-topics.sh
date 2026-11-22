#!/bin/sh
set -eu

create_topic() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create --if-not-exists --partitions 3 --replication-factor 3 "$@"
}

create_topic --topic events --config min.insync.replicas=2
create_topic --topic events.dlt
