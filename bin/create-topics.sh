#!/bin/bash

zookeeper=${ZOOKEEPER:-zookeeper}

kafka-topics --zookeeper ${zookeeper}:2181 --create --topic no-seat-ticket-request \
    --partitions 12 --replication-factor 1

kafka-topics --zookeeper ${zookeeper}:2181 --create --topic no-seat-ticket-response \
    --partitions 12 --replication-factor 1

kafka-topics --zookeeper ${zookeeper}:2181 --create --topic no-seat-remain-ticket \
    --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config delete.retention.ms=1000 \
    --config segment.ms=10000 \
    --config retention.ms=10000
