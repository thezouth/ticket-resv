#!/bin/bash

zookeeper=${ZOOKEEPER:-zookeeper}

kafka-topics --zookeeper ${zookeeper}:2181 --create --topic no-seat-ticket-request \
    --partitions 12 --replication-factor 1

kafka-topics --zookeeper ${zookeeper}:2181 --create --topic no-seat-ticket-response \
    --partitions 12 --replication-factor 1
