#!/bin/bash

topic=$1

kafka-console-consumer --bootstrap-server kafka:9092 --topic ${topic} --from-beginning
