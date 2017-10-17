#!/bin/sh

/data1/kafka/kafka/bin/kafka-console-consumer.sh --zookeeper kafka-pi-01:2181,kafka-pi-02:2181,kafka-pi-03:2181 --topic $1
