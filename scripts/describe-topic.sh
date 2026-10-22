#!/bin/bash
source $1/.env
mkdir -p $1/output

if [ "$1" == "local" ]; then
    docker exec kafka-1 kafka-topics \
        --bootstrap-server kafka-1:19092 \
        --command-config /etc/kafka/jaas/admin.properties \
        --describe --topic $KAFKA_TOPIC_NAME | tee $1/output/topic-describe.txt
else
    docker run --rm \
        -v $PWD/$1/secrets/client:/etc/kafka/client \
        -v $PWD/$1/secrets/config:/etc/kafka/jaas \
        confluentinc/cp-kafka:7.7.1 kafka-topics \
        --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
        --command-config /etc/kafka/jaas/admin.properties \
        --describe --topic $KAFKA_TOPIC_NAME | tee $1/output/topic-describe.txt
fi
