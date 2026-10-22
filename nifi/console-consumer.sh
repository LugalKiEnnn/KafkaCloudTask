#!/bin/bash
source ../local/.env
mkdir -p output

docker run --rm --network kafka-local \
    -v $PWD/../local/secrets/client:/etc/kafka/client \
    -v $PWD/../local/secrets/config:/etc/kafka/jaas \
    confluentinc/cp-schema-registry:7.7.1 kafka-avro-console-consumer \
    --bootstrap-server kafka-1:19092 \
    --consumer.config /etc/kafka/jaas/admin.properties \
    --property schema.registry.url=http://schema-registry:8081 \
    --property print.key=true \
    --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
    --topic $KAFKA_TOPIC_NAME \
    --from-beginning \
    --max-messages 10 | tee output/console-consumer.txt
