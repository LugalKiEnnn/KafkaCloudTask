#!/bin/bash
BS=kafka-1:19092
CFG=/etc/kafka/jaas/admin.properties

# Топик задания
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $KAFKA_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик Schema Registry
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic schemas \
    --partitions 1 \
    --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2

# Права producer
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$PRODUCER_USERNAME --producer --topic $KAFKA_TOPIC_NAME

# Права consumer
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$CONSUMER_USERNAME --consumer --topic $KAFKA_TOPIC_NAME --group '*'

# Права schema-registry
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --topic schemas
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --group '*'

# Права nifi
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$NIFI_USERNAME --consumer --topic $KAFKA_TOPIC_NAME --group nifi-group

kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $KAFKA_TOPIC_NAME
