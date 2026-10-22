#!/bin/bash
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $ADMIN_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$ADMIN_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $PRODUCER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$PRODUCER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $CONSUMER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$CONSUMER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $SCHEMA_REGISTRY_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$SCHEMA_REGISTRY_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $NIFI_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$NIFI_PASSWORD]"
