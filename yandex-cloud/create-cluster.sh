#!/bin/bash
source .env

# Создание кластера
yc managed-kafka cluster create \
  --name "$CLUSTER_NAME" \
  --environment production \
  --version 3.7 \
  --network-name "$NETWORK_NAME" \
  --zone-ids ru-central1-a,ru-central1-b,ru-central1-d \
  --brokers-count 1 \
  --resource-preset s3-c2-m8 \
  --disk-type network-ssd \
  --disk-size 32 \
  --assign-public-ip \
  --log-retention-ms 604800000 \
  --log-segment-bytes 1073741824

# Создание топика
yc managed-kafka topic create "$KAFKA_TOPIC_NAME" \
  --cluster-name "$CLUSTER_NAME" \
  --partitions 3 \
  --replication-factor 3 \
  --cleanup-policy delete \
  --retention-ms 604800000 \
  --segment-bytes 1073741824 \
  --min-insync-replicas 2

# Создание топика для Schema Registry
yc managed-kafka topic create schemas \
  --cluster-name "$CLUSTER_NAME" \
  --partitions 1 \
  --replication-factor 3 \
  --cleanup-policy compact \
  --min-insync-replicas 2

# Создание пользователей
yc managed-kafka user create "$ADMIN_USERNAME" --cluster-name "$CLUSTER_NAME" \
  --password "$ADMIN_PASSWORD" --permission topic=*,role=admin
yc managed-kafka user create "$PRODUCER_USERNAME" --cluster-name "$CLUSTER_NAME" \
  --password "$PRODUCER_PASSWORD" --permission topic="$KAFKA_TOPIC_NAME",role=producer
yc managed-kafka user create "$CONSUMER_USERNAME" --cluster-name "$CLUSTER_NAME" \
  --password "$CONSUMER_PASSWORD" --permission topic="$KAFKA_TOPIC_NAME",role=consumer
yc managed-kafka user create "$SCHEMA_REGISTRY_USERNAME" --cluster-name "$CLUSTER_NAME" \
  --password "$SCHEMA_REGISTRY_PASSWORD" \
  --permission topic=schemas,role=producer --permission topic=schemas,role=consumer

# Вывод параметров кластера
mkdir -p output
yc managed-kafka cluster get "$CLUSTER_NAME" | tee output/cluster.txt
yc managed-kafka cluster list-hosts "$CLUSTER_NAME" | tee -a output/cluster.txt
