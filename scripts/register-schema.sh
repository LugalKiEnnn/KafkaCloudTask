#!/bin/bash
source $1/.env
mkdir -p $1/output

SUBJECT=$KAFKA_TOPIC_NAME-value
SCHEMA=$(tr -d '\r\n' < schema/message.avsc | sed 's/"/\\"/g')

# Регистрация схемы
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$SCHEMA\"}" \
    $SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions
echo

# Проверка
curl -s $SCHEMA_REGISTRY_URL/subjects | tee $1/output/schema-registry.txt
echo | tee -a $1/output/schema-registry.txt
curl -s -X GET $SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions | tee -a $1/output/schema-registry.txt
echo | tee -a $1/output/schema-registry.txt
