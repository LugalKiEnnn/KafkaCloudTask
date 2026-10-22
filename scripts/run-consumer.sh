#!/bin/bash
set -a
source $1/.env
set +a

export KAFKA_SSL_TRUSTSTORE_LOCATION=$PWD/$1/secrets/client/client.truststore.jks
export KAFKA_SSL_TRUSTSTORE_PASSWORD=$CERT_PASSWORD
if [ "$1" == "local" ]; then
    export KAFKA_SSL_KEYSTORE_LOCATION=$PWD/$1/secrets/client/client.keystore.jks
    export KAFKA_SSL_KEYSTORE_PASSWORD=$CERT_PASSWORD
fi

mkdir -p $1/output
cd consumer
mvn -q clean package
java -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 -jar target/consumer.jar 2>&1 | tee ../$1/output/consumer.log
