#!/bin/bash
source .env

mkdir -p secrets/client secrets/config

# Сертификат Yandex Cloud
curl -s https://storage.yandexcloud.net/cloud-certs/CA.pem -o secrets/YandexInternalRootCA.crt

# Создание truststore
rm -f secrets/client/client.truststore.jks
keytool -importcert \
    -alias YandexCA \
    -file secrets/YandexInternalRootCA.crt \
    -keystore secrets/client/client.truststore.jks \
    -storepass $CERT_PASSWORD \
    -noprompt

# Настройки администратора
cat > secrets/config/admin.properties <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$ADMIN_USERNAME" password="$ADMIN_PASSWORD";
ssl.truststore.location=/etc/kafka/client/client.truststore.jks
ssl.truststore.password=$CERT_PASSWORD
PROPS
