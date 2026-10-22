package config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.util.Properties;

/**
 * Конфигурация продюсера
 */
@Slf4j
@Data
public class KafkaProducerConfig {
    // Имя топика
    private final String topicName;

    // Сервера kafka
    private final String bootstrapServers;
    // Название сериализатора ключей
    private final String keySerializer;
    // Название сериализатора значений
    private final String valueSerializer;
    // Конфигурация подтверждения отправки
    private final String acks;
    // Количество попыток отправки сообщения
    private final String retries;
    // Идемпотентность продюсера
    private final String enableIdempotence;

    // Адрес Schema Registry
    private final String schemaRegistryUrl;
    // Разрешить продюсеру самому регистрировать схему
    private final Boolean autoRegisterSchemas;
    // Использовать последнюю версию схемы из Schema Registry
    private final Boolean useLatestVersion;

    // Количество тестовых сообщений
    private final Integer messagesCount;
    // Интервал отправки сообщений
    private final Long messagesSendIntervalMs;

    private final String securityProtocol;
    private final String saslMechanism;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;

    private final String saslUsername;
    private final String saslPassword;

    public KafkaProducerConfig() {
        Configurations configs = new Configurations();

        try {
            Configuration config = configs.properties(new File("application.properties"));

            this.topicName = getEnvOrConfigValue(config, "topic.name", "KAFKA_TOPIC_NAME");
            this.bootstrapServers = getEnvOrConfigValue(config, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");

            this.keySerializer = config.getString("key.serializer");
            this.valueSerializer = config.getString("value.serializer");
            this.acks = config.getString("acks");
            this.retries = config.getString("retries");
            this.enableIdempotence = config.getString("enable.idempotence");

            this.schemaRegistryUrl = getEnvOrConfigValue(config, "schema.registry.url", "SCHEMA_REGISTRY_URL");
            this.autoRegisterSchemas = config.getBoolean("auto.register.schemas", false);
            this.useLatestVersion = config.getBoolean("use.latest.version", true);

            this.messagesCount = Integer.valueOf(getEnvOrConfigValue(config, "messages.count", "MESSAGES_COUNT"));
            this.messagesSendIntervalMs = Long.valueOf(getEnvOrConfigValue(config, "messages.send.interval.ms", "MESSAGE_SEND_INTERVAL"));

            this.securityProtocol = getEnvOrConfigValue(config, "security.protocol", "SECURITY_PROTOCOL");
            this.saslMechanism = config.getString("sasl.mechanism");
            this.sslTruststoreLocation = getEnvOrConfigValue(config, "ssl.truststore.location", "KAFKA_SSL_TRUSTSTORE_LOCATION");
            this.sslTruststorePassword = getEnvOrConfigValue(config, "ssl.truststore.password", "KAFKA_SSL_TRUSTSTORE_PASSWORD");
            this.sslKeystoreLocation = getEnvOrConfigValue(config, "ssl.keystore.location", "KAFKA_SSL_KEYSTORE_LOCATION");
            this.sslKeystorePassword = getEnvOrConfigValue(config, "ssl.keystore.password", "KAFKA_SSL_KEYSTORE_PASSWORD");

            this.saslUsername = getEnvOrConfigValue(config, "sasl.username", "PRODUCER_USERNAME");
            this.saslPassword = getEnvOrConfigValue(config, "sasl.password", "PRODUCER_PASSWORD");
        } catch (ConfigurationException cex) {
            log.error("Ошибка загрузки конфигурации: {}", cex.getMessage());

            throw new IllegalStateException("Ошибка загрузки конфигурации", cex.getCause());
        }

        checkConfiguration();
    }

    private void checkConfiguration() {
        if (this.topicName == null) {
            throw new IllegalArgumentException("Имя топика должно быть установлено");
        }

        if (this.bootstrapServers == null) {
            throw new IllegalArgumentException("Адреса kafka должны быть установлены");
        }

        if (this.schemaRegistryUrl == null) {
            throw new IllegalArgumentException("Адрес Schema Registry должен быть установлен");
        }

        if (this.saslUsername == null || this.saslPassword == null) {
            throw new IllegalArgumentException("Логин и пароль пользователя kafka должны быть установлены");
        }
    }

    /**
     * Получить настройки в виде свойств продюсера
     * @return Properties продюсера
     */
    public Properties toProducerProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer);

        if (this.acks != null) {
            props.put(ProducerConfig.ACKS_CONFIG, this.acks);
        }

        if (this.retries != null) {
            props.put(ProducerConfig.RETRIES_CONFIG, this.retries);
        }

        if (this.enableIdempotence != null) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, this.enableIdempotence);
        }

        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, this.autoRegisterSchemas);
        props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, this.useLatestVersion);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.sslTruststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.sslTruststorePassword);

        if (this.sslKeystoreLocation != null) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.sslKeystoreLocation);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.sslKeystorePassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.sslKeystorePassword);
        }

        String jaasConfigStr = "org.apache.kafka.common.security.scram.ScramLoginModule required username=" + "\"" + this.saslUsername + "\"" +
                " password=" + "\"" + this.saslPassword + "\";";
        props.put(SaslConfigs.SASL_MECHANISM, this.saslMechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigStr);

        return props;
    }

    /**
     * Поиск значения конфигурации либо в env переменных, либо в application.properties
     * @param config конфигурация
     * @param key ключ поиска в application.properties
     * @param envVar наименование переменной в env
     * @return найденное значение
     */
    private String getEnvOrConfigValue(Configuration config, String key, String envVar) {
        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String fileValue = config.getString(key, null);
        if (fileValue != null && !fileValue.isEmpty() && !fileValue.startsWith("${")) {
            return fileValue;
        }
        return null;
    }
}
