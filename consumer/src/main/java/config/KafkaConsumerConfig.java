package config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.util.Properties;

/**
 * Конфигурация консюмера
 */
@Slf4j
@Data
public class KafkaConsumerConfig {
    // Имя топика
    private final String topicName;

    // Сервера kafka
    private final String bootstrapServers;
    // Название id группы
    private final String groupId;
    // Название десериализатора ключей
    private final String keyDeserializer;
    // Название десериализатора значений
    private final String valueDeserializer;
    // Метод чтения сообщений
    private final String autoOffsetReset;
    // Работа авто-коммита
    private final Boolean enableAutoCommit;
    // Максимальное число читаемых записей
    private final Integer maxPollRecords;
    // Время таймаута сессии
    private final Integer sessionTimeoutMs;
    // Время чтения топика
    private final Long messagesPollIntervalMs;

    // Адрес Schema Registry
    private final String schemaRegistryUrl;

    private final String securityProtocol;
    private final String saslMechanism;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;

    private final String saslUsername;
    private final String saslPassword;

    public KafkaConsumerConfig() {
        Configurations configs = new Configurations();

        try {
            Configuration config = configs.properties(new File("application.properties"));

            this.topicName = getEnvOrConfigValue(config, "topic.name", "KAFKA_TOPIC_NAME");
            this.bootstrapServers = getEnvOrConfigValue(config, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");

            this.groupId = config.getString("group.id");
            this.keyDeserializer = config.getString("key.deserializer");
            this.valueDeserializer = config.getString("value.deserializer");
            this.autoOffsetReset = config.getString("auto.offset.reset");
            this.enableAutoCommit = config.getBoolean("enable.auto.commit");
            this.maxPollRecords = config.getInt("max.poll.records");
            this.sessionTimeoutMs = config.getInt("session.timeout.ms");
            this.messagesPollIntervalMs = config.getLong("messages.poll.interval.ms");

            this.schemaRegistryUrl = getEnvOrConfigValue(config, "schema.registry.url", "SCHEMA_REGISTRY_URL");

            this.securityProtocol = getEnvOrConfigValue(config, "security.protocol", "SECURITY_PROTOCOL");
            this.saslMechanism = config.getString("sasl.mechanism");
            this.sslTruststoreLocation = getEnvOrConfigValue(config, "ssl.truststore.location", "KAFKA_SSL_TRUSTSTORE_LOCATION");
            this.sslTruststorePassword = getEnvOrConfigValue(config, "ssl.truststore.password", "KAFKA_SSL_TRUSTSTORE_PASSWORD");
            this.sslKeystoreLocation = getEnvOrConfigValue(config, "ssl.keystore.location", "KAFKA_SSL_KEYSTORE_LOCATION");
            this.sslKeystorePassword = getEnvOrConfigValue(config, "ssl.keystore.password", "KAFKA_SSL_KEYSTORE_PASSWORD");

            this.saslUsername = getEnvOrConfigValue(config, "sasl.username", "CONSUMER_USERNAME");
            this.saslPassword = getEnvOrConfigValue(config, "sasl.password", "CONSUMER_PASSWORD");
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
     * Получить настройки в виде свойств консюмера
     * @return Properties консюмера
     */
    public Properties toConsumerProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);        // Уникальный идентификатор группы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.autoOffsetReset);        // Начало чтения с самого начала
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.enableAutoCommit);           // Автоматический коммит смещений
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords);           // Максимум записей за один poll
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeoutMs);           // Время ожидания активности от консьюмера

        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);   // Читать в сгенерированный класс Message

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
