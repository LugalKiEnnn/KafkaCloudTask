package kafka;

import config.KafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Класс продюсера
 */
@Slf4j
public class KafkaMessageProducer {
    private static volatile KafkaMessageProducer kafkaMessageProducer;

    private final KafkaProducerConfig config;
    private final Producer<String, Message> producer;

    /**
     * Получить экземпляр продюсера
     */
    public static KafkaMessageProducer getInstance() {
        synchronized (KafkaMessageProducer.class) {
            if (kafkaMessageProducer == null) {
                kafkaMessageProducer = new KafkaMessageProducer();
            }
        }
        return kafkaMessageProducer;
    }

    private KafkaMessageProducer() {
        this.config = new KafkaProducerConfig();

        this.producer = new KafkaProducer<>(this.config.toProducerProperties());
        log.info("Продюсер подключается к {}, топик {}", config.getBootstrapServers(), config.getTopicName());
    }

    public static synchronized void closeProducer() {
        if (kafkaMessageProducer != null) {
            kafkaMessageProducer.producer.close();
            kafkaMessageProducer = null;
            log.info("Продюсер остановлен");
        }
    }

    /**
     * Отправить заданное в конфигурации количество тестовых сообщений
     */
    public void sendTestMessages() throws ExecutionException, InterruptedException {
        for (int i = 1; i <= config.getMessagesCount(); i++) {
            Message message = Message.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setNumber(i)
                    .setText("Тестовое сообщение №" + i)
                    .setCreatedAt(System.currentTimeMillis())
                    .build();

            sendMessage(message);

            Thread.sleep(config.getMessagesSendIntervalMs());
        }
        log.info("Отправлено сообщений: {}", config.getMessagesCount());
    }

    /**
     * Отправить сообщение в топик kafka
     * @param message сообщение
     */
    public void sendMessage(Message message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, Message> record = new ProducerRecord<>(
                config.getTopicName(),
                message.getId(),
                message);
        RecordMetadata metadata = producer.send(record).get();
        log.info("Сообщение доставлено: key = {}, number = {}, topic = {}, partition = {}, offset = {}",
                record.key(), message.getNumber(), metadata.topic(), metadata.partition(), metadata.offset());
    }
}
