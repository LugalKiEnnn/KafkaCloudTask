package kafka;

import config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import model.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;

/**
 * Консюмер сообщений
 */
@Slf4j
public class MessageConsumer {
    private static volatile MessageConsumer messageConsumer;

    private final KafkaConsumerConfig config;
    private final Consumer<String, Message> consumer;

    public static MessageConsumer getInstance() {
        synchronized (MessageConsumer.class) {
            if (messageConsumer == null) {
                messageConsumer = new MessageConsumer();
            }
        }
        return messageConsumer;
    }

    private MessageConsumer() {
        this.config = new KafkaConsumerConfig();

        this.consumer = new KafkaConsumer<>(this.config.toConsumerProperties());
        this.consumer.subscribe(List.of(config.getTopicName()));
        log.info("Консюмер подписан на топик {}, группа {}", config.getTopicName(), config.getGroupId());
    }

    /**
     * Прочитать сообщения
     */
    public void pollMessages() {
        try {
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(config.getMessagesPollIntervalMs()));  // Получение сообщений

                for (ConsumerRecord<String, Message> record : records) {
                    Message message = record.value();
                    log.info("Получено сообщение: key = {}, number = {}, text = {}, partition = {}, offset = {}",
                            record.key(), message.getNumber(), message.getText(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
        } finally {
            this.consumer.close();
            log.info("Консюмер остановлен");
        }
    }

    /**
     * Прервать чтение из другого потока
     */
    public void stop() {
        consumer.wakeup();
    }
}
