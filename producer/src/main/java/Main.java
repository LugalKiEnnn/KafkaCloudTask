import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Выключение...");
            KafkaMessageProducer.closeProducer();
        }));

        try {
            log.info("Инициализация продюсера kafka...");
            KafkaMessageProducer.getInstance().sendTestMessages();
        } catch (Exception e) {
            log.error("Ошибка в продюсере: {}", e.getMessage(), e);
        } finally {
            KafkaMessageProducer.closeProducer();
        }
    }
}
