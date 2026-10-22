import kafka.MessageConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Выключение...");
            MessageConsumer.getInstance().stop();
        }));

        log.info("Инициализация консюмера kafka...");
        MessageConsumer.getInstance().pollMessages();
    }
}
