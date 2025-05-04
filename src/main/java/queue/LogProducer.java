package queue;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class LogProducer {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "LOG_QUEUE";
    private static final String DEVICE_ID = "device_" + ProcessHandle.current().pid();
    private static final long INTERVAL = 100;

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = session.createQueue(QUEUE_NAME);
            MessageProducer producer = session.createProducer(destination);
            connection.start();

            while (true) {
                String logMessage = createLogMessage();
                TextMessage message = session.createTextMessage(logMessage);
                System.out.println("Sending: " + logMessage);
                producer.send(message);
                Thread.sleep(INTERVAL);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String createLogMessage() {
        String[] logLevels = {"INFO", "WARN", "ERROR"};
        Random random = new Random();
        String logLevel = logLevels[random.nextInt(logLevels.length)];
        String messageContent = switch (logLevel) {
            case "INFO" -> "系统状态正常";
            case "WARN" -> "警告：资源即将耗尽";
            case "ERROR" -> "错误：系统故障";
            default -> "未知状态";
        };

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());

        return String.format("{\"device_id\":\"%s\",\"timestamp\":\"%s\",\"log_level\":\"%s\",\"message\":\"%s\"}",
                DEVICE_ID, timestamp, logLevel, messageContent);
    }
}
