package topic;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LogProducer {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String DEVICE_ID = "device_" + ProcessHandle.current().pid();
    private static final long INTERVAL = 100;

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            // 创建三个Topic
            Topic infoTopic = session.createTopic("LOG_INFO_TOPIC");
            Topic warnTopic = session.createTopic("LOG_WARN_TOPIC");
            Topic errorTopic = session.createTopic("LOG_ERROR_TOPIC");

            // 复用Producer
            Map<String, MessageProducer> producerMap = new HashMap<>();
            producerMap.put("INFO", session.createProducer(infoTopic));
            producerMap.put("WARN", session.createProducer(warnTopic));
            producerMap.put("ERROR", session.createProducer(errorTopic));

            connection.start();

            String[] levels = {"INFO", "WARN", "ERROR"};
            Random random = new Random();

            while (true) {
                String level = levels[random.nextInt(levels.length)];
                String messageContent = switch (level) {
                    case "INFO" -> "系统状态正常";
                    case "WARN" -> "警告：资源即将耗尽";
                    case "ERROR" -> "错误：系统故障";
                    default -> "未知状态";
                };

                String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                String logMessage = String.format(
                        "{\"device_id\":\"%s\",\"timestamp\":\"%s\",\"log_level\":\"%s\",\"message\":\"%s\"}",
                        DEVICE_ID, timestamp, level, messageContent
                );

                // 根据级别从 map 中选择对应 producer
                MessageProducer producer = producerMap.get(level);
                if (producer != null) {
                    producer.send(session.createTextMessage(logMessage));
                    System.out.println("Sending: " + logMessage);
                }

                Thread.sleep(INTERVAL);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
