package topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Log;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

public class LogAnalyzer {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String ANALYSIS_QUEUE = "ANALYSIS_QUEUE";
    private static final String ALERT_QUEUE = "ALERT_QUEUE";

    private static final int MAX_LOGS = 100;
    private static final int T = 3;
    private static final int S = 1;

    private static final Map<String, LinkedList<Log>> logBuffer = new ConcurrentHashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // 订阅3个日志主题
        Topic infoTopic = session.createTopic("LOG_INFO_TOPIC");
        Topic warnTopic = session.createTopic("LOG_WARN_TOPIC");
        Topic errorTopic = session.createTopic("LOG_ERROR_TOPIC");

        MessageConsumer infoConsumer = session.createConsumer(infoTopic);
        MessageConsumer warnConsumer = session.createConsumer(warnTopic);
        MessageConsumer errorConsumer = session.createConsumer(errorTopic);

        // 生产分析结果和告警信息
        MessageProducer analysisProducer = session.createProducer(session.createQueue(ANALYSIS_QUEUE));
        MessageProducer alertProducer = session.createProducer(session.createQueue(ALERT_QUEUE));

        MessageListener listener = message -> {
            if (message instanceof TextMessage) {
                try {
                    String json = ((TextMessage) message).getText();
                    Log log = mapper.readValue(json, Log.class);
                    processLog(log);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        infoConsumer.setMessageListener(listener);
        warnConsumer.setMessageListener(listener);
        errorConsumer.setMessageListener(listener);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(() -> analyzeLogs(session, analysisProducer), 0, T, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(() -> detectAlerts(session, alertProducer), 0, S, TimeUnit.SECONDS);

        connection.start();
    }

    // 缓存每个device的最近N条日志
    private static void processLog(Log log) {
        logBuffer.computeIfAbsent(log.getDeviceId(), k -> new LinkedList<>());
        LinkedList<Log> logs = logBuffer.get(log.getDeviceId());
        synchronized (logs) {
            if (logs.size() >= MAX_LOGS) {
                logs.removeFirst();
            }
            logs.addLast(log);
        }
    }

    // 日志统计分析
    private static void analyzeLogs(Session session, MessageProducer producer) {
        for (String deviceId : logBuffer.keySet()) {
            List<Log> logs = new ArrayList<>(logBuffer.get(deviceId));
            if (logs.isEmpty()) continue;

            int errorCount = 0, warnCount = 0;
            LocalDateTime lastErrorTime = null;

            for (Log log : logs) {
                switch (log.getLogLevel()) {
                    case "ERROR" -> {
                        errorCount++;
                        if (lastErrorTime == null || log.getTimestamp().isAfter(lastErrorTime)) {
                            lastErrorTime = log.getTimestamp();
                        }
                    }
                    case "WARN" -> warnCount++;
                }
            }

            int total = logs.size();
            double errorRatio = (double) errorCount / total;
            double warnRatio = (double) warnCount / total;

            try {
                Map<String, Object> result = new HashMap<>();
                result.put("device_id", deviceId);
                result.put("warn_count", warnCount);
                result.put("error_count", errorCount);
                result.put("error_ratio", errorRatio);
                result.put("warn_ratio", warnRatio);
                result.put("last_error_time", lastErrorTime != null ? lastErrorTime.toString() : "");

                String json = mapper.writeValueAsString(result);
                producer.send(session.createTextMessage(json));
                System.out.println("分析结果发送: " + json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 检测是否需要产生严重警告
    private static void detectAlerts(Session session, MessageProducer producer) {
        LocalDateTime now = LocalDateTime.now();
        for (String deviceId : logBuffer.keySet()) {
            List<Log> logs = new ArrayList<>(logBuffer.get(deviceId));
            if (logs.isEmpty()) continue;

            long recentTotal = logs.stream()
                    .filter(l -> Duration.between(l.getTimestamp(), now).getSeconds() <= S)
                    .count();

            if (recentTotal == 0) continue;

            long recentErrors = logs.stream()
                    .filter(l -> l.getLogLevel().equals("ERROR") &&
                            Duration.between(l.getTimestamp(), now).getSeconds() <= S)
                    .count();

            double errorRatio = (double) recentErrors / recentTotal;
            if (errorRatio > 0.5) {
                try {
                    Map<String, Object> alert = new HashMap<>();
                    alert.put("device_id", deviceId);
                    alert.put("alert_type", "CRITICAL");
                    alert.put("error_ratio", errorRatio);
                    alert.put("time", now.toString());

                    String json = mapper.writeValueAsString(alert);
                    producer.send(session.createTextMessage(json));
                    System.out.println("严重告警发送: " + json);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
