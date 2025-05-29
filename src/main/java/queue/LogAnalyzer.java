package queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Log;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogAnalyzer {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String LOG_QUEUE = "LOG_QUEUE";
    private static final String ANALYSIS_QUEUE = "ANALYSIS_QUEUE";
    private static final String ALERT_QUEUE = "ALERT_QUEUE";

    private static final int MAX_LOGS = 100;
    private static final int T = 3;
    private static final int S = 1;

    private static final Map<String, LinkedList<Log>> logBuffer = new ConcurrentHashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // 消费原始日志
        Destination logQueue = session.createQueue(LOG_QUEUE);
        MessageConsumer consumer = session.createConsumer(logQueue);

        // 生产分析结果、告警消息
        MessageProducer analysisProducer = session.createProducer(session.createQueue(ANALYSIS_QUEUE));
        MessageProducer alertProducer = session.createProducer(session.createQueue(ALERT_QUEUE));

        consumer.setMessageListener(message -> {
            if (message instanceof TextMessage) {
                try {
                    String json = ((TextMessage) message).getText();
                    Log log = mapper.readValue(json, Log.class);
                    processLog(log);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        // 每T秒分析一次并发送到ANALYSIS_QUEUE
        scheduler.scheduleAtFixedRate(() -> analyzeLogs(session, analysisProducer), 0, T, TimeUnit.SECONDS);
        // 每S秒判断是否需要发送严重告警
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
    private static void analyzeLogs(Session session, MessageProducer analysisProducer) {
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

                String resultJson = mapper.writeValueAsString(result);
                analysisProducer.send(session.createTextMessage(resultJson));
                System.out.println("分析结果发送: " + resultJson);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 检测是否需要产生严重警告
    private static void detectAlerts(Session session, MessageProducer alertProducer) {
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

            double ratio = (double) recentErrors / recentTotal;

            if (ratio > 0.5) {
                try {
                    Map<String, Object> alert = new HashMap<>();
                    alert.put("device_id", deviceId);
                    alert.put("alert_type", "CRITICAL");
                    alert.put("error_ratio", ratio);
                    alert.put("time", now.toString());

                    String alertJson = mapper.writeValueAsString(alert);
                    alertProducer.send(session.createTextMessage(alertJson));
                    System.out.println("严重告警发送: " + alertJson);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
