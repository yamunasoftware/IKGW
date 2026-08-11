package org.yamunasoftware.ikgw;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  private static final String topic = "imadds";
  private static final int initialDelay = 2;
  private static final int pollingPeriod = 10;

  public static void main(String[] args) {
    String kafkaUrl = Conf.getKafkaUrl();
    ObjectMapper objectMapper = new ObjectMapper();
    try (KafkaProducer<String, String> producer = setupProducer(kafkaUrl)) {
      Runnable task = () -> sendMessage(producer, objectMapper);
      scheduler.scheduleAtFixedRate(task, initialDelay, pollingPeriod, TimeUnit.SECONDS);
    }

    catch (Exception e) {
      logger.error("Error: Failed to Start Kafka Producer", e);
    }
  }

  private static void sendMessage(KafkaProducer<String, String> producer, ObjectMapper objectMapper) {
    try {
      ArrayList<SensorReading> readings = DataReadout.dataReadout();
      String message = objectMapper.writeValueAsString(readings);
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
      RecordMetadata metadata = producer.send(record).get();
      logger.info("Sent message with Partition: {}\nOffset: {}\nTimestamp: {}\n",
          metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    catch (Exception e) {
      logger.error("Error: Failed to Send Message", e);
    }
  }

  private static KafkaProducer<String, String> setupProducer(String url) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.RETRIES_CONFIG, 3);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    return new KafkaProducer<>(properties);
  }
}