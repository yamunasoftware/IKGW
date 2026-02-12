package org.yamunasoftware.ikgw;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Main {
  private static final String topic = "IMADDS";
  private static final int pollingPeriod = 10000;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    try {
      HashMap<String, String> systemInfo = Input.getSystemInfo();
      String deviceID = systemInfo.get("SYSTEM_ID");
      String deviceType = systemInfo.get("SYSTEM_TYPE");
      String kafkaURL = systemInfo.get("KAFKA_URL");
      KafkaProducer<String, String> producer = setupProducer(deviceID, kafkaURL);

      while (true) {
        try {
          sendMessages(producer, deviceID, deviceType);
          Thread.sleep(pollingPeriod);
        }

        catch (InterruptedException e) {
          logger.info("IKGW Stream Interrupted...\nClosing IKGW...");
          producer.flush();
          producer.close();

          Thread.currentThread().interrupt();
          logger.info("IKGW Closed Successfully.");
          break;
        }

        catch (Exception e) {
          logger.error("Error: Failed to Send Message", e);
        }
      }
    }

    catch (Exception e) {
      logger.error("Error: Failed to Setup Kafka Producer", e);
    }
  }

  private static void sendMessages(KafkaProducer<String, String> producer, String id, String type) throws Exception {
    ArrayList<String> messages = buildMessages(id, type);
    for (String message : messages) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, message);
      RecordMetadata metadata = producer.send(record).get();
      logger.info("Sent message from device {}\nPartition: {}\nOffset: {}\nTimestamp: {}\n",
          id, metadata.partition(), metadata.offset(), metadata.timestamp());
    }
  }

  private static ArrayList<String> buildMessages(String id, String type) {
    ArrayList<String> messages = new ArrayList<>();
    ArrayList<HashMap<Integer, HashMap<String, Float>>> data = Input.dataReadout();

    for (HashMap<Integer, HashMap<String, Float>> deviceData : data) {
      for (Integer channel : deviceData.keySet()) {
        StringBuilder message = new StringBuilder("{\"deviceID\":\"").append(id).append("\",")
            .append("\"deviceType\":\"").append(type).append("\",");

        HashMap<String, Float> values = deviceData.get(channel);
        double temperature = values.get("Temperature");
        double pressure = values.get("Pressure");
        double humidity = values.get("Humidity");

        message.append("\"channel\":").append(channel).append(",");
        message.append("\"temperature\":").append(temperature).append(",");
        message.append("\"pressure\":").append(pressure).append(",");
        message.append("\"humidity\":").append(humidity).append("}");
        messages.add(message.toString());
      }
    }
    return messages;
  }

  private static KafkaProducer<String, String> setupProducer(String id, String url) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, id);
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    properties.put(ProducerConfig.RETRIES_CONFIG, 3);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    return new KafkaProducer<>(properties);
  }
}