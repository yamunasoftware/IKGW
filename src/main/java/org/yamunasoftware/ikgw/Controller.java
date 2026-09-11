package org.yamunasoftware.ikgw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/ikgw/api")
public class Controller {
  private static final Logger logger = LoggerFactory.getLogger(Controller.class);

  private final String kafkaTopic;
  private final KafkaTemplate<String, SensorReadingMessage> kafkaTemplate;

  @Autowired
  public Controller(KafkaTemplate<String, SensorReadingMessage> kafkaTemplate, @Value("KAFKA_TOPIC") String kafkaTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaTopic = kafkaTopic;
  }

  @PostMapping("publish")
  public void publishMessage(@RequestBody SensorReading reading) {
    try {
      SensorReadingMessage message = new SensorReadingMessage(reading);
      kafkaTemplate.send(kafkaTopic, message);
    }

    catch (Exception e) {
      logger.error("Unexpected Error in POST /ikgw/api/publish", e);
    }
  }
}