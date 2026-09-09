package org.yamunasoftware.ikgw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/ikgw/api")
public class Controller {
  private final Logger logger = LoggerFactory.getLogger(Controller.class);
  private final KafkaTemplate<String, SensorReading> kafkaTemplate;

  @Autowired
  public Controller(KafkaTemplate<String, SensorReading> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @PostMapping("publish")
  public void publishMessage(@RequestBody SensorReading reading) {
    try {
      String topic = System.getenv("KAFKA_TOPIC");
      kafkaTemplate.send(topic, reading);
    }

    catch (Exception e) {
      logger.error("Unexpected Error in POST /ikgw/api/publish", e);
    }
  }
}