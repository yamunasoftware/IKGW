package org.yamunasoftware.ikgw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

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

  @GetMapping("health")
  public ResponseEntity<String> health() {
    try {
      return new  ResponseEntity<>(HttpStatus.OK);
    }

    catch (Exception e) {
      logger.error("Unexpected Error in GET /ikgw/api/health", e);
      return new  ResponseEntity<>("Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @PostMapping("publish")
  public ResponseEntity<String> publishMessage(@RequestBody SensorReading reading) {
    try {
      SensorReadingMessage message = new SensorReadingMessage(reading);
      kafkaTemplate.send(kafkaTopic, message);
      return new ResponseEntity<>(HttpStatus.OK);
    }

    catch (Exception e) {
      logger.error("Unexpected Error in POST /ikgw/api/publish", e);
      return new  ResponseEntity<>("Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}