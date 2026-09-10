package org.yamunasoftware.ikgw;

import java.time.Instant;

public class SensorReadingMessage {
  public String deviceId;
  public String deviceType;
  public int channel;
  public float temperature;
  public float humidity;
  public float pressure;
  public long readingTimestamp;
  public long recievedTimestamp;

  public SensorReadingMessage(SensorReading reading) {
    this.deviceId = reading.deviceId;
    this.deviceType = reading.deviceType;
    this.channel = reading.channel;
    this.temperature = reading.temperature;
    this.humidity = reading.humidity;
    this.pressure = reading.pressure;
    this.readingTimestamp = reading.timestamp;
    this.recievedTimestamp = Instant.now().getEpochSecond();
  }
}