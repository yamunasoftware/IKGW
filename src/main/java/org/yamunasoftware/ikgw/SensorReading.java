package org.yamunasoftware.ikgw;

public class SensorReading {
  public int channel;
  public String type;
  public float temperature;
  public float humidity;
  public float pressure;

  public SensorReading(int channel, String type, float temperature, float humidity, float pressure) {
    this.channel = channel;
    this.type = type;
    this.temperature = temperature;
    this.humidity = humidity;
    this.pressure = pressure / 1000;
  }
}