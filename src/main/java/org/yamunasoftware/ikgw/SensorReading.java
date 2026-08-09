package org.yamunasoftware.ikgw;

public class SensorReading {
  public String deviceId;
  public String deviceType;
  public int channel;
  public float temperature;
  public float humidity;
  public float pressure;

  public SensorReading(
      String deviceId, String deviceType, int channel, float temperature, float humidity, float pressure) {
    this.deviceId = deviceId;
    this.deviceType = deviceType;
    this.channel = channel;
    this.temperature = temperature;
    this.humidity = humidity;
    this.pressure = pressure / 1000;
  }
}