package org.yamunasoftware.ikgw;

import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.io.i2c.I2C;
import com.pi4j.io.i2c.I2CConfig;
import com.pi4j.io.i2c.I2CProvider;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import one.microproject.rpi.hardware.gpio.sensors.BME280;
import one.microproject.rpi.hardware.gpio.sensors.BME280Builder;
import one.microproject.rpi.hardware.gpio.sensors.impl.BME280Impl;

public class Readout {
  private static final Context context = Pi4J.newAutoContext();
  private static final Logger logger = LoggerFactory.getLogger(Readout.class);
  private static final int bus = 1;
  private static final int address = 0x70;
  private static final int channels = 8;
  private static final String multiplexerID = "TCA9548A";
  private static final String providerID = "linuxfs-i2c";

  public static ArrayList<SensorReading> dataReadout() {
    ArrayList<SensorReading> readout = new ArrayList<>();
    String[] systemConfig = Conf.getSystemConfig();
    String deviceId = systemConfig[0];
    String deviceType = systemConfig[1];

    for (int channel = 0; channel < channels; channel++) {
      initChannel(channel);
      SensorReading reading = readChannel(deviceId, deviceType, channel);
      readout.add(reading);
    }
    return readout;
  }

  private static SensorReading readChannel(String deviceId, String deviceType, int channel) {
    try (BME280 bme280 = BME280Builder.get().context(context).build()) {
      BME280Impl.Data data = bme280.getSensorValues();
      float temperature = data.getTemperature();
      float humidity = data.getRelativeHumidity();
      float pressure = data.getPressure();
      return new SensorReading(deviceId, deviceType, channel, temperature, humidity, pressure);
    }

    catch (Exception e) {
      logger.error("Error: Unable to Read Sensor on Channel {}", channel, e);
    }
    return null;
  }

  private static void initChannel(int channel) {
    I2CConfig config = I2C.newConfigBuilder(context).id(multiplexerID).bus(bus).device(address).build();
    I2CProvider provider = context.provider(providerID);
    try (I2C multiplexer = provider.create(config)) {
      int channelByte = 1 << channel;
      multiplexer.write((byte) (channelByte));
    }

    catch (Exception e) {
      logger.error("Error: Unable to Initialize Channel {}", channel, e);
    }
  }
}