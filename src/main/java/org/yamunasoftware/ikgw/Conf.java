package org.yamunasoftware.ikgw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

public class Conf {
  private static final Logger logger = LoggerFactory.getLogger(Conf.class);
  private static final String systemInfoFile = ".conf";

  public static String getKafkaUrl() {
    try {
      Path path = Path.of(systemInfoFile);
      String content = Files.readString(path);
      String[] lines = content.split("\n");

      for (String line : lines) {
        if (line.contains("KAFKA_URL")) {
          return line.replace("KAFKA_URL=", "");
        }
      }
    }

    catch (IOException e) {
      logger.error("Error: Unable to Open System Info", e);
    }
    return null;
  }

  public static String[] getSystemConfig() {
    String[] config = new String[2];
    try {
      Path path = Path.of(systemInfoFile);
      String content = Files.readString(path);
      String[] lines = content.split("\n");

      for (String line : lines) {
        if (line.contains("SYSTEM_ID")) {
          config[0] = line.replace("SYSTEM_INFO=", "");
        }

        else if (line.contains("SYSTEM_TYPE")) {
          config[1] = line.replace("SYSTEM_TYPE=", "");
        }
      }
    }

    catch (IOException e) {
      logger.error("Error: Unable to Read Conf File", e);
    }
    return config;
  }
}