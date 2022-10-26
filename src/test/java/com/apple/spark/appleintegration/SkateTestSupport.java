package com.apple.spark.appleintegration;

import com.apple.spark.AppConfig;
import com.apple.spark.BPGApplication;
import com.apple.spark.core.ApplicationMonitor;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class SkateTestSupport {

  private final String configFilePath = new File("config.yml").getAbsolutePath();

  private final DropwizardTestSupport<AppConfig> testSupport;

  private ApplicationMonitor applicationMonitor;

  public SkateTestSupport() {
    Path path;
    try {
      path = Files.createTempDirectory("h2_test_db_");
    } catch (Exception e) {
      throw new RuntimeException("Failed to create temp file for h2 db", e);
    }
    path.toFile().deleteOnExit();

    String connectionString =
        String.format(
            "jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MySQL;AUTO_SERVER=TRUE;USER=sa;PASSWORD=sa;",
            path.toFile().getAbsolutePath() + "/skate");

    testSupport =
        new DropwizardTestSupport<>(
            BPGApplication.class,
            configFilePath,
            ConfigOverride.config("server.applicationConnectors[0].port", "0"),
            ConfigOverride.config("dbStorageSOPS.connectionString", connectionString),
            ConfigOverride.config("dbStorageSOPS.user", "sa"),
            ConfigOverride.config("dbStorageSOPS.password", "sa"),
            ConfigOverride.config("dbStorageSOPS.dbName", "skate"));
  }

  public void before() {
    try {
      testSupport.before();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    applicationMonitor =
        new ApplicationMonitor(testSupport.getConfiguration(), new LoggingMeterRegistry());
    applicationMonitor.start();
  }

  public void after() {
    applicationMonitor.close();

    testSupport.after();
  }

  public int getLocalPort() {
    return testSupport.getLocalPort();
  }

  public AppConfig getConfiguration() {
    return testSupport.getConfiguration();
  }
}
