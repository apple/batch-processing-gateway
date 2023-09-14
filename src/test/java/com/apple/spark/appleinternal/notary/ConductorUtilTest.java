package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

public class ConductorUtilTest {
  private static SparkApplicationSpec sparkSpec;
  private static AppConfig appConfig;

  @Before
  public void setUp() {
    appConfig = new AppConfig();
    AppConfig.ConductorConfig conductorConfig = new AppConfig.ConductorConfig();
    conductorConfig.setConductorEndpoint("https://conductor-qa.aws.sea.g.apple.com");
    appConfig.setConductor(conductorConfig);
    sparkSpec = new SparkApplicationSpec();
    sparkSpec.setDriver(new DriverSpec());
    sparkSpec.setExecutor(new ExecutorSpec());
  }

  @Test
  public void testConductorEndpoint_Enabled() {
    ConductorUtil.setConductorEndpoint(appConfig, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNotNull(envVars);
    Assert.assertEquals(envVars.get(0).getName(), NotaryConstants.CONDUCTOR_ENDPOINT_KEY);
    Assert.assertEquals(envVars.get(0).getValue(), "https://conductor-qa.aws.sea.g.apple.com");
  }

  @Test
  public void testConductorEndpoint_Disabled() {
    appConfig.setConductor(null);
    ConductorUtil.setConductorEndpoint(appConfig, sparkSpec);
    List<EnvVar> envVars = sparkSpec.getDriver().getEnv();
    Assert.assertNull(envVars);
  }
}
