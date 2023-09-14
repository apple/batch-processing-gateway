package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConductorUtil {
  private static final Logger logger = LoggerFactory.getLogger(ConductorUtil.class);

  /**
   * If the Conductor flag is enabled, this function will add $CONDUCTOR_ENDPOINT as ENV VAR of the
   * Spark pod, which will be used by the Conductor aws profile inside the Spark pod.
   *
   * @param appConfig
   * @param sparkSpec
   */
  public static void setConductorEndpoint(AppConfig appConfig, SparkApplicationSpec sparkSpec) {
    AppConfig.ConductorConfig conductorConfig = appConfig.getConductor();
    if (conductorConfig == null) {
      logger.info("Conductor is not enabled.");
    } else {
      // get conductorEndpoint from app config
      String conductorEndpoint = conductorConfig.getConductorEndpoint();
      if (conductorEndpoint == null) {
        logger.warn("Missing Conductor Endpoint in the config!");
      } else {
        // set conductorEndpoint as env variable of driver pod and executor pod
        logger.info("Conductor Endpoint: {}", conductorEndpoint);
        EnvVar conductoreEndpointEnvVar =
            new EnvVar(NotaryConstants.CONDUCTOR_ENDPOINT_KEY, conductorEndpoint);

        if (sparkSpec.getDriver() == null) {
          sparkSpec.setDriver(new DriverSpec());
        }
        if (sparkSpec.getDriver().getEnv() == null) {
          sparkSpec.getDriver().setEnv(new ArrayList<EnvVar>());
        }
        sparkSpec.getDriver().getEnv().add(conductoreEndpointEnvVar);

        if (sparkSpec.getExecutor() == null) {
          sparkSpec.setExecutor(new ExecutorSpec());
        }
        if (sparkSpec.getExecutor().getEnv() == null) {
          sparkSpec.getExecutor().setEnv(new ArrayList<EnvVar>());
        }
        sparkSpec.getExecutor().getEnv().add(conductoreEndpointEnvVar);
      }
    }
  }
}
