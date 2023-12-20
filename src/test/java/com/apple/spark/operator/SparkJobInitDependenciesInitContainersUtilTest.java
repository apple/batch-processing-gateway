package com.apple.spark.operator;

import static com.apple.spark.appleinternal.AppleKerberosUtilConstants.DELEGATION_CONTAINER_NAME;
import static com.apple.spark.appleinternal.AppleKerberosUtilConstants.INIT_CONTAINER_ENV_KEY;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.appleinternal.AppleKerberosUtil;
import com.apple.spark.core.ApplicationSubmissionHelper;
import com.apple.spark.core.BPGStatsdConfig;
import com.apple.spark.util.TimerMetricContainer;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SparkJobInitDependenciesInitContainersUtilTest {

  public static final String CONTAINER_IMAGE =
      "docker.apple.com/aiml-datainfra/delegation-token-tool:snapshot-latest";
  MeterRegistry meterRegistry = BPGStatsdConfig.createMeterRegistry();
  String CONTENT_TYPE = "Content-Type: application/json";
  String proxyUser = "raimldpi";
  AppConfig appConfig = new AppConfig();
  String REQUEST_BODY_JSON =
      "{\n"
          + "  \"type\": \"Scala\",\n"
          + "  \"sparkVersion\": \"3.0\",\n"
          + "  \"mainApplicationFile\":"
          + " \"s3a://artifacts-bucket/uploaded/20220322_9am/metrics-1.0-SNAPSHOT.jar\",\n"
          + "  \"mainClass\": \"com.apple.Metrics\",\n"
          + "  \"deps\": {\n"
          + "    \"jars\": [\n"
          + "      \"s3a://artifacts-bucket/uploaded/20220322_9am/Dependency-0.7.45.jar\",\n"
          + "      \"s3a://artifacts-bucket/uploaded/20220322_9am/some-jar-1.4.4-3.jar\"\n"
          + "    ]\n"
          + "  },\n"
          + "  \"jobInitDependencies\": {\n"
          + "    \"urls\": [\n"
          + "      \"s3a://artifacts-bucket/uploaded/20220322_9am/Dependency-0.7.45.jar\",\n"
          + "      \"s3a://artifacts-bucket/uploaded/20220322_9am/some-jar-1.4.4-3.jar\"\n"
          + "    ]\n"
          + "  },\n"
          + "  \"queue\": \"sde\",\n"
          + "  \"sparkConf\": {\n"
          + "    \"spark.hadoop.hive.metastore.sasl.enabled\": \"true\",\n"
          + "    \"spark.hadoop.hive.metastore.uris\":"
          + " \"thrift://hive-metastore-siri-test.aws.ocean.g.apple.com:9083\"\n"
          + "  },\n"
          + "  \"driver\": {\n"
          + "    \"cores\": 1,\n"
          + "    \"memory\": \"1g\",\n"
          + "    \"env\": [\n"
          + "      {\n"
          + "        \"name\": \"tgt\",\n"
          + "        \"value\": \"xxxxxxxxxxx...\"\n"
          + "      }\n"
          + "    ]\n"
          + "  },\n"
          + "  \"executor\": {\n"
          + "    \"instances\": 2,\n"
          + "    \"cores\": 1,\n"
          + "    \"memory\": \"1g\"\n"
          + "  }\n"
          + "}";

  String REQUEST_BODY_NO_HMS_JSON =
      "{\n"
          + "  \"type\": \"Scala\",\n"
          + "  \"sparkVersion\": \"3.0\",\n"
          + "  \"mainApplicationFile\":"
          + " \"s3a://artifacts-bucket/uploaded/20220322_9am/metrics-1.0-SNAPSHOT.jar\",\n"
          + "  \"mainClass\": \"com.apple.Metrics\",\n"
          + "  \"deps\": {\n"
          + "    \"jars\": [\n"
          + "     "
          + " \"s3a://artifacts-bucket/uploaded/20220322_9am/Dependency-0.7.45.jar\",\n"
          + "      \"s3a://artifacts-bucket/uploaded/20220322_9am/some-jar-1.4.4-3.jar\"\n"
          + "    ]\n"
          + "  },\n"
          + "  \"queue\": \"sde\",\n"
          + "  \"driver\": {\n"
          + "    \"cores\": 1,\n"
          + "    \"memory\": \"1g\",\n"
          + "    \"env\": [\n"
          + "      {\n"
          + "        \"name\": \"tgt\",\n"
          + "        \"value\": \"xxxxxxxxxxx...\"\n"
          + "      }\n"
          + "    ]\n"
          + "  },\n"
          + "  \"executor\": {\n"
          + "    \"instances\": 2,\n"
          + "    \"cores\": 1,\n"
          + "    \"memory\": \"1g\"\n"
          + "  }\n"
          + "}";

  @Test
  public void noUserDependenciesOnRequest() {

    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request =
        ApplicationSubmissionHelper.parseSubmitRequest(REQUEST_BODY_NO_HMS_JSON, CONTENT_TYPE);
    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);

    SparkJobInitDependenciesInitContainerUtil.enableUserInitDependenciesSupport(
        sparkSpec, request, appConfig);

    Assert.assertNull(sparkSpec.getVolumes());

    Assert.assertNull(sparkSpec.getDriver().getInitContainers());
  }

  @Test
  public void enableUserDependenciesInitContainersTest() {

    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request =
        ApplicationSubmissionHelper.parseSubmitRequest(REQUEST_BODY_JSON, CONTENT_TYPE);
    TimerMetricContainer timerMetric = new TimerMetricContainer(meterRegistry);

    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);

    SparkJobInitDependenciesInitContainerUtil.enableUserInitDependenciesSupport(
        sparkSpec, request, appConfig);

    Assert.assertEquals(sparkSpec.getVolumes().get(0).getName(), "dependencies");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(0).getVolumeMounts().get(0).getName(),
        "dependencies");

    Assert.assertEquals(sparkSpec.getDriver().getGwInitContainers(), null);
    Assert.assertEquals(sparkSpec.getDriver().getInitContainers().size(), 1);
  }

  @Test
  public void enableUserDependenciesInitContainersTestWithKerberos() {

    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request =
        ApplicationSubmissionHelper.parseSubmitRequest(REQUEST_BODY_JSON, CONTENT_TYPE);
    TimerMetricContainer timerMetric = new TimerMetricContainer(meterRegistry);

    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);

    SparkJobInitDependenciesInitContainerUtil.enableUserInitDependenciesSupport(
        sparkSpec, request, appConfig);

    List<InitContainer> driverInitContainers =
        Collections.singletonList(new InitContainer(DELEGATION_CONTAINER_NAME, CONTAINER_IMAGE));
    appConfig.setDriverInitContainers(driverInitContainers);
    AppleKerberosUtil.enableKerberosSupport(sparkSpec, request, appConfig, proxyUser, timerMetric);

    Assert.assertEquals(sparkSpec.getVolumes().get(0).getName(), "dependencies");
    Assert.assertEquals(sparkSpec.getVolumes().get(1).getName(), "narrative");
    Assert.assertEquals(sparkSpec.getVolumes().get(2).getName(), "token-store");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(0).getVolumeMounts().get(0).getName(),
        "dependencies");

    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(0).getName(), "dependencies");
    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(1).getName(), "token-store");
    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(2).getName(), "narrative");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(0).getVolumeMounts().get(0).getName(),
        "dependencies");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(1).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);
    Assert.assertEquals(
        sparkSpec.getDriver().getGwInitContainers().get(0).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);

    Assert.assertEquals(sparkSpec.getDriver().getGwInitContainers().size(), 1);
    Assert.assertEquals(sparkSpec.getDriver().getInitContainers().size(), 2);
  }

  @Test
  public void enableUserDependenciesInitContainersTestAfterKerberos() {

    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request =
        ApplicationSubmissionHelper.parseSubmitRequest(REQUEST_BODY_JSON, CONTENT_TYPE);
    TimerMetricContainer timerMetric = new TimerMetricContainer(meterRegistry);

    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);

    List<InitContainer> driverInitContainers =
        Collections.singletonList(new InitContainer(DELEGATION_CONTAINER_NAME, CONTAINER_IMAGE));
    appConfig.setDriverInitContainers(driverInitContainers);
    AppleKerberosUtil.enableKerberosSupport(sparkSpec, request, appConfig, proxyUser, timerMetric);

    SparkJobInitDependenciesInitContainerUtil.enableUserInitDependenciesSupport(
        sparkSpec, request, appConfig);

    Assert.assertEquals(sparkSpec.getVolumes().get(0).getName(), "narrative");
    Assert.assertEquals(sparkSpec.getVolumes().get(1).getName(), "token-store");
    Assert.assertEquals(sparkSpec.getVolumes().get(2).getName(), "dependencies");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(1).getVolumeMounts().get(0).getName(),
        "dependencies");

    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(0).getName(), "token-store");
    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(1).getName(), "narrative");
    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(2).getName(), "dependencies");

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(0).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);
    Assert.assertEquals(
        sparkSpec.getDriver().getGwInitContainers().get(0).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);

    Assert.assertEquals(sparkSpec.getDriver().getGwInitContainers().size(), 1);
    Assert.assertEquals(sparkSpec.getDriver().getInitContainers().size(), 2);
  }

  @Test
  public void disableUserDependenciesInitContainersTestAfterKerberos() {

    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request =
        ApplicationSubmissionHelper.parseSubmitRequest(REQUEST_BODY_JSON, CONTENT_TYPE);
    TimerMetricContainer timerMetric = new TimerMetricContainer(meterRegistry);

    DriverSpec driverSpec = new DriverSpec();
    sparkSpec.setDriver(driverSpec);

    List<InitContainer> driverInitContainers =
        Collections.singletonList(new InitContainer(DELEGATION_CONTAINER_NAME, CONTAINER_IMAGE));
    appConfig.setDriverInitContainers(driverInitContainers);
    AppleKerberosUtil.enableKerberosSupport(sparkSpec, request, appConfig, proxyUser, timerMetric);

    Assert.assertEquals(sparkSpec.getVolumes().get(0).getName(), "narrative");
    Assert.assertEquals(sparkSpec.getVolumes().get(1).getName(), "token-store");

    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(0).getName(), "token-store");
    Assert.assertEquals(sparkSpec.getDriver().getVolumeMounts().get(1).getName(), "narrative");

    Assert.assertEquals(sparkSpec.getDriver().getGwInitContainers().size(), 1);
    Assert.assertEquals(sparkSpec.getDriver().getInitContainers().size(), 1);

    Assert.assertEquals(
        sparkSpec.getDriver().getInitContainers().get(0).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);
    Assert.assertEquals(
        sparkSpec.getDriver().getGwInitContainers().get(0).getEnv().get(0).getName(),
        INIT_CONTAINER_ENV_KEY);
  }
}
