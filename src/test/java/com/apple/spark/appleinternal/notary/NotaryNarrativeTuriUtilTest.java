package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.operator.Volume;
import com.apple.spark.operator.VolumeMount;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NotaryNarrativeTuriUtilTest {
  private static SparkApplicationSpec sparkApplicationSpec;
  private static AppConfig appconfi;

  @BeforeClass
  public static void setupClass() {
    sparkApplicationSpec = new SparkApplicationSpec();
    appconfi = new AppConfig();
    AppConfig.NotaryAppConfig notary = new AppConfig.NotaryAppConfig();
    notary.setNarrativeTuriPodDomain("aws");
    notary.setAppNameSpace("bpg-siri-aws-test");
    appconfi.setNotary(notary);
  }

  @Test
  public void testAddNotaryNarrativeTuriVolume() {
    String personId = "123456";
    NotaryNarrativeTuriUtil.addNotaryNarrativeTuriVolume(sparkApplicationSpec, appconfi, personId);
    List<Volume> volumeList = sparkApplicationSpec.getVolumes();
    Assert.assertEquals(volumeList.size(), 1);
    Volume notaryNarrativeTuriVolume = volumeList.get(0);
    Assert.assertEquals(notaryNarrativeTuriVolume.getName(), "turi-identity");

    VolumeMount driverVolumeMount = sparkApplicationSpec.getDriver().getVolumeMounts().get(0);
    Assert.assertEquals(driverVolumeMount.getName(), "turi-identity");

    VolumeMount execVolumeMount = sparkApplicationSpec.getExecutor().getVolumeMounts().get(0);
    Assert.assertEquals(execVolumeMount.getName(), "turi-identity");
  }

  @Test
  public void testAddNotaryNarrativeTuriVolume_NoDriverAndExecutor() {
    String personId = "123456";
    SparkApplicationSpec sparkApplicationSpecNoDriverAndExec = new SparkApplicationSpec();
    NotaryNarrativeTuriUtil.addNotaryNarrativeTuriVolume(
        sparkApplicationSpecNoDriverAndExec, appconfi, personId);
    List<Volume> volumeList = sparkApplicationSpecNoDriverAndExec.getVolumes();
    Assert.assertEquals(volumeList.size(), 1);
    Volume notaryNarrativeTuriVolume = volumeList.get(0);
    Assert.assertEquals(notaryNarrativeTuriVolume.getName(), "turi-identity");

    VolumeMount driverVolumeMount =
        sparkApplicationSpecNoDriverAndExec.getDriver().getVolumeMounts().get(0);
    Assert.assertEquals(driverVolumeMount.getName(), "turi-identity");

    VolumeMount execVolumeMount =
        sparkApplicationSpecNoDriverAndExec.getExecutor().getVolumeMounts().get(0);
    Assert.assertEquals(execVolumeMount.getName(), "turi-identity");
  }

  @Test
  public void testaddNotaryNarrativeTuriAnnotation() {
    String personId = "123456";
    NotaryNarrativeTuriUtil.addNotaryNarrativeTuriAnnotation(sparkApplicationSpec, personId);
    DriverSpec driverSpec = sparkApplicationSpec.getDriver();
    Map<String, String> driverAnnotations = driverSpec.getAnnotations();
    Assert.assertEquals(
        driverAnnotations.get(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY), "123456");

    ExecutorSpec executorSpec = sparkApplicationSpec.getExecutor();
    Map<String, String> executorAnnotations = executorSpec.getAnnotations();
    Assert.assertEquals(
        executorAnnotations.get(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY), "123456");
  }

  @Test
  public void testaddNotaryNarrativeTuriAnnotation_NoDriverAndExecutor() {
    String personId = "123456";
    SparkApplicationSpec sparkApplicationSpecNoDriverAndExec = new SparkApplicationSpec();
    NotaryNarrativeTuriUtil.addNotaryNarrativeTuriAnnotation(
        sparkApplicationSpecNoDriverAndExec, personId);
    DriverSpec driverSpec = sparkApplicationSpecNoDriverAndExec.getDriver();
    Map<String, String> driverAnnotations = driverSpec.getAnnotations();
    Assert.assertEquals(
        driverAnnotations.get(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY), "123456");

    ExecutorSpec executorSpec = sparkApplicationSpecNoDriverAndExec.getExecutor();
    Map<String, String> executorAnnotations = executorSpec.getAnnotations();
    Assert.assertEquals(
        executorAnnotations.get(NotaryConstants.NARRATIVE_TURI_ANNO_PERSON_ID_KEY), "123456");
  }

  @Test
  public void testAddNotaryNarrativeEnvVar() {
    NotaryNarrativeTuriUtil.addNotaryNarrativeEnvVar(sparkApplicationSpec);
    List<EnvVar> driverEnvVars = sparkApplicationSpec.getDriver().getEnv();
    org.testng.Assert.assertNotNull(driverEnvVars);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(0).getName(), NotaryConstants.MTLS_CRT_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(0).getValue(), NotaryConstants.MTLS_CRT_LOCATION_VAL);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(1).getName(), NotaryConstants.MTLS_KEY_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(1).getValue(), NotaryConstants.MTLS_KEY_LOCATION_VAL);

    List<EnvVar> execEnvVars = sparkApplicationSpec.getExecutor().getEnv();
    org.testng.Assert.assertNotNull(execEnvVars);
    org.testng.Assert.assertEquals(
        execEnvVars.get(0).getName(), NotaryConstants.MTLS_CRT_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        execEnvVars.get(0).getValue(), NotaryConstants.MTLS_CRT_LOCATION_VAL);
    org.testng.Assert.assertEquals(
        execEnvVars.get(1).getName(), NotaryConstants.MTLS_KEY_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        execEnvVars.get(1).getValue(), NotaryConstants.MTLS_KEY_LOCATION_VAL);
  }

  @Test
  public void testAddNotaryNarrativeEnvVar_NoDriverAndExecutor() {
    SparkApplicationSpec sparkApplicationSpecNoDriverAndExec = new SparkApplicationSpec();
    NotaryNarrativeTuriUtil.addNotaryNarrativeEnvVar(sparkApplicationSpecNoDriverAndExec);
    List<EnvVar> driverEnvVars = sparkApplicationSpecNoDriverAndExec.getDriver().getEnv();
    org.testng.Assert.assertNotNull(driverEnvVars);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(0).getName(), NotaryConstants.MTLS_CRT_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(0).getValue(), NotaryConstants.MTLS_CRT_LOCATION_VAL);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(1).getName(), NotaryConstants.MTLS_KEY_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        driverEnvVars.get(1).getValue(), NotaryConstants.MTLS_KEY_LOCATION_VAL);

    List<EnvVar> execEnvVars = sparkApplicationSpecNoDriverAndExec.getExecutor().getEnv();
    org.testng.Assert.assertNotNull(execEnvVars);
    org.testng.Assert.assertEquals(
        execEnvVars.get(0).getName(), NotaryConstants.MTLS_CRT_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        execEnvVars.get(0).getValue(), NotaryConstants.MTLS_CRT_LOCATION_VAL);
    org.testng.Assert.assertEquals(
        execEnvVars.get(1).getName(), NotaryConstants.MTLS_KEY_LOCATION_KEY);
    org.testng.Assert.assertEquals(
        execEnvVars.get(1).getValue(), NotaryConstants.MTLS_KEY_LOCATION_VAL);
  }
}
