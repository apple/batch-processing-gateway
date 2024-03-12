/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.spark.core;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.EnvVar;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.HostPathVolumeSource;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.operator.Volume;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ApplicationSubmissionHelperTest {

  private final String DUMMY_REQUEST_BODY_JSON =
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

  private final String DUMMY_REQUEST_BODY_YAML =
      "---\n"
          + "type: Scala\n"
          + "sparkVersion: '3.0'\n"
          + "mainApplicationFile:"
          + " s3a://artifacts-bucket/uploaded/20220322_9am/metrics-1.0-SNAPSHOT.jar\n"
          + "mainClass: com.apple.Metrics\n"
          + "deps:\n"
          + "  jars:\n"
          + "  - s3a://artifacts-bucket/uploaded/20220322_9am/Dependency-0.7.45.jar\n"
          + "  - s3a://artifacts-bucket/uploaded/20220322_9am/some-jar-1.4.4-3.jar\n"
          + "queue: sde\n"
          + "driver:\n"
          + "  cores: 1\n"
          + "  memory: 1g\n"
          + "  env:\n"
          + "  - name: tgt\n"
          + "    value: xxxxxxxxxxx...\n"
          + "executor:\n"
          + "  instances: 2\n"
          + "  cores: 1\n"
          + "  memory: 1g\n"
          + "\n";

  @Test
  public void getSparkConf_nullDefaultSparkConf() {
    AppConfig appConfig = new AppConfig();
    Map<String, String> defaultSparkConf = null;
    appConfig.setDefaultSparkConf(defaultSparkConf);

    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Map<String, String> sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);

    Assert.assertNull(sparkConf);

    sparkCluster.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 0);

    sparkCluster.getSparkConf().put("key1", "value1");
    sparkCluster.getSparkConf().put("key2", "value2");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.getSparkConf().put("key2", "value2_overwrite");
    request.getSparkConf().put("key3", "value3");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 3);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");

    // check application name
    Assert.assertEquals(sparkConf.get("spark.app.name"), null);

    // set application name in request, then spark conf could contain the spark.app.name config
    request.setApplicationName("app1");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 4);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");
    Assert.assertEquals(sparkConf.get("spark.app.name"), "app1");

    // check submission id populated correctly
    sparkCluster
        .getSparkConf()
        .put("key-with-submission-id", "value-{spark-application-resource-name}");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.get("key-with-submission-id"), "value-submission1");
  }

  @Test
  public void getSparkConf_emptyDefaultSparkConf() {
    AppConfig appConfig = new AppConfig();
    Map<String, String> defaultSparkConf = new HashMap<>();
    appConfig.setDefaultSparkConf(defaultSparkConf);

    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Map<String, String> sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertTrue(sparkConf.isEmpty());

    sparkCluster.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 0);

    sparkCluster.getSparkConf().put("key1", "value1");
    sparkCluster.getSparkConf().put("key2", "value2");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.getSparkConf().put("key2", "value2_overwrite");
    request.getSparkConf().put("key3", "value3");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 3);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");

    // check application name
    Assert.assertEquals(sparkConf.get("spark.app.name"), null);

    // set application name in request, then spark conf could contain the spark.app.name config
    request.setApplicationName("app1");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 4);
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");
    Assert.assertEquals(sparkConf.get("spark.app.name"), "app1");

    // check submission id populated correctly
    sparkCluster
        .getSparkConf()
        .put("key-with-submission-id", "value-{spark-application-resource-name}");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.get("key-with-submission-id"), "value-submission1");
  }

  @Test
  public void getSparkConf_nonEmptyDefaultSparkConf() {
    AppConfig appConfig = new AppConfig();
    Map<String, String> defaultSparkConf = new HashMap<>();
    defaultSparkConf.put("defaultKey1", "defaultValue1");
    defaultSparkConf.put("defaultKey2", "defaultValue2");
    appConfig.setDefaultSparkConf(defaultSparkConf);

    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    Map<String, String> sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf, defaultSparkConf);

    sparkCluster.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 2);
    Assert.assertEquals(sparkConf, defaultSparkConf);

    sparkCluster.getSparkConf().put("key1", "value1");
    sparkCluster.getSparkConf().put("key2", "value2");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 4);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.setSparkConf(new HashMap<>());
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 4);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2");

    request.getSparkConf().put("key2", "value2_overwrite");
    request.getSparkConf().put("key3", "value3");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 5);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");

    // spark cluster's spark conf overwrites default spark conf
    sparkCluster.getSparkConf().put("defaultKey2", "defaultValue2_overwrite");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 5);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2_overwrite");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");

    // submission request's spark conf overwrites default spark conf
    request.getSparkConf().put("defaultKey1", "defaultValue1_overwrite");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 5);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1_overwrite");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2_overwrite");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");

    // check application name
    Assert.assertEquals(sparkConf.get("spark.app.name"), null);

    // set application name in request, then spark conf could contain the spark.app.name config
    request.setApplicationName("app1");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.size(), 6);
    Assert.assertEquals(sparkConf.get("defaultKey1"), "defaultValue1_overwrite");
    Assert.assertEquals(sparkConf.get("defaultKey2"), "defaultValue2_overwrite");
    Assert.assertEquals(sparkConf.get("key1"), "value1");
    Assert.assertEquals(sparkConf.get("key2"), "value2_overwrite");
    Assert.assertEquals(sparkConf.get("key3"), "value3");
    Assert.assertEquals(sparkConf.get("spark.app.name"), "app1");

    // check submission id populated correctly from default spark config
    defaultSparkConf.put("key-with-submission-id", "value-{spark-application-resource-name}");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.get("key-with-submission-id"), "value-submission1");

    // check submission id populated correctly from spark config inside spark cluster
    sparkCluster
        .getSparkConf()
        .put("key-with-submission-id", "value2-{spark-application-resource-name}");
    sparkConf =
        ApplicationSubmissionHelper.getSparkConf("submission1", request, appConfig, sparkCluster);
    Assert.assertEquals(sparkConf.get("key-with-submission-id"), "value2-submission1");
  }

  @Test
  public void getVolumes() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    List<Volume> volumes = ApplicationSubmissionHelper.getVolumes(request, sparkCluster);
    Assert.assertNull(volumes);

    sparkCluster.setVolumes(new ArrayList<>());
    sparkCluster
        .getVolumes()
        .add(new Volume("volume1", new HostPathVolumeSource("/path1", "Directory")));
    sparkCluster
        .getVolumes()
        .add(new Volume("volume2", new HostPathVolumeSource("/path2", "Directory")));

    volumes = ApplicationSubmissionHelper.getVolumes(request, sparkCluster);
    Assert.assertEquals(volumes.size(), 2);
    Assert.assertEquals(volumes.get(0).getName(), "volume1");
    Assert.assertEquals(volumes.get(0).getHostPath().getPath(), "/path1");
    Assert.assertEquals(volumes.get(0).getHostPath().getType(), "Directory");
    Assert.assertEquals(volumes.get(1).getName(), "volume2");
    Assert.assertEquals(volumes.get(1).getHostPath().getPath(), "/path2");
    Assert.assertEquals(volumes.get(1).getHostPath().getType(), "Directory");

    // when there are volumes in request,
    // they should overwrite values from spark cluster
    sparkCluster.setVolumes(new ArrayList<>());
    sparkCluster
        .getVolumes()
        .add(new Volume("volume1", new HostPathVolumeSource("/path1", "Directory")));
    sparkCluster
        .getVolumes()
        .add(new Volume("volume2", new HostPathVolumeSource("/path2", "Directory")));

    request.setVolumes(new ArrayList<>());
    request
        .getVolumes()
        .add(new Volume("volume5", new HostPathVolumeSource("/path5", "Directory")));
    request
        .getVolumes()
        .add(new Volume("volume6", new HostPathVolumeSource("/path6", "Directory")));

    volumes = ApplicationSubmissionHelper.getVolumes(request, sparkCluster);
    Assert.assertEquals(volumes.size(), 2);
    Assert.assertEquals(volumes.get(0).getName(), "volume5");
    Assert.assertEquals(volumes.get(0).getHostPath().getPath(), "/path5");
    Assert.assertEquals(volumes.get(0).getHostPath().getType(), "Directory");
    Assert.assertEquals(volumes.get(1).getName(), "volume6");
    Assert.assertEquals(volumes.get(1).getHostPath().getPath(), "/path6");
    Assert.assertEquals(volumes.get(1).getHostPath().getType(), "Directory");
  }

  @Test
  public void getDriverSpec() {
    // TODO: add tests
  }

  @Test
  public void getExecutorSpec() {
    // TODO: add tests
  }

  @Test
  public void populateEnv() {
    SparkApplicationSpec sparkSpec = new SparkApplicationSpec();
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig.SparkCluster sparkCluster = new AppConfig.SparkCluster();
    ApplicationSubmissionHelper.populateEnv(sparkSpec, request, sparkCluster);
    Assert.assertNull(sparkSpec.getDriver());
    Assert.assertNull(sparkSpec.getExecutor());

    sparkCluster.setDriver(new DriverSpec());
    sparkCluster.getDriver().setEnv(new ArrayList<>());
    sparkCluster.getDriver().getEnv().add(new EnvVar("env1", "value1"));
    sparkCluster.getDriver().getEnv().add(new EnvVar("env2", "value2"));
    sparkSpec = new SparkApplicationSpec();
    ApplicationSubmissionHelper.populateEnv(sparkSpec, request, sparkCluster);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().size(), 2);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getName(), "env1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getValue(), "value1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getName(), "env2");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getValue(), "value2");
    Assert.assertNull(sparkSpec.getExecutor());

    sparkCluster.setExecutor(new ExecutorSpec());
    sparkCluster.getExecutor().setEnv(new ArrayList<>());
    sparkCluster.getExecutor().getEnv().add(new EnvVar("env3", "value3"));
    sparkCluster.getExecutor().getEnv().add(new EnvVar("env4", "value4"));
    sparkSpec = new SparkApplicationSpec();
    ApplicationSubmissionHelper.populateEnv(sparkSpec, request, sparkCluster);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().size(), 2);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getName(), "env1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getValue(), "value1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getName(), "env2");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getValue(), "value2");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().size(), 2);
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getName(), "env3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getValue(), "value3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getName(), "env4");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getValue(), "value4");

    // there is env in request, that will overwrite values from spark cluster
    request.setDriver(new DriverSpec());
    request.getDriver().setEnv(new ArrayList<>());
    request.getDriver().getEnv().add(new EnvVar("env2", "value2_overwrite"));
    request.getDriver().getEnv().add(new EnvVar("env3", "value3"));
    sparkSpec = new SparkApplicationSpec();
    ApplicationSubmissionHelper.populateEnv(sparkSpec, request, sparkCluster);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().size(), 3);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getName(), "env1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getValue(), "value1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getName(), "env2");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getValue(), "value2_overwrite");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(2).getName(), "env3");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(2).getValue(), "value3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().size(), 2);
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getName(), "env3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getValue(), "value3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getName(), "env4");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getValue(), "value4");

    request.setExecutor(new ExecutorSpec());
    request.getExecutor().setEnv(new ArrayList<>());
    request.getExecutor().getEnv().add(new EnvVar("env4", "value4_overwrite"));
    request.getExecutor().getEnv().add(new EnvVar("env5", "value5"));
    sparkSpec = new SparkApplicationSpec();
    ApplicationSubmissionHelper.populateEnv(sparkSpec, request, sparkCluster);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().size(), 3);
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getName(), "env1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(0).getValue(), "value1");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getName(), "env2");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(1).getValue(), "value2_overwrite");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(2).getName(), "env3");
    Assert.assertEquals(sparkSpec.getDriver().getEnv().get(2).getValue(), "value3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().size(), 3);
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getName(), "env3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(0).getValue(), "value3");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getName(), "env4");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(1).getValue(), "value4_overwrite");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(2).getName(), "env5");
    Assert.assertEquals(sparkSpec.getExecutor().getEnv().get(2).getValue(), "value5");
  }

  @Test
  public void generateSubmissionId() {
    Assert.assertTrue(ApplicationSubmissionHelper.generateSubmissionId("c01").startsWith("c01-"));
    Assert.assertTrue(ApplicationSubmissionHelper.generateSubmissionId("abc").startsWith("abc-"));
    Assert.assertEquals(36, ApplicationSubmissionHelper.generateSubmissionId("d01").length());
  }

  @Test
  public void getClusterIdFromSubmissionId() {
    Assert.assertEquals("c01", ApplicationSubmissionHelper.getClusterIdFromSubmissionId("c01-abc"));
    Assert.assertEquals(
        "c01", ApplicationSubmissionHelper.getClusterIdFromSubmissionId("c01-123-abc"));
    Assert.assertEquals("ddd", ApplicationSubmissionHelper.getClusterIdFromSubmissionId("ddd-abc"));
  }

  @Test(expectedExceptions = InvalidSubmissionIdException.class)
  public void getClusterIdFromSubmissionId_nullValue() {
    ApplicationSubmissionHelper.getClusterIdFromSubmissionId(null);
  }

  @Test(expectedExceptions = InvalidSubmissionIdException.class)
  public void getClusterIdFromSubmissionId_emptyString() {
    ApplicationSubmissionHelper.getClusterIdFromSubmissionId("");
  }

  @Test(expectedExceptions = InvalidSubmissionIdException.class)
  public void getClusterIdFromSubmissionId_invalidString() {
    ApplicationSubmissionHelper.getClusterIdFromSubmissionId("invalid_string");
  }

  @Test
  public void validateQueueToken() {
    AppConfig appConfig = new AppConfig();
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);

    appConfig.setQueues(new ArrayList<>());
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", "", appConfig);

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("anotherQueue");
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);

    queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("queue1");
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void validateQueueToken_nullQueueTokenWithSecureQueue() {
    AppConfig appConfig = new AppConfig();
    appConfig.setQueues(new ArrayList<>());

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("anotherQueue");
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);

    queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("queue1");
    queueConfig.setSecure(true);
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void validateQueueToken_emptyStringQueueTokenWithSecureQueue() {
    AppConfig appConfig = new AppConfig();
    appConfig.setQueues(new ArrayList<>());

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("anotherQueue");
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", null, appConfig);

    queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("queue1");
    queueConfig.setSecure(true);
    appConfig.getQueues().add(queueConfig);
    ApplicationSubmissionHelper.validateQueueToken("queue1", "", appConfig);
  }

  @Test
  public void requestBodyCheck_suspiciousFilePathLike() {
    Assert.assertTrue(ApplicationSubmissionHelper.looksLikeFilePath("~/Downloads/cool_stuff.txt"));
    Assert.assertTrue(ApplicationSubmissionHelper.looksLikeFilePath("/Users/tim/Downloads"));

    Assert.assertFalse(ApplicationSubmissionHelper.looksLikeFilePath(DUMMY_REQUEST_BODY_JSON));
    Assert.assertFalse(ApplicationSubmissionHelper.looksLikeFilePath(DUMMY_REQUEST_BODY_YAML));
  }
}
