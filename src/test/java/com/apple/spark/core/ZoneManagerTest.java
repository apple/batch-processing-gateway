package com.apple.spark.core;

import static com.apple.spark.core.ZoneManager.ROUND_ROBIN_ZONE_PICKER_NAME;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ZoneManagerTest {

  @Test
  private void testNoZoneSetInQueue() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig appConfig = new AppConfig();
    String queueName = "test-queue-no-zone-set";
    String submissionId = "test-submission-id";
    AppConfig.QueueConfig testQueueConfig = new AppConfig.QueueConfig();
    testQueueConfig.setName(queueName);
    appConfig.setQueues(List.of(testQueueConfig));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    List<String> output =
        ZoneManager.pickZones(request, appConfig, queueName, sparkClusterSpec, submissionId);

    Assert.assertNull(output);
  }

  @Test
  private void testEmptyZoneListSetInQueue() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig appConfig = new AppConfig();
    String queueName = "test-queue-empty-zone-list";
    String submissionId = "test-submission-id";
    AppConfig.QueueConfig testQueueConfig = new AppConfig.QueueConfig();
    testQueueConfig.setAllowedZones(Collections.emptyList());
    testQueueConfig.setName(queueName);
    appConfig.setQueues(List.of(testQueueConfig));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    List<String> output =
        ZoneManager.pickZones(request, appConfig, queueName, sparkClusterSpec, submissionId);

    Assert.assertNull(output);
  }

  @Test
  private void testSomeEmptyZoneSetInQueue() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig appConfig = new AppConfig();
    String queueName = "test-queue-some-empty-zone";
    String submissionId = "test-submission-id";
    AppConfig.QueueConfig testQueueConfig = new AppConfig.QueueConfig();
    List<String> list = new ArrayList<>();
    list.add(null);
    testQueueConfig.setAllowedZones(list);
    testQueueConfig.setName(queueName);
    appConfig.setQueues(List.of(testQueueConfig));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    Assert.assertThrows(
        RuntimeException.class,
        () -> ZoneManager.pickZones(request, appConfig, queueName, sparkClusterSpec, submissionId));
  }

  @Test
  private void testOneZone() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    AppConfig appConfig = new AppConfig();
    String queueName = "test-queue-one-zone-one-queue";
    String submissionId = "test-submission-id";
    AppConfig.QueueConfig testQueueConfig = new AppConfig.QueueConfig();
    testQueueConfig.setName(queueName);
    testQueueConfig.setAllowedZones(List.of("ZoneA"));
    appConfig.setQueues(List.of(testQueueConfig));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    List<String> output =
        ZoneManager.pickZones(request, appConfig, queueName, sparkClusterSpec, submissionId);

    Assert.assertEquals(output.size(), 1);
    Assert.assertEquals(output.get(0), "ZoneA");
  }

  @Test
  private void testRoundRobinTwoZones() {
    SubmitApplicationRequest request1 = new SubmitApplicationRequest();
    String submissionId1 = "test-submission-id-1";
    AppConfig appConfig = new AppConfig();
    String queueName = "test-queue-two-zone-one-queue";
    AppConfig.QueueConfig testQueueConfig = new AppConfig.QueueConfig();
    testQueueConfig.setName(queueName);
    testQueueConfig.setAllowedZones(List.of("ZoneA", "ZoneB"));
    testQueueConfig.setZonePickerName(ROUND_ROBIN_ZONE_PICKER_NAME);
    appConfig.setQueues(List.of(testQueueConfig));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    List<String> pick1 =
        ZoneManager.pickZones(request1, appConfig, queueName, sparkClusterSpec, submissionId1);

    Assert.assertEquals(pick1.size(), 1);
    Assert.assertEquals(pick1.get(0), "ZoneA");

    SubmitApplicationRequest request2 = new SubmitApplicationRequest();
    String submissionId2 = "test-submission-id-2";

    List<String> pick2 =
        ZoneManager.pickZones(request2, appConfig, queueName, sparkClusterSpec, submissionId2);

    Assert.assertEquals(pick2.size(), 1);
    Assert.assertEquals(pick2.get(0), "ZoneB");

    SubmitApplicationRequest request3 = new SubmitApplicationRequest();
    String submissionId3 = "test-submission-id-3";

    List<String> pick3 =
        ZoneManager.pickZones(request3, appConfig, queueName, sparkClusterSpec, submissionId3);

    Assert.assertEquals(pick3.size(), 1);
    Assert.assertEquals(pick3.get(0), "ZoneA");
  }

  @Test
  private void testRoundRobinTwoQueuesTwoZones() {
    AppConfig appConfig = new AppConfig();
    String queueName1 = "test-queue-two-zone-one-queue-1";
    AppConfig.QueueConfig testQueueConfig1 = new AppConfig.QueueConfig();
    testQueueConfig1.setName(queueName1);
    testQueueConfig1.setAllowedZones(List.of("ZoneA", "ZoneB"));
    testQueueConfig1.setZonePickerName(ROUND_ROBIN_ZONE_PICKER_NAME);

    String queueName2 = "test-queue-two-zone-one-queue-2";
    AppConfig.QueueConfig testQueueConfig2 = new AppConfig.QueueConfig();
    testQueueConfig2.setName(queueName2);
    testQueueConfig2.setAllowedZones(List.of("ZoneC", "ZoneD"));
    testQueueConfig2.setZonePickerName(ROUND_ROBIN_ZONE_PICKER_NAME);

    appConfig.setQueues(List.of(testQueueConfig1, testQueueConfig2));
    AppConfig.SparkCluster sparkClusterSpec = new AppConfig.SparkCluster();

    List<String> q1pick1 =
        ZoneManager.pickZones(
            new SubmitApplicationRequest(),
            appConfig,
            queueName1,
            sparkClusterSpec,
            "test-submission-id-1");

    Assert.assertEquals(q1pick1.size(), 1);
    Assert.assertEquals(q1pick1.get(0), "ZoneA");

    List<String> q2pick1 =
        ZoneManager.pickZones(
            new SubmitApplicationRequest(),
            appConfig,
            queueName2,
            sparkClusterSpec,
            "test-submission-id-2");

    Assert.assertEquals(q2pick1.size(), 1);
    Assert.assertEquals(q2pick1.get(0), "ZoneC");

    List<String> q2pick2 =
        ZoneManager.pickZones(
            new SubmitApplicationRequest(),
            appConfig,
            queueName2,
            sparkClusterSpec,
            "test-submission-id-3");

    Assert.assertEquals(q2pick2.size(), 1);
    Assert.assertEquals(q2pick2.get(0), "ZoneD");

    List<String> q1pick2 =
        ZoneManager.pickZones(
            new SubmitApplicationRequest(),
            appConfig,
            queueName1,
            sparkClusterSpec,
            "test-submission-id-4");

    Assert.assertEquals(q1pick2.size(), 1);
    Assert.assertEquals(q1pick2.get(0), "ZoneB");
  }
}
