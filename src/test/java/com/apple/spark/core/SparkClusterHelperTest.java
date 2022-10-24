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
import java.util.Arrays;
import java.util.Collections;
import javax.ws.rs.WebApplicationException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SparkClusterHelperTest {

  @Test
  public void chooseSparkCluster_weighting() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(10);
    sparkCluster1.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster1.setQueues(Collections.singletonList(SparkClusterHelper.DEFAULT_QUEUE));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(10);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster2.setQueues(Collections.singletonList(SparkClusterHelper.DEFAULT_QUEUE));

    AppConfig.SparkCluster sparkCluster3 = new AppConfig.SparkCluster();
    sparkCluster3.setId("c03");
    sparkCluster3.setWeight(80);
    sparkCluster3.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster3.setQueues(Collections.singletonList(SparkClusterHelper.DEFAULT_QUEUE));

    AppConfig.SparkCluster sparkCluster4 = new AppConfig.SparkCluster();
    sparkCluster4.setId("c04");
    sparkCluster4.setWeight(0);
    sparkCluster4.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster4.setQueues(Collections.singletonList(SparkClusterHelper.DEFAULT_QUEUE));

    AppConfig.SparkCluster sparkCluster5 = new AppConfig.SparkCluster();
    sparkCluster5.setId("c05");
    sparkCluster5.setWeight(80);
    sparkCluster5.setSparkVersions(Collections.singletonList("2.4"));
    sparkCluster5.setQueues(Collections.singletonList(SparkClusterHelper.DEFAULT_QUEUE));

    appConfig.setSparkClusters(
        Arrays.asList(sparkCluster1, sparkCluster2, sparkCluster3, sparkCluster4, sparkCluster5));

    int[] counts = new int[5];
    int totalSubmissions = 10000;
    for (int i = 0; i < totalSubmissions; ++i) {
      SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
      submitApplicationRequest.setSparkVersion("3.1");
      AppConfig.SparkCluster chosenCluster =
          SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
      switch (chosenCluster.getId()) {
        case "c01":
          counts[0] += 1;
          break;
        case "c02":
          counts[1] += 1;
          break;
        case "c03":
          counts[2] += 1;
          break;
        case "c04":
          counts[3] += 1;
          break;
        case "c05":
          counts[4] += 1;
          break;
      }
    }

    Assert.assertEquals(counts[3], 0);
    Assert.assertEquals(counts[4], 0);
    Assert.assertTrue(counts[0] <= 1500 && counts[0] >= 500);
    Assert.assertTrue(counts[1] <= 1500 && counts[1] >= 500);
    Assert.assertTrue(counts[2] <= 8500 && counts[2] >= 7500);
  }

  @Test
  public void chooseSparkCluster_clusterHasDefaultQueue_requestHasNoQueue() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Arrays.asList("3.0", "3.1"));
    sparkCluster2.setQueues(Arrays.asList(SparkClusterHelper.DEFAULT_QUEUE));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    AppConfig.SparkCluster chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c02");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void chooseSparkCluster_clusterHasNoDefaultQueue_requestHasNoQueue() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Arrays.asList("3.0", "3.1"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
  }

  @Test
  public void chooseSparkCluster_clusterIdInSubmission() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Arrays.asList("3.0", "3.1"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    submitApplicationRequest.setClusterId("c02");
    AppConfig.SparkCluster chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c02");
  }

  @Test
  public void chooseSparkCluster_explicitQueueInSubmission() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));
    sparkCluster1.setQueues(Arrays.asList("q1", "q2"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster2.setQueues(Arrays.asList(SparkClusterHelper.DEFAULT_QUEUE));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    submitApplicationRequest.setQueue("q2");
    AppConfig.SparkCluster chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c01");

    submitApplicationRequest.setSparkVersion("3.1");
    submitApplicationRequest.setQueue(null);
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c02");
  }

  @Test
  public void chooseSparkCluster_queueByMatchingUser() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));
    sparkCluster1.setQueues(Arrays.asList("q1", "q2", SparkClusterHelper.DEFAULT_QUEUE));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));
    sparkCluster2.setQueues(Arrays.asList("q1", "q2", SparkClusterHelper.DEFAULT_QUEUE));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("q2");
    queueConfig.setUsers(Arrays.asList("user1", "user2"));
    appConfig.setQueues(Collections.singletonList(queueConfig));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    AppConfig.SparkCluster chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c01");
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user2");
    Assert.assertEquals(chosenCluster.getId(), "c01");
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user99");
    Assert.assertEquals(chosenCluster.getId(), "c01");

    submitApplicationRequest.setSparkVersion("3.1");
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertEquals(chosenCluster.getId(), "c02");
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user2");
    Assert.assertEquals(chosenCluster.getId(), "c02");
    chosenCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user99");
    Assert.assertEquals(chosenCluster.getId(), "c02");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void chooseSparkCluster_unsupportedSparkVersion() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.2");
    SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void chooseSparkCluster_noQueueSupportedWithRequestedSparkVersion() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));
    sparkCluster1.setQueues(Arrays.asList("q1", "q2"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.1");
    submitApplicationRequest.setQueue("q2");
    SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
  }

  @Test(expectedExceptions = WebApplicationException.class)
  public void chooseSparkCluster_userMappedToNotSupportedQueue() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));
    sparkCluster1.setQueues(Arrays.asList("q1", "q2"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.1"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("q2");
    queueConfig.setUsers(Collections.singletonList("user1"));
    appConfig.setQueues(Collections.singletonList(queueConfig));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.1");
    SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
  }

  @Test
  public void chooseSparkCluster_multipleQueuesSupported() {
    AppConfig appConfig = new AppConfig();

    AppConfig.SparkCluster sparkCluster1 = new AppConfig.SparkCluster();
    sparkCluster1.setId("c01");
    sparkCluster1.setWeight(100);
    sparkCluster1.setSparkVersions(Arrays.asList("2.4", "3.0"));
    sparkCluster1.setQueues(Arrays.asList("q1", "q2"));

    AppConfig.SparkCluster sparkCluster2 = new AppConfig.SparkCluster();
    sparkCluster2.setId("c02");
    sparkCluster2.setWeight(100);
    sparkCluster2.setSparkVersions(Collections.singletonList("3.0"));
    sparkCluster2.setQueues(Arrays.asList("q1", "q2"));

    appConfig.setSparkClusters(Arrays.asList(sparkCluster1, sparkCluster2));

    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("q2");
    queueConfig.setUsers(Collections.singletonList("user1"));
    appConfig.setQueues(Collections.singletonList(queueConfig));

    SubmitApplicationRequest submitApplicationRequest = new SubmitApplicationRequest();
    submitApplicationRequest.setSparkVersion("3.0");
    AppConfig.SparkCluster sparkCluster =
        SparkClusterHelper.chooseSparkCluster(appConfig, submitApplicationRequest, "user1");
    Assert.assertTrue(sparkCluster.getId().equals("c01") || sparkCluster.getId().equals("c02"));
  }

  @DataProvider
  public Object[][] data() {
    return new String[][] {
      {"queue1", "queue1"},
      {"parent.queue1", "parent.queue1"},
      {"parent..queue1", "parent.queue1"},
      {"parent..queue1...", "parent.queue1"},
      {"...parent..queue1..", "parent.queue1"}
    };
  }

  @Test(dataProvider = "data")
  public void normalizeQueue(String raw, String expected) {
    Assert.assertEquals(expected, SparkClusterHelper.normalizeQueue(raw));
  }
}
