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

import static com.apple.spark.core.Constants.CONFIG_PICKED_ZONES;
import static com.apple.spark.core.SparkPodNodeAffinityHelper.*;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.*;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkPodNodeAffinityHelperTest {

  @Test
  public void apply_null_allowed_zone() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    request.setSparkConf(new HashMap<>());
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();
    applyAvailabilityZoneExpression(matchExpressions, request);
    Assert.assertEquals(matchExpressions.size(), 0);
  }

  @Test
  public void apply_zero_allowed_zone() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    request.setSparkConf(Map.of(CONFIG_PICKED_ZONES, ""));
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();
    applyAvailabilityZoneExpression(matchExpressions, request);
    Assert.assertEquals(matchExpressions.size(), 0);
  }

  @Test
  public void apply_one_allowed_zone() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    request.setSparkConf(Map.of(CONFIG_PICKED_ZONES, "ZoneA"));
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();
    applyAvailabilityZoneExpression(matchExpressions, request);
    Assert.assertEquals(matchExpressions.size(), 1);
    Assert.assertEquals(matchExpressions.get(0).getKey(), K8S_ZONE_LABEL);
    Assert.assertEquals(matchExpressions.get(0).getValues()[0], "ZoneA");
  }

  @Test
  public void apply_two_allowed_zones() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    request.setSparkConf(Map.of(CONFIG_PICKED_ZONES, "ZoneA,ZoneB"));
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();
    applyAvailabilityZoneExpression(matchExpressions, request);

    Assert.assertEquals(matchExpressions.size(), 1);
    Assert.assertEquals(matchExpressions.get(0).getKey(), K8S_ZONE_LABEL);
    Assert.assertEquals(matchExpressions.get(0).getValues()[0], "ZoneA");
    Assert.assertEquals(matchExpressions.get(0).getValues()[1], "ZoneB");
  }

  @Test
  public void consistent_zones_pick_driver_executor() {
    SubmitApplicationRequest request = new SubmitApplicationRequest();
    request.setSparkConf(Map.of(CONFIG_PICKED_ZONES, "ZoneA"));
    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig queueConfig = new AppConfig.QueueConfig();
    queueConfig.setName("test-queue");
    appConfig.setQueues(List.of(queueConfig));
    ExecutorSpec executorSpec = new ExecutorSpec();
    request.setExecutor(executorSpec);

    NodeAffinity driverNodeAffinity =
        createNodeAffinityForSparkPods(request, appConfig, "test-queue", null, true);

    NodeSelectorRequirement[] driverMatchExpressions =
        driverNodeAffinity
            .getRequiredDuringSchedulingIgnoredDuringExecution()
            .getNodeSelectorTerms()[0]
            .getMatchExpressions();
    Assert.assertEquals(driverMatchExpressions.length, 1);
    Assert.assertEquals(driverMatchExpressions[0].getKey(), K8S_ZONE_LABEL);
    Assert.assertEquals(driverMatchExpressions[0].getValues().length, 1);
    Assert.assertEquals(driverMatchExpressions[0].getValues()[0], "ZoneA");

    NodeAffinity executorNodeAffinity =
        createNodeAffinityForSparkPods(request, appConfig, "test-queue", null, false);

    NodeSelectorRequirement[] executorMatchExpressions =
        executorNodeAffinity
            .getRequiredDuringSchedulingIgnoredDuringExecution()
            .getNodeSelectorTerms()[0]
            .getMatchExpressions();
    Assert.assertEquals(executorMatchExpressions.length, 1);
    Assert.assertEquals(executorMatchExpressions[0].getKey(), K8S_ZONE_LABEL);
    Assert.assertEquals(executorMatchExpressions[0].getValues().length, 1);
    Assert.assertEquals(executorMatchExpressions[0].getValues()[0], "ZoneA");
  }
}
