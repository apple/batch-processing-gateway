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

import static com.apple.spark.core.SparkPodNodeAffinityHelper.*;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.NodeSelectorRequirement;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkPodNodeAffinityHelperTest {

  @Test
  public void matchExpressions_null() {
    String nodeArch = null;
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    Assert.assertNull(driverNodeAffinity);
    Assert.assertNull(executorNodeAffinity);
  }

  @Test
  public void cas_ondemand_arch_null() {
    String nodeArch = null;
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5"));
    qc.setName("poc");
    qc.setSupportGpu(true);
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    // when nodeArch is null, it means users is using non-gravitons
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("amd65"::contains));
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("amd64"::contains));
  }

  @Test
  public void cas_ondemand_arch_arm64() {
    String nodeArch = "arm64";
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(executorNodeAffinity.getValues()[0], "graviton_a");
  }

  @Test
  public void cas_ondemand_arch_amd64() {
    String nodeArch = "amd64";
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("amd65"::contains));
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("amd64"::contains));
  }

  @Test
  public void cas_spot_arch_null() {
    String nodeArch = null;
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5", "graviton_b"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("m5"::contains));
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("c5"::contains));
    // should not include graviton
    Assert.assertFalse(
        Arrays.stream(executorNodeAffinity.getValues()).anyMatch("graviton_b"::contains));
    // should not include graviton
    Assert.assertFalse(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("amd64"::contains));
  }

  @Test
  public void cas_spot_arch_amd64() {
    String nodeArch = "amd64";
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5", "graviton_a"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("m5"::contains));
    Assert.assertTrue(Arrays.stream(executorNodeAffinity.getValues()).anyMatch("c5"::contains));
    // should not include graviton
    Assert.assertFalse(
        Arrays.stream(executorNodeAffinity.getValues()).anyMatch("graviton_a"::contains));
  }

  @Test
  public void cas_spot_arch_arm64() {
    String nodeArch = "arm64";
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey("node_group_category");
    qc.setDriverNodeLabelValues(List.of("spark_driver"));

    qc.setExecutorNodeLabelKey("node_group_category");
    qc.setExecutorNodeLabelValues(Arrays.asList("amd64", "amd65", "graviton_a"));
    qc.setExecutorSpotNodeLabelValues(Arrays.asList("m5", "c5", "graviton_a"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];
    NodeSelectorRequirement executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions()[0];

    Assert.assertEquals(driverNodeAffinity.getKey(), "node_group_category");
    Assert.assertEquals(driverNodeAffinity.getValues()[0], "spark_driver");
    Assert.assertEquals(executorNodeAffinity.getKey(), "node_group_category");
    Assert.assertTrue(
        Arrays.stream(executorNodeAffinity.getValues()).anyMatch("graviton_a"::contains));
  }

  @Test
  public void karpenter_ondemand_arch_null() {
    String nodeArch = null;
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);
  }

  @Test
  public void karpenter_ondemand_arch_arm64() {
    String nodeArch = "arm64";
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);
  }

  @Test
  public void karpenter_ondemand_arch_amd64() {
    String nodeArch = "amd64";
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);
  }

  @Test
  public void karpenter_spot_arch_arm64() {
    String nodeArch = "arm64";
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_SPOT);
  }

  @Test
  public void karpenter_spot_arch_amd64() {
    String nodeArch = "amd64";
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute"));
    qc.setName("poc");
    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();
    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_SPOT);
  }

  @Test
  public void karpenter_spot_arch_amd64_gpu() {
    String nodeArch = "amd64";
    boolean isOnDemand = false;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute", "gpu"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute", "gpu"));

    qc.setName("poc");

    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, true)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, true)
            .getMatchExpressions();

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "gpu");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_AMD64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_SPOT);
  }

  @Test
  public void karpenter_ondemand_arch_arm64_gpu_false() {
    String nodeArch = "arm64";
    boolean isOnDemand = true;

    AppConfig appConfig = new AppConfig();
    AppConfig.QueueConfig qc = new AppConfig.QueueConfig();
    qc.setDriverNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setDriverNodeLabelValues(List.of("compute"));

    qc.setExecutorNodeLabelKey(KARPENTER_PROVISIONER_NAME_KEY);
    qc.setExecutorNodeLabelValues(List.of("compute", "gpu2"));

    qc.setName("poc");

    appConfig.setQueues(List.of(qc));

    NodeSelectorRequirement[] driverNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", true, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(driverNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(driverNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(driverNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(driverNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(driverNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        driverNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);

    NodeSelectorRequirement[] executorNodeAffinity =
        createNodeSelectorTermForSparkPods(appConfig, "poc", false, isOnDemand, nodeArch, false)
            .getMatchExpressions();

    Assert.assertEquals(executorNodeAffinity[0].getKey(), KARPENTER_PROVISIONER_NAME_KEY);
    Assert.assertEquals(executorNodeAffinity[0].getValues()[0], "compute");

    Assert.assertEquals(executorNodeAffinity[1].getKey(), K8S_ARCH_KEY);
    Assert.assertEquals(executorNodeAffinity[1].getValues()[0], KARPENTER_ARCH_VALUE_ARM64);

    Assert.assertEquals(executorNodeAffinity[2].getKey(), KARPENTER_CAPACITY_TYPE_KEY);
    Assert.assertEquals(
        executorNodeAffinity[2].getValues()[0], KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND);
  }
}
