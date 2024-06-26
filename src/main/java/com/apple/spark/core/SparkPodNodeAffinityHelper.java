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

import static com.apple.spark.core.Constants.*;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.*;
import com.google.common.annotations.VisibleForTesting;
import java.util.*;

public class SparkPodNodeAffinityHelper {

  public static final String K8S_ZONE_LABEL = "topology.kubernetes.io/zone";

  public static NodeAffinity createNodeAffinityForSparkPods(
      final SubmitApplicationRequest request,
      final AppConfig appConfig,
      final String queueName,
      final AppConfig.SparkCluster sparkCluster,
      final boolean isDriver) {

    NodeAffinity nodeAffinity = new NodeAffinity();

    NodeSelectorTerm nodeSelectorTermForRequired =
        createNodeSelectorTermForRequired(request, appConfig, queueName, sparkCluster, isDriver);

    if (nodeSelectorTermForRequired.getMatchExpressions() != null) {
      nodeAffinity.setRequiredDuringSchedulingIgnoredDuringExecution(
          new RequiredDuringSchedulingIgnoredDuringExecutionTerm(
              new NodeSelectorTerm[] {nodeSelectorTermForRequired}));
    }

    return nodeAffinity;
  }

  private static NodeSelectorTerm createNodeSelectorTermForRequired(
      final SubmitApplicationRequest request,
      final AppConfig appConfig,
      final String queue,
      final AppConfig.SparkCluster sparkCluster,
      final boolean isDriver) {

    NodeSelectorTerm podNodeSelectorTerm = new NodeSelectorTerm();
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();

    if (isDriver) {
      applyDriverNodeLabelAffinity(matchExpressions, appConfig, queue);
    } else {
      applyExecutorNodeLabelAffinity(matchExpressions, request, appConfig, queue);
    }
    applyAvailabilityZoneExpression(matchExpressions, request);

    if (!matchExpressions.isEmpty()) {
      podNodeSelectorTerm.setMatchExpressions(
          matchExpressions.toArray(NodeSelectorRequirement[]::new));
    }
    return podNodeSelectorTerm;
  }

  @VisibleForTesting
  protected static void applyAvailabilityZoneExpression(
      final List<NodeSelectorRequirement> matchExpressions,
      final SubmitApplicationRequest request) {

    if (request == null
        || request.getSparkConf() == null
        || !request.getSparkConf().containsKey(CONFIG_PICKED_ZONES)
        || request.getSparkConf().get(CONFIG_PICKED_ZONES) == null
        || request.getSparkConf().get(CONFIG_PICKED_ZONES).isEmpty()) {
      return;
    }

    String[] pickedZone =
        request.getSparkConf().get(CONFIG_PICKED_ZONES).split(PICKED_ZONES_DELIMITER);
    if (pickedZone.length == 0) {
      return;
    }

    matchExpressions.add(
        new NodeSelectorRequirement(
            K8S_ZONE_LABEL, NodeSelectorOperator.NodeSelectorOpIn.toString(), pickedZone));
  }

  private static void applyDriverNodeLabelAffinity(
      final List<NodeSelectorRequirement> matchExpressions,
      final AppConfig appConfig,
      final String parentQueue) {
    List<String> driverNodeLabelValues = getDriverNodeLabelValuesForQueue(appConfig, parentQueue);
    String nodeLabelKey = getDriverNodeLabelKeyForQueue(appConfig, parentQueue);
    if (driverNodeLabelValues == null || driverNodeLabelValues.isEmpty()) {
      return;
    }
    // set hard requiredDuringSchedulingIgnoredDuringExecutionTerm to driver
    NodeSelectorRequirement nodeSelectorRequirement =
        new NodeSelectorRequirement(
            nodeLabelKey,
            NodeSelectorOperator.NodeSelectorOpIn.toString(),
            driverNodeLabelValues.toArray(new String[0])); // set "spark_driver" as nodeselctor

    matchExpressions.add(nodeSelectorRequirement);
  }

  private static String getDriverNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverNodeLabelKey() != null) {
        return queueConfig.getDriverNodeLabelKey();
      }
    }
    return DEFAULT_DRIVER_NODE_LABEL_KEY;
  }

  private static List<String> getDriverNodeLabelValuesForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverNodeLabelValues() != null) {
        res.addAll(queueConfig.getDriverNodeLabelValues());
      }
    }
    return res;
  }

  private static void applyExecutorNodeLabelAffinity(
      final List<NodeSelectorRequirement> matchExpressions,
      final SubmitApplicationRequest request,
      final AppConfig appConfig,
      final String parentQueue) {

    List<String> executorNodeLabelValues = new ArrayList<>();

    if (request.getSpotInstance()) {
      executorNodeLabelValues = getExecutorSpotNodeLabelValuesForQueue(appConfig, parentQueue);
    } else {
      executorNodeLabelValues = getExecutorNodeLabelValuesForQueue(appConfig, parentQueue);
    }

    String nodeLabelKey = getExecutorNodeLabelKeyForQueue(appConfig, parentQueue);

    if (executorNodeLabelValues == null || executorNodeLabelValues.isEmpty()) {
      return;
    }
    // set hard requiredDuringSchedulingIgnoredDuringExecutionTerm to executor
    NodeSelectorRequirement nodeSelectorRequirement =
        new NodeSelectorRequirement(
            nodeLabelKey,
            NodeSelectorOperator.NodeSelectorOpIn.toString(),
            executorNodeLabelValues.toArray(new String[0]));

    matchExpressions.add(nodeSelectorRequirement);
  }

  private static List<String> getExecutorSpotNodeLabelValuesForQueue(
      AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorSpotNodeLabelValues() != null) {
        res.addAll(queueConfig.getExecutorSpotNodeLabelValues());
      }
    }
    return res;
  }

  private static String getExecutorNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelKey() != null) {
        return queueConfig.getExecutorNodeLabelKey();
      }
    }
    return DEFAULT_EXECUTOR_NODE_LABEL_KEY;
  }

  private static List<String> getExecutorNodeLabelValuesForQueue(
      AppConfig appConfig, String queue) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelValues() != null) {
        res.addAll(queueConfig.getExecutorNodeLabelValues());
      }
    }
    return res;
  }
}
