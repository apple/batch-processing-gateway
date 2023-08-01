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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SparkPodNodeAffinityHelper {

  // Gateway rely on this key to differentiate the node affinity between K8S Cluster Autoscaler and
  // Karpenter
  public static final String KARPENTER_PROVISIONER_NAME_KEY = "karpenter-provisioner-name";

  public static final String KARPENTER_PROVISIONER_GPU_VALUE = "gpu";

  public static final String K8S_ARCH_KEY = "kubernetes.io/arch";
  public static final String KARPENTER_ARCH_VALUE_ARM64 = "arm64";
  public static final String KARPENTER_ARCH_VALUE_AMD64 = "amd64";

  public static final String KARPENTER_CAPACITY_TYPE_KEY = "karpenter.sh/capacity-type";
  public static final String KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND = "on-demand";
  public static final String KARPENTER_CAPACITY_TYPE_VALUE_SPOT = "spot";

  public static boolean isKarpenterEnabled(String queueConfigNodeKey) {
    return queueConfigNodeKey.equals(KARPENTER_PROVISIONER_NAME_KEY);
  }

  // need to support add both preferred affinity and multiple
  public static NodeSelectorTerm createNodeSelectorTermForSparkPods(
      AppConfig appConfig,
      String queue,
      boolean isDriver,
      boolean isOnDemand,
      String nodeArch,
      boolean isGPUJob) {

    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    NodeSelectorTerm podNodeSelectorTerm = new NodeSelectorTerm();
    List<NodeSelectorRequirement> matchExpressions = new ArrayList<>();

    String driverNodeLabelKey = getNodeLabelKeyForQueue(appConfig, queue, true);
    List<String> driverNodeLabelValues = getNodeLabelValuesForQueue(appConfig, queue, true);
    List<String> driverNodeLabelValuesNonGPU =
        getNodeLabelValuesNonGPUForQueue(driverNodeLabelValues, KARPENTER_PROVISIONER_GPU_VALUE);

    String executorNodeLabelKey = getNodeLabelKeyForQueue(appConfig, queue, false);
    List<String> executorNodeLabelValues = getNodeLabelValuesForQueue(appConfig, queue, false);
    List<String> executorNodeLabelValuesNonGPU =
        getNodeLabelValuesNonGPUForQueue(executorNodeLabelValues, KARPENTER_PROVISIONER_GPU_VALUE);
    List<String> executorNodeLabelValuesGPUOnly =
        getNodeLabelValuesGPUOnlyForQueue(executorNodeLabelValues, KARPENTER_PROVISIONER_GPU_VALUE);

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelValues() != null) {
        if (isDriver) {
          // driver node should be always allocated to On-Demand machines
          if (isKarpenterEnabled(driverNodeLabelKey)) {
            // Apply Karpenter provisioner label
            // When Karpenter enabled , we will keep driver running on non-gpu
            NodeSelectorRequirement karpenterProvisionerLabel =
                createMatchExpression(driverNodeLabelKey, driverNodeLabelValuesNonGPU);
            matchExpressions.add(karpenterProvisionerLabel);

            // Apply Karpenter node arch label
            NodeSelectorRequirement karpenterNodeArchLabel =
                createNodeArchMatchExpression(nodeArch);
            matchExpressions.add(karpenterNodeArchLabel);

            // Apply Karpenter capacity type label, driver will be always run on on-demand machines
            NodeSelectorRequirement karpenterCapacityTypeLabel =
                createCapactiyTypeMatchExpression(true);
            matchExpressions.add(karpenterCapacityTypeLabel);

          } else {
            // Apply K8S Cluster Autoscaler capacity type label
            NodeSelectorRequirement k8sAutoscalerNodeLabel =
                createK8SAutoscalerMatchExpression(isDriver, true, appConfig, queue, nodeArch);
            matchExpressions.add(k8sAutoscalerNodeLabel);
          }
        } else {
          // driver node should be always allocated to On-Demand machines
          if (isKarpenterEnabled(queueConfig.getExecutorNodeLabelKey())) {
            // Apply Karpenter provisioner label
            // When Karpenter enabled and gpu needed, we need allow executor to run gpu provisioner
            if (isGPUJob) {
              NodeSelectorRequirement karpenterProvisionerLabel =
                  createMatchExpression(executorNodeLabelKey, executorNodeLabelValuesGPUOnly);
              matchExpressions.add(karpenterProvisionerLabel);
            } else {
              NodeSelectorRequirement karpenterProvisionerLabel =
                  createMatchExpression(executorNodeLabelKey, executorNodeLabelValuesNonGPU);
              matchExpressions.add(karpenterProvisionerLabel);
            }

            // Apply Karpenter node arch label
            NodeSelectorRequirement karpenterNodeArchLabel =
                createNodeArchMatchExpression(nodeArch);
            matchExpressions.add(karpenterNodeArchLabel);

            // Apply Karpenter capacity type label
            NodeSelectorRequirement karpenterCapacityTypeLabel =
                createCapactiyTypeMatchExpression(isOnDemand);
            matchExpressions.add(karpenterCapacityTypeLabel);

          } else {
            // Apply K8S Cluster Autoscaler capacity type label
            NodeSelectorRequirement k8sAutoscalerNodeLabel =
                createK8SAutoscalerMatchExpression(
                    isDriver, isOnDemand, appConfig, queue, nodeArch);
            matchExpressions.add(k8sAutoscalerNodeLabel);
          }
        }
        podNodeSelectorTerm.setMatchExpressions(
            matchExpressions.toArray(NodeSelectorRequirement[]::new));
      }
    }
    return podNodeSelectorTerm;
  }

  public static NodeSelectorRequirement createMatchExpression(
      String podNodeLabelKey, List<String> podNodeLabelValues) {
    NodeSelectorRequirement nodeSelectorRequirement =
        new NodeSelectorRequirement(
            podNodeLabelKey,
            NodeSelectorOperator.NodeSelectorOpIn.toString(),
            podNodeLabelValues.toArray(new String[0]));
    return nodeSelectorRequirement;
  }

  public static NodeSelectorRequirement createCapactiyTypeMatchExpression(boolean isOnDemand) {
    if (isOnDemand)
      return new NodeSelectorRequirement(
          KARPENTER_CAPACITY_TYPE_KEY,
          NodeSelectorOperator.NodeSelectorOpIn.toString(),
          new String[] {KARPENTER_CAPACITY_TYPE_VALUE_ON_DEMAND});
    else {
      return new NodeSelectorRequirement(
          KARPENTER_CAPACITY_TYPE_KEY,
          NodeSelectorOperator.NodeSelectorOpIn.toString(),
          new String[] {KARPENTER_CAPACITY_TYPE_VALUE_SPOT});
    }
  }

  public static NodeSelectorRequirement createNodeArchMatchExpression(String nodeArch) {
    if (nodeArch == null) {
      // set default arch to arm64 to save cost
      return new NodeSelectorRequirement(
          K8S_ARCH_KEY,
          NodeSelectorOperator.NodeSelectorOpIn.toString(),
          new String[] {KARPENTER_ARCH_VALUE_ARM64});
    } else {
      // client tool should make sure node arch is limited to the supported arches
      return new NodeSelectorRequirement(
          K8S_ARCH_KEY, NodeSelectorOperator.NodeSelectorOpIn.toString(), new String[] {nodeArch});
    }
  }

  public static NodeSelectorRequirement createK8SAutoscalerMatchExpression(
      boolean isDriver, boolean isOnDemand, AppConfig appConfig, String queue, String nodeArch) {

    if (isDriver) {

      String driverNodeLabelKey = getDriverNodeLabelKeyForQueue(appConfig, queue);
      List<String> driverNodeLabelValues =
          getDriverNodeLabelValuesForQueue(appConfig, queue, nodeArch);

      return new NodeSelectorRequirement(
          driverNodeLabelKey,
          NodeSelectorOperator.NodeSelectorOpIn.toString(),
          driverNodeLabelValues.toArray(new String[0]));

    } else {
      String executorNodeLabelKey = getExecutorNodeLabelKeyForQueue(appConfig, queue);
      if (isOnDemand) {
        List<String> executorNodeLabelValues =
            getExecutorNodeLabelValuesForQueue(appConfig, queue, nodeArch);
        return new NodeSelectorRequirement(
            executorNodeLabelKey,
            NodeSelectorOperator.NodeSelectorOpIn.toString(),
            executorNodeLabelValues.toArray(new String[0]));

      } else {
        List<String> executorSpotNodeLabelValues =
            getExecutorSpotNodeLabelValuesForQueue(appConfig, queue, nodeArch);
        return new NodeSelectorRequirement(
            executorNodeLabelKey,
            NodeSelectorOperator.NodeSelectorOpIn.toString(),
            executorSpotNodeLabelValues.toArray(new String[0]));
      }
    }
  }

  private static String getNodeLabelKeyForQueue(
      AppConfig appConfig, String queue, boolean isDriver) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null) {
        if (isDriver) {
          if (queueConfig.getDriverNodeLabelKey() != null) {
            return queueConfig.getDriverNodeLabelKey();
          } else {
            return DEFAULT_DRIVER_NODE_LABEL_KEY;
          }
        } else {
          if (queueConfig.getExecutorNodeLabelKey() != null) {
            return queueConfig.getExecutorNodeLabelKey();
          } else {
            return DEFAULT_EXECUTOR_NODE_LABEL_KEY;
          }
        }
      }
    }
    return isDriver ? DEFAULT_DRIVER_NODE_LABEL_KEY : DEFAULT_EXECUTOR_NODE_LABEL_KEY;
  }

  private static List<String> getNodeLabelValuesForQueue(
      AppConfig appConfig, String queue, boolean isDriver) {

    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (isDriver) {
        if (queueConfig != null && queueConfig.getDriverNodeLabelValues() != null) {
          res.addAll(queueConfig.getDriverNodeLabelValues());
        }
      } else {
        if (queueConfig != null && queueConfig.getExecutorNodeLabelValues() != null) {
          res.addAll(queueConfig.getExecutorNodeLabelValues());
        }
      }
    }
    return res;
  }

  public static String getExecutorNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
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

  public static List<String> getExecutorNodeLabelValuesForQueue(
      AppConfig appConfig, String queue, String nodeArch) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorNodeLabelValues() != null) {
        for (String nodeLabelValue : queueConfig.getExecutorNodeLabelValues()) {
          if (nodeArch != null
              && INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeArch::contains)) {
            if (INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          } else {
            if (!INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          }
        }
        if (res.size() == 0) res.addAll(queueConfig.getExecutorNodeLabelValues());
      }
    }
    return res;
  }

  public static List<String> getExecutorSpotNodeLabelValuesForQueue(
      AppConfig appConfig, String queue, String nodeArch) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getExecutorSpotNodeLabelValues() != null) {
        for (String nodeLabelValue : queueConfig.getExecutorSpotNodeLabelValues()) {
          if (nodeArch != null
              && INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeArch::contains)) {
            if (INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          } else {
            if (!INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          }
        }
        if (res.size() == 0) res.addAll(queueConfig.getExecutorSpotNodeLabelValues());
      }
    }
    return res;
  }

  public static String getDriverNodeLabelKeyForQueue(AppConfig appConfig, String queue) {
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

  public static List<String> getDriverNodeLabelValuesForQueue(
      AppConfig appConfig, String queue, String nodeArch) {
    Optional<AppConfig.QueueConfig> queueConfigOptional =
        appConfig.getQueues().stream().filter(t -> t.getName().equals(queue)).findFirst();
    List<String> res = new ArrayList<>();

    if (queueConfigOptional.isPresent()) {
      AppConfig.QueueConfig queueConfig = queueConfigOptional.get();
      if (queueConfig != null && queueConfig.getDriverNodeLabelValues() != null) {
        for (String nodeLabelValue : queueConfig.getDriverNodeLabelValues()) {
          if (nodeArch != null
              && INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeArch::contains)) {
            if (INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          } else {
            if (!INSTANCE_ARCH_ARM_NODE_LABEL_SET.stream().anyMatch(nodeLabelValue::contains)) {
              res.add(nodeLabelValue);
            }
          }
        }
        if (res.size() == 0) res.addAll(queueConfig.getDriverNodeLabelValues());
      }
    }
    return res;
  }

  public static boolean isGpuEnabled(SubmitApplicationRequest request) {
    return request.getExecutor().getGpu() != null;
  }

  public static List<String> getNodeLabelValuesNonGPUForQueue(
      List<String> nodeLabelValues, String pattern) {
    return nodeLabelValues.stream().filter(u -> !u.contains(pattern)).collect(Collectors.toList());
  }

  public static List<String> getNodeLabelValuesGPUOnlyForQueue(
      List<String> nodeLabelValues, String pattern) {
    return nodeLabelValues.stream().filter(u -> u.contains(pattern)).collect(Collectors.toList());
  }
}
