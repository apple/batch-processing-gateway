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
import com.apple.spark.crd.SparkClusterCrdDiscovery;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.crd.costattrib.CostAttributionCrdDiscovery;
import com.apple.spark.crd.costattrib.CostAttributionSpec;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClusterHelper {

  public static final String DEFAULT_QUEUE = BatchSchedulerConstants.YUNIKORN_SPARK_DEFAULT_QUEUE;

  private static final Logger logger = LoggerFactory.getLogger(SparkClusterHelper.class);

  public static String getQueue(
      AppConfig appConfig, SubmitApplicationRequest request, String user) {
    String queue = null;
    if (!StringUtils.isEmpty(request.getQueue())) {
      queue = normalizeQueue(request.getQueue());
      logger.info("Use queue {} inside submission request from user {}", queue, user);
    } else if (appConfig.getQueues() != null) {
      List<AppConfig.QueueConfig> queueConfigs =
          appConfig.getQueues().stream()
              .filter(q -> q.containUser(user))
              .collect(Collectors.toList());
      if (queueConfigs.size() > 1) {
        Collections.shuffle(queueConfigs);
      }
      if (queueConfigs.size() > 0) {
        queue = queueConfigs.get(0).getName();
        logger.info("Use matched queue {} for user {}", queue, user);
      }
    }

    if (StringUtils.isEmpty(queue)) {
      logger.info(
          "No queue found for submission request from user {}, will use {} queue",
          user,
          DEFAULT_QUEUE);
      queue = DEFAULT_QUEUE;
    }

    ApplicationSubmissionHelper.validateQueueToken(queue, request.getQueueToken(), appConfig);

    return queue;
  }

  /*
  Note that each Spark "cluster" is a namespace. In other words, a K8s cluster may consist of one or more Spark clusters from gateway's perspective.
  Each Spark cluster has an associated weight configured in the ConfigMap YAML file for the corresponding environment (dev, stg and prod).
  The Spark cluster a job is routed to is determined stochastically by sampling from the following probability mass function:

  Pr(c01) = weight(c01) / (weight(c01) + weight(c02) + ... + weight(c0n))
  Pr(c02) = weight(c02) / (weight(c01) + weight(c02) + ... + weight(c0n))
  ...
  Pr(c0n) = weight(c0n) / (weight(c01) + weight(c02) + ... + weight(c0n))

  For some examples of how sampling works, refer to the test chooseSparkCluster_weighting in SparkClusterHelperTest
   */
  public static VirtualSparkClusterSpec chooseSparkCluster(
      AppConfig appConfig, SubmitApplicationRequest request, String user) {
    // If a user provided a specific cluster id to execute request on, return that cluster if it
    // exists and has the required version of Spark
    List<VirtualSparkClusterSpec> concatenatedSparkClusters = concatenateSparkClusters(appConfig);
    if (!StringUtils.isEmpty(request.getClusterId())) {
      Optional<VirtualSparkClusterSpec> sparkClusterOptional;
      sparkClusterOptional =
          concatenatedSparkClusters.stream()
              .filter(t -> StringUtils.equals(t.getId(), request.getClusterId()))
              .filter(t -> t.matchSparkVersion(request.getSparkVersion()))
              .findFirst();
      if (!sparkClusterOptional.isPresent()) {
        throw new WebApplicationException(
            String.format(
                "Cluster with id %s having Spark version %s not found",
                request.getClusterId(), request.getSparkVersion()),
            Response.Status.BAD_REQUEST);
      }
      logger.info(
          "Found spark cluster {} based on cluster id {}",
          sparkClusterOptional.get().getId(),
          request.getClusterId());
      return sparkClusterOptional.get();
    }

    // Filter clusters by spark version
    List<VirtualSparkClusterSpec> sparkClusters =
        concatenatedSparkClusters.stream()
            .filter(t -> t.getWeight() > 0 && t.matchSparkVersion(request.getSparkVersion()))
            .collect(Collectors.toList());
    if (sparkClusters.size() == 0) {
      throw new WebApplicationException(
          String.format(
              "Spark version not supported out of total %s spark clusters: %s",
              concatenatedSparkClusters.size(), request.getSparkVersion()),
          Response.Status.BAD_REQUEST);
    }

    String queue = getQueue(appConfig, request, user);

    // Filter clusters by queue if necessary
    if (queue != null) {
      // Routing is based on parent queue
      final String queueNameCopy = getParentQueue(queue);
      sparkClusters =
          sparkClusters.stream()
              .filter(t -> t.matchQueue(queueNameCopy))
              .collect(Collectors.toList());
      if (sparkClusters.size() == 0) {
        throw new WebApplicationException(
            String.format(
                "Cluster with queue %s having Spark version %s not found",
                queue, request.getSparkVersion()),
            Response.Status.BAD_REQUEST);
      }
      logger.info("Found {} spark clusters based on queue {}", sparkClusters.size(), queue);
    }

    if (sparkClusters.size() > 1) {
      // Use weight of each spark cluster to determine which cluster to use
      List<Pair<VirtualSparkClusterSpec, Double>> clusterWeightPairs =
          sparkClusters.stream()
              .map(cluster -> new Pair<>(cluster, (double) cluster.getWeight()))
              .collect(Collectors.toList());
      EnumeratedDistribution<VirtualSparkClusterSpec> weightedDistribution =
          new EnumeratedDistribution<>(clusterWeightPairs);
      return weightedDistribution.sample();
    }
    return sparkClusters.get(0);
  }

  public static List<VirtualSparkClusterSpec> concatenateSparkClusters(AppConfig appConfig) {

    List<VirtualSparkClusterSpec> list = new ArrayList<>();

    SparkClusterCrdDiscovery sparkClusterCrdDiscovery = SparkClusterCrdDiscovery.getInstance();

    String gatewayNamespace = appConfig.getGatewayNamespace();
    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      gatewayNamespace = KubernetesHelper.tryGetServiceAccountNamespace();
    }
    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      logger.info("Cannot get gateway namespace, skip loading VirtualSparkClusters CRD");
    } else {
      list.addAll(sparkClusterCrdDiscovery.getClusters(gatewayNamespace));
    }

    List<VirtualSparkClusterSpec> appConfigSparkClusters = appConfig.getSparkClusters();
    if (appConfigSparkClusters == null || appConfigSparkClusters.isEmpty()) {
      logger.info("Cannot get spark clusters, skip loading spark clusters in config.yml");
    } else {
      list.addAll(appConfigSparkClusters);
    }

    return list;
  }

  public static CostAttributionSpec getCostAttribution(AppConfig appConfig) {

    List<CostAttributionSpec> list = new ArrayList<>();
    CostAttributionCrdDiscovery costAttributionCrdDiscovery =
        CostAttributionCrdDiscovery.getInstance();

    String gatewayNamespace = appConfig.getGatewayNamespace();
    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      gatewayNamespace = KubernetesHelper.tryGetServiceAccountNamespace();
    }
    if (gatewayNamespace == null || gatewayNamespace.isEmpty()) {
      logger.info("Cannot get gateway namespace, skip loading CostAttribution CRD");
    } else {
      list.addAll(costAttributionCrdDiscovery.getCostAttribs(gatewayNamespace));
    }

    list.sort(
        (o1, o2) -> {
          int versionOrder =
              Integer.compare(Integer.parseInt(o2.getVersion()), Integer.parseInt(o1.getVersion()));
          if (versionOrder != 0) {
            return versionOrder;
          } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
              return sdf.parse(o2.getCreatedTime()).compareTo(sdf.parse(o1.getCreatedTime()));
            } catch (ParseException e) {
              throw new RuntimeException(e);
            }
          }
        });

    return list.isEmpty() ? null : list.get(0);
  }

  public static String normalizeQueue(String queue) {
    // replace repeating dots with single dot
    String regexRepeatingDots = "([\\.])\\1{1,}";
    String q1 = queue.replaceAll(regexRepeatingDots, "$1");

    // Trim leading and trailing dots
    String regexTrimDots = "[\\.]*(.*[^\\.])[\\.]*";
    Pattern r = Pattern.compile(regexTrimDots);

    Matcher m = r.matcher(q1);
    if (m.find()) {
      return m.group(1);
    } else {
      return null;
    }
  }

  public static String getParentQueue(String queue) {
    String[] parsedQueue = queue.split("\\.");
    return parsedQueue[0];
  }
}
