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

import static com.apple.spark.core.Constants.MONITOR_KILLED_APPS;
import static com.apple.spark.core.Constants.MONITOR_RUNNING_APPS;

import com.apple.spark.AppConfig;
import com.apple.spark.operator.DriverInfo;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.util.CounterMetricContainer;
import com.apple.spark.util.DateTimeUtils;
import com.apple.spark.util.GaugeMetricContainer;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Running application monitor is a component that monitors all the running applications on a Spark
 * cluster. This class needs to be thread safe.
 */
public class RunningApplicationMonitor {

  private static final Logger logger = LoggerFactory.getLogger(RunningApplicationMonitor.class);

  private static final long DEFAULT_INTERVAL = 30 * 1000;

  // The monitor instance relies on this hashmap to go through all the running applications.
  // Thus it relies on the instance caller to keep informing about the applications.
  private final ConcurrentHashMap<NamespaceAndName, RunningAppInfo> runningApplications;
  private final GaugeMetricContainer gaugeMetricContainer;
  private final CounterMetricContainer counterMetricContainer;

  // how long (milliseconds) to check and delete long-running applications
  private final long deleteInterval;
  private final AppConfig.SparkCluster sparkCluster;

  public RunningApplicationMonitor(
      AppConfig.SparkCluster sparkCluster, Timer timer, MeterRegistry meterRegistry) {
    this(sparkCluster, timer, DEFAULT_INTERVAL, meterRegistry);
  }

  /** Start a timer to kill all apps that have been running for too long */
  public RunningApplicationMonitor(
      AppConfig.SparkCluster sparkCluster,
      Timer timer,
      long deleteIntervalMillis,
      MeterRegistry meterRegistry) {
    this.sparkCluster = sparkCluster;
    this.deleteInterval = deleteIntervalMillis;
    this.runningApplications = new ConcurrentHashMap<>();
    this.gaugeMetricContainer = new GaugeMetricContainer(meterRegistry);
    this.counterMetricContainer = new CounterMetricContainer(meterRegistry);

    String eksClusterTagName =
        sparkCluster.getEksCluster() == null ? "" : sparkCluster.getEksCluster();
    String namespaceTagName =
        sparkCluster.getSparkApplicationNamespace() == null
            ? ""
            : sparkCluster.getSparkApplicationNamespace();
    this.gaugeMetricContainer.register(
        MONITOR_RUNNING_APPS,
        () -> this.runningApplications.size(),
        Tag.of("eks", eksClusterTagName),
        Tag.of("spark_app_ns", namespaceTagName));

    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            try {
              deleteLongRunningApplications();
            } catch (Throwable ex) {
              logger.warn("Failed to check and delete long running application", ex);
            }
          }
        },
        deleteInterval,
        deleteInterval);
    logger.info(
        "Scheduled checking and deleting long running applications for {}/{} every {} milliseconds",
        sparkCluster.getEksCluster(),
        sparkCluster.getSparkApplicationNamespace(),
        deleteInterval);
  }

  public static long getMaxRunningMillis(SparkApplication sparkApplication) {
    if (sparkApplication.getMetadata() == null
        || sparkApplication.getMetadata().getLabels() == null) {
      return Constants.DEFAULT_MAX_RUNNING_MILLIS;
    }
    String labelValue =
        sparkApplication.getMetadata().getLabels().get(Constants.MAX_RUNNING_MILLIS_LABEL);
    if (labelValue == null || labelValue.isEmpty()) {
      return Constants.DEFAULT_MAX_RUNNING_MILLIS;
    }
    try {
      return Long.parseLong(labelValue);
    } catch (Throwable ex) {
      logger.warn(
          String.format(
              "Failed to parse value %s for label %s on %s",
              labelValue,
              Constants.MAX_RUNNING_MILLIS_LABEL,
              sparkApplication.getMetadata().getName()),
          ex);
      return Constants.DEFAULT_MAX_RUNNING_MILLIS;
    }
  }

  /**
   * Notify the monitor instance about an application CRD update. The monitor instance relies on
   * this function to keep its hashmap updated.
   *
   * @param prevCRDState the previous CRD state
   * @param currCRDState the current CRD state
   */
  public void onUpdate(
          SparkApplication prevCRDState, SparkApplication currCRDState) {
    String newState = SparkApplicationResourceHelper.getState(currCRDState);
    if (SparkConstants.RUNNING_STATE.equalsIgnoreCase(newState)) {
      String name = currCRDState.getMetadata().getName();
      if (name == null) {
        logger.warn("Did not get name for spark application");
        return;
      }
      String creationTimeStr = currCRDState.getMetadata().getCreationTimestamp();
      if (creationTimeStr == null) {
        logger.warn("Did not get creation timestamp for {}", name);
        return;
      }
      Long creationTime = DateTimeUtils.parseOrNull(creationTimeStr);
      if (creationTime == null) {
        logger.warn("Failed to parse creation timestamp {} for {}", creationTimeStr, name);
        return;
      }
      String namespace = currCRDState.getMetadata().getNamespace();
      if (namespace == null) {
        logger.warn("Did not get namespace for {}", name);
        return;
      }

      NamespaceAndName namespaceAndName = new NamespaceAndName(namespace, name);
      if (SparkConstants.isApplicationStopped(newState)) {
        runningApplications.remove(namespaceAndName);
      } else {
        long maxRunningTime = getMaxRunningMillis(currCRDState);
        runningApplications.put(namespaceAndName, new RunningAppInfo(creationTime, maxRunningTime));
      }
    }
  }

  /** Kill all the apps that have been running for too long. */
  public void deleteLongRunningApplications() {
    List<NamespaceAndName> expiredApplications =
        runningApplications.entrySet().stream()
            .filter(t -> t.getValue().exceedMaxRunningTime())
            .map(t -> t.getKey())
            .collect(Collectors.toList());
    for (NamespaceAndName app : expiredApplications) {
      RunningAppInfo runningAppInfo = runningApplications.remove(app);
      if (runningAppInfo != null) {
        logger.info(
            "Killing application {}/{} due to running too long ({} milliseconds)",
            app.getNamespace(),
            app.getName(),
            System.currentTimeMillis() - runningAppInfo.getCreationTimeMillis());
        try {
          killApplication(app.getNamespace(), app.getName());
        } catch (Throwable ex) {
          logger.warn(
              String.format("Failed to kill application %s/%s", app.getNamespace(), app.getName()),
              ex);
        }
      }
    }
  }

  public int getApplicationCount() {
    return runningApplications.size();
  }

  /**
   * Kill an application by deleting the driver pod.
   *
   * @param namespace the namespace of the app
   * @param appName the name of the app (typically submission ID)
   */
  protected void killApplication(String namespace, String appName) {
    try (KubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster)) {
      MixedOperation<SparkApplication, SparkApplicationResourceList, Resource<SparkApplication>>
              sparkApplicationClient =
              client.resources(SparkApplication.class, SparkApplicationResourceList.class);

      SparkApplication sparkApplicationResource =
              sparkApplicationClient.inNamespace(namespace).withName(appName).get();

      if (sparkApplicationResource == null) {
        logger.warn(
            "Failed to kill application {}/{} due to application not found", namespace, appName);
        return;
      }
      DriverInfo driverInfo = sparkApplicationResource.getStatus().getDriverInfo();
      if (driverInfo == null) {
        logger.warn("Failed to kill application {}/{} due to driver not found", namespace, appName);
        return;
      }
      String driverPodName = driverInfo.getPodName();
      logger.info(
          "Killing application {}/{} by deleting driver pod {}", namespace, appName, driverPodName);
      client.pods().inNamespace(namespace).withName(driverPodName).delete();

      // Submit metric for the queue in which the app is killed
      String queueTagValue = null;
      if (sparkApplicationResource.getMetadata().getLabels() != null) {
        queueTagValue =
            sparkApplicationResource.getMetadata().getLabels().get(Constants.QUEUE_LABEL);
      }
      if (queueTagValue == null) {
        queueTagValue = "";
      }
      counterMetricContainer.increment(
          MONITOR_KILLED_APPS,
          Tag.of("eks", sparkCluster.getEksCluster()),
          Tag.of("spark_app_ns", namespace),
          Tag.of("queue", queueTagValue));
    }
  }

  public static class RunningAppInfo {

    private final long creationTimeMillis;
    private final long maxRunningMillis;

    public RunningAppInfo(long creationTimeMillis, long maxRunningMillis) {
      this.creationTimeMillis = creationTimeMillis;
      this.maxRunningMillis = maxRunningMillis;
    }

    public long getCreationTimeMillis() {
      return creationTimeMillis;
    }

    public long getMaxRunningMillis() {
      return maxRunningMillis;
    }

    @Override
    public String toString() {
      return "RunningAppInfo{"
          + "creationTimeMillis="
          + creationTimeMillis
          + ", maxRunningMillis="
          + maxRunningMillis
          + '}';
    }

    public boolean exceedMaxRunningTime() {
      return System.currentTimeMillis() - creationTimeMillis > maxRunningMillis;
    }
  }
}
