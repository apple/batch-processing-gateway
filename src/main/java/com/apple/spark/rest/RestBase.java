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

package com.apple.spark.rest;

import com.apple.spark.AppConfig;
import com.apple.spark.core.ApplicationSubmissionHelper;
import com.apple.spark.core.Constants;
import com.apple.spark.core.KubernetesHelper;
import com.apple.spark.operator.SparkApplicationResource;
import com.apple.spark.operator.SparkApplicationResourceDoneable;
import com.apple.spark.operator.SparkApplicationResourceList;
import com.apple.spark.util.CounterMetricContainer;
import com.apple.spark.util.TimerMetricContainer;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.util.concurrent.RateLimiter;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@OpenAPIDefinition(
    info =
        @Info(
            title = "Batch Processing Gateway API",
            version = "2.0",
            description =
                "Batch Processing Gateway API is a RESTful web service to submit,"
                    + " examine and delete Spark jobs."),
    tags = {
      @Tag(name = "Submission", description = "APIs to submit jobs"),
      @Tag(name = "Deletion", description = "APIs to delete jobs"),
      @Tag(name = "Examination", description = "APIs to examine jobs"),
      @Tag(name = "Storage", description = "API to upload artifact to S3"),
      @Tag(name = "Admin", description = "Admin related APIs"),
      @Tag(name = "Health Check", description = "Service availability health check")
    })
public class RestBase {

  protected static final String REQUEST_METRIC_NAME =
      String.format("statsd.%s.request", Constants.SERVICE_ABBR);
  protected static final String REQUEST_LATENCY_METRIC_NAME =
      String.format("statsd.%s.request_latency", Constants.SERVICE_ABBR);
  protected static final int PERMITS_PER_SECOND_LIST_SUBMISSIONS = 20;
  private static final Logger logger = LoggerFactory.getLogger(RestBase.class);
  protected final AppConfig appConfig;
  // request metrics for different user
  protected final CounterMetricContainer requestCounters;
  protected final TimerMetricContainer timerMetrics;
  private final MetricRegistry registry;
  protected RateLimiter rateLimiterListSubmissions =
      RateLimiter.create(PERMITS_PER_SECOND_LIST_SUBMISSIONS);

  protected RestBase(AppConfig appConfig, MeterRegistry meterRegistry) {
    this.appConfig = appConfig;
    this.registry = SharedMetricRegistries.getDefault();
    this.requestCounters = new CounterMetricContainer(meterRegistry);
    this.timerMetrics = new TimerMetricContainer(meterRegistry);
  }

  protected AppConfig getAppConfig() {
    return appConfig;
  }

  protected List<AppConfig.SparkCluster> getSparkClusters() {
    return appConfig.getSparkClusters();
  }

  protected AppConfig.SparkCluster getSparkCluster(String submissionId) {
    String clusterId;
    try {
      clusterId = ApplicationSubmissionHelper.getClusterIdFromSubmissionId(submissionId);
    } catch (Throwable ex) {
      throw new WebApplicationException(
          String.format("submissionId %s is invalid", submissionId), Response.Status.BAD_REQUEST);
    }

    Optional<AppConfig.SparkCluster> sparkClusterOptional =
        appConfig.getSparkClusters().stream()
            .filter(t -> StringUtils.equals(t.getId(), clusterId))
            .findFirst();
    if (!sparkClusterOptional.isPresent()) {
      throw new WebApplicationException(
          String.format("Cluster %s (in submissionId: %s) not exists", clusterId, submissionId),
          Response.Status.BAD_REQUEST);
    }
    return sparkClusterOptional.get();
  }

  protected SparkApplicationResource getSparkApplicationResource(String submissionId) {
    AppConfig.SparkCluster sparkCluster = getSparkCluster(submissionId);
    com.codahale.metrics.Timer timer =
        registry.timer(this.getClass().getSimpleName() + ".getSparkApplicationResource.k8s-time");
    try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
        com.codahale.metrics.Timer.Context context = timer.time()) {
      CustomResourceDefinitionContext crdContext = KubernetesHelper.getSparkApplicationCrdContext();
      SparkApplicationResource sparkApplication =
          client
              .customResources(
                  crdContext,
                  SparkApplicationResource.class,
                  SparkApplicationResourceList.class,
                  SparkApplicationResourceDoneable.class)
              .inNamespace(sparkCluster.getSparkApplicationNamespace())
              .withName(submissionId)
              .get();
      context.stop();
      if (sparkApplication == null) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
      return sparkApplication;
    }
  }

  protected SparkApplicationResourceList getSparkApplicationResourcesByUser(
      AppConfig.SparkCluster sparkCluster, String user) {
    return getSparkApplicationResourcesByLabel(sparkCluster, Constants.PROXY_USER_LABEL, user);
  }

  protected SparkApplicationResourceList getSparkApplicationResourcesByLabel(
      AppConfig.SparkCluster sparkCluster, String labelName, String labelValue) {
    com.codahale.metrics.Timer timer =
        registry.timer(
            this.getClass().getSimpleName() + ".getSparkApplicationResourcesByUser.k8s-time");
    try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
        com.codahale.metrics.Timer.Context context = timer.time()) {
      CustomResourceDefinitionContext crdContext = KubernetesHelper.getSparkApplicationCrdContext();
      SparkApplicationResourceList list =
          client
              .customResources(
                  crdContext,
                  SparkApplicationResource.class,
                  SparkApplicationResourceList.class,
                  SparkApplicationResourceDoneable.class)
              .inNamespace(sparkCluster.getSparkApplicationNamespace())
              .withLabel(labelName, labelValue)
              .list();
      context.stop();
      if (list == null) {
        return new SparkApplicationResourceList();
      }
      return list;
    }
  }

  protected SparkApplicationResourceList getSparkApplicationResources(
      AppConfig.SparkCluster sparkCluster) {
    com.codahale.metrics.Timer timer =
        registry.timer(this.getClass().getSimpleName() + ".getSparkApplicationResources.k8s-time");
    try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
        com.codahale.metrics.Timer.Context context = timer.time()) {
      CustomResourceDefinitionContext crdContext = KubernetesHelper.getSparkApplicationCrdContext();
      SparkApplicationResourceList list =
          client
              .customResources(
                  crdContext,
                  SparkApplicationResource.class,
                  SparkApplicationResourceList.class,
                  SparkApplicationResourceDoneable.class)
              .inNamespace(sparkCluster.getSparkApplicationNamespace())
              .list();
      context.stop();
      if (list == null) {
        return new SparkApplicationResourceList();
      }
      return list;
    }
  }

  protected Pod getPod(String podName, AppConfig.SparkCluster sparkCluster) {
    com.codahale.metrics.Timer timer =
        registry.timer(this.getClass().getSimpleName() + ".getPod.k8s-time");
    try (DefaultKubernetesClient client = KubernetesHelper.getK8sClient(sparkCluster);
        com.codahale.metrics.Timer.Context context = timer.time()) {
      Pod pod =
          client
              .pods()
              .inNamespace(sparkCluster.getSparkApplicationNamespace())
              .withName(podName)
              .get();
      context.stop();
      return pod;
    }
  }

  protected void checkRateForListSubmissions(String endpointLogInfo) {
    boolean acquired = rateLimiterListSubmissions.tryAcquire();
    if (!acquired) {
      throw new WebApplicationException(
          String.format(
              "Too many requests for %s endpoint (limit: %s requests per second)",
              endpointLogInfo, PERMITS_PER_SECOND_LIST_SUBMISSIONS),
          Response.Status.TOO_MANY_REQUESTS);
    }
  }
}
