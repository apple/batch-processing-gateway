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

package com.apple.spark.api;

import static com.apple.spark.core.Constants.*;
import static org.apache.spark.network.util.JavaUtils.byteStringAsGb;

import com.apple.spark.AppConfig;
import com.apple.spark.core.Constants;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.crd.VirtualSparkClusterSpec;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.util.ConfigUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubmissionSummary extends SubmissionStatus {

  private static final Logger logger = LoggerFactory.getLogger(SubmissionSummary.class);
  private String submissionId;
  private String user;
  private String queue = "";
  private String dagName = "";
  private int driverCores = 1;
  private Long driverMemoryGB = 0L;
  private int executorInstances = 1;
  private int executorCores = 1;
  private int totalCores = 1;
  private Long executorMemoryGB = 0L;
  private Long totalMemoryGB = 0L;
  private String sparkVersion;
  private String applicationName;
  private String sparkUIUrl;
  private String applicationMetricsUrl;
  private String splunkUrl;
  private List<String> applicationArguments;

  public String getSubmissionId() {
    return submissionId;
  }

  public void setSubmissionId(String submissionId) {
    this.submissionId = submissionId;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getSparkVersion() {
    return sparkVersion;
  }

  public void setSparkVersion(String sparkVersion) {
    this.sparkVersion = sparkVersion;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public void copyFrom(
      SparkApplication sparkApplication,
      VirtualSparkClusterSpec sparkCluster,
      AppConfig appConfig) {
    this.copyFrom(sparkApplication);

    setSparkUIUrlBasedOnState(sparkCluster, appConfig);

    setAppMetricsUrlBasedOnConfig(appConfig);
    setSplunkUrlBasedOnConfig(appConfig);
  }

  public void setAppMetricsUrlBasedOnConfig(AppConfig appConfig) {

    String appMetricsDashboardUrl = appConfig.getAppMetricsDashboardUrl();

    if (StringUtils.isEmpty(appMetricsDashboardUrl)) {
      // Make Siri as the default one
      appMetricsDashboardUrl = SIRI_APP_METRICS_DASHBOARD;
      logger.warn(
          String.format(
              "appMetricsDashboardUrl is not set in Skate configuration, fall back to: %s",
              SIRI_APP_METRICS_DASHBOARD));
    } else {
      logger.info(String.format("appMetricsDashboardUrl is set to: %s", appMetricsDashboardUrl));
    }
    String fullDashboardUrl = getDashboardUrl(appMetricsDashboardUrl);
    setApplicationMetricsUrl(fullDashboardUrl);
  }

  public void setSplunkUrlBasedOnConfig(AppConfig appConfig) {

    String splunkBaseUrl = appConfig.getSplunkBaseUrl();

    if (StringUtils.isEmpty(splunkBaseUrl)) {
      // Make Siri as the default one
      splunkBaseUrl = SIRI_SPLUNK;
      logger.warn(
          String.format(
              "splunkBaseUrl is not set in Skate configuration, fall back to: %s", SIRI_SPLUNK));
    } else {
      logger.info(String.format("splunkBaseUrl is set to: %s", splunkBaseUrl));
    }
    String fullSplunkUrl = getSplunkUrl(splunkBaseUrl);
    setSplunkUrl(fullSplunkUrl);
  }

  public void setSparkUIUrlBasedOnState(VirtualSparkClusterSpec sparkCluster, AppConfig appConfig) {
    if (!StringUtils.isEmpty(sparkCluster.getSparkUIUrl())
        && (SparkConstants.RUNNING_STATE.equalsIgnoreCase(getApplicationState())
            || SparkConstants.SPOT_TIMEOUT_STATE.equalsIgnoreCase(getApplicationState()))) {
      String url = ConfigUtil.getSparkUIUrl(sparkCluster, submissionId);
      setSparkUIUrl(url);
    } else if (SparkConstants.COMPLETED_STATE.equalsIgnoreCase(getApplicationState())
        || SparkConstants.FAILED_STATE.equalsIgnoreCase(getApplicationState())) {
      String url =
          ConfigUtil.getSparkHistoryUrl(appConfig.getSparkHistoryDns(), getSparkApplicationId());
      setSparkUIUrl(url);
    }
  }

  protected String getDashboardUrl(String appMetricsDashboardUrl) {

    // Setting the submission id drop-down bar and time range
    return String.format(
        "%s?tpl_var_submission_id=%s&from_ts=%d&to_ts=%d",
        appMetricsDashboardUrl, submissionId, getCreationTime(), getTerminationTime());
  }

  protected String getSplunkUrl(String splunkBaseUrl) {

    try {
      // 1) Adding search query in the url
      // 2) Raw Url includes spaces, encode it
      return splunkBaseUrl
          + (splunkBaseUrl.endsWith("/") ? "" : "/")
          + "en-US/app/search/search?q="
          + URLEncoder.encode(
              String.format(SPLUNK_SEARCH_QUERY, submissionId), StandardCharsets.UTF_8.toString());

    } catch (UnsupportedEncodingException e) {
      throw new WebApplicationException(
          String.format("Error while encoding Splunk URL for jobs summary"),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public void copyFrom(SparkApplication sparkApplication) {
    super.copyFrom(sparkApplication);
    setSubmissionId(sparkApplication.getMetadata().getName());
    if (sparkApplication.getMetadata().getLabels() != null) {
      String user = sparkApplication.getMetadata().getLabels().get(Constants.PROXY_USER_LABEL);
      String queue = "";
      if (sparkApplication.getMetadata().getLabels().containsKey(Constants.QUEUE_LABEL)) {
        queue = sparkApplication.getMetadata().getLabels().get(Constants.QUEUE_LABEL);
      }
      String dagName = "";

      int driverCores = 1;
      Long driverMemoryGB = 0L;
      int executorInstances = 1;
      int executorCores = 1;
      Long executorMemoryGB = 0L;

      if (sparkApplication.getSpec().getDriver().getLabels() != null) {
        if (sparkApplication
            .getSpec()
            .getDriver()
            .getLabels()
            .containsKey(Constants.DAG_NAME_LABEL)) {
          dagName =
              sparkApplication.getSpec().getDriver().getLabels().get(Constants.DAG_NAME_LABEL);
        }
      }
      if (sparkApplication.getSpec().getDriver().getMemory() != null) {
        try {
          driverMemoryGB = byteStringAsGb(sparkApplication.getSpec().getDriver().getMemory());
        } catch (Exception e) {
          logger.info(
              "Cannot convert Driver memory size string to Gb for {}, {}.",
              submissionId,
              e.toString());
        }
      }
      if (sparkApplication.getSpec().getDriver().getCores() != null) {
        driverCores = sparkApplication.getSpec().getDriver().getCores();
      }
      if (sparkApplication.getSpec().getExecutor().getCores() != null) {
        executorCores = sparkApplication.getSpec().getExecutor().getCores();
      }
      if (sparkApplication.getSpec().getExecutor().getInstances() != null) {
        executorInstances = sparkApplication.getSpec().getExecutor().getInstances();
      }
      if (sparkApplication.getSpec().getExecutor().getMemory() != null) {
        try {
          executorMemoryGB = byteStringAsGb(sparkApplication.getSpec().getExecutor().getMemory());
        } catch (Exception e) {
          logger.info(
              "Cannot convert Executor memory size string to Gb for {}, {}.",
              submissionId,
              e.toString());
        }
      }

      setDriverMemoryGB(driverMemoryGB);
      setDriverCores(driverCores);
      setExecutorInstances(executorInstances);
      setExecutorMemoryGB(executorMemoryGB);
      setExecutorCores(executorCores);

      setTotalResourcesBasedOnSpec();

      if (user != null) {
        setUser(user);
      }
      if (queue != null) {
        setQueue(queue);
      }
      if (dagName != null) {
        setDagName(dagName);
      }
    }
    SparkApplicationSpec spec = sparkApplication.getSpec();
    if (spec != null) {
      setSparkVersion(spec.getSparkVersion());
      setApplicationArguments(spec.getArguments());
      if (spec.getSparkConf() != null) {
        String appNameConfigValue = spec.getSparkConf().get(SparkConstants.SPARK_APP_NAME_CONFIG);
        if (appNameConfigValue != null) {
          setApplicationName(appNameConfigValue);
        }
      }
    }
  }

  // This method sets total cores and memory
  // It assumes executorInstances, executorMemoryGB and driverMemoryGB has already been set
  public void setTotalResourcesBasedOnSpec() {
    setTotalMemoryGB(executorInstances * executorMemoryGB + driverMemoryGB);
    setTotalCores(executorInstances * executorCores + driverCores);
  }

  public String getDagName() {
    return dagName;
  }

  public void setDagName(String dagName) {
    this.dagName = dagName;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public int getExecutorInstances() {
    return executorInstances;
  }

  public void setExecutorInstances(int executorInstances) {
    this.executorInstances = executorInstances;
  }

  public int getExecutorCores() {
    return executorCores;
  }

  public void setExecutorCores(int executorCores) {
    this.executorCores = executorCores;
  }

  public Long getExecutorMemoryGB() {
    return executorMemoryGB;
  }

  public void setExecutorMemoryGB(Long executorMemoryGB) {
    this.executorMemoryGB = executorMemoryGB;
  }

  public int getDriverCores() {
    return driverCores;
  }

  public void setDriverCores(int driverCores) {
    this.driverCores = driverCores;
  }

  public int getTotalCores() {
    return totalCores;
  }

  public void setTotalCores(int totalCores) {
    this.totalCores = totalCores;
  }

  public Long getDriverMemoryGB() {
    return driverMemoryGB;
  }

  public void setDriverMemoryGB(Long driverMemoryGB) {
    this.driverMemoryGB = driverMemoryGB;
  }

  public Long getTotalMemoryGB() {
    return totalMemoryGB;
  }

  public void setTotalMemoryGB(Long totalMemoryGB) {
    this.totalMemoryGB = totalMemoryGB;
  }

  public String getSparkUIUrl() {
    return sparkUIUrl;
  }

  public void setSparkUIUrl(String sparkUIUrl) {
    this.sparkUIUrl = sparkUIUrl;
  }

  public String getApplicationMetricsUrl() {
    return applicationMetricsUrl;
  }

  public void setApplicationMetricsUrl(String applicationMetricsUrl) {
    this.applicationMetricsUrl = applicationMetricsUrl;
  }

  public String getSplunkUrl() {
    return splunkUrl;
  }

  public void setSplunkUrl(String splunkUrl) {
    this.splunkUrl = splunkUrl;
  }

  public List<String> getApplicationArguments() {
    return applicationArguments;
  }

  public void setApplicationArguments(List<String> applicationArguments) {
    this.applicationArguments = applicationArguments;
  }
}
