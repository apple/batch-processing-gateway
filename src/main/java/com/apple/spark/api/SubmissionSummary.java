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

import static org.apache.spark.network.util.JavaUtils.byteStringAsGb;

import com.apple.spark.AppConfig;
import com.apple.spark.core.Constants;
import com.apple.spark.core.SparkConstants;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationSpec;
import com.apple.spark.util.ConfigUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
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
      SparkApplication sparkApplicationResource,
      AppConfig.SparkCluster sparkCluster,
      AppConfig appConfig) {
    this.copyFrom(sparkApplicationResource);
    if (!StringUtils.isEmpty(sparkCluster.getSparkUIUrl())
        && SparkConstants.RUNNING_STATE.equalsIgnoreCase(getApplicationState())) {
      String url = ConfigUtil.getSparkUIUrl(sparkCluster, submissionId);
      setSparkUIUrl(url);
    } else if (SparkConstants.COMPLETED_STATE.equalsIgnoreCase(getApplicationState())
        || SparkConstants.FAILED_STATE.equalsIgnoreCase(getApplicationState())) {
      String url =
          ConfigUtil.getSparkHistoryUrl(appConfig.getSparkHistoryDns(), getSparkApplicationId());
      setSparkUIUrl(url);
    }
  }

  @Override
  public void copyFrom(SparkApplication sparkApplicationResource) {
    super.copyFrom(sparkApplicationResource);
    setSubmissionId(sparkApplicationResource.getMetadata().getName());
    if (sparkApplicationResource.getMetadata().getLabels() != null) {
      String user =
          sparkApplicationResource.getMetadata().getLabels().get(Constants.PROXY_USER_LABEL);
      String queue = "";
      if (sparkApplicationResource.getMetadata().getLabels().containsKey(Constants.QUEUE_LABEL)) {
        queue = sparkApplicationResource.getMetadata().getLabels().get(Constants.QUEUE_LABEL);
      }
      String dagName = "";

      int driverCores = 1;
      Long driverMemoryGB = 0L;
      int executorInstances = 1;
      int executorCores = 1;
      Long executorMemoryGB = 0L;

      if (sparkApplicationResource.getSpec().getDriver().getLabels() != null) {
        if (sparkApplicationResource
            .getSpec()
            .getDriver()
            .getLabels()
            .containsKey(Constants.DAG_NAME_LABEL)) {
          dagName =
              sparkApplicationResource
                  .getSpec()
                  .getDriver()
                  .getLabels()
                  .get(Constants.DAG_NAME_LABEL);
        }
      }
      if (sparkApplicationResource.getSpec().getDriver().getMemory() != null) {
        try {
          driverMemoryGB =
              byteStringAsGb(sparkApplicationResource.getSpec().getDriver().getMemory());
        } catch (Exception e) {
          logger.info(
              "Cannot convert Driver memory size string to Gb for {}, {}.",
              submissionId,
              e.toString());
        }
      }
      if (sparkApplicationResource.getSpec().getDriver().getCores() != null) {
        driverCores = sparkApplicationResource.getSpec().getDriver().getCores();
      }
      if (sparkApplicationResource.getSpec().getExecutor().getCores() != null) {
        executorCores = sparkApplicationResource.getSpec().getExecutor().getCores();
      }
      if (sparkApplicationResource.getSpec().getExecutor().getInstances() != null) {
        executorInstances = sparkApplicationResource.getSpec().getExecutor().getInstances();
      }
      if (sparkApplicationResource.getSpec().getExecutor().getMemory() != null) {
        try {
          executorMemoryGB =
              byteStringAsGb(sparkApplicationResource.getSpec().getExecutor().getMemory());
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
      setTotalMemoryGB(executorInstances * executorMemoryGB + driverMemoryGB);
      setTotalCores(executorInstances * executorCores + driverCores);

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
    SparkApplicationSpec spec = sparkApplicationResource.getSpec();
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

  public List<String> getApplicationArguments() {
    return applicationArguments;
  }

  public void setApplicationArguments(List<String> applicationArguments) {
    this.applicationArguments = applicationArguments;
  }
}
