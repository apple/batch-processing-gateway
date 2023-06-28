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

import com.apple.spark.operator.Dependencies;
import com.apple.spark.operator.DriverSpec;
import com.apple.spark.operator.ExecutorSpec;
import com.apple.spark.operator.RestartPolicy;
import com.apple.spark.operator.Volume;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
// only serialize non-null values, and deserialize missing values as null.
@Schema(description = "Submit Application Request")
public class SubmitApplicationRequest {

  @Schema(description = "If specified, the suffix will be appended to the submission ID")
  private String submissionIdSuffix;

  @Schema(
      description =
          "Name of the application. This will help listing submissions"
              + " under the same application later.")
  private String applicationName;

  @Schema(
      description =
          "Should be: Java / Scala / Python. If not specified,"
              + " the API will try to figure out by itself.")
  private String type;

  @Hidden private String mode;
  private String image;

  @Schema(
      description =
          "To enable Spot Instance feature that schedules all Spark Executor pods to Spot nodes")
  private boolean spotInstance;

  @Schema(
      required = true,
      description = "Spark version in the format of x.y, where x and y are integers.")
  private String sparkVersion;

  @Schema(
      description =
          "The main class in the jar provided in mainApplicationFile for a Java/Scala Spark job",
      example = "com.spark.examples.OneStageApp")
  private String mainClass;

  @Schema(
      required = true,
      description =
          "For Java/Scala Spark jobs, provide the full path to the jar file. For PySpark jobs,"
              + " provide the full path to the Python file.")
  private String mainApplicationFile;

  private List<String> arguments;

  private Map<String, String> sparkConf;
  @Hidden private List<Volume> volumes;

  @Schema(required = true)
  private DriverSpec driver;

  @Schema(required = true)
  private ExecutorSpec executor;

  @Hidden private Double driverOverheadFactor;
  @Hidden private Double executorOverheadFactor;

  @Hidden private Double overheadFactor;

  private Dependencies deps;

  private String pythonVersion;
  @Hidden private String imagePullPolicy;
  @Hidden private RestartPolicy restartPolicy;

  @Hidden private String clusterId;

  @Schema(description = "If no queue is specified, a default 'poc' queue will be used.")
  private String queue;

  @Hidden private String queueToken;

  public Double getDriverOverheadFactor() {
    return driverOverheadFactor;
  }

  public void setDriverOverheadFactor(Double driverOverheadFactor) {
    this.driverOverheadFactor = driverOverheadFactor;
  }

  public Double getExecutorOverheadFactor() {
    return executorOverheadFactor;
  }

  public void setExecutorOverheadFactor(Double executorOverheadFactor) {
    this.executorOverheadFactor = executorOverheadFactor;
  }

  public Double getOverheadFactor() {
    return overheadFactor;
  }

  public void setOverheadFactor(Double overheadFactor) {
    this.overheadFactor = overheadFactor;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getSubmissionIdSuffix() {
    return submissionIdSuffix;
  }

  public void setSubmissionIdSuffix(String submissionIdSuffix) {
    this.submissionIdSuffix = submissionIdSuffix;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public boolean getSpotInstance() {
    return spotInstance;
  }

  public void setSpotInstance(boolean spotInstance) {
    this.spotInstance = spotInstance;
  }

  public String getSparkVersion() {
    return sparkVersion;
  }

  public void setSparkVersion(String sparkVersion) {
    this.sparkVersion = sparkVersion;
  }

  public String getMainClass() {
    return mainClass;
  }

  public void setMainClass(String mainClass) {
    this.mainClass = mainClass;
  }

  public String getMainApplicationFile() {
    return mainApplicationFile;
  }

  public void setMainApplicationFile(String mainApplicationFile) {
    this.mainApplicationFile = mainApplicationFile;
  }

  public List<String> getArguments() {
    return arguments;
  }

  public void setArguments(List<String> arguments) {
    this.arguments = arguments;
  }

  public Map<String, String> getSparkConf() {
    return sparkConf;
  }

  public void setSparkConf(Map<String, String> sparkConf) {
    this.sparkConf = sparkConf;
  }

  public List<Volume> getVolumes() {
    return volumes;
  }

  public void setVolumes(List<Volume> volumes) {
    this.volumes = volumes;
  }

  public DriverSpec getDriver() {
    return driver;
  }

  public void setDriver(DriverSpec driver) {
    this.driver = driver;
  }

  public ExecutorSpec getExecutor() {
    return executor;
  }

  public void setExecutor(ExecutorSpec executor) {
    this.executor = executor;
  }

  public Dependencies getDeps() {
    return deps;
  }

  public void setDeps(Dependencies deps) {
    this.deps = deps;
  }

  public String getPythonVersion() {
    return pythonVersion;
  }

  public void setPythonVersion(String pythonVersion) {
    this.pythonVersion = pythonVersion;
  }

  public String getImagePullPolicy() {
    return imagePullPolicy;
  }

  public void setImagePullPolicy(String imagePullPolicy) {
    this.imagePullPolicy = imagePullPolicy;
  }

  public RestartPolicy getRestartPolicy() {
    return restartPolicy;
  }

  public void setRestartPolicy(RestartPolicy restartPolicy) {
    this.restartPolicy = restartPolicy;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getQueueToken() {
    return queueToken;
  }

  public void setQueueToken(String queueToken) {
    this.queueToken = queueToken;
  }
}
