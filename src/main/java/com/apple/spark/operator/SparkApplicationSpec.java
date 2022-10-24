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

package com.apple.spark.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
// This class is created to match Spark Operator API.
// Some fields may be missing and we could add later.
// https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/pkg/apis/sparkoperator.k8s.io/v1beta2/types.go
public class SparkApplicationSpec {

  private String type;
  private String sparkVersion;
  @Hidden private String mode;
  private String originalUser;
  private String proxyUser;
  private String image;
  @Hidden private String imagePullPolicy;
  @Hidden private String imagePullSecrets;
  private String mainClass;
  private String mainApplicationFile;
  private List<String> arguments;
  private Map<String, String> sparkConf;
  @Hidden private String sparkConfigMap;

  @Hidden private String batchScheduler;
  @Hidden private BatchSchedulerConfiguration batchSchedulerOptions;

  @Hidden private List<Volume> volumes;

  private DriverSpec driver;
  private ExecutorSpec executor;

  private Dependencies deps;
  @Hidden private RestartPolicy restartPolicy;

  private String pythonVersion;
  @Hidden private String memoryOverheadFactor;

  @Hidden private MonitoringSpec monitoring;

  @Hidden private Long timeToLiveSeconds;

  @Hidden private SparkUIConfiguration sparkUIOptions;

  @Hidden private DynamicAllocation dynamicAllocation;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSparkVersion() {
    return sparkVersion;
  }

  public void setSparkVersion(String sparkVersion) {
    this.sparkVersion = sparkVersion;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getOriginalUser() {
    return originalUser;
  }

  public void setOriginalUser(String originalUser) {
    this.originalUser = originalUser;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public void setProxyUser(String proxyUser) {
    this.proxyUser = proxyUser;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public String getImagePullPolicy() {
    return imagePullPolicy;
  }

  public void setImagePullPolicy(String imagePullPolicy) {
    this.imagePullPolicy = imagePullPolicy;
  }

  public String getImagePullSecrets() {
    return imagePullSecrets;
  }

  public void setImagePullSecrets(String imagePullSecrets) {
    this.imagePullSecrets = imagePullSecrets;
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

  public String getSparkConfigMap() {
    return sparkConfigMap;
  }

  public void setSparkConfigMap(String sparkConfigMap) {
    this.sparkConfigMap = sparkConfigMap;
  }

  public List<Volume> getVolumes() {
    return volumes;
  }

  public void setVolumes(List<Volume> volumes) {
    this.volumes = volumes;
  }

  public String getBatchScheduler() {
    return batchScheduler;
  }

  public void setBatchScheduler(String batchScheduler) {
    this.batchScheduler = batchScheduler;
  }

  public BatchSchedulerConfiguration getBatchSchedulerOptions() {
    return batchSchedulerOptions;
  }

  public void setBatchSchedulerOptions(BatchSchedulerConfiguration batchSchedulerOptions) {
    this.batchSchedulerOptions = batchSchedulerOptions;
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

  public RestartPolicy getRestartPolicy() {
    return restartPolicy;
  }

  public void setRestartPolicy(RestartPolicy restartPolicy) {
    this.restartPolicy = restartPolicy;
  }

  public String getPythonVersion() {
    return pythonVersion;
  }

  public void setPythonVersion(String pythonVersion) {
    this.pythonVersion = pythonVersion;
  }

  public String getMemoryOverheadFactor() {
    return memoryOverheadFactor;
  }

  public void setMemoryOverheadFactor(String memoryOverheadFactor) {
    this.memoryOverheadFactor = memoryOverheadFactor;
  }

  public MonitoringSpec getMonitoring() {
    return monitoring;
  }

  public void setMonitoring(MonitoringSpec monitoring) {
    this.monitoring = monitoring;
  }

  public Long getTimeToLiveSeconds() {
    return timeToLiveSeconds;
  }

  public void setTimeToLiveSeconds(Long timeToLiveSeconds) {
    this.timeToLiveSeconds = timeToLiveSeconds;
  }

  public SparkUIConfiguration getSparkUIOptions() {
    return sparkUIOptions;
  }

  public void setSparkUIOptions(SparkUIConfiguration sparkUIOptions) {
    this.sparkUIOptions = sparkUIOptions;
  }

  public DynamicAllocation getDynamicAllocation() {
    return dynamicAllocation;
  }

  public void setDynamicAllocation(DynamicAllocation dynamicAllocation) {
    this.dynamicAllocation = dynamicAllocation;
  }

  public static class Builder {
    private final SparkApplicationSpec sparkApplicationSpec = new SparkApplicationSpec();

    public Builder() {}

    public Builder withOriginalUser(String originalUser) {
      this.sparkApplicationSpec.originalUser = originalUser;
      return this;
    }

    public Builder withProxyUser(String proxyUser) {
      this.sparkApplicationSpec.proxyUser = proxyUser;
      return this;
    }

    public Builder withMode(String mode) {
      this.sparkApplicationSpec.mode = mode;
      return this;
    }

    public Builder withType(String type) {
      this.sparkApplicationSpec.type = type;
      return this;
    }

    public Builder withImage(String image) {
      this.sparkApplicationSpec.image = image;
      return this;
    }

    public Builder withSparkVersion(String sparkVersion) {
      this.sparkApplicationSpec.sparkVersion = sparkVersion;
      return this;
    }

    public Builder withMainClass(String mainClass) {
      this.sparkApplicationSpec.mainClass = mainClass;
      return this;
    }

    public Builder withMainApplicationFile(String mainApplicationFile) {
      this.sparkApplicationSpec.mainApplicationFile = mainApplicationFile;
      return this;
    }

    public Builder withArguments(List<String> arguments) {
      this.sparkApplicationSpec.arguments = arguments;
      return this;
    }

    public Builder withImagePullPolicy(String imagePullPolicy) {
      this.sparkApplicationSpec.imagePullPolicy = imagePullPolicy;
      return this;
    }

    public Builder withRestartPolicy(RestartPolicy restartPolicy) {
      this.sparkApplicationSpec.restartPolicy = restartPolicy;
      return this;
    }

    public Builder withVolumes(List<Volume> volumes) {
      this.sparkApplicationSpec.volumes = volumes;
      return this;
    }

    public Builder withBatchScheduler(String batchScheduler) {
      this.sparkApplicationSpec.batchScheduler = batchScheduler;
      return this;
    }

    public Builder withBatchSchedulerOptions(BatchSchedulerConfiguration batchSchedulerOptions) {
      this.sparkApplicationSpec.batchSchedulerOptions = batchSchedulerOptions;
      return this;
    }

    public Builder withDriver(DriverSpec driver) {
      this.sparkApplicationSpec.driver = driver;
      return this;
    }

    public Builder withExecutor(ExecutorSpec executor) {
      this.sparkApplicationSpec.executor = executor;
      return this;
    }

    public Builder withDeps(Dependencies deps) {
      this.sparkApplicationSpec.deps = deps;
      return this;
    }

    public Builder withPythonVersion(String pythonVersion) {
      this.sparkApplicationSpec.pythonVersion = pythonVersion;
      return this;
    }

    public Builder withTimeToLiveSeconds(Long timeToLiveSeconds) {
      this.sparkApplicationSpec.timeToLiveSeconds = timeToLiveSeconds;
      return this;
    }

    public Builder extendSparkConf(Map<String, String> sparkConf) {
      if (sparkConf == null || sparkConf.size() == 0) {
        return this;
      }

      if (this.sparkApplicationSpec.sparkConf == null) {
        this.sparkApplicationSpec.sparkConf = new HashMap<>();
      }

      for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
        this.sparkApplicationSpec.sparkConf.put(entry.getKey(), entry.getValue());
      }

      return this;
    }

    public Builder withSparkUIConfiguration(SparkUIConfiguration sparkUIConfiguration) {
      this.sparkApplicationSpec.sparkUIOptions = sparkUIConfiguration;
      return this;
    }

    public Builder extendVolumes(List<Volume> volumes) {
      if (volumes == null || volumes.size() == 0) {
        return this;
      }

      if (this.sparkApplicationSpec.volumes == null) {
        this.sparkApplicationSpec.volumes = new ArrayList<>();
      }

      this.sparkApplicationSpec.volumes.addAll(volumes);
      return this;
    }

    public SparkApplicationSpec build() {
      return this.sparkApplicationSpec;
    }
  }
}
