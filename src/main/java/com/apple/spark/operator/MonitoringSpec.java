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

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MonitoringSpec {

  private Boolean exposeDriverMetrics;
  private Boolean exposeExecutorMetrics;
  private String metricsProperties;
  private String metricsPropertiesFile;
  private PrometheusSpec prometheus;

  public Boolean getExposeDriverMetrics() {
    return exposeDriverMetrics;
  }

  public void setExposeDriverMetrics(Boolean exposeDriverMetrics) {
    this.exposeDriverMetrics = exposeDriverMetrics;
  }

  public Boolean getExposeExecutorMetrics() {
    return exposeExecutorMetrics;
  }

  public void setExposeExecutorMetrics(Boolean exposeExecutorMetrics) {
    this.exposeExecutorMetrics = exposeExecutorMetrics;
  }

  public String getMetricsProperties() {
    return metricsProperties;
  }

  public void setMetricsProperties(String metricsProperties) {
    this.metricsProperties = metricsProperties;
  }

  public String getMetricsPropertiesFile() {
    return metricsPropertiesFile;
  }

  public void setMetricsPropertiesFile(String metricsPropertiesFile) {
    this.metricsPropertiesFile = metricsPropertiesFile;
  }

  public PrometheusSpec getPrometheus() {
    return prometheus;
  }

  public void setPrometheus(PrometheusSpec prometheus) {
    this.prometheus = prometheus;
  }
}
