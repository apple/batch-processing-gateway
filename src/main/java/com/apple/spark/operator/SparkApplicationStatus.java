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
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkApplicationStatus {

  private String sparkApplicationId;
  private String terminationTime;
  private ApplicationState applicationState;
  private Integer executionAttempts;
  private DriverInfo driverInfo;
  private Map<String, String> executorState;

  public String getSparkApplicationId() {
    return sparkApplicationId;
  }

  public void setSparkApplicationId(String sparkApplicationId) {
    this.sparkApplicationId = sparkApplicationId;
  }

  public String getTerminationTime() {
    return terminationTime;
  }

  public void setTerminationTime(String terminationTime) {
    this.terminationTime = terminationTime;
  }

  public ApplicationState getApplicationState() {
    return applicationState;
  }

  public void setApplicationState(ApplicationState applicationState) {
    this.applicationState = applicationState;
  }

  public Integer getExecutionAttempts() {
    return executionAttempts;
  }

  public void setExecutionAttempts(Integer executionAttempts) {
    this.executionAttempts = executionAttempts;
  }

  public DriverInfo getDriverInfo() {
    return driverInfo;
  }

  public void setDriverInfo(DriverInfo driverInfo) {
    this.driverInfo = driverInfo;
  }

  public Map<String, String> getExecutorState() {
    return executorState;
  }

  public void setExecutorState(Map<String, String> executorState) {
    this.executorState = executorState;
  }
}
