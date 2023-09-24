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

import static com.apple.spark.core.SparkConstants.RUNNING_STATE;
import static com.apple.spark.core.SparkConstants.SUBMITTED_STATE;

import com.apple.spark.core.Constants;
import com.apple.spark.operator.SparkApplication;
import com.apple.spark.operator.SparkApplicationStatus;
import com.apple.spark.util.DateTimeUtils;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubmissionStatus {

  private Long creationTime;
  private String sparkApplicationId;
  private Integer executionAttempts;
  private Long terminationTime;
  private Long duration;
  private String applicationState;
  private String applicationErrorMessage;

  public void copyFrom(SparkApplication sparkApplicationResource) {
    Long creationTime =
        DateTimeUtils.parseOrNull(sparkApplicationResource.getMetadata().getCreationTimestamp());
    this.setCreationTime(creationTime);
    if (sparkApplicationResource.getStatus() != null) {
      SparkApplicationStatus status = sparkApplicationResource.getStatus();
      this.setSparkApplicationId(status.getSparkApplicationId());
      this.setExecutionAttempts(
          status.getExecutionAttempts() == null ? 1 : status.getExecutionAttempts());
      this.setTerminationTime(DateTimeUtils.parseOrNull(status.getTerminationTime()));
      if (status.getApplicationState() != null) {
        this.setApplicationState(status.getApplicationState().getState());
        this.setApplicationErrorMessage(status.getApplicationState().getErrorMessage());
      }
    }
    if (this.getApplicationState() == null) {
      this.setApplicationState(Constants.UNKNOWN_STATE);
    }
  }

  public Long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Long creationTime) {
    this.creationTime = creationTime;
  }

  public String getSparkApplicationId() {
    return sparkApplicationId;
  }

  public void setSparkApplicationId(String sparkApplicationId) {
    this.sparkApplicationId = sparkApplicationId;
  }

  public Integer getExecutionAttempts() {
    return executionAttempts;
  }

  public void setExecutionAttempts(Integer executionAttempts) {
    this.executionAttempts = executionAttempts;
  }

  public Long getTerminationTime() {
    return terminationTime;
  }

  public void setTerminationTime(Long terminationTime) {
    this.terminationTime = terminationTime;
  }

  public String getApplicationState() {
    return applicationState;
  }

  public void setApplicationState(String applicationState) {
    this.applicationState = applicationState;
  }

  public String getApplicationErrorMessage() {
    return applicationErrorMessage;
  }

  public void setApplicationErrorMessage(String applicationErrorMessage) {
    this.applicationErrorMessage = applicationErrorMessage;
  }

  public Long getDuration() {
    Long duration = 0L;
    if (creationTime != null) {
      if (terminationTime == null) {
        if (getApplicationState().equals(RUNNING_STATE)
            || getApplicationState().equals(SUBMITTED_STATE)) {
          duration = System.currentTimeMillis() - getCreationTime();
        }
      } else {
        duration = getTerminationTime() - getCreationTime();
      }
    }
    return duration;
  }

  public void setDuration(Long duration) {
    this.duration = duration;
  }
}
