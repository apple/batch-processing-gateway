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
public class DynamicAllocation {

  private boolean enabled;
  private Integer initialExecutors;
  private Integer minExecutors;
  private Integer maxExecutors;
  private Long shuffleTrackingTimeout;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Integer getInitialExecutors() {
    return initialExecutors;
  }

  public void setInitialExecutors(Integer initialExecutors) {
    this.initialExecutors = initialExecutors;
  }

  public Integer getMinExecutors() {
    return minExecutors;
  }

  public void setMinExecutors(Integer minExecutors) {
    this.minExecutors = minExecutors;
  }

  public Integer getMaxExecutors() {
    return maxExecutors;
  }

  public void setMaxExecutors(Integer maxExecutors) {
    this.maxExecutors = maxExecutors;
  }

  public Long getShuffleTrackingTimeout() {
    return shuffleTrackingTimeout;
  }

  public void setShuffleTrackingTimeout(Long shuffleTrackingTimeout) {
    this.shuffleTrackingTimeout = shuffleTrackingTimeout;
  }
}
