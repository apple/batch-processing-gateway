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

import static com.apple.spark.core.SparkConstants.EXECUTOR_CORE_DEFAULT;
import static com.apple.spark.core.SparkConstants.EXECUTOR_MEM_DEFAULT;
import static com.apple.spark.core.SparkConstants.NUM_EXECUTORS_DEFAULT;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.Hidden;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutorSpec extends SparkPodSpec {

  private Integer instances;
  @Hidden private Boolean deleteOnTermination;

  public ExecutorSpec() {
    this.setCores(EXECUTOR_CORE_DEFAULT);
    this.setMemory(EXECUTOR_MEM_DEFAULT);
    this.setInstances(NUM_EXECUTORS_DEFAULT);
  }

  public void copyFrom(ExecutorSpec another) {
    super.copyFrom(another);
    if (another.instances != null) {
      this.instances = another.instances;
    }
    this.deleteOnTermination = another.deleteOnTermination;
  }

  public Integer getInstances() {
    return instances;
  }

  public void setInstances(Integer instances) {
    this.instances = instances;
  }

  public Boolean getDeleteOnTermination() {
    return deleteOnTermination;
  }

  public void setDeleteOnTermination(Boolean deleteOnTermination) {
    this.deleteOnTermination = deleteOnTermination;
  }
}
