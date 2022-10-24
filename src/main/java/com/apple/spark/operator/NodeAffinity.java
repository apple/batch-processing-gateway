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
public class NodeAffinity {

  private PreferredSchedulingTerm[] preferredDuringSchedulingIgnoredDuringExecution;
  private RequiredDuringSchedulingIgnoredDuringExecutionTerm
      requiredDuringSchedulingIgnoredDuringExecution;

  public NodeAffinity() {}

  public NodeAffinity(PreferredSchedulingTerm[] preferredDuringSchedulingIgnoredDuringExecution) {
    this.preferredDuringSchedulingIgnoredDuringExecution =
        preferredDuringSchedulingIgnoredDuringExecution;
  }

  public NodeAffinity(
      PreferredSchedulingTerm[] preferredDuringSchedulingIgnoredDuringExecution,
      RequiredDuringSchedulingIgnoredDuringExecutionTerm
          requiredDuringSchedulingIgnoredDuringExecution) {
    this.preferredDuringSchedulingIgnoredDuringExecution =
        preferredDuringSchedulingIgnoredDuringExecution;
    this.requiredDuringSchedulingIgnoredDuringExecution =
        requiredDuringSchedulingIgnoredDuringExecution;
  }

  public NodeAffinity(
      RequiredDuringSchedulingIgnoredDuringExecutionTerm
          requiredDuringSchedulingIgnoredDuringExecution) {
    this.requiredDuringSchedulingIgnoredDuringExecution =
        requiredDuringSchedulingIgnoredDuringExecution;
  }

  public PreferredSchedulingTerm[] getPreferredDuringSchedulingIgnoredDuringExecution() {
    return preferredDuringSchedulingIgnoredDuringExecution;
  }

  public void setPreferredDuringSchedulingIgnoredDuringExecution(
      PreferredSchedulingTerm[] preferredDuringSchedulingIgnoredDuringExecution) {
    this.preferredDuringSchedulingIgnoredDuringExecution =
        preferredDuringSchedulingIgnoredDuringExecution;
  }

  public RequiredDuringSchedulingIgnoredDuringExecutionTerm
      getRequiredDuringSchedulingIgnoredDuringExecution() {
    return requiredDuringSchedulingIgnoredDuringExecution;
  }

  public void setRequiredDuringSchedulingIgnoredDuringExecution(
      RequiredDuringSchedulingIgnoredDuringExecutionTerm
          requiredDuringSchedulingIgnoredDuringExecution) {
    this.requiredDuringSchedulingIgnoredDuringExecution =
        requiredDuringSchedulingIgnoredDuringExecution;
  }
}
