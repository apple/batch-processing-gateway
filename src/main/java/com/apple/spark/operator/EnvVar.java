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

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnvVar {

  private String name;
  private String value;

  @Hidden private EnvVarSource valueFrom;

  public EnvVar() {}

  public EnvVar(String name, String value) {
    this.name = name;
    this.value = value;
    this.valueFrom = valueFrom;
  }

  public EnvVar(String name, String value, EnvVarSource valueFrom) {
    this.name = name;
    this.value = value;
    this.valueFrom = valueFrom;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public EnvVarSource getValueFrom() {
    return valueFrom;
  }

  public void setValueFrom(EnvVarSource valueFrom) {
    this.valueFrom = valueFrom;
  }
}
